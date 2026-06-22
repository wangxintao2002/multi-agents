"""A0: harness-only scaling sweep (no LLM, no agent).

Runs the official SWE-bench evaluation harness on GOLD patches across a sweep of
``max_workers``, with our Sampler recording host + docker utilization throughout.
This isolates the cost of the pure sandbox + test-execution layer: start container,
apply gold patch, run the repo's test suite, judge. No agent, no model, no lease
policy -- so any scaling knee here is intrinsic to the workload, not to a scaffold.

Caveat (recorded in the design): gold replay runs the final test suite once per
instance. A real agent issues many small commands + repeated partial test runs, so
A0 is the *lower bound* on per-task sandbox demand; A1 captures full interactive use.

Usage:
  python run_a0_harness.py --config config.yaml --workers 1 2 4 8 16 20
"""

from __future__ import annotations

import argparse
import subprocess
import sys
import time
from pathlib import Path

import yaml

sys.path.insert(0, str(Path(__file__).resolve().parent))

from dataset import exclude_instance_ids, image_for, load_instances, select  # noqa: E402
from events import EventBus  # noqa: E402
from resources import Sampler  # noqa: E402

HERE = Path(__file__).resolve().parent


def run_harness_cell(instance_ids: list[str], *, subset_path: str, split: str,
                     max_workers: int, run_dir: Path, sampler_cfg: dict,
                     docker_exe: str, timeout: int = 1800) -> dict:
    """Run the gold-patch harness for one max_workers value, sampling throughout."""
    cell_dir = run_dir / f"workers{max_workers}"
    cell_dir.mkdir(parents=True, exist_ok=True)
    bus = EventBus()
    sampler = Sampler(
        bus,
        fast_interval=sampler_cfg["fast_interval"],
        slow_interval=sampler_cfg["slow_interval"],
        docker_exe=docker_exe,
        name_prefix="sweb.eval.",
    )

    run_id = f"a0_w{max_workers}_{int(time.time())}"
    print(f"\n=== A0 CELL workers={max_workers}: {len(instance_ids)} gold evals ===")
    bus.emit("a0_cell_start", max_workers=max_workers, n=len(instance_ids))
    sampler.start()
    t0 = time.perf_counter()

    # Invoke the harness as a subprocess so its internal multiprocessing/logging
    # stays isolated from ours. predictions_path='gold' => gold patches.
    cmd = [
        sys.executable, "-m", "swebench.harness.run_evaluation",
        "--dataset_name", subset_path,
        "--split", split,
        "--predictions_path", "gold",
        "--max_workers", str(max_workers),
        "--run_id", run_id,
        "--cache_level", "env",        # keep env images, rebuild instance layer as needed
        "--timeout", str(timeout),
        "--instance_ids", *instance_ids,
        "--report_dir", str(cell_dir),
    ]
    proc = subprocess.run(cmd, capture_output=True, text=True, cwd=str(cell_dir))
    makespan = time.perf_counter() - t0
    sampler.stop()
    sampler.join(timeout=5)

    bus.emit("a0_cell_end", max_workers=max_workers, makespan_s=makespan,
             returncode=proc.returncode)
    (cell_dir / "harness_stdout.txt").write_text(proc.stdout[-50000:])
    (cell_dir / "harness_stderr.txt").write_text(proc.stderr[-50000:])
    n_events = bus.write_jsonl(cell_dir / "events.jsonl")

    thr = len(instance_ids) / (makespan / 60.0) if makespan > 0 else 0.0
    print(f"  makespan={makespan:.1f}s  throughput={thr:.2f} tasks/min  "
          f"rc={proc.returncode}  events={n_events}")
    return {"max_workers": max_workers, "makespan_s": makespan,
            "throughput_per_min": thr, "returncode": proc.returncode}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default=str(HERE / "config.yaml"))
    ap.add_argument("--workers", nargs="*", type=int, default=[1, 2, 4, 8, 16, 20])
    ap.add_argument("--smoke", type=int, default=0)
    ap.add_argument("--run-id", default=None)
    ap.add_argument("--no-prebuild", action="store_true",
                    help="Skip the unmeasured warm-up pass (NOT recommended; "
                         "leaves the sweep open to cache-warming confound).")
    ap.add_argument("--shuffle-order", action="store_true",
                    help="Randomize sweep order (seeded) as a second guard against "
                         "order-dependent cache effects.")
    args = ap.parse_args()

    cfg = yaml.safe_load(Path(args.config).read_text())
    from minisweagent.run.benchmarks.swebench import DATASET_MAPPING
    subset_path = DATASET_MAPPING.get(cfg["dataset"]["subset"], cfg["dataset"]["subset"])
    split = cfg["dataset"]["split"]

    all_inst = load_instances(cfg["dataset"]["subset"], split)
    n = args.smoke if args.smoke else cfg["dataset"]["n_instances"]
    strategy = "cached" if args.smoke else cfg["dataset"]["selection"]
    instances = select(all_inst, n=n, strategy=strategy, seed=cfg["dataset"]["seed"])
    instances = exclude_instance_ids(instances, cfg["dataset"].get("exclude_instance_ids"))
    instance_ids = [i["instance_id"] for i in instances]
    print(f"A0 on {len(instance_ids)} instances ({strategy}): {instance_ids}")

    run_id = args.run_id or f"A0_{int(time.time())}"
    run_dir = HERE / "results" / run_id
    run_dir.mkdir(parents=True, exist_ok=True)
    (run_dir / "selected_instances.txt").write_text(
        "\n".join(f"{i['instance_id']}\t{i['repo']}\t{image_for(i)}" for i in instances))

    # --- Unified prebuild (UNMEASURED) ---------------------------------------
    # Critical for a clean scaling curve: the harness builds env + instance image
    # layers on first encounter and caches them. If we just swept 1,2,4,...,20 in
    # order, later cells would inherit warm caches built by earlier cells, so a
    # throughput rise could be cache warm-up rather than concurrency. We therefore
    # do ONE warm-up pass at max_workers=1 (not recorded) so every measured cell
    # starts from the same fully-built, warm-cache state.
    if not args.no_prebuild:
        print(f"\n=== PREBUILD (unmeasured) workers=1: warming all image layers ===")
        t0 = time.perf_counter()
        run_harness_cell(instance_ids, subset_path=subset_path, split=split,
                         max_workers=1, run_dir=run_dir / "_prebuild",
                         sampler_cfg=cfg["sampler"],
                         docker_exe=cfg["run"]["docker_executable"])
        print(f"  prebuild done in {time.perf_counter()-t0:.0f}s "
              f"(discarded; all subsequent cells now share warm caches)")

    summary = []
    sweep = list(args.workers)
    if args.shuffle_order:
        import random as _r
        _r.Random(cfg["dataset"]["seed"]).shuffle(sweep)
        print(f"Sweep order (shuffled): {sweep}")
    for w in sweep:
        summary.append(run_harness_cell(
            instance_ids, subset_path=subset_path, split=split, max_workers=w,
            run_dir=run_dir, sampler_cfg=cfg["sampler"],
            docker_exe=cfg["run"]["docker_executable"]))
    summary.sort(key=lambda r: r["max_workers"])  # canonical order in the CSV

    # Tiny CSV so the scaling curve is readable without analyze.py.
    import csv
    with (run_dir / "a0_scaling.csv").open("w", newline="") as f:
        wr = csv.DictWriter(f, fieldnames=["max_workers", "makespan_s",
                                           "throughput_per_min", "returncode"])
        wr.writeheader()
        wr.writerows(summary)
    print(f"\nA0 DONE. Scaling summary -> {run_dir / 'a0_scaling.csv'}")


if __name__ == "__main__":
    main()
