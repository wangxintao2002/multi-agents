"""A1/A2 orchestrator: run N mini-swe-agent tasks concurrently against real Docker,
with a deliberately-capped slot pool, and record a full event trace per (C, lease) cell.

For each slot capacity C in the sweep, this submits all N instances at once to a
ThreadPoolExecutor(N). Each worker builds an InstrumentedLitellmModel +
InstrumentedDockerEnvironment(image=that instance) + DefaultAgent and runs it.
The shared SlotPool(C) arbitrates container slots; the Sampler records utilization.
Everything is flushed to results/<run_id>/<cell>/.

Usage:
  python run_experiment.py --config config.yaml
  python run_experiment.py --config config.yaml --capacities 2 --lease task_lease --smoke 2
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import yaml

# --- make sibling modules importable when run as a script -------------------
sys.path.insert(0, str(Path(__file__).resolve().parent))

from dataset import exclude_instance_ids, image_for, load_instances, select  # noqa: E402
from events import EventBus  # noqa: E402
from instrumented import InstrumentedDockerEnvironment, InstrumentedLitellmModel  # noqa: E402
from resources import Sampler, SlotPool  # noqa: E402

HERE = Path(__file__).resolve().parent
SWEBENCH_YAML = None  # resolved at runtime from the installed package


def _clean_proxy_env() -> None:
    """The host's NO_PROXY contains '[::1]' which breaks httpx ('Invalid port').
    OpenRouter is reachable directly, so we drop proxy vars for clean API timing."""
    for k in ("ALL_PROXY", "all_proxy", "HTTP_PROXY", "http_proxy",
              "HTTPS_PROXY", "https_proxy", "NO_PROXY", "no_proxy"):
        os.environ.pop(k, None)


def _load_agent_templates() -> dict:
    """Pull the system/instance templates + env settings from mini-swe-agent's
    bundled swebench.yaml so our agent behaves like the official runner."""
    from minisweagent.config import builtin_config_dir

    path = Path(builtin_config_dir) / "benchmarks" / "swebench.yaml"
    return yaml.safe_load(path.read_text())


def pre_pull_images(instances: list[dict], bus: EventBus, docker_exe: str) -> None:
    """Pull every image in a timed, serial phase before the contended run, so
    cold-pull latency is recorded separately and doesn't blow the 120s
    container-start pull_timeout under load."""
    for inst in instances:
        img = image_for(inst)
        # docker pull is a no-op (fast) if already present.
        t0 = time.perf_counter()
        try:
            r = subprocess.run([docker_exe, "image", "inspect", img],
                               capture_output=True, text=True, timeout=30)
            present = r.returncode == 0
        except Exception:
            present = False
        if present:
            bus.emit("image_prepull", instance_id=inst["instance_id"], image=img,
                     cached=True, duration_s=time.perf_counter() - t0)
            continue
        try:
            subprocess.run([docker_exe, "pull", img], capture_output=True, text=True,
                           timeout=1200, check=True)
            bus.emit("image_prepull", instance_id=inst["instance_id"], image=img,
                     cached=False, ok=True, duration_s=time.perf_counter() - t0)
        except Exception as e:
            bus.emit("image_prepull", instance_id=inst["instance_id"], image=img,
                     cached=False, ok=False, err=f"{type(e).__name__}: {str(e)[:200]}",
                     duration_s=time.perf_counter() - t0)


def run_one_task(inst: dict, *, cfg: dict, templates: dict, pool: SlotPool,
                 bus: EventBus, lease_mode: str, cell_dir: Path) -> dict:
    """Run a single instance end-to-end. Returns a small result record."""
    from minisweagent.agents.default import DefaultAgent
    from minisweagent.exceptions import Submitted

    instance_id = inst["instance_id"]
    bus.emit("task_submit", task_id=instance_id, repo=inst.get("repo"))
    t_start = time.perf_counter()

    model = env = None
    exit_status = submission = None
    err = None
    try:
        model = InstrumentedLitellmModel(
            bus=bus, task_id=instance_id,
            model_name=cfg["model"]["name"],
            model_kwargs=cfg["model"].get("model_kwargs", {}),
            cost_tracking=cfg["model"].get("cost_tracking", "ignore_errors"),
            observation_template=templates["model"]["observation_template"],
            format_error_template=templates["model"]["format_error_template"],
        )
        env_settings = dict(templates["environment"])
        env_settings.pop("environment_class", None)
        env = InstrumentedDockerEnvironment(
            pool=pool, bus=bus, task_id=instance_id, lease_mode=lease_mode,
            image=image_for(inst), **env_settings,
        )
        agent = DefaultAgent(
            model, env,
            system_template=templates["agent"]["system_template"],
            instance_template=templates["agent"]["instance_template"],
            step_limit=cfg["limits"]["step_limit"],
            cost_limit=cfg["limits"]["cost_limit"],
            wall_time_limit_seconds=cfg["limits"]["wall_time_limit_seconds"],
            output_path=cell_dir / "trajs" / f"{instance_id}.traj.json",
        )
        info = agent.run(inst["problem_statement"])
        exit_status = info.get("exit_status")
        submission = info.get("submission", "")
    except Submitted as e:
        exit_status = "Submitted"
        submission = e.args[0]["extra"]["submission"] if e.args else ""
    except Exception as e:
        exit_status = type(e).__name__
        err = str(e)[:300]
    finally:
        # Ensure container teardown + slot release even on error.
        if env is not None:
            try:
                env.cleanup()
            except Exception:
                pass
        flow = time.perf_counter() - t_start
        bus.emit("task_done", task_id=instance_id, exit_status=exit_status,
                 flow_s=flow, err=err)

    # Persist patch for optional later evaluation (not evaluated this round).
    if submission:
        pdir = cell_dir / "patches"
        pdir.mkdir(parents=True, exist_ok=True)
        (pdir / f"{instance_id}.patch").write_text(submission)
    return {"instance_id": instance_id, "exit_status": exit_status,
            "flow_s": flow, "err": err}


def run_cell(instances: list[dict], *, capacity: int, lease_mode: str, cfg: dict,
             templates: dict, run_dir: Path) -> None:
    """Run one (C, lease) cell: all instances submitted at once under SlotPool(C)."""
    cell_name = f"C{'inf' if capacity >= cfg['run']['inf_capacity'] else capacity}_{lease_mode}"
    cell_dir = run_dir / cell_name
    cell_dir.mkdir(parents=True, exist_ok=True)
    bus = EventBus()
    pool = SlotPool(capacity, bus, mode=lease_mode)
    sampler = Sampler(
        bus,
        fast_interval=cfg["sampler"]["fast_interval"],
        slow_interval=cfg["sampler"]["slow_interval"],
        docker_exe=cfg["run"]["docker_executable"],
    )

    print(f"\n=== CELL {cell_name}: N={len(instances)} C={capacity} lease={lease_mode} ===")
    bus.emit("cell_start", cell=cell_name, capacity=capacity, lease_mode=lease_mode,
             n_tasks=len(instances))
    sampler.start()
    t0 = time.perf_counter()
    results = []
    with ThreadPoolExecutor(max_workers=cfg["run"]["offered_concurrency"]) as ex:
        futures = {
            ex.submit(run_one_task, inst, cfg=cfg, templates=templates, pool=pool,
                      bus=bus, lease_mode=lease_mode, cell_dir=cell_dir): inst["instance_id"]
            for inst in instances
        }
        for fut in as_completed(futures):
            try:
                results.append(fut.result())
            except Exception as e:
                print(f"  task {futures[fut]} crashed: {e}")
    makespan = time.perf_counter() - t0
    sampler.stop()
    sampler.join(timeout=5)
    bus.emit("cell_end", cell=cell_name, makespan_s=makespan)

    n_events = bus.write_jsonl(cell_dir / "events.jsonl")
    n_solved = sum(1 for r in results if r["exit_status"] == "Submitted")
    print(f"  makespan={makespan:.1f}s  submitted={n_solved}/{len(results)}  events={n_events}")
    print(f"  -> {cell_dir}")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default=str(HERE / "config.yaml"))
    ap.add_argument("--capacities", nargs="*", default=None,
                    help="Override slot capacities, e.g. --capacities 2 4")
    ap.add_argument("--lease", default=None,
                    choices=["task_lease", "exec_lease", "exec_lease_stop"],
                    help="Lease discipline to run.")
    ap.add_argument("--smoke", type=int, default=0, help="Use only the first K instances")
    ap.add_argument("--run-id", default=None)
    args = ap.parse_args()

    _clean_proxy_env()
    os.environ.setdefault("MSWEA_COST_TRACKING", "ignore_errors")

    cfg = yaml.safe_load(Path(args.config).read_text())
    if args.lease:
        cfg["run"]["lease_mode"] = args.lease
    lease_mode = cfg["run"]["lease_mode"]

    templates = _load_agent_templates()

    # Resolve capacities (render "inf" -> inf_capacity).
    caps_spec = args.capacities if args.capacities is not None else cfg["run"]["slot_capacities"]
    inf_cap = cfg["run"]["inf_capacity"]
    capacities = [inf_cap if str(c).lower() == "inf" else int(c) for c in caps_spec]

    # Select instances.
    all_inst = load_instances(cfg["dataset"]["subset"], cfg["dataset"]["split"])
    n = args.smoke if args.smoke else cfg["dataset"]["n_instances"]
    strategy = "cached" if args.smoke else cfg["dataset"]["selection"]
    instances = select(all_inst, n=n, strategy=strategy, seed=cfg["dataset"]["seed"])
    instances = exclude_instance_ids(instances, cfg["dataset"].get("exclude_instance_ids"))
    print(f"Selected {len(instances)} instances ({strategy}): "
          f"{[i['instance_id'] for i in instances]}")

    run_id = args.run_id or f"A1_{lease_mode}_{int(time.time())}"
    run_dir = HERE / "results" / run_id
    run_dir.mkdir(parents=True, exist_ok=True)
    (run_dir / "selected_instances.txt").write_text(
        "\n".join(f"{i['instance_id']}\t{i['repo']}\t{image_for(i)}" for i in instances))

    # Pre-pull images once (shared across all cells).
    if cfg["run"]["pre_pull_images"]:
        prepull_bus = EventBus()
        print("Pre-pulling images (timed)...")
        pre_pull_images(instances, prepull_bus, cfg["run"]["docker_executable"])
        prepull_bus.write_jsonl(run_dir / "prepull_events.jsonl")

    for cap in capacities:
        run_cell(instances, capacity=cap, lease_mode=lease_mode, cfg=cfg,
                 templates=templates, run_dir=run_dir)

    print(f"\nDONE. Results in {run_dir}")


if __name__ == "__main__":
    main()
