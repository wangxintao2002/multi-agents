"""Analyze a run directory: fold events into per-task time attribution, the
sandbox lease three-way split (held / active / idle-held), throughput, and the
pre-registered H0/H1/H2 verdicts.

Reads each cell's events.jsonl. Produces, per run dir:
  - per_task.csv      : one row per (cell, task) with time components
  - summary.csv       : one row per cell with throughput, flow-time pctiles, idle-held%
  - resource_timeline.csv : one row per slow resource sample
  - *.png             : attribution stacked bars, idle-held, throughput vs C, utilization
  - verdict.md        : applies the pre-registered criteria and states the conclusion

Usage:
  python analyze.py results/A1_task_lease_1700000000
  python analyze.py results/A0_1700000000 --a0
"""

from __future__ import annotations

import argparse
import json
import statistics
from pathlib import Path

# matplotlib is optional; degrade to CSV-only if missing.
try:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    HAVE_MPL = True
except Exception:
    HAVE_MPL = False


def load_events(path: Path) -> tuple[list[dict], dict]:
    origin = {}
    evs = []
    for line in path.read_text().splitlines():
        e = json.loads(line)
        if e.get("kind") == "_run_origin":
            origin = e
        else:
            evs.append(e)
    return evs, origin


def pctile(xs: list[float], p: float) -> float:
    if not xs:
        return 0.0
    xs = sorted(xs)
    k = (len(xs) - 1) * p
    lo = int(k)
    hi = min(lo + 1, len(xs) - 1)
    return xs[lo] + (xs[hi] - xs[lo]) * (k - lo)


def _num(value) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _container_task_map(evs: list[dict]) -> dict[str, str]:
    mapping = {}
    for e in evs:
        if e["kind"] == "container_start_end" and e.get("ok") and e.get("container_id"):
            mapping[str(e["container_id"])] = e.get("task_id")
    return mapping


def _task_for_container(container_id, mapping: dict[str, str]) -> str | None:
    if not container_id:
        return None
    cid = str(container_id)
    for known, task_id in mapping.items():
        if cid.startswith(known) or known.startswith(cid):
            return task_id
    return None


def analyze_cell(evs: list[dict]) -> dict:
    """Compute per-task time components and cell-level aggregates for an A1/A2 cell."""
    cell_meta = next((e for e in evs if e["kind"] == "cell_start"), {})
    cell_end = next((e for e in evs if e["kind"] == "cell_end"), {})
    capacity = cell_meta.get("capacity")
    lease_mode = cell_meta.get("lease_mode")
    makespan = cell_end.get("makespan_s")

    # group events by task
    tasks: dict[str, dict] = {}
    def t(task_id):
        return tasks.setdefault(task_id, {
            "llm_wait": 0.0, "sandbox_exec": 0.0, "container_start": 0.0,
            "container_resume": 0.0, "container_suspend": 0.0, "container_stop": 0.0,
            "slot_wait_initial": 0.0, "slot_wait_midtask": 0.0,
            "lease_held": 0.0, "flow_s": None, "exit_status": None,
            "n_llm": 0, "n_llm_failed": 0, "n_exec": 0, "n_resume": 0,
            "total_tokens": 0, "n_429": 0,
            "n_resource_samples": 0,
            "_mem_samples": [], "_cpu_samples": [],
            "_acq": {},  # acq_id -> granted mono (for held-time accounting)
        })

    for e in evs:
        k = e["kind"]
        tid = e.get("task_id")
        if k == "llm_query_end":
            d = t(tid); d["llm_wait"] += e.get("wall_s", 0.0); d["n_llm"] += 1
            d["total_tokens"] += e.get("total_tokens") or 0
            if e.get("ok") is False:
                d["n_llm_failed"] += 1
        elif k == "exec_end":
            d = t(tid); d["sandbox_exec"] += e.get("duration_s", 0.0); d["n_exec"] += 1
        elif k == "container_start_end":
            d = t(tid); d["container_start"] += e.get("duration_s", 0.0)
        elif k == "container_resume":
            d = t(tid); d["container_resume"] += e.get("duration_s", 0.0); d["n_resume"] += 1
        elif k == "container_suspend":
            d = t(tid); d["container_suspend"] += e.get("duration_s", 0.0)
        elif k == "container_stop":
            d = t(tid); d["container_stop"] += e.get("duration_s", 0.0)
        elif k == "llm_attempt" and e.get("rate_limited"):
            t(tid)["n_429"] += 1
        elif k == "slot_acquire_granted":
            d = t(tid)
            if e.get("phase") == "container_start":
                d["slot_wait_initial"] += e.get("wait_s", 0.0)
            else:
                d["slot_wait_midtask"] += e.get("wait_s", 0.0)
            d["_acq"][e.get("acq_id")] = e.get("mono")
        elif k == "slot_release":
            d = t(tid)
            g = d["_acq"].pop(e.get("acq_id"), None)
            if g is not None:
                d["lease_held"] += e.get("mono") - g
        elif k == "task_done":
            d = t(tid); d["flow_s"] = e.get("flow_s"); d["exit_status"] = e.get("exit_status")

    container_to_task = _container_task_map(evs)
    for e in evs:
        if e["kind"] != "sample_slow":
            continue
        for stat in e.get("per_container", []) or []:
            tid = _task_for_container(stat.get("container_id"), container_to_task)
            if tid is None:
                continue
            d = t(tid)
            mem = _num(stat.get("mem_usage_mb"))
            cpu = _num(stat.get("cpu_pct"))
            if mem is not None:
                d["_mem_samples"].append(mem)
            if cpu is not None:
                d["_cpu_samples"].append(cpu)
            if mem is not None or cpu is not None:
                d["n_resource_samples"] += 1

    # finalize: idle-held = lease_held - active sandbox operations. For task_lease,
    # cleanup happens before the slot release, so container_stop is active while
    # held. For exec_lease modes cleanup happens outside the pool lease.
    rows = []
    for tid, d in tasks.items():
        d.pop("_acq", None)
        mem_samples = d.pop("_mem_samples", [])
        cpu_samples = d.pop("_cpu_samples", [])
        d["peak_mem_mb"] = max(mem_samples) if mem_samples else 0.0
        d["avg_mem_mb"] = statistics.mean(mem_samples) if mem_samples else 0.0
        d["peak_cpu_pct"] = max(cpu_samples) if cpu_samples else 0.0
        d["avg_cpu_pct"] = statistics.mean(cpu_samples) if cpu_samples else 0.0
        active = (d["sandbox_exec"] + d["container_start"]
                  + d["container_resume"] + d["container_suspend"])
        if lease_mode == "task_lease":
            active += d["container_stop"]
        d["sandbox_active"] = active
        d["sandbox_idle_held"] = max(0.0, d["lease_held"] - active)
        d["idle_held_frac"] = (d["sandbox_idle_held"] / d["lease_held"]) if d["lease_held"] > 0 else 0.0
        # accounted = the wall-clock pieces we can attribute
        d["accounted_s"] = (d["llm_wait"] + d["sandbox_exec"] + d["container_start"]
                            + d["container_resume"] + d["container_suspend"]
                            + d["container_stop"] + d["slot_wait_initial"]
                            + d["slot_wait_midtask"])
        d["task_id"] = tid
        rows.append(d)

    flows = [r["flow_s"] for r in rows if r["flow_s"]]
    fast_samples = [e for e in evs if e["kind"] == "sample_fast"]
    host_cpu = [_num(e.get("host_cpu_pct")) for e in fast_samples]
    host_cpu = [x for x in host_cpu if x is not None]
    host_mem = [_num(e.get("host_mem_used_gb")) for e in fast_samples]
    host_mem = [x for x in host_mem if x is not None]
    running = [_num(e.get("n_running_containers")) for e in fast_samples]
    running = [x for x in running if x is not None]
    resource_rows = resource_series(evs, {"t0_mono": 0})
    total_mem = [r["total_mem_usage_mb"] for r in resource_rows if r["total_mem_usage_mb"] is not None]
    single_mem = [r["max_container_mem_mb"] for r in resource_rows if r["max_container_mem_mb"] is not None]
    total_cpu = [r["total_container_cpu_pct"] for r in resource_rows if r["total_container_cpu_pct"] is not None]
    solved = sum(1 for r in rows if r["exit_status"] == "Submitted")
    n = len(rows)
    agg = {
        "capacity": capacity, "lease_mode": lease_mode, "n_tasks": n,
        "makespan_s": makespan,
        "throughput_per_hour": (n / (makespan / 3600.0)) if makespan else 0.0,
        "solved": solved,
        "flow_p50": pctile(flows, 0.5), "flow_p95": pctile(flows, 0.95),
        "mean_llm_wait": statistics.mean([r["llm_wait"] for r in rows]) if rows else 0,
        "mean_sandbox_active": statistics.mean([r["sandbox_active"] for r in rows]) if rows else 0,
        "mean_idle_held": statistics.mean([r["sandbox_idle_held"] for r in rows]) if rows else 0,
        "mean_idle_held_frac": statistics.mean([r["idle_held_frac"] for r in rows]) if rows else 0,
        "mean_slot_wait_initial": statistics.mean([r["slot_wait_initial"] for r in rows]) if rows else 0,
        "mean_slot_wait_midtask": statistics.mean([r["slot_wait_midtask"] for r in rows]) if rows else 0,
        "total_429": sum(r["n_429"] for r in rows),
        "total_tokens": sum(r["total_tokens"] for r in rows),
        "max_running_containers": max(running) if running else 0,
        "mean_host_cpu_pct": statistics.mean(host_cpu) if host_cpu else 0,
        "peak_host_cpu_pct": max(host_cpu) if host_cpu else 0,
        "peak_host_mem_used_gb": max(host_mem) if host_mem else 0,
        "mean_total_container_mem_mb": statistics.mean(total_mem) if total_mem else 0,
        "peak_total_container_mem_mb": max(total_mem) if total_mem else 0,
        "peak_single_container_mem_mb": max(single_mem) if single_mem else 0,
        "mean_total_container_cpu_pct": statistics.mean(total_cpu) if total_cpu else 0,
        "peak_total_container_cpu_pct": max(total_cpu) if total_cpu else 0,
    }
    return {"agg": agg, "rows": rows}


def util_series(evs: list[dict], origin: dict) -> list[dict]:
    t0 = origin.get("t0_mono", 0)
    out = []
    for e in evs:
        if e["kind"] == "sample_fast":
            out.append({
                "t": e.get("mono", 0) - t0,
                "n_containers": e.get("n_running_containers"),
                "n_containers_all": e.get("n_running_all"),
                "cpu": e.get("host_cpu_pct"),
                "mem_used_gb": e.get("host_mem_used_gb"),
                "mem_avail_gb": e.get("host_mem_avail_gb"),
            })
    return out


def resource_series(evs: list[dict], origin: dict) -> list[dict]:
    t0 = origin.get("t0_mono", 0)
    out = []
    for e in evs:
        if e["kind"] != "sample_slow":
            continue
        stats = e.get("per_container", []) or []
        mems = [_num(s.get("mem_usage_mb")) for s in stats]
        mems = [m for m in mems if m is not None]
        cpus = [_num(s.get("cpu_pct")) for s in stats]
        cpus = [c for c in cpus if c is not None]
        out.append({
            "t": e.get("mono", 0) - t0,
            "n_containers_sampled": len(stats),
            "total_mem_usage_mb": sum(mems) if mems else None,
            "max_container_mem_mb": max(mems) if mems else None,
            "mean_container_mem_mb": statistics.mean(mems) if mems else None,
            "total_container_cpu_pct": sum(cpus) if cpus else None,
            "max_container_cpu_pct": max(cpus) if cpus else None,
            "mean_container_cpu_pct": statistics.mean(cpus) if cpus else None,
        })
    return out


def write_csv(path: Path, rows: list[dict], fields: list[str]) -> None:
    import csv
    with path.open("w", newline="") as f:
        wr = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        wr.writeheader()
        wr.writerows(rows)


def plot_attribution(cells: list[dict], out: Path) -> None:
    if not HAVE_MPL:
        return
    labels = [f"C={c['agg']['capacity']}" for c in cells]
    comps = ["mean_llm_wait", "mean_sandbox_active", "mean_idle_held",
             "mean_slot_wait_initial", "mean_slot_wait_midtask"]
    nice = ["LLM wait", "sandbox active", "idle-held", "slot wait (init)", "slot wait (mid)"]
    import numpy as np
    bottom = np.zeros(len(cells))
    fig, ax = plt.subplots(figsize=(8, 5))
    for comp, lab in zip(comps, nice):
        vals = np.array([c["agg"][comp] for c in cells])
        ax.bar(labels, vals, bottom=bottom, label=lab)
        bottom += vals
    ax.set_ylabel("mean per-task seconds")
    ax.set_title("Per-task time attribution by slot capacity")
    ax.legend()
    fig.tight_layout(); fig.savefig(out, dpi=120); plt.close(fig)


def plot_throughput(cells: list[dict], out: Path) -> None:
    if not HAVE_MPL:
        return
    caps = [c["agg"]["capacity"] for c in cells]
    thr = [c["agg"]["throughput_per_hour"] for c in cells]
    fig, ax = plt.subplots(figsize=(7, 5))
    ax.plot(caps, thr, marker="o")
    ax.set_xlabel("slot capacity C"); ax.set_ylabel("throughput (tasks/hour)")
    ax.set_title("Throughput vs sandbox slot capacity")
    fig.tight_layout(); fig.savefig(out, dpi=120); plt.close(fig)


def verdict(cells: list[dict]) -> str:
    """Apply pre-registered H1/H2-style criteria from the design."""
    cells = sorted(cells, key=lambda c: c["agg"]["capacity"])
    hi = cells[-1]["agg"]
    lines = ["# Verdict (pre-registered criteria)\n"]
    idle = statistics.mean([c["agg"]["mean_idle_held_frac"] for c in cells])
    lines.append(f"- Mean idle-held fraction across cells: **{idle:.0%}**")
    if idle > 0.5:
        lines.append("  - **H1 supported**: the scaffold holds the sandbox slot mostly idle "
                     "(LLM think time), so there is reclaimable capacity.")
    else:
        lines.append("  - H1 weak: slots are mostly actively used; little to reclaim.")
    if len(cells) > 1 and hi["throughput_per_hour"] and cells[0]["agg"]["throughput_per_hour"]:
        lo = cells[0]["agg"]
        drop = 1 - lo["throughput_per_hour"] / hi["throughput_per_hour"]
        lines.append(f"- Throughput at C={lo['capacity']} vs C={hi['capacity']}: "
                     f"{lo['throughput_per_hour']:.1f} vs {hi['throughput_per_hour']:.1f} tasks/hr "
                     f"(**{drop:+.0%}**)")
        if drop > 0.15:
            lines.append("  - Constraining slots materially cut throughput => sandbox is a binding resource here.")
        else:
            lines.append("  - Tightening slots barely moved throughput => LLM API likely the binding resource; "
                        "sandbox scheduling story is weak on this workload (an honest, reportable result).")
    else:
        lines.append("- Single-cell run: no slot-capacity scaling claim is made; use this run for resource "
                     "characterization, not throughput-vs-C evidence.")
    dom = max(["mean_llm_wait", "mean_sandbox_active"], key=lambda k: hi[k])
    lines.append(f"- Dominant time component at C={hi['capacity']}: **{dom}**")
    return "\n".join(lines) + "\n"


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("run_dir")
    ap.add_argument("--a0", action="store_true", help="A0 harness run (uses a0_scaling.csv)")
    args = ap.parse_args()
    run_dir = Path(args.run_dir)

    if args.a0:
        # A0 already wrote a0_scaling.csv; just plot it + utilization.
        print("A0 mode: see a0_scaling.csv; plotting scaling curve.")
        import csv
        rows = list(csv.DictReader((run_dir / "a0_scaling.csv").open()))
        if HAVE_MPL and rows:
            w = [int(r["max_workers"]) for r in rows]
            thr = [float(r["throughput_per_min"]) for r in rows]
            fig, ax = plt.subplots(figsize=(7, 5))
            ax.plot(w, thr, marker="o")
            ax.set_xlabel("max_workers"); ax.set_ylabel("throughput (tasks/min)")
            ax.set_title("A0 harness-only scaling (gold patches)")
            fig.tight_layout(); fig.savefig(run_dir / "a0_scaling.png", dpi=120)
            print(f"  -> {run_dir / 'a0_scaling.png'}")
        return

    cell_dirs = sorted([d for d in run_dir.iterdir() if d.is_dir() and (d / "events.jsonl").exists()])
    cells = []
    all_task_rows = []
    for cd in cell_dirs:
        evs, origin = load_events(cd / "events.jsonl")
        res = analyze_cell(evs)
        cells.append(res)
        for r in res["rows"]:
            r["cell"] = cd.name
            all_task_rows.append(r)
        # per-cell utilization series
        write_csv(cd / "utilization.csv", util_series(evs, origin),
                  ["t", "n_containers", "n_containers_all", "cpu", "mem_used_gb", "mem_avail_gb"])
        write_csv(cd / "resource_timeline.csv", resource_series(evs, origin), [
            "t", "n_containers_sampled", "total_mem_usage_mb", "max_container_mem_mb",
            "mean_container_mem_mb", "total_container_cpu_pct",
            "max_container_cpu_pct", "mean_container_cpu_pct"])
        a = res["agg"]
        print(f"{cd.name}: makespan={a['makespan_s']:.0f}s thr={a['throughput_per_hour']:.1f}/hr "
              f"idle-held={a['mean_idle_held_frac']:.0%} "
              f"peak-mem={a['peak_total_container_mem_mb']:.0f}MB "
              f"solved={a['solved']}/{a['n_tasks']}")

    if not cells:
        print("No cells found.")
        return

    write_csv(run_dir / "summary.csv", [c["agg"] for c in cells], list(cells[0]["agg"].keys()))
    write_csv(run_dir / "per_task.csv", all_task_rows, [
        "cell", "task_id", "exit_status", "flow_s", "llm_wait", "sandbox_exec",
        "container_start", "container_resume", "container_suspend", "container_stop",
        "sandbox_active", "lease_held", "sandbox_idle_held",
        "idle_held_frac", "slot_wait_initial", "slot_wait_midtask", "accounted_s",
        "n_llm", "n_llm_failed", "n_exec", "n_resume", "total_tokens", "n_429",
        "n_resource_samples", "peak_mem_mb", "avg_mem_mb", "peak_cpu_pct", "avg_cpu_pct"])
    plot_attribution(cells, run_dir / "attribution.png")
    plot_throughput(cells, run_dir / "throughput.png")
    (run_dir / "verdict.md").write_text(verdict(cells))
    print(f"\nWrote summary.csv, per_task.csv, verdict.md, *.png to {run_dir}")
    print("\n" + verdict(cells))


if __name__ == "__main__":
    main()
