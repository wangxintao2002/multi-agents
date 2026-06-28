"""Replay recorded SWE-bench tool-call traces under sandbox lease policies.

Unlike ``run_experiment.py``, this runner never calls an LLM.  It sleeps for a
deterministic synthetic LLM delay and then executes recorded bash commands in
real SWE-bench containers.  The emitted events are intentionally compatible with
``analyze.py``.

Usage:
  python run_replay.py --config config.replay.yaml
  python run_replay.py --config config.replay.yaml --lease exec_lease_stop --delay 10
"""

from __future__ import annotations

import argparse
import copy
import json
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import yaml

sys.path.insert(0, str(Path(__file__).resolve().parent))

from events import EventBus  # noqa: E402
from instrumented import InstrumentedDockerEnvironment  # noqa: E402
from resources import Sampler, SlotPool  # noqa: E402
from run_experiment import (  # noqa: E402
    _load_agent_templates,
    apply_docker_run_args,
    normalize_image_for_runtime,
)

HERE = Path(__file__).resolve().parent


def _load_traces(trace_dir: Path) -> list[dict]:
    paths = sorted(trace_dir.glob("*.replay.json"))
    if not paths:
        raise SystemExit(f"no *.replay.json traces found in {trace_dir}")
    traces = []
    for path in paths:
        trace = json.loads(path.read_text())
        if trace.get("schema_version") != 1:
            raise SystemExit(f"unsupported trace schema in {path}: {trace.get('schema_version')}")
        trace["_trace_path"] = str(path)
        traces.append(trace)
    return traces


def _expand_traces(traces: list[dict], duplicate_factor: int) -> list[dict]:
    if duplicate_factor <= 1:
        for trace in traces:
            trace["task_id"] = trace["instance"]["instance_id"]
        return traces
    out = []
    for rep in range(duplicate_factor):
        for trace in traces:
            item = copy.deepcopy(trace)
            base = item["instance"]["instance_id"]
            item["task_id"] = f"{base}__rep{rep + 1}"
            out.append(item)
    return out


def _delay_for_step(step: dict, cfg: dict) -> float:
    mode = cfg["trace"].get("delay_mode", "fixed")
    if mode == "fixed":
        return float(cfg["trace"].get("fixed_delay_s", 0.0))
    if mode == "recorded":
        value = step.get("recorded_llm_wait_s")
        if value is None:
            raise ValueError("recorded delay requested but trace step has no recorded_llm_wait_s")
        return float(value)
    if mode == "scaled":
        value = step.get("recorded_llm_wait_s")
        if value is None:
            raise ValueError("scaled delay requested but trace step has no recorded_llm_wait_s")
        return float(value) * float(cfg["trace"].get("delay_scale", 1.0))
    raise ValueError(f"unknown delay_mode: {mode}")


def _trace_image(trace: dict, docker_exe: str) -> str:
    image = trace.get("instance", {}).get("image")
    if not image:
        raise ValueError(f"trace has no image: {trace.get('_trace_path')}")
    return normalize_image_for_runtime(image, docker_exe)


def run_one_trace(
    trace: dict,
    *,
    cfg: dict,
    templates: dict,
    pool: SlotPool,
    bus: EventBus,
    lease_mode: str,
) -> dict:
    from minisweagent.exceptions import Submitted

    task_id = trace["task_id"]
    base_instance_id = trace["instance"]["instance_id"]
    bus.emit("task_submit", task_id=task_id, base_instance_id=base_instance_id,
             repo=trace["instance"].get("repo"), replay=True)
    t_start = time.perf_counter()
    env = None
    exit_status = "Completed"
    err = None
    n_actions = 0
    try:
        env_settings = dict(templates["environment"])
        env_settings.pop("environment_class", None)
        apply_docker_run_args(env_settings, cfg)
        env_settings["image"] = _trace_image(trace, cfg["run"]["docker_executable"])
        env = InstrumentedDockerEnvironment(
            pool=pool,
            bus=bus,
            task_id=task_id,
            lease_mode=lease_mode,
            **env_settings,
        )
        for step in trace.get("steps", []):
            call_idx = int(step["step_idx"])
            delay_s = _delay_for_step(step, cfg)
            t0 = time.perf_counter()
            bus.emit("llm_query_start", task_id=task_id, call_idx=call_idx,
                     synthetic=True, delay_mode=cfg["trace"].get("delay_mode", "fixed"))
            if delay_s > 0:
                time.sleep(delay_s)
            bus.emit("llm_query_end", task_id=task_id, call_idx=call_idx,
                     wall_s=time.perf_counter() - t0, ok=True, synthetic=True,
                     configured_delay_s=delay_s)

            for action_idx, action in enumerate(step.get("actions", []), start=1):
                n_actions += 1
                expected = action.get("expected_returncode")
                try:
                    result = env.execute({"command": action["command"]}, cwd="/testbed",
                                         timeout=cfg["limits"].get("command_timeout_s"))
                except Submitted as e:
                    exit_status = "Submitted"
                    bus.emit("replay_action_submitted", task_id=task_id, call_idx=call_idx,
                             action_idx=action_idx)
                    raise e
                rc = result.get("returncode")
                bus.emit("replay_action_end", task_id=task_id, call_idx=call_idx,
                         action_idx=action_idx, returncode=rc,
                         expected_returncode=expected, matched=(expected is None or rc == expected))
    except Submitted:
        pass
    except Exception as e:
        exit_status = type(e).__name__
        err = str(e)[:300]
    finally:
        if env is not None:
            try:
                env.cleanup()
            except Exception:
                pass
        flow = time.perf_counter() - t_start
        bus.emit("task_done", task_id=task_id, base_instance_id=base_instance_id,
                 exit_status=exit_status, flow_s=flow, err=err, n_replay_actions=n_actions)
    return {
        "task_id": task_id,
        "base_instance_id": base_instance_id,
        "exit_status": exit_status,
        "flow_s": flow,
        "err": err,
        "n_replay_actions": n_actions,
    }


def run_cell(
    traces: list[dict],
    *,
    capacity: int,
    lease_mode: str,
    cfg: dict,
    templates: dict,
    run_dir: Path,
) -> None:
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
        cgroup_parent=cfg["run"].get("cgroup_parent"),
    )

    print(f"\n=== REPLAY CELL {cell_name}: N={len(traces)} C={capacity} lease={lease_mode} ===")
    bus.emit("cell_start", cell=cell_name, capacity=capacity, lease_mode=lease_mode,
             n_tasks=len(traces), cgroup_parent=cfg["run"].get("cgroup_parent"),
             replay=True, delay_mode=cfg["trace"].get("delay_mode", "fixed"),
             fixed_delay_s=cfg["trace"].get("fixed_delay_s"),
             delay_scale=cfg["trace"].get("delay_scale"))
    sampler.start()
    t0 = time.perf_counter()
    results = []
    with ThreadPoolExecutor(max_workers=cfg["run"]["offered_concurrency"]) as ex:
        futures = {
            ex.submit(run_one_trace, trace, cfg=cfg, templates=templates, pool=pool,
                      bus=bus, lease_mode=lease_mode): trace["task_id"]
            for trace in traces
        }
        for fut in as_completed(futures):
            try:
                results.append(fut.result())
            except Exception as e:
                print(f"  trace {futures[fut]} crashed: {e}")
    makespan = time.perf_counter() - t0
    sampler.stop()
    sampler.join(timeout=5)
    bus.emit("cell_end", cell=cell_name, makespan_s=makespan)
    n_events = bus.write_jsonl(cell_dir / "events.jsonl")
    n_completed = sum(1 for r in results if r["exit_status"] in {"Completed", "Submitted"})
    print(f"  makespan={makespan:.1f}s  completed={n_completed}/{len(results)}  events={n_events}")
    print(f"  -> {cell_dir}")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default=str(HERE / "config.replay.yaml"))
    ap.add_argument("--capacities", nargs="*", default=None)
    ap.add_argument("--lease", default=None,
                    choices=["task_lease", "exec_lease", "exec_lease_stop"])
    ap.add_argument("--delay", type=float, default=None,
                    help="Override trace.fixed_delay_s and force delay_mode=fixed.")
    ap.add_argument("--run-id", default=None)
    args = ap.parse_args()

    cfg = yaml.safe_load(Path(args.config).read_text())
    if args.lease:
        cfg["run"]["lease_mode"] = args.lease
    if args.delay is not None:
        cfg["trace"]["delay_mode"] = "fixed"
        cfg["trace"]["fixed_delay_s"] = args.delay
    lease_mode = cfg["run"]["lease_mode"]
    docker_exe = cfg["run"]["docker_executable"]

    caps_spec = args.capacities if args.capacities is not None else cfg["run"]["slot_capacities"]
    inf_cap = cfg["run"]["inf_capacity"]
    capacities = [inf_cap if str(c).lower() == "inf" else int(c) for c in caps_spec]

    trace_dir = Path(cfg["trace"]["trace_dir"])
    if not trace_dir.is_absolute():
        trace_dir = HERE / trace_dir
    traces = _expand_traces(_load_traces(trace_dir), int(cfg["trace"].get("duplicate_factor", 1)))
    templates = _load_agent_templates()

    run_id = args.run_id or (
        f"replay_{lease_mode}_delay{cfg['trace'].get('fixed_delay_s', cfg['trace'].get('delay_mode'))}_"
        f"{int(time.time())}"
    )
    run_dir = HERE / "results" / run_id
    run_dir.mkdir(parents=True, exist_ok=True)
    (run_dir / "selected_instances.txt").write_text("\n".join(
        f"{trace['task_id']}\t{trace['instance'].get('repo')}\t{_trace_image(trace, docker_exe)}"
        for trace in traces
    ))
    (run_dir / "replay_metadata.json").write_text(json.dumps({
        "trace_dir": str(trace_dir),
        "delay_mode": cfg["trace"].get("delay_mode"),
        "fixed_delay_s": cfg["trace"].get("fixed_delay_s"),
        "delay_scale": cfg["trace"].get("delay_scale"),
        "duplicate_factor": cfg["trace"].get("duplicate_factor", 1),
        "n_traces": len(traces),
        "docker_executable": docker_exe,
    }, indent=2, sort_keys=True) + "\n")

    for cap in capacities:
        run_cell(traces, capacity=cap, lease_mode=lease_mode, cfg=cfg,
                 templates=templates, run_dir=run_dir)
    print(f"\nDONE. Results in {run_dir}")


if __name__ == "__main__":
    main()
