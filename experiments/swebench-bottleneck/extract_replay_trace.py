"""Extract deterministic replay traces from mini-swe-agent trajectories.

The trace preserves complete bash commands from ``*.traj.json``.  The matching
``events.jsonl`` is optional and is used only to attach recorded LLM wait times.

Examples:
  python extract_replay_trace.py \
    --source results/podman_pilot_exec_lease_stop_20260628/Cinf_exec_lease_stop \
    --out-dir traces/podman_pilot_stop_cinf
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from dataset import image_for, load_instances  # noqa: E402


def _instance_id_from_traj(path: Path) -> str:
    name = path.name
    suffix = ".traj.json"
    if name.endswith(suffix):
        return name[: -len(suffix)]
    return path.stem


def _resolve_source(source: Path, events_arg: str | None) -> tuple[Path, Path | None]:
    if (source / "trajs").is_dir():
        traj_dir = source / "trajs"
        default_events = source / "events.jsonl"
    else:
        traj_dir = source
        default_events = source.parent / "events.jsonl"
    events = Path(events_arg) if events_arg else default_events
    return traj_dir, events if events.exists() else None


def _load_instance_index(subset: str, split: str) -> dict[str, dict]:
    try:
        return {inst["instance_id"]: inst for inst in load_instances(subset, split)}
    except Exception:
        return {}


def _load_recorded_waits(events_path: Path | None) -> dict[str, list[float]]:
    waits: dict[str, list[float]] = {}
    if events_path is None:
        return waits
    for line in events_path.read_text().splitlines():
        try:
            ev = json.loads(line)
        except json.JSONDecodeError:
            continue
        if ev.get("kind") != "llm_query_end" or ev.get("ok") is not True:
            continue
        task_id = ev.get("task_id")
        if not task_id:
            continue
        waits.setdefault(task_id, []).append(float(ev.get("wall_s") or 0.0))
    return waits


def _tool_returncodes(messages: list[dict]) -> dict[str, int]:
    out: dict[str, int] = {}
    for msg in messages:
        if msg.get("role") != "tool":
            continue
        tool_call_id = msg.get("tool_call_id")
        if not tool_call_id:
            continue
        rc = msg.get("extra", {}).get("returncode")
        if rc is None:
            continue
        try:
            out[str(tool_call_id)] = int(rc)
        except (TypeError, ValueError):
            pass
    return out


def _actions_from_assistant(msg: dict) -> list[dict]:
    actions = msg.get("extra", {}).get("actions") or []
    parsed: list[dict] = []
    for action in actions:
        command = action.get("command")
        if command:
            parsed.append({
                "tool_call_id": action.get("tool_call_id"),
                "command": command,
            })
    if parsed:
        return parsed

    # Fallback for trajectories without mini-swe-agent's normalized
    # ``extra.actions`` field.
    for call in msg.get("tool_calls") or []:
        fn = call.get("function") or {}
        if fn.get("name") != "bash":
            continue
        try:
            args = json.loads(fn.get("arguments") or "{}")
        except json.JSONDecodeError:
            continue
        command = args.get("command")
        if command:
            parsed.append({
                "tool_call_id": call.get("id"),
                "command": command,
            })
    return parsed


def _extract_one(
    traj_path: Path,
    *,
    instance_index: dict[str, dict],
    recorded_waits: dict[str, list[float]],
    events_path: Path | None,
) -> tuple[dict, list[str]]:
    warnings: list[str] = []
    data = json.loads(traj_path.read_text())
    messages = data.get("messages") or []
    instance_id = _instance_id_from_traj(traj_path)
    env_cfg = data.get("info", {}).get("config", {}).get("environment", {})
    inst = instance_index.get(instance_id, {})

    waits = recorded_waits.get(instance_id, [])
    wait_idx = 0
    returncodes = _tool_returncodes(messages)
    steps: list[dict] = []
    for msg in messages:
        if msg.get("role") != "assistant":
            continue
        actions = _actions_from_assistant(msg)
        if not actions:
            continue
        step_actions = []
        for action in actions:
            tool_call_id = action.get("tool_call_id")
            step_actions.append({
                "tool_call_id": tool_call_id,
                "command": action["command"],
                "expected_returncode": returncodes.get(str(tool_call_id)) if tool_call_id else None,
            })
        recorded = waits[wait_idx] if wait_idx < len(waits) else None
        steps.append({
            "step_idx": len(steps) + 1,
            "recorded_llm_wait_s": recorded,
            "actions": step_actions,
        })
        wait_idx += 1

    if waits and wait_idx != len(waits):
        warnings.append(
            f"{instance_id}: used {wait_idx} action-producing LLM waits, "
            f"but events had {len(waits)} successful LLM waits"
        )
    if not steps:
        warnings.append(f"{instance_id}: no replayable assistant tool calls found")

    image = env_cfg.get("image")
    if not image and inst:
        image = image_for(inst)
    trace = {
        "schema_version": 1,
        "source": {
            "traj_path": str(traj_path),
            "events_path": str(events_path) if events_path else None,
        },
        "instance": {
            "instance_id": instance_id,
            "repo": inst.get("repo"),
            "image": image,
        },
        "steps": steps,
    }
    return trace, warnings


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--source", required=True, help="Cell dir containing trajs/, or a traj directory.")
    ap.add_argument("--events", default=None, help="Optional matching events.jsonl.")
    ap.add_argument("--out-dir", required=True)
    ap.add_argument("--subset", default="lite")
    ap.add_argument("--split", default="test")
    ap.add_argument("--limit", type=int, default=0)
    args = ap.parse_args()

    source = Path(args.source)
    traj_dir, events_path = _resolve_source(source, args.events)
    if not traj_dir.is_dir():
        raise SystemExit(f"trajectory directory not found: {traj_dir}")
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    instance_index = _load_instance_index(args.subset, args.split)
    waits = _load_recorded_waits(events_path)
    trajs = sorted(traj_dir.glob("*.traj.json"))
    if args.limit:
        trajs = trajs[: args.limit]
    if not trajs:
        raise SystemExit(f"no *.traj.json files found in {traj_dir}")

    all_warnings: list[str] = []
    manifest = []
    for traj_path in trajs:
        trace, warnings = _extract_one(
            traj_path,
            instance_index=instance_index,
            recorded_waits=waits,
            events_path=events_path,
        )
        all_warnings.extend(warnings)
        instance_id = trace["instance"]["instance_id"]
        out_path = out_dir / f"{instance_id}.replay.json"
        out_path.write_text(json.dumps(trace, indent=2, sort_keys=True) + "\n")
        manifest.append({
            "instance_id": instance_id,
            "path": str(out_path),
            "n_steps": len(trace["steps"]),
            "n_actions": sum(len(step["actions"]) for step in trace["steps"]),
            "image": trace["instance"].get("image"),
        })

    (out_dir / "manifest.json").write_text(json.dumps({
        "schema_version": 1,
        "source": str(source),
        "events_path": str(events_path) if events_path else None,
        "traces": manifest,
        "warnings": all_warnings,
    }, indent=2, sort_keys=True) + "\n")

    print(f"wrote {len(manifest)} traces to {out_dir}")
    for row in manifest:
        print(f"  {row['instance_id']}: steps={row['n_steps']} actions={row['n_actions']}")
    for warning in all_warnings:
        print(f"warning: {warning}", file=sys.stderr)


if __name__ == "__main__":
    main()
