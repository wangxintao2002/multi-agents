"""Resource layer: the Docker slot pool (the thing we deliberately make scarce)
and a background sampler that records host + docker utilization over time.

SlotPool models "how many containers may be concurrently held". It is the single
arbitrated resource in this experiment. Two lease disciplines:

  - task_lease : a task acquires one slot at container start and holds it for the
                 whole task (current coding-agent scaffold behavior). C therefore
                 caps how many tasks can even *begin* their agent loop.
  - exec_lease : a task holds a slot only while a `docker run`/`docker exec` is
                 actually running, releasing during LLM "think" time. C caps
                 concurrent *executions*; more tasks can be in flight than C.

The pool records every acquire-request / acquire-granted / release with timestamps
so analyze.py can compute blocked-on-slot time and (for task_lease) idle-held time.
"""

from __future__ import annotations

import json
import re
import subprocess
import threading
import time
from dataclasses import dataclass
from pathlib import Path

from events import EventBus


@dataclass
class _Lease:
    task_id: str
    phase: str  # "container_start" or "exec" — what the slot is being held for


class SlotPool:
    """A counting semaphore with instrumentation.

    ``capacity`` is the number of concurrent slots. Use a large number (e.g. 64)
    to approximate C=inf. ``mode`` is informational here; the *caller* (the
    instrumented env) decides where to acquire/release based on the configured
    lease discipline. The pool just times and counts.
    """

    def __init__(self, capacity: int, bus: EventBus, *, mode: str = "task_lease") -> None:
        self.capacity = capacity
        self.mode = mode
        self.bus = bus
        self._sem = threading.BoundedSemaphore(capacity)
        # Live bookkeeping for fairness/starvation analysis.
        self._held = 0
        self._waiting = 0
        self._lock = threading.Lock()
        self._seq = 0  # monotonically increasing acquire id

    def acquire(self, task_id: str, phase: str) -> dict:
        """Block until a slot is free. Emits slot_acquire_request then slot_acquire_granted.

        Returns a small handle dict the caller passes back to ``release``.
        """
        with self._lock:
            self._seq += 1
            acq_id = self._seq
            self._waiting += 1
            waiting_now = self._waiting
        self.bus.emit(
            "slot_acquire_request",
            task_id=task_id,
            phase=phase,
            acq_id=acq_id,
            waiting=waiting_now,
            held=self._held,
            capacity=self.capacity,
            mode=self.mode,
        )
        t_req = time.perf_counter()
        self._sem.acquire()
        t_grant = time.perf_counter()
        with self._lock:
            self._waiting -= 1
            self._held += 1
            held_now = self._held
        self.bus.emit(
            "slot_acquire_granted",
            task_id=task_id,
            phase=phase,
            acq_id=acq_id,
            wait_s=t_grant - t_req,
            held=held_now,
            capacity=self.capacity,
            mode=self.mode,
        )
        return {"acq_id": acq_id, "task_id": task_id, "phase": phase}

    def release(self, handle: dict) -> None:
        with self._lock:
            self._held -= 1
            held_now = self._held
        self._sem.release()
        self.bus.emit(
            "slot_release",
            task_id=handle["task_id"],
            phase=handle["phase"],
            acq_id=handle["acq_id"],
            held=held_now,
            capacity=self.capacity,
            mode=self.mode,
        )


_SIZE_FACTORS_MB = {
    "b": 1e-6,
    "kb": 1e-3,
    "kib": 1024 / 1e6,
    "mb": 1.0,
    "mib": 1024 * 1024 / 1e6,
    "gb": 1000.0,
    "gib": 1024 * 1024 * 1024 / 1e6,
}

_CGROUP_ROOT = Path("/sys/fs/cgroup")


def _parse_percent(value: str | None) -> float | None:
    if not value:
        return None
    try:
        return float(value.strip().rstrip("%"))
    except ValueError:
        return None


def _parse_size_mb(value: str | None) -> float | None:
    if not value:
        return None
    m = re.match(r"^\s*([0-9.]+)\s*([A-Za-z]+)\s*$", value)
    if not m:
        return None
    factor = _SIZE_FACTORS_MB.get(m.group(2).lower())
    if factor is None:
        return None
    try:
        return float(m.group(1)) * factor
    except ValueError:
        return None


def _parse_mem_usage(value: str | None) -> tuple[float | None, float | None]:
    if not value:
        return None, None
    parts = [p.strip() for p in value.split("/", 1)]
    usage = _parse_size_mb(parts[0])
    limit = _parse_size_mb(parts[1]) if len(parts) > 1 else None
    return usage, limit


def _read_int(path: Path) -> int | None:
    try:
        raw = path.read_text().strip()
        if raw == "max":
            return None
        return int(raw)
    except Exception:
        return None


def _bytes_to_mb(value: int | None) -> float | None:
    if value is None:
        return None
    return round(value / 1e6, 3)


def _systemd_slice_candidates(name: str) -> list[Path]:
    if not name.endswith(".slice") or "-" not in name:
        return []
    base = name.removesuffix(".slice")
    parts = base.split("-")
    candidates = []
    path_parts = []
    for i, _part in enumerate(parts):
        slice_name = "-".join(parts[: i + 1]) + ".slice"
        path_parts.append(slice_name)
    candidates.append(_CGROUP_ROOT.joinpath(*path_parts))
    return candidates


def _resolve_cgroup_path(cgroup_parent: str | None) -> Path | None:
    if not cgroup_parent:
        return None
    parent = cgroup_parent.strip()
    if not parent:
        return None
    candidates = [
        _CGROUP_ROOT / parent.lstrip("/"),
        *_systemd_slice_candidates(parent),
    ]
    for path in candidates:
        if path.exists():
            return path
    # The slice may not exist until the first container starts; returning the most
    # likely path lets callers keep trying cheaply on later samples.
    return candidates[-1]


def _read_cgroup_memory(cgroup_path: Path | None) -> dict:
    if cgroup_path is None or not cgroup_path.exists():
        return {}
    fields = {
        "cgroup_path": str(cgroup_path),
        "cgroup_memory_current_mb": _bytes_to_mb(_read_int(cgroup_path / "memory.current")),
        "cgroup_memory_max_mb": _bytes_to_mb(_read_int(cgroup_path / "memory.max")),
        "cgroup_memory_swap_current_mb": _bytes_to_mb(_read_int(cgroup_path / "memory.swap.current")),
        "cgroup_memory_swap_max_mb": _bytes_to_mb(_read_int(cgroup_path / "memory.swap.max")),
    }
    try:
        for line in (cgroup_path / "memory.events").read_text().splitlines():
            key, value = line.split(maxsplit=1)
            fields[f"cgroup_memory_events_{key}"] = int(value)
    except Exception:
        pass
    return fields


class Sampler(threading.Thread):
    """Daemon thread sampling host + docker utilization to events.jsonl.

    Fast signals (host CPU/mem via psutil, running-container count) every
    ``fast_interval`` s. Heavy signal (per-container RSS via ``docker stats``,
    docker disk via ``docker system df``) every ``slow_interval`` s, because
    ``docker stats --no-stream`` itself costs ~1-2s and we don't want it to perturb
    the very contention we're measuring.

    ``name_prefix`` scopes ``n_running_containers`` to this experiment's container
    family. A1/A2 mini-swe-agent containers use ``minisweagent-``; A0's official
    SWE-bench harness uses ``sweb.eval.``.
    """

    def __init__(
        self,
        bus: EventBus,
        *,
        fast_interval: float = 1.0,
        slow_interval: float = 5.0,
        docker_exe: str = "docker",
        name_prefix: str = "minisweagent-",
        cgroup_parent: str | None = None,
    ) -> None:
        super().__init__(daemon=True)
        self.bus = bus
        self.fast_interval = fast_interval
        self.slow_interval = slow_interval
        self.docker_exe = docker_exe
        self.name_prefix = name_prefix
        self.cgroup_parent = cgroup_parent
        self._cgroup_path = _resolve_cgroup_path(cgroup_parent)
        self._stop_event = threading.Event()
        try:
            import psutil  # noqa: F401

            self._psutil_ok = True
        except Exception:
            self._psutil_ok = False

    def stop(self) -> None:
        self._stop_event.set()

    def _count_running_containers(self) -> tuple[int | None, int | None]:
        """Return (ours, all): containers whose name starts with the experiment
        prefix, and the host-wide total. We report both so other workloads on the
        box don't silently inflate the experiment's concurrency signal."""
        try:
            out = subprocess.run(
                [self.docker_exe, "ps", "--format", "{{.Names}}"],
                capture_output=True, text=True, timeout=10,
            )
            names = [ln.strip() for ln in out.stdout.splitlines() if ln.strip()]
            ours = sum(1 for n in names if n.startswith(self.name_prefix))
            return ours, len(names)
        except Exception:
            return None, None

    def _docker_stats(self) -> list[dict]:
        try:
            out = subprocess.run(
                [self.docker_exe, "stats", "--no-stream", "--format", "{{json .}}"],
                capture_output=True, text=True, timeout=30,
            )
            rows = []
            for ln in out.stdout.splitlines():
                try:
                    raw = json.loads(ln)
                except json.JSONDecodeError:
                    continue
                name = raw.get("Name") or raw.get("NameOrID") or ""
                if not name.startswith(self.name_prefix):
                    continue
                mem_usage_mb, mem_limit_mb = _parse_mem_usage(raw.get("MemUsage"))
                row = {
                    "container_id": raw.get("Container"),
                    "name": name,
                    "mem_usage": raw.get("MemUsage"),
                    "mem_usage_mb": round(mem_usage_mb, 3) if mem_usage_mb is not None else None,
                    "mem_limit_mb": round(mem_limit_mb, 3) if mem_limit_mb is not None else None,
                    "mem_pct": _parse_percent(raw.get("MemPerc")),
                    "cpu_pct": _parse_percent(raw.get("CPUPerc")),
                }
                try:
                    row["pids"] = int(raw["PIDs"])
                except Exception:
                    row["pids"] = None
                rows.append(row)
            return rows
        except Exception:
            return []

    def _docker_df(self) -> str | None:
        try:
            out = subprocess.run(
                [self.docker_exe, "system", "df", "--format", "{{.Type}}:{{.Size}}:{{.Reclaimable}}"],
                capture_output=True, text=True, timeout=30,
            )
            return out.stdout.replace("\n", "; ").strip()
        except Exception:
            return None

    def run(self) -> None:
        import time as _t

        psutil = None
        if self._psutil_ok:
            import psutil  # type: ignore
            psutil.cpu_percent(interval=None)  # prime the first call

        last_slow = 0.0
        while not self._stop_event.is_set():
            now = _t.perf_counter()
            ours, all_c = self._count_running_containers()
            fields: dict = {
                "n_running_containers": ours,
                "n_running_all": all_c,
                "container_name_prefix": self.name_prefix,
                "cgroup_parent": self.cgroup_parent,
            }
            if self.cgroup_parent and (self._cgroup_path is None or not self._cgroup_path.exists()):
                self._cgroup_path = _resolve_cgroup_path(self.cgroup_parent)
            fields.update(_read_cgroup_memory(self._cgroup_path))
            if psutil is not None:
                vm = psutil.virtual_memory()
                fields["host_cpu_pct"] = psutil.cpu_percent(interval=None)
                fields["host_mem_used_gb"] = round(vm.used / 1e9, 2)
                fields["host_mem_avail_gb"] = round(vm.available / 1e9, 2)
            self.bus.emit("sample_fast", **fields)

            if now - last_slow >= self.slow_interval:
                last_slow = now
                stats = self._docker_stats()
                self.bus.emit(
                    "sample_slow",
                    n_stats=len(stats),
                    per_container=stats,
                    docker_df=self._docker_df(),
                )
            self._stop_event.wait(self.fast_interval)
