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

import subprocess
import threading
import time
from dataclasses import dataclass

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


class Sampler(threading.Thread):
    """Daemon thread sampling host + docker utilization to samples.csv.

    Fast signals (host CPU/mem via psutil, running-container count) every
    ``fast_interval`` s. Heavy signal (per-container RSS via ``docker stats``,
    docker disk via ``docker system df``) every ``slow_interval`` s, because
    ``docker stats --no-stream`` itself costs ~1-2s and we don't want it to perturb
    the very contention we're measuring.
    """

    def __init__(
        self,
        bus: EventBus,
        *,
        fast_interval: float = 1.0,
        slow_interval: float = 5.0,
        docker_exe: str = "docker",
        name_prefix: str = "minisweagent-",
    ) -> None:
        super().__init__(daemon=True)
        self.bus = bus
        self.fast_interval = fast_interval
        self.slow_interval = slow_interval
        self.docker_exe = docker_exe
        self.name_prefix = name_prefix
        self._stop = threading.Event()
        try:
            import psutil  # noqa: F401

            self._psutil_ok = True
        except Exception:
            self._psutil_ok = False

    def stop(self) -> None:
        self._stop.set()

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
                [self.docker_exe, "stats", "--no-stream", "--format",
                 "{{.Name}}\t{{.MemUsage}}\t{{.CPUPerc}}"],
                capture_output=True, text=True, timeout=30,
            )
            rows = []
            for ln in out.stdout.splitlines():
                parts = ln.split("\t")
                if len(parts) == 3:
                    rows.append({"name": parts[0], "mem": parts[1], "cpu": parts[2]})
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
        while not self._stop.is_set():
            now = _t.perf_counter()
            ours, all_c = self._count_running_containers()
            fields: dict = {"n_running_containers": ours, "n_running_all": all_c}
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
            self._stop.wait(self.fast_interval)
