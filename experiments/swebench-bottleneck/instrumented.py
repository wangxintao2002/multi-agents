"""Instrumented subclasses of mini-swe-agent's Docker env and litellm model.

We do NOT fork mini-swe-agent. We subclass and override the few methods that are
the timing boundaries, emitting events into a shared EventBus. The agent loop
(DefaultAgent) is used unchanged.

Timing boundaries
-----------------
LLM wait        : LitellmModel.query  (includes tenacity backoff = true wall wait)
LLM api latency : LitellmModel._query (single attempt; also where we see 429s)
container start : DockerEnvironment._start_container (docker run ... sleep 2h)
sandbox exec    : DockerEnvironment.execute (the docker exec subprocess.run)
blocked-on-slot : SlotPool.acquire (recorded by the pool itself)

Lease discipline
----------------
task_lease : acquire in _start_container, release in cleanup. Slot held for the
             whole task. (C caps how many tasks can start.)
exec_lease : acquire a slot at the start of _start_container's docker run AND at
             the start of each execute(); release at the end of each. Slot is
             free during LLM think time. (C caps concurrent executions.)
"""

from __future__ import annotations

import subprocess
import time

import litellm
from minisweagent.environments.docker import DockerEnvironment
from minisweagent.models.litellm_model import LitellmModel

from events import EventBus
from resources import SlotPool


class InstrumentedLitellmModel(LitellmModel):
    """Times LLM calls and counts rate-limit (429) events.

    ``task_id`` and ``bus`` are passed as plain kwargs and popped before the
    pydantic config is built (the base __init__ forwards **kwargs to the config
    model, which would reject unknown fields).
    """

    def __init__(self, *, bus: EventBus, task_id: str, **kwargs):
        self.bus = bus
        self.task_id = task_id
        self._n_calls = 0
        super().__init__(**kwargs)

    def _query(self, messages, **kwargs):
        """Single API attempt. tenacity may call this multiple times via query()."""
        t0 = time.perf_counter()
        try:
            resp = super()._query(messages, **kwargs)
            self.bus.emit(
                "llm_attempt",
                task_id=self.task_id,
                ok=True,
                latency_s=time.perf_counter() - t0,
            )
            return resp
        except litellm.exceptions.RateLimitError as e:
            self.bus.emit(
                "llm_attempt",
                task_id=self.task_id,
                ok=False,
                rate_limited=True,
                latency_s=time.perf_counter() - t0,
                err=str(e)[:200],
            )
            raise
        except Exception as e:
            self.bus.emit(
                "llm_attempt",
                task_id=self.task_id,
                ok=False,
                rate_limited=False,
                latency_s=time.perf_counter() - t0,
                err=f"{type(e).__name__}: {str(e)[:200]}",
            )
            raise

    def query(self, messages, **kwargs):
        """Full query incl. retries. Wall time here == true 'LLM wait' for the step."""
        self._n_calls += 1
        call_idx = self._n_calls
        t0 = time.perf_counter()
        self.bus.emit("llm_query_start", task_id=self.task_id, call_idx=call_idx)
        try:
            message = super().query(messages, **kwargs)
        except Exception as e:
            # Retries exhausted / auth / context errors. The wall time spent waiting
            # (incl. all backoff) is real LLM-wait and must NOT be dropped, else we
            # underestimate the API bottleneck exactly when it bit hardest.
            self.bus.emit(
                "llm_query_end",
                task_id=self.task_id,
                call_idx=call_idx,
                wall_s=time.perf_counter() - t0,
                ok=False,
                err=f"{type(e).__name__}: {str(e)[:200]}",
            )
            raise
        wall = time.perf_counter() - t0
        # Pull token usage out of the persisted raw response if available.
        usage = {}
        try:
            resp = message.get("extra", {}).get("response", {})
            u = resp.get("usage") if isinstance(resp, dict) else None
            if u:
                usage = {
                    "prompt_tokens": u.get("prompt_tokens"),
                    "completion_tokens": u.get("completion_tokens"),
                    "total_tokens": u.get("total_tokens"),
                }
        except Exception:
            pass
        self.bus.emit(
            "llm_query_end",
            task_id=self.task_id,
            call_idx=call_idx,
            wall_s=wall,
            ok=True,
            **usage,
        )
        return message


class InstrumentedDockerEnvironment(DockerEnvironment):
    """Docker env that acquires slots from a SlotPool and times container ops.

    Extra kwargs (popped before the pydantic config is built):
      pool      : SlotPool
      bus       : EventBus
      task_id   : str
      lease_mode: "task_lease" | "exec_lease" | "exec_lease_stop"

    Lease modes
    -----------
    task_lease      : hold the slot for the whole task (current scaffold behavior).
    exec_lease      : hold the slot only during docker run/exec; container stays
                      alive during LLM wait. Measures *exec-concurrency* limiting
                      (frees the scheduling slot, NOT host RAM).
    exec_lease_stop : additionally `docker stop` the container during LLM wait and
                      `docker start` it before the next exec. Frees real RAM (the
                      honest suspend/wake) and pays the restart latency, which we
                      record as container_resume time. Requires NOT using --rm.
    """

    def __init__(self, *, pool: SlotPool, bus: EventBus, task_id: str,
                 lease_mode: str = "task_lease", **kwargs):
        self.pool = pool
        self.bus = bus
        self.task_id = task_id
        self.lease_mode = lease_mode
        self._task_lease_handle = None  # held for whole task in task_lease mode
        self._suspended = False         # exec_lease_stop: is the container stopped?
        # exec_lease_stop must keep the container across stop/start, so it cannot
        # use --rm (which deletes on stop, losing the agent's filesystem work).
        if lease_mode == "exec_lease_stop":
            kwargs.setdefault("run_args", [])
        # NOTE: base __init__ calls _start_container() at the end, so all the
        # above must be set first.
        super().__init__(**kwargs)

    # ---- low-level docker stop/start (synchronous, timed) -------------------
    def _docker_cmd(self, *args: str, timeout: int = 120) -> int:
        import subprocess
        r = subprocess.run([self.config.executable, *args],
                           capture_output=True, text=True, timeout=timeout)
        return r.returncode

    def _suspend(self) -> None:
        """exec_lease_stop: stop the container to free RAM during LLM wait.
        Uses --time 0 (immediate SIGKILL): PID 1 is `sleep` and ignores SIGTERM, so
        a grace period would just stall. The container is retained (no --rm) so a
        later `docker start` resumes it with its filesystem intact."""
        if self._suspended or not self.container_id:
            return
        t0 = time.perf_counter()
        try:
            self._docker_cmd("stop", "--time", "0", self.container_id, timeout=30)
            self._suspended = True
        finally:
            self.bus.emit("container_suspend", task_id=self.task_id,
                          duration_s=time.perf_counter() - t0)

    def _resume(self) -> None:
        """exec_lease_stop: restart the stopped container before an exec.
        The restart latency is the real cost of suspend/wake and is recorded."""
        if not self._suspended or not self.container_id:
            return
        t0 = time.perf_counter()
        try:
            self._docker_cmd("start", self.container_id)
            self._suspended = False
        finally:
            self.bus.emit("container_resume", task_id=self.task_id,
                          duration_s=time.perf_counter() - t0)

    # ---- container lifecycle -------------------------------------------------
    def _start_container(self):
        # In all modes the container's creation is an "exec-like" op that needs a slot.
        handle = self.pool.acquire(self.task_id, phase="container_start")
        t0 = time.perf_counter()
        self.bus.emit("container_start_begin", task_id=self.task_id, image=self.config.image)
        try:
            super()._start_container()
        except Exception as e:
            # Container never came up (e.g. pull timeout / missing image): release
            # the slot now, else it would leak and permanently shrink capacity.
            self.pool.release(handle)
            self.bus.emit(
                "container_start_end",
                task_id=self.task_id,
                duration_s=time.perf_counter() - t0,
                ok=False,
                err=f"{type(e).__name__}: {str(e)[:200]}",
            )
            raise
        self.bus.emit(
            "container_start_end",
            task_id=self.task_id,
            duration_s=time.perf_counter() - t0,
            ok=True,
            container_id=(self.container_id or "")[:12],
        )
        if self.lease_mode == "task_lease":
            # Keep holding the slot for the whole task.
            self._task_lease_handle = handle
        else:
            # exec_lease / exec_lease_stop: release the slot now; re-acquire per exec.
            if self.lease_mode == "exec_lease_stop":
                self._suspend()  # free RAM while waiting for the first LLM response
            self.pool.release(handle)

    # ---- per-action execution ------------------------------------------------
    def execute(self, action: dict, cwd: str = "", *, timeout: int | None = None):
        handle = None
        if self.lease_mode in ("exec_lease", "exec_lease_stop"):
            handle = self.pool.acquire(self.task_id, phase="exec")
            if self.lease_mode == "exec_lease_stop":
                self._resume()  # bring the container back before running the command
        t0 = time.perf_counter()
        cmd_preview = (action.get("command", "") or "")[:120]
        self.bus.emit("exec_begin", task_id=self.task_id, cmd=cmd_preview)
        rc = None
        try:
            # Base execute() may raise Submitted via _check_finished; we must still
            # release the slot, so timing/release happen in finally.
            result = super().execute(action, cwd, timeout=timeout)
            rc = result.get("returncode")
            return result
        finally:
            dt = time.perf_counter() - t0
            self.bus.emit("exec_end", task_id=self.task_id, duration_s=dt, returncode=rc)
            if handle is not None:
                if self.lease_mode == "exec_lease_stop":
                    self._suspend()  # free RAM again while the next LLM call runs
                self.pool.release(handle)

    # ---- teardown ------------------------------------------------------------
    def cleanup(self):
        """Synchronous teardown so the slot is released only after the container is
        actually gone (mini-swe-agent's base cleanup is async `Popen(... &)`, which
        would let running-container count briefly exceed C). Timed as container_stop.

        We use `rm -f` (immediate SIGKILL + remove) rather than `stop` with a grace
        period: the container's PID 1 is `sleep`, which ignores SIGTERM, so `docker
        stop` would block the full --time window (~60s) per task and inflate makespan.
        These are throwaway eval containers, so forced removal is correct and fast."""
        cid = getattr(self, "container_id", None)
        if cid is None:
            return
        t0 = time.perf_counter()
        ok = True
        try:
            if self._docker_cmd("rm", "-f", cid, timeout=60) != 0:
                ok = False
        except Exception:
            ok = False
        finally:
            self.container_id = None  # idempotent: __del__ won't double-stop
            self.bus.emit("container_stop", task_id=self.task_id,
                          duration_s=time.perf_counter() - t0, ok=ok)
            if self.lease_mode == "task_lease" and self._task_lease_handle is not None:
                self.pool.release(self._task_lease_handle)
                self._task_lease_handle = None
