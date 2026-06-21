"""Thread-safe event bus shared by all instrumented components.

Every timed event is appended here with dual timestamps:
  - ``t``  : ``time.time()`` wall-clock (absolute, for aligning with samples.csv)
  - ``mono``: ``time.perf_counter()`` monotonic (for durations; immune to clock skew)

The orchestrator owns one ``EventBus`` per run and flushes it to ``events.jsonl``.
Events are intentionally schema-light: a ``kind`` string + arbitrary fields. ``analyze.py``
is the single place that knows how to fold them into per-task attribution.
"""

from __future__ import annotations

import json
import threading
import time
from pathlib import Path


class EventBus:
    def __init__(self) -> None:
        self._events: list[dict] = []
        self._lock = threading.Lock()
        # Single t0 captured at construction so every event can also be expressed
        # as seconds-since-run-start without each caller needing the origin.
        self.t0_wall = time.time()
        self.t0_mono = time.perf_counter()

    def emit(self, kind: str, **fields) -> None:
        ev = {
            "kind": kind,
            "t": time.time(),
            "mono": time.perf_counter(),
            **fields,
        }
        with self._lock:
            self._events.append(ev)

    def snapshot(self) -> list[dict]:
        with self._lock:
            return list(self._events)

    def write_jsonl(self, path: Path) -> int:
        """Flush all events to a JSONL file. Returns the number of events written."""
        events = self.snapshot()
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", encoding="utf-8") as f:
            # Header line records the run origin so analyze.py can reconstruct
            # absolute<->relative time without guessing.
            f.write(json.dumps({"kind": "_run_origin", "t0_wall": self.t0_wall, "t0_mono": self.t0_mono}) + "\n")
            for ev in events:
                f.write(json.dumps(ev, default=str) + "\n")
        return len(events)
