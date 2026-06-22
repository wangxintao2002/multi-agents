# SWE-bench Sandbox Bottleneck Experiment (Phase A)

**Question.** When many LLM agents run concurrently against real Docker sandboxes,
what actually binds throughput — the sandbox/test-execution layer, host resources,
or the remote LLM API? This decides whether *client-side sandbox scheduling* is a
real contribution or a pseudo-need.

This host is huge (503 GiB RAM, 128 cores, 5.4 TB free), so simply "run 20 and look"
would only ever show the API as the bottleneck. We therefore (1) deliberately cap the
Docker slot pool `C` to create contention, and (2) attribute wall-clock time into
LLM-wait / sandbox-exec / blocked-on-slot / overhead so the conclusion is evidence,
not guess.

## Layout

| File | Role |
|------|------|
| `config.yaml` | All knobs: model, dataset, limits, C-sweep, lease mode, sampler |
| `events.py` | Thread-safe `EventBus` (dual perf_counter+wall timestamps) → `events.jsonl` |
| `resources.py` | `SlotPool` (instrumented semaphore, task/exec lease) + `Sampler` daemon |
| `instrumented.py` | `InstrumentedDockerEnvironment` + `InstrumentedLitellmModel` (subclass, no fork) |
| `dataset.py` | SWE-bench Lite instance selection (head / stratified / cached) + image naming |
| `run_a0_harness.py` | **A0**: gold-patch harness scaling sweep over `max_workers` (no LLM) |
| `run_experiment.py` | **A1/A2**: N agents concurrent under `SlotPool(C)`, C-sweep |
| `analyze.py` | Folds events → per-task attribution, idle-held, throughput, verdict |
| `results/<run_id>/` | Per-cell `events.jsonl`, trajectories, patches, summary, plots |

## The three experiments (A0 → A1 → A2)

- **A0 — harness-only scaling.** Gold-patch replay through the official SWE-bench
  harness across `max_workers ∈ {1,2,4,8,16,20}`. No agent, no LLM, no lease policy.
  Any scaling knee here is intrinsic to the sandbox+test workload. *Lower bound* on
  sandbox demand (runs the test suite once; an agent runs many commands).
- **A1 — task-lease profiling.** Real agents, slot held for the whole task (current
  scaffold semantics). Core metric: **idle-held = lease_held − active_exec** — slot
  time wasted during LLM think. C-sweep `{2,4,8,∞}`.
- **A2 — exec-lease comparison.** Two sub-modes:
  - `exec_lease`: slot held only during `docker run`/`exec`, released during LLM
    wait. Container stays alive — measures *exec-concurrency* limiting. Frees the
    scheduling slot, **NOT** host RAM.
  - `exec_lease_stop`: additionally `docker stop`s the container during LLM wait
    and `docker start`s it before the next exec. **Frees real RAM** (the honest
    suspend/wake) and pays the restart latency, recorded as `container_resume`
    time (~0.8s/resume observed). This is the mode that supports a *hard* resource
    reclaim claim; `exec_lease` alone only shows slot-concurrency effects.
  Throughput gain over A1 = the reclaimable value AgentOS suspend/wake captures,
  net of resume cost (which is measured, not hidden).

## Measurement-fidelity guards (from review)

- **A0 cache confound**: an unmeasured `--workers 1` prebuild pass warms all env +
  instance image layers before the sweep, so every measured cell starts from the
  same warm-cache state (a throughput rise reflects concurrency, not cache warm-up).
  `--shuffle-order` adds a seeded order-randomization as a second guard.
- **Container count is experiment-scoped**: the Sampler counts only containers
  matching the experiment's prefix (`sweb.eval.*` for A0, `minisweagent-*` for
  A1/A2) as `n_running_containers` and also reports the host-wide total
  (`n_running_all`), so unrelated workloads on the box don't silently inflate the
  concurrency signal.
- **Synchronous teardown**: cleanup uses `docker rm -f` (immediate) and releases the
  slot only after the container is gone, so running-container count never transiently
  exceeds C. (`docker stop` would block ~60s because PID 1 is `sleep`.)
- **Failed LLM calls still recorded**: `llm_query_end` is emitted with `ok=false` and
  the wall time even when retries are exhausted, so API-bottleneck time isn't
  under-counted exactly when it bit hardest.

## Pre-registered criteria (in `analyze.py` `verdict()`)

- idle-held fraction > 50% → reclaimable capacity exists (H1).
- throughput drop from C=∞ to C=2 > 15% → sandbox is a binding resource here.
- else dominant component = LLM-wait → API binds; sandbox-scheduling story is weak
  (an honest, reportable result that redirects weight to comms/token leverage).

## Running

```bash
VENV=../../.venv-swebench/bin/python
# A0 scaling sweep (cheap, no API cost) — prebuild pass runs automatically
$VENV run_a0_harness.py --workers 1 2 4 8 16 20 --run-id A0_main
$VENV analyze.py results/A0_main --a0
# A1 task-lease C-sweep
$VENV run_experiment.py --lease task_lease --run-id A1_main
$VENV analyze.py results/A1_main
# A2: exec_lease (slot only) and exec_lease_stop (real RAM reclaim) — if A1 idle-held high
$VENV run_experiment.py --lease exec_lease --run-id A2_slot
$VENV run_experiment.py --lease exec_lease_stop --run-id A2_stop
$VENV analyze.py results/A2_stop
```

Smoke (cached images, 1-2 tasks): add `--smoke 2`.

## Environment notes

- Model: `openrouter/deepseek/deepseek-v4-flash` via `OPENROUTER_API_KEY`.
- `MSWEA_COST_TRACKING=ignore_errors` is required (DeepSeek not in litellm cost map);
  cost reads 0, so `step_limit` + `wall_time_limit_seconds` are the real guards.
- Proxy: the host's `NO_PROXY` contains `[::1]` which breaks httpx; `run_experiment.py`
  unsets proxy vars in-process (OpenRouter is reachable directly).
- Images/containers live on root (5.4 TB); code/results on /home.
