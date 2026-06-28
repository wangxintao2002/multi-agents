# SWE-bench Sandbox Replay Harness Design

## Goal

Build a deterministic replay harness for the sandbox scheduling experiments.
The harness should remove live LLM variability from the core comparison while
preserving the real SWE-bench container images, `/testbed` filesystem, shell
commands, container lifecycle, cgroup sampling, and existing lease modes.

The primary question is:

> Given the same tool-call trace and the same synthetic LLM idle windows, does
> releasing idle sandboxes improve resource footprint or throughput under memory
> pressure after accounting for suspend/restore overhead?

This harness is not intended to measure solve rate or agent reasoning quality.
It is a scheduling and resource-control experiment.

## Non-Goals

- Do not call OpenRouter or any live LLM during replay.
- Do not evaluate patches or compare SWE-bench correctness.
- Do not implement selective suspend in the first version.
- Do not introduce a new analysis format if the existing `analyze.py` can consume
  the emitted events.
- Do not use truncated `exec_begin.cmd` from `events.jsonl` as the command source.

## Existing Context

The current experiment stack already has the right resource instrumentation:

- `run_experiment.py` starts concurrent mini-swe-agent tasks.
- `InstrumentedDockerEnvironment` implements `task_lease`, `exec_lease`, and
  `exec_lease_stop`.
- `SlotPool` records acquire, grant, release, and wait time.
- `Sampler` records running containers, cgroup memory, `memory.events`, host CPU,
  host memory, and container stats.
- `analyze.py` folds `events.jsonl` into per-task and per-cell metrics.
- Mini-swe-agent `.traj.json` files contain full assistant tool calls, including
  complete bash commands.

The replay harness should reuse the resource layer and the analysis pipeline.

## Chosen Approach

Use trace replay from existing `.traj.json` files.

Each replay task is derived from one prior live agent trajectory:

1. Extract the ordered assistant bash tool calls from `.traj.json`.
2. Group tool calls by LLM step.
3. Attach a deterministic LLM idle delay to each step.
4. Start the real SWE-bench container image for the instance.
5. For each step, sleep for the synthetic LLM delay, then execute the recorded
   bash commands in `/testbed`.
6. Emit the same event kinds expected by `analyze.py`.

This keeps the sandbox workload representative without allowing live model output
or API latency to decide the result.

## Trace Format

Write extracted traces to JSON so replay runs are reproducible and inspectable.

Example:

```json
{
  "schema_version": 1,
  "source": {
    "traj_path": "results/.../trajs/django__django-12308.traj.json",
    "events_path": "results/.../events.jsonl"
  },
  "instance": {
    "instance_id": "django__django-12308",
    "repo": "django/django",
    "image": "swebench/sweb.eval.x86_64.django_..."
  },
  "steps": [
    {
      "step_idx": 1,
      "recorded_llm_wait_s": 4.36,
      "actions": [
        {
          "tool_call_id": "call_...",
          "command": "grep -n \"display_for_field\" /testbed/django/contrib/admin/utils.py",
          "expected_returncode": 0
        }
      ]
    }
  ]
}
```

The full command must come from assistant `tool_calls[*].function.arguments` or
`extra.actions` in `.traj.json`. The existing `events.jsonl` command preview is
only a validation aid because it is truncated.

## Delay Modes

The first implementation should support three delay modes:

- `recorded`: use the original `llm_query_end.wall_s` for each step.
- `fixed`: use one constant delay such as 0, 3, 10, or 30 seconds.
- `scaled`: multiply recorded delays by a scalar, for example 0.5 or 2.0.

`fixed` is the most important mode for break-even analysis because it lets us
ask how long an idle window must be before suspend/restore becomes worthwhile.

## Replay Runner

Add a new script:

```text
experiments/swebench-bottleneck/run_replay.py
```

The runner should:

1. Load a replay config.
2. Load extracted trace JSON files.
3. Create one `EventBus`, `SlotPool`, and `Sampler` per cell.
4. Run all traces with a `ThreadPoolExecutor`.
5. For each trace, create `InstrumentedDockerEnvironment` with the selected
   lease mode and runtime.
6. Emit synthetic `llm_query_start` and `llm_query_end` events around `sleep()`.
7. Execute each recorded action through `env.execute(...)`.
8. Cleanup the container and emit `task_done`.
9. Write `events.jsonl` using the same cell layout as current runs.

The output directory should remain compatible with `analyze.py`:

```text
results/<run_id>/
  selected_instances.txt
  Cinf_task_lease/events.jsonl
  Cinf_exec_lease_stop/events.jsonl
```

## Extractor

Add a new script:

```text
experiments/swebench-bottleneck/extract_replay_trace.py
```

The extractor should:

1. Accept a trajectory directory or a run/cell directory.
2. Optionally accept the matching `events.jsonl`.
3. Emit one trace JSON per instance.
4. Preserve tool-call grouping by assistant message.
5. Record expected return codes from following tool messages when available.
6. Warn, but do not fail, if recorded LLM waits cannot be aligned exactly.

Alignment rule:

- If `events.jsonl` is available, match `llm_query_end` by task id and call index.
- If not available, set `recorded_llm_wait_s` to `null` and require `fixed` delay
  mode for replay.

## Config

Add a replay config file such as:

```yaml
trace:
  trace_dir: traces/podman_pilot_stop_cinf
  delay_mode: fixed
  fixed_delay_s: 10
  delay_scale: 1.0
  duplicate_factor: 1

run:
  docker_executable: podman
  lease_mode: task_lease
  slot_capacities: ["inf"]
  inf_capacity: 64
  offered_concurrency: 6
  pre_pull_images: false
  cgroup_parent: null

sampler:
  fast_interval: 1.0
  slow_interval: 5.0

limits:
  command_timeout_s: 120
```

`duplicate_factor` should duplicate traces with unique task ids only for stress
tests. The first validation should keep it at 1.

## First Experiment Matrix

Use the existing 6-task Podman pilot traces:

- `django__django-12308`
- `sympy__sympy-11400`
- `scikit-learn__scikit-learn-10297`
- `pytest-dev__pytest-9359`
- `pylint-dev__pylint-7228`
- `sphinx-doc__sphinx-8721`

Run:

```text
lease_mode = task_lease, exec_lease_stop
delay_mode = fixed
fixed_delay_s = 0, 3, 10, 30
offered_concurrency = 6
capacity = inf
runtime = podman
```

Then repeat the most informative delay under a hard cgroup memory cap, for
example the existing 80M or a slightly less brittle value selected from prior
measurements.

## Metrics

Primary metrics:

- makespan
- throughput
- flow p50 and p95
- live container seconds
- mean and max running containers
- peak cgroup memory
- `memory.events max`, `oom`, and `oom_kill`
- total `container_suspend` time
- total `container_resume` time
- suspend/resume p50 and p95
- slot wait initial and midtask

Derived metrics:

- adjusted makespan excluding synthetic LLM sleep
- suspend overhead per reclaimed live-container-second
- break-even idle delay for `exec_lease_stop`
- memory stability under cap, measured by fewer `memory.events` and no OOM kills

The replay result should be treated as positive only if `exec_lease_stop` reduces
resource pressure or OOM events enough to compensate for suspend/restore overhead.
If it only reduces running containers but increases makespan and OOM events, the
policy is not useful in that regime.

## Validation

Before running the full matrix:

1. Extract one trace and manually inspect that commands are complete.
2. Replay one instance with `fixed_delay_s=0` and `task_lease`.
3. Run `analyze.py` successfully on the output.
4. Compare replay command count against the source trajectory action count.
5. Confirm that replay emits no live `llm_attempt` events.

## Risks

- Recorded commands may depend on previous command output. This is acceptable
  because replay executes the same sequence in a fresh container for that same
  instance, but divergence can still happen if a command was nondeterministic.
- Parallel tool calls are grouped in one LLM step. Replaying them serially is
  simpler and deterministic, but may overestimate sandbox time compared with true
  parallel tool execution. The first version should record this limitation.
- `exec_lease_stop` may perturb workload state through stop/start cold paths. This
  is part of the mechanism being measured, not noise.
- Replay does not prove solve-rate impact. After selecting a scheduling policy,
  run a live agent-in-loop experiment to check that the policy does not damage
  task behavior.

## Acceptance Criteria

- A trace can be extracted from existing `.traj.json` files without calling an LLM.
- A replay run can execute the same commands in real SWE-bench containers.
- Output can be analyzed by the existing `analyze.py`.
- Fixed-delay sweeps produce stable comparisons across lease modes.
- The harness can answer whether `exec_lease_stop` has a break-even idle window
  under the selected memory cap.
