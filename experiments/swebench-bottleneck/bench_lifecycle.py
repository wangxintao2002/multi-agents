"""Microbenchmark sandbox suspend/restore lifecycle cost on real SWE-bench images.

This intentionally avoids the LLM/agent loop. It measures the mechanism cost
that sits under exec_lease_stop: suspend latency, restore latency, the first
post-restore docker exec, and how much container cgroup memory disappears while
the sandbox is suspended.

Default run:
  python bench_lifecycle.py --repetitions 20

Smoke:
  python bench_lifecycle.py --repos django --first-exec-kinds cheap --repetitions 1
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import math
import os
import platform
import re
import shutil
import subprocess
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

HERE = Path(__file__).resolve().parent

DEFAULT_REPOS = ["django", "astropy", "matplotlib", "scikit-learn", "sympy"]
REPO_PATTERNS = {
    "django": ["django_1776_django", "django"],
    "astropy": ["astropy_1776_astropy", "astropy"],
    "matplotlib": ["matplotlib_1776_matplotlib", "matplotlib"],
    "scikit-learn": ["scikit-learn_1776_scikit-learn", "scikit-learn"],
    "sympy": ["sympy_1776_sympy", "sympy"],
}
FIRST_EXEC_COMMANDS = {
    "cheap": "true",
    "python": "python - <<'PY'\nprint('ok')\nPY",
    "repo": (
        "python -m pytest --version || "
        "pytest --version || "
        "python - <<'PY'\n"
        "import os\n"
        "print('pytest unavailable; testbed_entries=%d' % len(os.listdir('/testbed')))\n"
        "PY"
    ),
}
RAW_FIELDS = [
    "run_id", "trial_id", "repo", "image", "mechanism", "first_exec_kind",
    "resident_target_mb", "repetition", "lifecycle_concurrency",
    "container_name", "container_id",
    "container_start_s", "warmup_s", "suspend_s", "restore_s",
    "first_exec_s", "restore_plus_first_exec_s", "resident_ready_s",
    "rss_before_mb", "rss_after_suspend_mb", "rss_after_restore_mb",
    "memory_freed_mb", "resident_observed_mb", "memory_source", "snapshot_size_mb",
    "resident_pid", "process_preserved",
    "state_preserved", "success", "exit_code", "stdout_excerpt",
    "stderr_excerpt", "error",
]
SUMMARY_METRICS = [
    "container_start_s", "warmup_s", "suspend_s", "restore_s",
    "first_exec_s", "restore_plus_first_exec_s", "resident_ready_s",
    "rss_before_mb", "rss_after_suspend_mb", "rss_after_restore_mb",
    "memory_freed_mb", "resident_observed_mb", "snapshot_size_mb",
]


@dataclass(frozen=True)
class ImageTarget:
    repo: str
    image: str


@dataclass(frozen=True)
class TimedResult:
    duration_s: float
    returncode: int
    stdout: str
    stderr: str


@dataclass(frozen=True)
class TrialSpec:
    target: ImageTarget
    mechanism: str
    first_exec_kind: str
    resident_target_mb: int
    repetition: int


def _now_utc() -> str:
    return dt.datetime.now(dt.UTC).strftime("%Y%m%d_%H%M%S")


def _safe_id(value: str) -> str:
    return re.sub(r"[^a-zA-Z0-9_.-]+", "-", value).strip("-")[:48]


def _short(text: str, limit: int = 500) -> str:
    text = (text or "").replace("\r", "").strip()
    if len(text) <= limit:
        return text
    return text[: limit - 3] + "..."


def timed_run(cmd: list[str], *, timeout: int) -> TimedResult:
    t0 = time.perf_counter()
    try:
        r = subprocess.run(
            cmd,
            text=True,
            capture_output=True,
            timeout=timeout,
            encoding="utf-8",
            errors="replace",
        )
        return TimedResult(time.perf_counter() - t0, r.returncode, r.stdout, r.stderr)
    except subprocess.TimeoutExpired as e:
        stdout = e.stdout.decode("utf-8", "replace") if isinstance(e.stdout, bytes) else (e.stdout or "")
        stderr = e.stderr.decode("utf-8", "replace") if isinstance(e.stderr, bytes) else (e.stderr or "")
        return TimedResult(time.perf_counter() - t0, 124, stdout, stderr or f"timeout after {timeout}s")


def docker_exec(docker: str, container_id: str, command: str, *, timeout: int) -> TimedResult:
    return timed_run(
        [docker, "exec", container_id, "bash", "-lc", f"mkdir -p /testbed && cd /testbed && {command}"],
        timeout=timeout,
    )


def start_resident_holder(
    docker: str,
    container_id: str,
    target_mb: int,
    *,
    baseline_mb: float | None,
    wait_timeout_s: float,
    ready_fraction: float,
    command_timeout: int,
) -> tuple[float, float | None, str, str]:
    """Start a background process that touches resident memory inside the sandbox.

    Returns (ready_s, observed_mb, pid, error). observed_mb is the container's
    cgroup memory.current when the target threshold is met, or the last observed
    value on timeout.
    """
    if target_mb <= 0:
        observed_mb, _source = container_memory_mb(docker, container_id)
        return 0.0, observed_mb, "", ""

    script = r"""
cat > /tmp/lifecycle_hold_mem.py <<'PY'
import sys
import time

mb = int(sys.argv[1])
buf = bytearray(mb * 1024 * 1024)
for i in range(0, len(buf), 4096):
    buf[i] = 1
print("ready", mb, flush=True)
time.sleep(7200)
PY
nohup python /tmp/lifecycle_hold_mem.py "$1" >/tmp/lifecycle_hold_mem.log 2>&1 &
echo $! > /tmp/lifecycle_hold_mem.pid
cat /tmp/lifecycle_hold_mem.pid
"""
    launch = docker_exec(
        docker,
        container_id,
        f"bash -s -- {target_mb} <<'BASH'\n{script}\nBASH",
        timeout=command_timeout,
    )
    if launch.returncode != 0:
        return 0.0, None, "", f"resident holder launch failed rc={launch.returncode}: {_short(launch.stderr or launch.stdout)}"
    pid = launch.stdout.strip().splitlines()[-1] if launch.stdout.strip() else ""
    baseline = baseline_mb if baseline_mb is not None else 0.0
    threshold = baseline + (target_mb * ready_fraction)

    t0 = time.perf_counter()
    last_mb = None
    while time.perf_counter() - t0 < wait_timeout_s:
        current_mb, _source = container_memory_mb(docker, container_id)
        if current_mb is not None:
            last_mb = current_mb
            if current_mb >= threshold:
                return time.perf_counter() - t0, current_mb, pid, ""
        alive = docker_exec(
            docker,
            container_id,
            "test -f /tmp/lifecycle_hold_mem.pid && kill -0 $(cat /tmp/lifecycle_hold_mem.pid)",
            timeout=min(command_timeout, 10),
        )
        if alive.returncode != 0:
            log = docker_exec(docker, container_id, "cat /tmp/lifecycle_hold_mem.log 2>/dev/null || true",
                              timeout=min(command_timeout, 10))
            return time.perf_counter() - t0, last_mb, pid, f"resident holder exited early: {_short(log.stdout)}"
        time.sleep(0.25)
    return time.perf_counter() - t0, last_mb, pid, (
        f"resident holder did not reach {threshold:.1f} MB within {wait_timeout_s:.1f}s"
    )


def holder_process_alive(docker: str, container_id: str, *, timeout: int) -> bool | None:
    check = docker_exec(
        docker,
        container_id,
        "test -f /tmp/lifecycle_hold_mem.pid && kill -0 $(cat /tmp/lifecycle_hold_mem.pid)",
        timeout=timeout,
    )
    if check.returncode == 0:
        return True
    missing = docker_exec(
        docker,
        container_id,
        "test ! -f /tmp/lifecycle_hold_mem.pid",
        timeout=timeout,
    )
    if missing.returncode == 0:
        return None
    return False


def list_local_images(docker: str) -> list[str]:
    r = timed_run([docker, "image", "ls", "--format", "{{.Repository}}:{{.Tag}}"], timeout=60)
    if r.returncode != 0:
        raise RuntimeError(f"failed to list docker images: {_short(r.stderr or r.stdout)}")
    return sorted({
        line.strip()
        for line in r.stdout.splitlines()
        if line.strip() and "<none>" not in line
    })


def _image_rank(image: str) -> tuple[int, str]:
    score = 0
    if image.startswith("swebench/"):
        score -= 100
    if image.startswith("docker.io/swebench/"):
        score -= 90
    if image.startswith("localhost:"):
        score += 20
    if image.startswith("docker."):
        score += 30
    if not image.endswith(":latest"):
        score += 5
    return score, image


def infer_repo_from_image(image: str) -> str:
    lower = image.lower()
    for repo, patterns in REPO_PATTERNS.items():
        if any(pattern in lower for pattern in patterns):
            return repo
    return "custom"


def parse_explicit_images(values: Iterable[str]) -> list[ImageTarget]:
    targets = []
    for value in values:
        if "=" in value:
            repo, image = value.split("=", 1)
            targets.append(ImageTarget(repo=repo.strip(), image=image.strip()))
        else:
            targets.append(ImageTarget(repo=infer_repo_from_image(value), image=value.strip()))
    return targets


def autodiscover_targets(docker: str, repos: list[str]) -> list[ImageTarget]:
    images = list_local_images(docker)
    targets = []
    for repo in repos:
        patterns = REPO_PATTERNS.get(repo, [repo])
        matches = [
            image for image in images
            if any(pattern.lower() in image.lower() for pattern in patterns)
        ]
        if not matches:
            continue
        targets.append(ImageTarget(repo=repo, image=sorted(matches, key=_image_rank)[0]))
    return targets


def docker_inspect(docker: str, container_id: str) -> dict | None:
    r = timed_run([docker, "inspect", container_id], timeout=30)
    if r.returncode != 0:
        return None
    try:
        data = json.loads(r.stdout)
    except json.JSONDecodeError:
        return None
    return data[0] if data else None


def container_pid(docker: str, container_id: str) -> int:
    info = docker_inspect(docker, container_id)
    if not info:
        return 0
    try:
        return int(info.get("State", {}).get("Pid") or 0)
    except (TypeError, ValueError):
        return 0


def cgroup_memory_mb_for_pid(pid: int) -> tuple[float | None, str]:
    if pid <= 0:
        return 0.0, "stopped"
    cgroup_file = Path(f"/proc/{pid}/cgroup")
    if not cgroup_file.exists():
        return None, "missing_proc_cgroup"
    rel_paths = []
    for line in cgroup_file.read_text(errors="replace").splitlines():
        parts = line.split(":", 2)
        if len(parts) != 3:
            continue
        controllers = parts[1].split(",") if parts[1] else []
        rel = parts[2].lstrip("/")
        if parts[0] == "0" or "memory" in controllers:
            rel_paths.append(rel)

    for rel in rel_paths:
        path = Path("/sys/fs/cgroup") / rel / "memory.current"
        if path.exists():
            return int(path.read_text().strip()) / (1024 * 1024), f"cgroup_v2:{path}"
        path = Path("/sys/fs/cgroup/memory") / rel / "memory.usage_in_bytes"
        if path.exists():
            return int(path.read_text().strip()) / (1024 * 1024), f"cgroup_v1:{path}"
    return None, "missing_memory_current"


def container_memory_mb(docker: str, container_id: str) -> tuple[float | None, str]:
    return cgroup_memory_mb_for_pid(container_pid(docker, container_id))


def dir_size_mb(path: Path) -> float | None:
    if not path.exists():
        return None
    total = 0
    try:
        for p in path.rglob("*"):
            if p.is_file():
                total += p.stat().st_size
    except OSError:
        return None
    return total / (1024 * 1024)


def docker_checkpoint_dir(docker: str, container_id: str, checkpoint_name: str) -> Path | None:
    info = docker_inspect(docker, container_id)
    long_id = info.get("Id") if info else None
    if not long_id:
        return None
    path = Path("/var/lib/docker/containers") / long_id / "checkpoints" / checkpoint_name
    return path if path.exists() else None


def suspend_container(docker: str, mechanism: str, container_id: str, checkpoint_name: str) -> TimedResult:
    if mechanism == "docker-stop-start":
        return timed_run([docker, "stop", "--time", "0", container_id], timeout=60)
    if mechanism == "docker-checkpoint":
        return timed_run([docker, "checkpoint", "create", container_id, checkpoint_name], timeout=300)
    raise ValueError(f"unknown mechanism: {mechanism}")


def restore_container(docker: str, mechanism: str, container_id: str, checkpoint_name: str) -> TimedResult:
    if mechanism == "docker-stop-start":
        return timed_run([docker, "start", container_id], timeout=120)
    if mechanism == "docker-checkpoint":
        return timed_run([docker, "start", "--checkpoint", checkpoint_name, container_id], timeout=300)
    raise ValueError(f"unknown mechanism: {mechanism}")


def cleanup_container(docker: str, container_id: str | None, container_name: str) -> None:
    target = container_id or container_name
    if target:
        timed_run([docker, "rm", "-f", target], timeout=60)


def run_trial(
    *,
    run_id: str,
    docker: str,
    target: ImageTarget,
    mechanism: str,
    first_exec_kind: str,
    resident_target_mb: int,
    repetition: int,
    lifecycle_concurrency: int,
    command_timeout: int,
    resident_wait_timeout: float,
    resident_ready_fraction: float,
) -> dict:
    trial_id = uuid.uuid4().hex[:12]
    container_name = f"lifecycle-{_safe_id(run_id)}-{_safe_id(target.repo)}-{trial_id}"
    checkpoint_name = f"cp-{trial_id}"
    marker = f"{run_id}:{trial_id}:{target.repo}:{first_exec_kind}:{resident_target_mb}:{repetition}"
    row = {field: "" for field in RAW_FIELDS}
    row.update({
        "run_id": run_id,
        "trial_id": trial_id,
        "repo": target.repo,
        "image": target.image,
        "mechanism": mechanism,
        "first_exec_kind": first_exec_kind,
        "resident_target_mb": resident_target_mb,
        "repetition": repetition,
        "lifecycle_concurrency": lifecycle_concurrency,
        "container_name": container_name,
    })
    container_id = None
    try:
        start = timed_run(
            [docker, "run", "-d", "--name", container_name, target.image, "sleep", "2h"],
            timeout=300,
        )
        row["container_start_s"] = f"{start.duration_s:.6f}"
        row["stdout_excerpt"] = _short(start.stdout)
        row["stderr_excerpt"] = _short(start.stderr)
        if start.returncode != 0:
            row["error"] = f"docker run failed rc={start.returncode}"
            return row
        container_id = start.stdout.strip()
        row["container_id"] = container_id[:12]

        warmup_cmd = (
            f"printf '%s\\n' {json.dumps(marker)} > /testbed/.lifecycle_marker && "
            "python -V >/dev/null 2>&1 && pwd >/dev/null && ls /testbed >/dev/null"
        )
        warmup = docker_exec(docker, container_id, warmup_cmd, timeout=command_timeout)
        row["warmup_s"] = f"{warmup.duration_s:.6f}"
        if warmup.returncode != 0:
            row["error"] = f"warmup failed rc={warmup.returncode}: {_short(warmup.stderr or warmup.stdout)}"
            return row

        baseline_mb, memory_source = container_memory_mb(docker, container_id)
        ready_s, observed_mb, resident_pid, resident_err = start_resident_holder(
            docker,
            container_id,
            resident_target_mb,
            baseline_mb=baseline_mb,
            wait_timeout_s=resident_wait_timeout,
            ready_fraction=resident_ready_fraction,
            command_timeout=command_timeout,
        )
        row["resident_ready_s"] = f"{ready_s:.6f}"
        row["resident_pid"] = resident_pid
        row["resident_observed_mb"] = "" if observed_mb is None else f"{observed_mb:.6f}"
        if resident_err:
            row["error"] = resident_err
            return row

        before_mb, memory_source = container_memory_mb(docker, container_id)
        row["rss_before_mb"] = "" if before_mb is None else f"{before_mb:.6f}"
        row["memory_source"] = memory_source

        suspend = suspend_container(docker, mechanism, container_id, checkpoint_name)
        row["suspend_s"] = f"{suspend.duration_s:.6f}"
        if suspend.returncode != 0:
            row["error"] = f"suspend failed rc={suspend.returncode}: {_short(suspend.stderr or suspend.stdout)}"
            return row

        after_suspend_mb, after_suspend_source = container_memory_mb(docker, container_id)
        row["rss_after_suspend_mb"] = "" if after_suspend_mb is None else f"{after_suspend_mb:.6f}"
        if row["memory_source"] in ("", "stopped") and after_suspend_source:
            row["memory_source"] = after_suspend_source
        if before_mb is not None and after_suspend_mb is not None:
            row["memory_freed_mb"] = f"{max(0.0, before_mb - after_suspend_mb):.6f}"

        if mechanism == "docker-checkpoint":
            cp_dir = docker_checkpoint_dir(docker, container_id, checkpoint_name)
            cp_size = dir_size_mb(cp_dir) if cp_dir else None
            row["snapshot_size_mb"] = "" if cp_size is None else f"{cp_size:.6f}"

        restore = restore_container(docker, mechanism, container_id, checkpoint_name)
        row["restore_s"] = f"{restore.duration_s:.6f}"
        if restore.returncode != 0:
            row["error"] = f"restore failed rc={restore.returncode}: {_short(restore.stderr or restore.stdout)}"
            return row

        after_restore_mb, after_restore_source = container_memory_mb(docker, container_id)
        row["rss_after_restore_mb"] = "" if after_restore_mb is None else f"{after_restore_mb:.6f}"
        if row["memory_source"] in ("", "stopped") and after_restore_source:
            row["memory_source"] = after_restore_source

        preserved = holder_process_alive(docker, container_id, timeout=min(command_timeout, 10))
        row["process_preserved"] = "" if preserved is None else str(preserved).lower()

        first_cmd = FIRST_EXEC_COMMANDS[first_exec_kind]
        first = docker_exec(docker, container_id, first_cmd, timeout=command_timeout)
        row["first_exec_s"] = f"{first.duration_s:.6f}"
        row["restore_plus_first_exec_s"] = f"{restore.duration_s + first.duration_s:.6f}"
        row["exit_code"] = first.returncode
        row["stdout_excerpt"] = _short(first.stdout)
        row["stderr_excerpt"] = _short(first.stderr)

        verify = docker_exec(
            docker,
            container_id,
            f"test -f /testbed/.lifecycle_marker && grep -Fx {json.dumps(marker)} /testbed/.lifecycle_marker",
            timeout=command_timeout,
        )
        state_preserved = verify.returncode == 0
        row["state_preserved"] = str(state_preserved).lower()
        row["success"] = str(first.returncode == 0 and state_preserved).lower()
        return row
    except Exception as e:
        row["error"] = f"{type(e).__name__}: {_short(str(e))}"
        return row
    finally:
        cleanup_container(docker, container_id, container_name)


def _to_float(value: str | int | float | None) -> float | None:
    if value in ("", None):
        return None
    try:
        x = float(value)
    except (TypeError, ValueError):
        return None
    return x if math.isfinite(x) else None


def pctile(values: list[float], p: float) -> float | None:
    if not values:
        return None
    xs = sorted(values)
    k = (len(xs) - 1) * p
    lo = int(k)
    hi = min(lo + 1, len(xs) - 1)
    return xs[lo] + (xs[hi] - xs[lo]) * (k - lo)


def summarize(rows: list[dict]) -> list[dict]:
    groups: dict[tuple[str, str, str, str, str, str], list[dict]] = {}
    for row in rows:
        resident_target_mb = str(row.get("resident_target_mb", "0"))
        groups.setdefault(
            ("per_image", row["mechanism"], row["repo"], row["image"], row["first_exec_kind"], resident_target_mb),
            [],
        ).append(row)
        groups.setdefault(
            ("overall", row["mechanism"], "ALL", "ALL", row["first_exec_kind"], resident_target_mb),
            [],
        ).append(row)
        groups.setdefault(
            ("overall", row["mechanism"], "ALL", "ALL", "ALL", resident_target_mb),
            [],
        ).append(row)
        groups.setdefault(
            ("overall", row["mechanism"], "ALL", "ALL", row["first_exec_kind"], "ALL"),
            [],
        ).append(row)
        groups.setdefault(
            ("overall", row["mechanism"], "ALL", "ALL", "ALL", "ALL"),
            [],
        ).append(row)

    summary_rows = []
    for (scope, mechanism, repo, image, first_exec_kind, resident_target_mb), group_rows in sorted(groups.items()):
        out = {
            "scope": scope,
            "mechanism": mechanism,
            "repo": repo,
            "image": image,
            "first_exec_kind": first_exec_kind,
            "resident_target_mb": resident_target_mb,
            "n_trials": len(group_rows),
            "n_success": sum(1 for r in group_rows if r.get("success") == "true"),
            "n_process_preserved": sum(1 for r in group_rows if r.get("process_preserved") == "true"),
        }
        out["success_rate"] = f"{out['n_success'] / out['n_trials']:.6f}" if out["n_trials"] else ""
        for metric in SUMMARY_METRICS:
            values = [v for v in (_to_float(r.get(metric)) for r in group_rows) if v is not None]
            for label, p in (("p50", 0.50), ("p90", 0.90), ("p95", 0.95)):
                val = pctile(values, p)
                out[f"{metric}_{label}"] = "" if val is None else f"{val:.6f}"
            out[f"{metric}_max"] = "" if not values else f"{max(values):.6f}"
        summary_rows.append(out)
    return summary_rows


def write_csv(path: Path, rows: list[dict], fieldnames: list[str] | None = None) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if fieldnames is None:
        names = set()
        for row in rows:
            names.update(row)
        fieldnames = sorted(names)
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def command_availability(docker: str) -> dict:
    docker_info = timed_run([docker, "info", "--format", "{{json .}}"], timeout=30)
    info = {}
    if docker_info.returncode == 0:
        try:
            raw = json.loads(docker_info.stdout)
            info = {
                "server_version": raw.get("ServerVersion"),
                "experimental_build": raw.get("ExperimentalBuild"),
                "cgroup_driver": raw.get("CgroupDriver"),
                "cgroup_version": raw.get("CgroupVersion"),
                "driver": raw.get("Driver"),
                "n_cpu": raw.get("NCPU"),
                "mem_total_bytes": raw.get("MemTotal"),
            }
        except json.JSONDecodeError:
            info = {"error": _short(docker_info.stderr or docker_info.stdout)}
    return {
        "docker": shutil.which(docker) or docker,
        "podman": shutil.which("podman"),
        "criu": shutil.which("criu"),
        "runc": shutil.which("runc"),
        "docker_checkpoint_cli": timed_run([docker, "checkpoint", "--help"], timeout=30).returncode == 0,
        "docker_info": info,
    }


def parse_resident_mbs(values: Iterable[str]) -> list[int]:
    parsed = []
    for value in values:
        for part in str(value).split(","):
            part = part.strip()
            if not part:
                continue
            mb = int(part)
            if mb < 0:
                raise argparse.ArgumentTypeError("resident memory values must be non-negative")
            parsed.append(mb)
    return parsed or [0]


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser()
    ap.add_argument("--run-id", default=f"lifecycle_{_now_utc()}")
    ap.add_argument("--results-dir", default=str(HERE / "results"))
    ap.add_argument("--docker-executable", default=os.getenv("MSWEA_DOCKER_EXECUTABLE", "docker"))
    ap.add_argument("--repos", nargs="*", default=DEFAULT_REPOS)
    ap.add_argument("--image", action="append", default=[],
                    help="Explicit image target. Use repo=image or just image. May repeat.")
    ap.add_argument("--mechanisms", nargs="*", default=["docker-stop-start"],
                    choices=["docker-stop-start", "docker-checkpoint"])
    ap.add_argument("--first-exec-kinds", nargs="*", default=["cheap", "python", "repo"],
                    choices=sorted(FIRST_EXEC_COMMANDS))
    ap.add_argument("--resident-mb", nargs="+", default=["0"],
                    help="Resident memory targets in MB, e.g. --resident-mb 0 64 256 or 0,64,256.")
    ap.add_argument("--resident-wait-timeout", type=float, default=60.0,
                    help="Seconds to wait for the in-container memory holder to reach target.")
    ap.add_argument("--resident-ready-fraction", type=float, default=0.80,
                    help="Treat resident memory as ready when baseline + target*fraction is observed.")
    ap.add_argument("--repetitions", type=int, default=20)
    ap.add_argument("--lifecycle-concurrency", type=int, default=1,
                    help="Number of independent lifecycle trials to run concurrently.")
    ap.add_argument("--command-timeout", type=int, default=120)
    ap.add_argument("--strict-repos", action="store_true",
                    help="Fail if any requested repo has no local image.")
    args = ap.parse_args()
    if args.lifecycle_concurrency < 1:
        raise SystemExit("--lifecycle-concurrency must be >= 1")
    return args


def build_trial_specs(
    *,
    targets: list[ImageTarget],
    mechanisms: list[str],
    first_exec_kinds: list[str],
    resident_mbs: list[int],
    repetitions: int,
) -> list[TrialSpec]:
    specs = []
    for target in targets:
        for mechanism in mechanisms:
            for first_exec_kind in first_exec_kinds:
                for resident_target_mb in resident_mbs:
                    for repetition in range(1, repetitions + 1):
                        specs.append(TrialSpec(
                            target=target,
                            mechanism=mechanism,
                            first_exec_kind=first_exec_kind,
                            resident_target_mb=resident_target_mb,
                            repetition=repetition,
                        ))
    return specs


def run_spec(args: argparse.Namespace, spec: TrialSpec) -> dict:
    return run_trial(
        run_id=args.run_id,
        docker=args.docker_executable,
        target=spec.target,
        mechanism=spec.mechanism,
        first_exec_kind=spec.first_exec_kind,
        resident_target_mb=spec.resident_target_mb,
        repetition=spec.repetition,
        lifecycle_concurrency=args.lifecycle_concurrency,
        command_timeout=args.command_timeout,
        resident_wait_timeout=args.resident_wait_timeout,
        resident_ready_fraction=args.resident_ready_fraction,
    )


def main() -> None:
    args = parse_args()
    run_dir = Path(args.results_dir) / args.run_id
    run_dir.mkdir(parents=True, exist_ok=True)
    resident_mbs = parse_resident_mbs(args.resident_mb)

    targets = parse_explicit_images(args.image)
    if not targets:
        targets = autodiscover_targets(args.docker_executable, args.repos)
        found = {t.repo for t in targets}
        missing = [repo for repo in args.repos if repo not in found]
        if missing and args.strict_repos:
            raise SystemExit(f"missing local SWE-bench images for repos: {missing}")
        if missing:
            print(f"warning: skipping repos with no local image: {missing}")
    if not targets:
        raise SystemExit("no image targets found; use --image repo=image or pull SWE-bench images first")

    specs = build_trial_specs(
        targets=targets,
        mechanisms=args.mechanisms,
        first_exec_kinds=args.first_exec_kinds,
        resident_mbs=resident_mbs,
        repetitions=args.repetitions,
    )

    metadata = {
        "run_id": args.run_id,
        "started_at_utc": dt.datetime.now(dt.UTC).isoformat(),
        "host": platform.uname()._asdict(),
        "cwd": str(Path.cwd()),
        "argv": os.sys.argv,
        "config": {
            "repos": args.repos,
            "mechanisms": args.mechanisms,
            "first_exec_kinds": args.first_exec_kinds,
            "resident_mbs": resident_mbs,
            "resident_wait_timeout": args.resident_wait_timeout,
            "resident_ready_fraction": args.resident_ready_fraction,
            "repetitions": args.repetitions,
            "lifecycle_concurrency": args.lifecycle_concurrency,
            "command_timeout": args.command_timeout,
        },
        "availability": command_availability(args.docker_executable),
        "targets": [target.__dict__ for target in targets],
    }
    (run_dir / "metadata.json").write_text(json.dumps(metadata, indent=2, sort_keys=True))
    write_csv(run_dir / "selected_images.csv", [target.__dict__ for target in targets],
              fieldnames=["repo", "image"])

    rows = []
    total = len(specs)
    workers = min(args.lifecycle_concurrency, total) if total else 1
    print(f"Running {total} lifecycle trials with concurrency={workers}", flush=True)
    with ThreadPoolExecutor(max_workers=workers) as pool:
        future_to_spec = {
            pool.submit(run_spec, args, spec): (i, spec)
            for i, spec in enumerate(specs, start=1)
        }
        for completed, future in enumerate(as_completed(future_to_spec), start=1):
            original_index, spec = future_to_spec[future]
            try:
                row = future.result()
            except Exception as e:
                row = {field: "" for field in RAW_FIELDS}
                row.update({
                    "run_id": args.run_id,
                    "repo": spec.target.repo,
                    "image": spec.target.image,
                    "mechanism": spec.mechanism,
                    "first_exec_kind": spec.first_exec_kind,
                    "resident_target_mb": spec.resident_target_mb,
                    "repetition": spec.repetition,
                    "lifecycle_concurrency": args.lifecycle_concurrency,
                    "error": f"{type(e).__name__}: {_short(str(e))}",
                })
            rows.append(row)
            status = "ok" if row.get("success") == "true" else "fail"
            print(
                f"[done {completed}/{total}; spec {original_index}] {status} "
                f"{spec.target.repo} {spec.mechanism} {spec.first_exec_kind} "
                f"resident={spec.resident_target_mb}MB rep={spec.repetition}",
                flush=True,
            )
            write_csv(run_dir / "raw.csv", rows, RAW_FIELDS)

    summary_rows = summarize(rows)
    write_csv(run_dir / "summary.csv", summary_rows)

    print(f"\nDONE. Results in {run_dir}")
    for row in summary_rows:
        if row["scope"] == "overall" and row["repo"] == "ALL":
            resident_label = row["resident_target_mb"]
            resident_text = "resident=ALL" if resident_label == "ALL" else f"resident={resident_label}MB"
            print(
                f"{row['mechanism']} {row['first_exec_kind']} {resident_text}: "
                f"n={row['n_trials']} success={row['success_rate']} "
                f"restore+exec p50={row.get('restore_plus_first_exec_s_p50', '')} "
                f"p95={row.get('restore_plus_first_exec_s_p95', '')} "
                f"freed p50={row.get('memory_freed_mb_p50', '')} MB"
            )


if __name__ == "__main__":
    main()
