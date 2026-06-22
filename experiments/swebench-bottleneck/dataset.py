"""Instance selection for the experiment.

We load SWE-bench Lite and pick N instances. Three strategies:
  - head      : first N by instance_id (cheap smoke; biased repo distribution)
  - stratified: round-robin across repos (fixed seed) for a representative sample
  - cached    : prefer instances whose docker image is already present locally
                (fastest startup; lets a pilot skip multi-GB cold pulls)

Image names are derived with mini-swe-agent's own helper so they match exactly
what the Docker env will start.
"""

from __future__ import annotations

import random
import subprocess
from collections import defaultdict

from minisweagent.run.benchmarks.swebench import (
    DATASET_MAPPING,
    get_swebench_docker_image_name,
)


def load_instances(subset: str, split: str) -> list[dict]:
    from datasets import load_dataset

    path = DATASET_MAPPING.get(subset, subset)
    return list(load_dataset(path, split=split))


def _locally_present_images() -> set[str]:
    try:
        out = subprocess.run(
            ["docker", "images", "--format", "{{.Repository}}:{{.Tag}}"],
            capture_output=True, text=True, timeout=30,
        )
        return {ln.strip() for ln in out.stdout.splitlines() if ln.strip()}
    except Exception:
        return set()


def select(instances: list[dict], *, n: int, strategy: str, seed: int = 42) -> list[dict]:
    instances = sorted(instances, key=lambda x: x["instance_id"])
    if strategy == "head":
        return instances[:n]

    if strategy == "cached":
        present = _locally_present_images()
        def is_cached(inst):
            img = get_swebench_docker_image_name(inst)
            # docker images may list without the docker.io/ prefix
            return img in present or img.replace("docker.io/", "") in present
        cached = [i for i in instances if is_cached(i)]
        chosen = cached[:n]
        if len(chosen) < n:  # backfill with uncached to reach n
            rest = [i for i in instances if i not in chosen]
            chosen += rest[: n - len(chosen)]
        return chosen

    # stratified: round-robin across repos for balance, deterministic with seed.
    by_repo: dict[str, list[dict]] = defaultdict(list)
    for inst in instances:
        by_repo[inst["repo"]].append(inst)
    rng = random.Random(seed)
    repos = sorted(by_repo)
    for r in repos:
        rng.shuffle(by_repo[r])
    chosen: list[dict] = []
    i = 0
    while len(chosen) < n and any(by_repo[r] for r in repos):
        r = repos[i % len(repos)]
        if by_repo[r]:
            chosen.append(by_repo[r].pop())
        i += 1
    return chosen[:n]


def exclude_instance_ids(instances: list[dict], exclude_ids: list[str] | None) -> list[dict]:
    if not exclude_ids:
        return instances
    excluded = set(exclude_ids)
    return [inst for inst in instances if inst["instance_id"] not in excluded]


def select_instance_ids(instances: list[dict], instance_ids: list[str]) -> list[dict]:
    by_id = {inst["instance_id"]: inst for inst in instances}
    missing = [iid for iid in instance_ids if iid not in by_id]
    if missing:
        raise ValueError(f"unknown SWE-bench instance ids: {missing}")
    return [by_id[iid] for iid in instance_ids]


def image_for(instance: dict) -> str:
    return get_swebench_docker_image_name(instance)
