# Sandbox Checkpoint/Restore & Lightweight Suspend/Resume — Landscape

**Why this doc exists.** Our `exec_lease_stop` arm (idle-release via `docker stop --time 0` +
`docker start` per tool call) eliminated idle RAM holding (peak container RAM **−52%**, mean
running containers **−44%**, idle-held 71.6% → 0%, `solved` unchanged) **but inflated per-task
wall-clock latency by +138%** (flow p50 142s → 339s) because `docker start` (container re-init +
entrypoint re-exec) sits on the critical path of every step. The naive mechanism is too heavy.
This doc surveys how to make suspend/resume cheap, mapped to our constraints.

Researched 2026-06 via Claude Code `deep-research` (26 sources, 128 claims, 25 adversarially
verified 3-vote, 0 refuted). Source tiers flagged throughout: **[peer-reviewed]**, **[primary
docs]**, **[vendor/maintainer-claimed]**, **[preprint]**.

## Our constraints (the rubric every option is judged against)

1. **Must free REAL host RAM** during the idle window — not just freeze CPU. (Plain cgroup-freezer
   / `docker pause` does NOT qualify.)
2. **Resume latency** target: sub-second to low-single-digit seconds.
3. **Must preserve FILESYSTEM state** across suspend/resume.
4. **Process-state preservation is OPTIONAL.** Our current SWE-bench workload is *stateless-exec*
   (each tool call is an independent `bash -lc`, nothing long-lived between calls), so we do **not**
   need process checkpointing for it. Future general agents (running dev server, browser) would.
5. Scale to **hundreds of concurrent sandboxes** on one Linux node.
6. **Open-source / self-hostable**; Linux host we control; we only call a remote LLM API (we do NOT
   control the inference engine).

---

## The two conceptual traps (read before trusting any "ms" number)

**Trap 1 — "free RAM" and "fast resume" are in tension.** Memory is only freed when it is actually
*evicted* (dumped to a file and the process killed, streamed off-node, or swapped to disk). The
fastest restores (template `fork()`, Firecracker's default `MAP_PRIVATE`-from-file) are fast
*precisely because memory stays resident / CoW-shared* — i.e. **not freed**. The one lever that
breaks the tradeoff is **working-set prefetch**: evict everything, then on resume prefetch only the
small recorded working set instead of faulting page-by-page. REAP shows working sets are tiny
(8–99 MB, **24 MB avg, ~9% of footprint**), eliminating up to **97%** of page faults and cutting
cold-start **3.7×** (hello-world 232 ms → 60 ms). **[peer-reviewed: REAP, ASPLOS'21]**

**Trap 2 — two different operations get the same "ms" label.**
- **(A) Restore from a shared/template snapshot** (cold-start acceleration): 5–30 ms, but it
  restores a *generic pre-booted* image, keeps memory resident/CoW-shared (doesn't free per-agent
  RAM), and carries *no per-agent state*.
- **(B) Resume MY specific suspended sandbox** with its accumulated state — the operation we
  actually need during LLM-wait. This must read back that sandbox's unique memory: E2B documents
  **~1 s**; CRIU lazy-restore is "fast-to-start, page-in deferred". This is the operation that
  genuinely frees RAM and whose cost we care about.

Most headline ms-numbers are (A). Do not use them to predict (B). DeltaBox's contribution is
essentially making (B) fast (template fork for the unchanged bulk + CRIU-lazy for the delta).

---

## Table A — Techniques to make checkpoint/restore cheap

| Technique | Frees real RAM? | Resume latency | Preserves process state? | Maturity / source |
|---|---|---|---|---|
| cgroup freezer / `docker pause` | ❌ freezes CPU only, pages stay resident | instant | yes (frozen) | mature, but **fails requirement #1** — MetalBear built then *deprecated* this [blog] |
| zram / zswap compress idle pages | ⚠️ partial — zram alone keeps compressed pages in RAM; needs disk-backed swap to truly free | fast decompress | yes | mature [primary: chrisdown 2026-03] |
| CRIU full dump + kill | ✅ | slow (full eager reload) | yes | mature, baseline |
| **CRIU lazy / post-copy restore** (`--lazy-pages`, userfaultfd) | ✅ | fast start, page-in **deferred** off critical path | yes | mature, Linux 4.11+ [primary: criu.org] ⭐ |
| CRIU incremental `pre-dump` (soft-dirty) | ✅ | cuts per-suspend cost; only a few hundred KB at cut-over | yes | mature [primary: criu.org/Iterative_migration] |
| criu-image-streamer (stream off-node, S3/GCS) | ✅ | 0.1 / 1.4 CPUsec/GB (**I/O layer only, maintainer-reported**) | yes | mature; numbers not independent |
| Firecracker snapshot + `MAP_PRIVATE` demand paging | ⚠️ depends on eviction (default loads from file) | VM-state few ms + page-fault tail | yes | mature, v1.1+ [primary docs] |
| **Working-set prefetch (REAP record-and-prefetch)** | ✅ (only ~24 MB working set resident) | kills 97% faults, 3.7× (232→60 ms) | yes | **[peer-reviewed ASPLOS'21, artifact-evaluated]** ⭐⭐ |
| Template `fork()` / pre-warmed pool (Lambda SnapStart pattern) | ❌ CoW-shares template pages | sub-second (vendor) / 5–30 ms warm | depends | production-mature, **vendor-claimed** [AWS] |

Key sources: criu.org (Lazy_migration, Userfaultfd, --lazy-pages, Iterative_migration);
REAP https://marioskogias.github.io/docs/reap.pdf ; FaaSnap (EuroSys'22)
https://www.sysnet.ucsd.edu/~voelker/pubs/faasnap-eurosys22.pdf ; Firecracker snapshot &
UFFD docs (github.com/firecracker-microvm/firecracker/blob/main/docs/snapshotting/);
AWS Lambda SnapStart blog (2025-08); criu-image-streamer
(github.com/checkpoint-restore/criu-image-streamer).

**FaaSnap/REAP baseline number worth remembering:** a vanilla Firecracker snapshot restore of a
hello-world is **>200 ms** (≈50 ms VMM/emulation restore + ≈182 ms scattered guest page faults) vs
**1–4 ms** warm; snapshot cold-start execution is ~95% slower than warm — *the same critical-path
eager-load penalty our `docker start` problem has.* **[peer-reviewed]**

---

## Table B — Open-source sandbox projects with lightweight suspend/resume

| Project | Backend | Frees RAM on suspend? | Resume latency | License / nature |
|---|---|---|---|---|
| **CRIU (runc / `podman container checkpoint`)** | container + CRIU | ✅ dump+kill | lazy: sub-second start + page-in | open-source, self-hostable ⭐ |
| gVisor (`runsc checkpoint`) | userspace-kernel | ✅ | `--background` lazy restore; **no containerd plumbing yet, raw runsc only** | Apache-2.0 |
| **E2B** | Firecracker microVM | ✅ pause releases | **~1 s (official docs)** | open-source core, self-hostable ⭐ |
| Firecracker | microVM | ✅ (if snapshot evicted) | VM few ms + UFFD page-in; 5–30 ms warm | Apache-2.0 |
| microsandbox | microVM (libkrun, runs OCI images) | ✅ | self-reported "sub-second" (vendor) | open-source |
| Modal mem-snapshots | gVisor C/R | ✅ | ~2.5× faster than cold (vendor) | **closed service** |
| CubeSandbox v0.3 "CubeCoW" (Tencent) | reflink CoW **filesystem** snapshot | FS-only (RAM via stopping container) | ~100 ms snapshot/clone/rollback (vendor) | open-source |
| Fly Machines "Sprites" | Firecracker | ✅ | <1 s warm (vendor) | closed service |
| OpenHands exec-sandbox | QEMU microVM | ✅ | snapshot restore (community PR) | open-source |

**Honesty note on Table B:** most rows come from a **single source / vendor blog** — the 25 hard-
verified claims are overwhelmingly technique-level (Table A). E2B ~1 s and CRIU-lazy mechanics are
the better-grounded entries; treat vendor sub-second figures as leads, not settled. Modal builds on
**gVisor C/R (not CRIU)**, chosen for the userspace-kernel isolation boundary.

---

## DeltaBox — adjacent, NOT our scenario (corrected)

**DeltaBox** (arXiv:2605.22781, 2026-05, IPADS-affiliated; v2 2026-06-08) is a **checkpoint +
rollback** system for **agent state EXPLORATION** — test-time tree search, RL, branching — where an
agent forward-explores then reverts/branches to a saved point under a fixed time budget (abstract:
"high-frequency state exploration … rapid checkpoint and rollback (C/R) of the complete sandbox
state"). It **never addresses freeing host RAM during idle.** Confirmed by re-reading the abstract.

**The distinction that matters is NOT "checkpoint vs snapshot" (near-synonyms) but the operation's
goal:**

| | DeltaBox (checkpoint + rollback) | What we need (snapshot/suspend + restore-forward) |
|---|---|---|
| Goal | revert to a prior state for branching/undo | suspend during idle, **free RAM**, then resume **forward** |
| Memory | template kept resident, CoW-shared (~11 MB/child) — **not freed** | must be **evicted** to count as freed |
| "restore" semantics | discards forward progress (revert) | preserves progress (resume forward) |

- **Reusable building blocks (parts, not the machine):** DeltaCR (incremental CRIU dumps) and
  DeltaFS (freeze the writable overlay layer, then switch layers) — the FS-layer-switch is relevant
  to our cheap track.
- **Wrong direction for us:** its headline fast path is `fork()` from a *frozen, resident* template
  — fast precisely because it KEEPS state resident. If we evict to free RAM we drop to its CRIU-lazy
  slow path = plain CRIU, with no DeltaBox value-add.
- **Earlier overstatement corrected:** DeltaBox is *not* "our scenario" and *not* a positioning
  threat to the "free RAM during LLM-wait" angle — it shares only the surface (agent sandboxes,
  SWE-bench, ms-scale C/R); its purpose is orthogonal. Our angle is *less* occupied than first
  implied. Our differentiation remains **client-side policy** (when/whether to suspend under coupled
  remote-API + local-OS budget) on top of a cheap existing suspend/resume mechanism.
- **Caveat:** non-peer-reviewed preprint, numbers author-self-reported, one verifier dissented on
  the headline latencies (2-1). Treat as promising-but-unverified.

---

## Recommendation — two tracks

### Cheap track (current stateless-exec SWE-bench workload — do this first)
Each tool call is an independent `bash -lc`, so **no process-state checkpoint is needed.** Two
options, both staying in the container world (no backend swap):
1. **Replace `docker stop`/`start` with container-level CRIU lazy-restore** — `runc` /
   `podman container checkpoint` (dump+kill = truly frees RAM) + `--lazy-pages` restore (fast
   start, page-in deferred). Cuts per-resume cost vs full container re-init.
2. **Filesystem CoW snapshot + stop container** (CubeCoW / overlayfs-reflink style) — stopping frees
   RAM, restore switches the FS back in ~100 ms.

Combine with the **selective-suspend policy** already concluded from the `exec_lease_stop` result
(suspend only when idle is predicted long enough to amortize resume AND under memory pressure). On a
fat host with no scarcity, a good policy suspends ~never → ~no cost; under scarcity it suspends and
earns the footprint back.

### Heavy track (future general agents needing live process state — dev server, browser)
microVM snapshot + pause (E2B / Firecracker + UFFD) or CRIU + working-set prefetch. Reusable
*mechanisms* from DeltaBox (incremental CRIU dumps + overlay FS-layer switch) — but NOT its
rollback-oriented resident-template fast path, which is the wrong direction for RAM-freeing. Cost:
swapping the sandbox backend.

---

## Decisive missing measurement (the open question this research can NOT answer)

No source measures the **RAM-freed-vs-resume-latency curve on *our* host + *our* workload.** The
cheap, decisive next step: on the existing rig, swap one arm from `docker stop/start` to
**`runc`/`podman checkpoint --lazy-pages`** (or FS-CoW) and measure its resume cost against the
`docker start` baseline's +138%. **Prove out the cheap track in the container world before
considering a microVM backend swap.**

Open questions still unresolved:
- Which OSS *agent-sandbox* projects truly FREE host RAM on suspend vs only freeze CPU, with
  documented resume latency + license (Table B is single-sourced for most rows).
- cgroup-v2 freezer + forced swap-out to zram/zswap: does it meet requirement #1 with acceptable
  resume latency at hundreds of idle sandboxes? Not addressed by any confirmed claim.
- Steady-state host-RAM footprint at hundreds of concurrent suspended sandboxes per approach —
  where is the crossover where dump-and-evict beats keep-resident-CoW (DeltaBox keeps ~11 MB/child)?

---

## Primary sources

- CRIU: https://criu.org/Lazy_migration · https://criu.org/Userfaultfd ·
  https://criu.org/CLI/opt/--lazy-pages · https://criu.org/Iterative_migration ·
  https://criu.org/Memory_changes_tracking
- criu-image-streamer: https://github.com/checkpoint-restore/criu-image-streamer
- Adrian Reber (pre/post-copy combine): https://lisas.de/~adrian/posts/2016-Oct-14-combining-pre-copy-and-post-copy-migration.html
- REAP (ASPLOS'21): https://marioskogias.github.io/docs/reap.pdf
- FaaSnap (EuroSys'22): https://www.sysnet.ucsd.edu/~voelker/pubs/faasnap-eurosys22.pdf
- Firecracker snapshot + UFFD docs: https://github.com/firecracker-microvm/firecracker/blob/main/docs/snapshotting/handling-page-faults-on-snapshot-resume.md · https://github.com/firecracker-microvm/firecracker/blob/main/docs/snapshotting/snapshot-support.md
- AWS Lambda SnapStart: https://aws.amazon.com/blogs/compute/under-the-hood-how-aws-lambda-snapstart-optimizes-function-startup-latency/
- Catalyzer (ASPLOS'20): https://arxiv.org/abs/2102.12892
- E2B persistence: https://e2b.dev/docs/sandbox/persistence
- gVisor C/R: https://gvisor.dev/docs/user_guide/checkpoint_restore/
- Podman checkpoint: https://docs.podman.io/en/stable/markdown/podman-container-checkpoint.1.html
- Modal mem-snapshots: https://modal.com/blog/mem-snapshots
- DeltaBox: https://arxiv.org/abs/2605.22781
- microsandbox: https://github.com/superradcompany/microsandbox
- CubeSandbox: https://github.com/TencentCloud/CubeSandbox
- cgroup-freezer pause war-story (MetalBear): https://metalbear.com/blog/on-pausing-containers-how-we-built-and-why-we-deprecated-our-container-pause-feature/
- zram vs zswap: https://chrisdown.name/2026/03/24/zswap-vs-zram-when-to-use-what.html
