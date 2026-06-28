# Execution-Scoped Leasing — 实验设计与命题

**这份文档的定位**：把多轮讨论收敛出的研究问题、被证明的命题、实验设计与诚实边界写死，作为
Doc1（系统设计文档）"资源管理与调度"一章的底稿。它的前传是
[`checkpoint-restore-landscape.md`](./checkpoint-restore-landscape.md)——那篇解释了我们**为什么放弃**
"挂起容器省 RAM"这条线；这篇解释我们**转向**了什么。

术语沿用代码与历史 run：`task_lease` / `exec_lease` / `exec_lease_stop`，C = 并发 active 执行容量。

---

## 1. 研究问题

> Coding agents 大量墙钟时间花在 LLM think phase。**task-scoped** 资源预约会让 agent 在整段任务里
> 占着一份 active 执行容量——哪怕它正在等模型、什么都没执行。**execution-scoped leasing** 把
> "agent in-flight 并发"与"active tool execution 并发"解耦：agent 可以很多，同时真正在跑
> pytest/build/search 的数量受控。命题是：在**同一份** active 执行容量下，execution-scoped leasing
> 的吞吐天花板高于 task-scoped，差额来自回收被 think-time 空占的容量。

与早期设想的一处关键差别（靶子的转移）：我们**不**去证明"真实 SWE-bench 天然被 active capacity
卡住"。在 128 核 / 540GB 这台机器 + 单一 LLM endpoint 下，那个定言句既难证、也大概率为假（要让
active capacity 自然 bind 需要 N≈300 个 agent，而那会先把 LLM 打到限流）。我们改证一个**可证且部署
相关的条件句**：

> 给定每节点 active 执行容量 C（一个真实的供给参数），execution-scoped leasing 在其下占优。

C 由 `cpu.max=K` 设定，建模"把一堆 agent 塞进一台 K 核节点"——没人会给单个 agent 配 128 核，每节点
active capacity 本就是有限供给。这与"用 cgroup `memory.max` 模拟更小节点"是同一个论证：固定一个真实
供给参数，不是伪造瓶颈。

---

## 2. 核心命题：被证明的 / 被固定的 / 被排除的

这是全文最重要的一张表。它存在的目的，是堵住这个实验**最容易被攻击的点**：把"CPU 上限"这个
*控制变量*误读成"并发受限于 CPU"这个*结论*。CPU 上限在这里的角色是显微镜的载玻片——让对象能被
看见的固定装置，不是被观察的对象。

| 被证明的命题（结论） | 被固定的前提（控制变量） | 被排除的替代解释 |
|---|---|---|
| 给定**固定**的 active 执行容量 C，execution-scoped leasing 的吞吐天花板 **高于** task-scoped。 | C 由 `cpu.max=K` 设定 = 建模 K 核节点的真实供给容量。**它是前提，不是测出来的瓶颈。** | ❌"并发受限于 CPU"——CPU 上限是我们设的，把前提当结论是循环论证。 |
| task_lease 把约 **66%** 的容量用于 think-time 空占（历史 idle-held 实测）。 | LLM 用**固定延迟 think-stub** 替代，think 分布采样自实测 `llm_wait`。把 think-time 变成受控 IV。 | ❌"真实 SWE-bench 天然 active-bound"——不声称；C 是供给参数，非自然瓶颈。 |
| 回收这部分空占可在**同等资源**下把 in-flight agent 数从 ≤C 抬到 ≫C，从而提吞吐与利用率。 | 工作负载固定（SWE-bench stateless-exec），不换数据集；实例集 stratified ≥18，重复 3 次。 | ❌"exec_lease 赢在省 RAM"——它**不停容器、不碰 RAM**，赢在 think 时**归还 active 名额**。 |
| 收益结构来自 reservation 粒度（整任务 vs 单次执行）与 agent 工作结构（think-heavy）的**错配**。 | 同一 LLM/温度/step_limit/wall_time；C 设为唯一稀缺资源（必要时 `cpu.max` enforce）。 | ❌小样本噪声——`+130%` pilot 数据坐在 6 实例 makespan 70~90s 的噪声区，必须 ≥18×3 重跑才算数。 |
| | | ❌LLM 限流差异——think-stub 把 429 变量彻底移除，N 可自由增长。 |

一句话读法：**被证明的不是"什么是瓶颈"，而是"面对同一个瓶颈，task_lease 浪费了它、exec_lease
没浪费"。** 瓶颈是谁不重要；重要的是浪费的结构。

---

## 3. 为什么这不是 trivial 的 "thread pool 常识"

"阻塞时释放稀缺资源"作为**机制**不新（连接池在事务 think 时还连接、async I/O 释放线程）。贡献不在
发明，在四点：

1. **现状是 task_lease。** 当前 coding-agent scaffold（含 mini-swe-agent）普遍 `docker run ... sleep 2h`
   把容器及其名额钉死整段任务——没人按执行粒度租。
2. **浪费被量化为具体数字：idle-held 66%。** 不是"可能有浪费"，是"测出来三分之二的容量被 think-time
   空占"。
3. **think-heavy 是 agent 负载的结构性特征**，不是偶然——正是它把 reservation 粒度错配的代价放大，也是
   agent workload 区别于传统 RPC/web 的地方。
4. **把它做成运行时的一等调度原语**，配上正确的资源模型，对应 SOW 的统一事件模型：一个等 LLM 的 agent
   是 **RUNNABLE-but-blocked**，不该占着 active 执行名额，正如阻塞的进程不占 CPU。idle→让出名额是一个
   event，资源可用→唤醒是一个 event。

---

## 4. 设计演进：我们如何收敛到这里

记录把方案空间剪枝掉的几个**负结果**，避免重走。三次都是同一个陷阱的变形：**把"在我设的条件下成立"
误读成"在真实世界里必然成立"。**

- **`verdict.md` 自动结论"LLM is binding resource"不可信。** 那是一段启发式的 else 兜底分支（"收紧 C
  吞吐没变 ⇒ 大概是 LLM"），真正测到的只有"沙箱 slot 数不是限制因素"。且全程 `total_429 = 0`，不是
  rate limit。
- **80M 聚合硬上限 run 是个有价值的负结果，但归因要修正。** `exec_lease_stop` 在 `MemoryMax=80M
  MemorySwapMax=0` 下触发 5 次 OOM-kill、`memory.events.max` 达 425（task_lease 仅 75），尽管驻留容器更少
  （1.19 vs 3.03）。驻留少却压力大 ⇒ 压力来自 **stop/start 的冷启动尖峰**叠加，不是稳态驻留。它否定的是
  "always-stop + swap=0 + 聚合硬上限"这个特定组合，**不是"解耦"本身**。注意该 run 容器级内存采集全为 0
  （rootless podman 与 `docker stats` 不兼容），即近乎盲测——这是必须先修的前置。
- **对 stateless-exec，"挂起省 RAM"是伪命题。** 每次工具调用是独立 `bash -lc`，进程退出即由内核回收
  内存；idle 容器本就轻，active 进程才重而 active 时不能停。两头一夹，suspend 能抢救的 RAM ≈ 0。故
  `exec_lease_stop` / checkpoint 整条线对当前负载死亡 → 降为 stateful future work（见 §8）。
- **真正的赢家是 `exec_lease`，不是 `exec_lease_stop`。** pilot C=2：task_lease 126/hr、exec_lease 291/hr、
  exec_lease_stop 233/hr。exec_lease 不停容器、不碰 RAM，只在 think 时归还 slot——它利用了"idle 容器轻、
  不必 suspend"这个事实。**但这组 +130% 是噪声区数据，仅作线索，结论需 §5 重跑。**

可复用的硬资产：podman 把容器 restore p50 从 docker 的 19.3s 压到 **2.0s**（同机同负载，−89%），lifecycle
微基准已坐实。即便主线转向 exec_lease，这个数仍是 stateful future work 的基础。

---

## 5. 实验设计

下一步主实验是 `task_lease` vs `exec_lease`（把 `exec_lease_stop` 移出主线）。需带齐三个前置。

### 前置（必须先做）

1. **修测量。** rootless podman 下 `docker stats` 抓不到容器级内存（历史 run 全 0）。改为遍历每个容器的
   cgroup scope 直接读 `memory.current` / `memory.peak`；新增 **per-execution 的 CPU 归因**（一个 active
   执行吃多少核、多少瞬时 RAM）——这是把 C 绑定到"K 核能撑几个并发 pytest"的依据。
2. **think-stub 拿掉 LLM 变量。** 用固定延迟 stub 替换真实 LLM 调用，延迟从实测 `llm_wait` 分布采样。
   调度故事**唯一依赖**的 LLM 属性是"agent 有 X% 墙钟处于 blocked-not-executing"，故 stub 不失真，且
   彻底移除 429、让 N 自由增长。代价是丢端到端真实性 → 用一发"真 LLM、中等 N"run 验证 stub 的 think
   分布对得上。
3. **样本量。** ≥18 实例 stratified、每档重复 3 次，报均值+离散。`+130%` 这类 6 实例数据一律不作结论。

### 三步

| 步骤 | 做什么 | 产出 / 判定 |
|---|---|---|
| **标定** | podman，递增 (C=cpu.max=K, N)，测单 active 执行的 CPU/瞬时 RAM、idle vs active footprint、think 占比 | 定 C 的网格；确认内存/CPU 谁先 bind，保证 C 是唯一稀缺资源 |
| **主实验** | 固定 `cpu.max=K` + think-stub，sweep N，比 `task_lease`（in-flight≤C）vs `exec_lease`（active≤C, in-flight≫C） | 吞吐曲线 + agents-per-core 利用率；**预测**：N 宽松两臂齐平，N 过 C 后 task_lease 先塌 |
| **验证** | 真 LLM、中等 N，跑一发 | 验证 think-stub 分布与真实一致；端到端 sanity |

---

## 6. 预注册判据（先写死）

1. 存在某 N 档，使 `exec_lease` 相对 `task_lease` 吞吐 **≥ +10%**（命中 SOW 2.2）。
2. 在该档，solved **不低于** task_lease（质量护栏）。
3. 在该档，`exec_lease` 达到的 in-flight 显著 > C，且 agents-per-core 利用率显著高于 task_lease。
4. N 宽松档（需求 < C）两臂吞吐无显著差——诚实对照，证明收益来自容量饱和而非机制本身。
5. 主实验全程 OOM-kill = 0 / CPU 是唯一 binding 资源（标定保证）；否则该档作废。

---

## 7. 诚实边界（提前标注，否则会被审稿戳）

- **in-flight 不是免费的，exec_lease 天花板不是无穷。** 它把 ceiling 从 C 抬到
  `min(LLM 容量, per-agent 开销容量)`。几百个容器时，conmon/fd/基座 RAM 的 per-agent 开销会成为**新
  天花板**。要测出来，不能假设无穷。诚实表述："exec_lease 把 in-flight ceiling 从 C 大幅抬高，但抬到哪由
  per-agent 开销决定。"
- **C 是建模的供给参数，不是实测的自然瓶颈。** 见 §1、§2。任何"SWE-bench 卡在 CPU"的表述都是错的。
- **think-stub 牺牲端到端真实性换取 LLM 变量的可控性**，用 §5 验证步补偿。

---

## 8. checkpoint / suspend 线：降为 stateful future work

`exec_lease_stop` + checkpoint/restore 对 **stateless-exec** 负载已被否定（§4），但对 **stateful** 负载仍然
成立：常驻 dev server / 浏览器等 tool call 之间有活进程、idle 真握 GB 级 RAM 的场景——即
`checkpoint-restore-landscape.md` 的 heavy track。届时 podman restore 2s（含可上的 `--lazy-pages` /
working-set prefetch）是其基础。明确列为 future work，**不是死路，是另一类负载的主线**。
