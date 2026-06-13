有，而且已经形成一条比较明确的研究路线。但先给一个重要结论：

**目前还没有专门评测或改进 Claude Code Dynamic Workflows 的论文。**该功能是 **2026 年 5 月 28 日**才发布的。现有论文研究的是与它高度同构的“代码化 agent workflow、动态任务图、运行时重规划和模型路由”。

## Claude Code 的机制

官方定义是：

- Claude 生成一段 JavaScript 编排脚本。
- 脚本控制分支、循环、并发、阶段和中间状态。
- 实际读写文件、运行命令的是 subagents。
- 最多 **16 个 agent 同时运行**，一次 workflow 最多创建 **1,000 个 agent**。
- 中间结果保存在脚本变量中，不占主对话上下文。
- 可以保存成 `.claude/workflows/*.js` 重复运行。

所以它本质上是：

> **LLM 生成 workflow program + 确定性 runtime 调度 + LLM subagents 执行**

来源：[Claude Code Dynamic Workflows 文档](https://code.claude.com/docs/en/workflows)、[Opus 4.8 发布公告](https://www.anthropic.com/news/claude-opus-4-8)

## 最相关论文

| 论文 | 核心改进 | 与 Claude Code 的关系 |
|---|---|---|
| [DyFlow](https://arxiv.org/abs/2509.26062), 2025 | 执行过程中根据中间结果、错误动态修改子目标和后续 workflow | **最直接相关**。解决“脚本生成后结构就固定”的问题 |
| [Workflow-R1](https://arxiv.org/abs/2602.01202), 2026 | 将 workflow 构造建模为持续的 `思考→调用→观察→修改` 策略，用 RL 学习 | 可让 workflow 根据测试结果持续重规划 |
| [Multi-Agent Collaboration via Evolving Orchestration](https://arxiv.org/abs/2505.19591), 2025 | 中央 orchestrator 动态选择、重复调用、淘汰或终止 agent | 可优化 subagent 数量和调用顺序 |
| [AgentConductor](https://arxiv.org/abs/2602.17100), 2026 | 根据任务难度和执行反馈动态生成及修改分层 DAG | 代码任务上最高提升 14.6% pass@1，token 最多减少 68% |
| [FlowReasoner](https://arxiv.org/abs/2504.15257), 2025 | 为每个 query 单独生成代码化 multi-agent workflow | 非常接近 Claude 当前“一次任务生成一段 JS” |
| [MaAS](https://openreview.net/forum?id=imcyVlzpXh), ICML 2025 Oral | 从 agentic supernet 中按 query 选择不同复杂度的 workflow | 简单任务少开 agent，复杂任务增加搜索、测试和审查 |
| [AOrchestra](https://arxiv.org/abs/2602.03786), 2026 | 动态生成 `Instruction, Context, Tools, Model` 四元组，按需创建 subagent | 可改进固定角色和所有 agent 共用同一模型的问题 |
| [AFlow](https://arxiv.org/abs/2410.10762), ICLR 2025 | 把 workflow 表示为代码，用 MCTS 搜索更好的控制流 | 非常适合离线优化保存下来的 `.claude/workflows/*.js` |
| [Flow](https://openreview.net/forum?id=sLKDbuyq99), ICLR 2025 | 用 AOV 图表示 workflow，根据历史执行动态调整任务分配和并行度 | 与 Claude 的阶段、依赖、fan-out 结构接近 |
| [AgentDropout](https://arxiv.org/abs/2503.18891), ACL 2025 | 按轮删除低贡献 agent 和通信边 | prompt token 降 21.6%，completion token 降 18.4% |

## 性能优化方向

仅优化“谁做什么”还不够，动态 workflow 的主要成本实际上来自并发、关键路径和缓存。

- [LAMaS](https://arxiv.org/abs/2601.10560), 2026：优化 DAG 的关键路径和层内并行，相比 MaAS 将关键路径缩短 **38–46%**。
- [Autellix](https://arxiv.org/abs/2502.13965), 2025：把完整 agent program 而非单次 LLM call 作为调度单位，吞吐提高 **4–15 倍**。
- [Helium](https://arxiv.org/abs/2603.16104), 2026：做 workflow-aware prefix/KV cache 和全局调度，最高约 **1.56 倍加速**。
- [AgentOpt](https://arxiv.org/abs/2604.06296), 2026：搜索各 workflow 阶段的模型分配，在相同准确率下，不同组合成本可相差 **13–32 倍**。
- [InfraMind](https://arxiv.org/abs/2606.11440), 2026-06：非常新的方向，根据实时队列、KV cache、延迟和预算调整拓扑、模型和推理深度。

## 可靠性方向

Claude 当前 workflow 的薄弱点可能不是生成脚本，而是失败后的恢复。

- [SHIELDA](https://arxiv.org/abs/2508.07935)：异常分类、局部恢复、控制流调整和状态恢复。
- [SagaLLM](https://arxiv.org/abs/2503.11951)：引入事务、补偿、验证和回滚。
- [AgenTracer](https://arxiv.org/abs/2509.03312)：定位哪个 agent、哪个步骤导致失败，并用反馈改进 workflow。
- [AgentDropoutV2](https://arxiv.org/abs/2602.23258)：运行时拦截错误输出，先修正，修不好再阻断传播。

## 我的判断

如果要对 Claude Code Dynamic Workflows 做真正有效的改进，优先级应该是：

1. **从一次性生成脚本升级为运行中可修改的 workflow**
   借鉴 DyFlow、Workflow-R1。

2. **按任务难度动态控制 agent 数量和验证深度**
   借鉴 MaAS、AgentConductor、AgentDropout。

3. **让每个 subagent 独立选择模型、工具和上下文**
   借鉴 AOrchestra、AgentOpt。

4. **优化关键路径而不只是 token 总量**
   借鉴 LAMaS、Autellix、Helium。

5. **加入 checkpoint、失败归因、回滚和局部重试**
   借鉴 SHIELDA、SagaLLM、AgenTracer。

目前最大的研究空白是：这些方法大多只在 HumanEval、MATH、QA 等短任务上验证，还没有充分验证在 **数小时、多仓库、有副作用、有测试和 merge 冲突的真实软件工程 workflow** 上是否仍然成立。

综合入口推荐阅读这篇 2026 年综述：[From Static Templates to Dynamic Runtime Graphs](https://arxiv.org/abs/2603.22386)。它收录了 77 项相关工作，并明确区分了离线模板优化、query-level 图生成和执行中图修改。