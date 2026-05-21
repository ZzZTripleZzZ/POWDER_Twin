# dt_sync — 支持漂移感知的实时 RAN 数字孪生与反事实调度决策系统

*POWDER × Tiny_Twin 数字孪生系统，Mac 作为编排节点*  
*目标会议：MobiCom / SIGCOMM / NSDI / INFOCOM*  
*版本：v2.1 — 2026-04-30（M1–M4 全部实现完毕）*

---

## 一句话贡献

> **"一个实时、漂移感知的 RAN 数字孪生系统，具备有界的度量层 MAC 状态等价性，以及一个选择性精度反事实调度决策引擎——在不中断生产流量的前提下，在 TTI 级 rollout 上安全评估固定权重候选调度策略。"**

| 机制 | 作用 | 代码文件 | 状态 |
|---|---|---|---|
| **M1** 漂移感知实时校准 | KL 漂移度量 `D(t)`；触发式 EMA 重校准 | `channel_converter.py` + `state_sync.py` | ✅ 完成 |
| **M2** 反事实调度决策引擎 | 在实时孪生上运行固定权重候选策略 rollout；自举置信区间改进量下界；部署门控 | `counterfactual_oracle.py` + `e3_rl_eval.py` | ✅ 完成 |
| **M3** 选择性精度孪生 | 三档 PHY 精度（`full_iq` / `sparse_tap` / `mac_only`）；决策敏感性驱动的模式选择 | `selective_fidelity.py` + Tiny_Twin C 补丁 | ✅ 完成 |
| **M4** 可证明的 MAC 状态等价性 | 度量层状态偏差的形式化上界 Δ ≤ K + ⌈(R+S)/T_TTI⌉；REQ/REP 对账协议 | `mac_equivalence.py` + `proto/` | ✅ 完成 |

---

## 一、动机

### 1.1 实时 RAN 中的调度策略更新问题

现代 O-RAN 系统允许第三方 xApp 通过实时权重注入（如 EdgeRIC）覆盖 gNB 的调度器。这引出了一个关键的未解决问题：

**在将一个新调度策略部署到生产环境之前，如何确认它能提升 KPI？**

当前的做法非常保守：先用合成 trace 进行实验室测试，再缓慢灰度发布并人工监控。这个流程需要数天到数周，而且在灰度窗口期仍可能导致真实用户的 QoS 下降。其结果是运营商实际上把调度器冻结了——一个离线训练的策略可能在信道条件已经改变后，依然在生产环境中运行数月。

数字孪生的理论前景正是要解决这一问题：先在网络的虚拟副本中运行候选策略、观察 KPI，仅在安全的前提下才部署。但这一前景在实时 RAN 系统中至今未能实现，原因有三：

1. **孪生会漂移。** 真实无线信道是非平稳的。初始化时校准的 CIR，随着 UE 移动、干扰变化和硬件温漂，在数分钟内就会失效。一个过时的孪生给出错误的 KPI 预测，从根本上否定了整个方案的意义。

2. **状态等价性无法保证。** 调度器的决策依赖 MAC 层状态：HARQ 缓冲区内容、缓冲区状态报告（BSR）、队列深度、CQI 历史。目前没有任何已发表的工作正式分析孪生的 MAC 状态是否与真实 gNB 匹配——这个差距可能使所有 KPI 比较失效。

3. **全精度 PHY 仿真计算代价太高。** 在商用硬件上，以 1ms TTI 速率进行 100 抽头 CIR 卷积运算，每次调用需约 5ms——超出预算 5 倍。现有孪生要么牺牲精度（离线、预计算 CIR），要么依赖专用 FPGA 硬件。

三星 2025 年 Sim2Real 报告量化了这一现状：在商用 O-RAN 部署上，**孪生与真实网络的 KPI 相似度仅为 39%**。这个差距不是细节问题，而是核心未解问题。

### 1.2 为何现在是解决这一问题的最佳时机

以下三项进展使解决方案在当下变得可行：

- **Tiny_Twin**（2025）证明了完整的 OAI NR 协议栈（gNB + UE + CN）可以通过 RFsimulator 在商用 CPU 上运行，消除了 FPGA 的硬件要求。
- **EdgeRIC**（NSDI'24）暴露了实时 ZMQ 调度权重注入接口，使得通过相同协议进行固定权重影子策略评估成为可能。完整 per-TTI 因果动作评估需要 EdgeRIC wire protocol 增加动作确认，留到下一轮实现。
- **POWDER** 提供了稳定的 B210 OTA 测试平台，具有可复现的信道条件，支持孪生与真实网络之间的地面真值比较。

我们的系统 dt_sync 首次将上述三者整合为一个**闭环实时孪生**，能够自校准、维持形式化状态等价性、根据当前决策动态调整计算预算，并在任何部署前提供统计保证的策略排名。

---

## 二、问题定义与挑战

### 2.1 形式化问题

给定一个实时 5G gNB，服务 $U$ 个 UE，信道 $h(t)$ 随时间变化，以及一组候选调度策略 $\{\pi_1, \ldots, \pi_M\}$，**在不向真实用户部署任何未测试策略的前提下，安全地识别出使每 TTI 期望奖励最大化的策略 $\pi^*$**。

约束条件：
- 评估期间不中断真实用户的服务。
- 统计置信度：被部署的策略须以不低于 $1-\alpha$ 的概率高于基线策略。
- 计算预算受限：评估必须在与 TTI 结构兼容的时间内完成（即孪生运行速度须快于实时）。
- 形式化正确性：用于评估的初始孪生状态必须与真实 gNB 状态在有界范围内一致。

### 2.2 技术挑战

**挑战 C1：信道非平稳性与静默漂移**

真实 CIR 因 UE 移动、多径演化和硬件效应，在 1–100 秒的时间尺度上发生变化。在初始化时完成的校准，若无在线反馈，到时间 T 时已经失效。核心难点在于：一个运行着过时校准的孪生，**从孪生内部无法被检测到**——它仍然输出看似合理的结果。我们需要一个同时利用**真实侧与孪生侧**观测值计算的漂移信号。

我们的洞见：在滑动窗口上计算真实与孪生的联合 (CQI, BLER) 分布之间的 KL 散度 $D(t) = \mathrm{KL}[\hat{p}_\text{real} \| \hat{p}_\text{twin}]$，无需已知地面真值信道即可检测漂移。当 $D(t) > \tau$ 时，触发 EMA 重校准；否则冻结校准（避免漂移导致的过拟合）。

**挑战 C2：MAC 状态偏差缺乏形式化上界**

调度器关心的是 MAC 状态，而不仅仅是信道。HARQ 重传时隙、缓冲区状态报告和调度队列深度通过 ZMQ protobuf（EdgeRIC）从真实侧传递到孪生侧。网络抖动意味着孪生看到的 MAC 状态滞后于真实 gNB，且滞后量未知、可能无界。如果这个滞后很大，孪生仿真的初始状态是错误的，导致 KPI 估计有偏。

目前没有任何 RAN 数字孪生论文对这一滞后进行形式化分析。我们证明了闭合形式的上界：$\Delta \le K + \lceil (R+S)/T_\text{TTI} \rceil$，其中 K 是对账间隔，R 是 REQ/REP 往返时延，S 是单向序列化延迟。对于 POWDER 参数（K=100，R=2ms，S=0.5ms）：**Δ ≤ 103 TTI = 103ms**——远小于 H=200 TTI 的仿真水平线，使系统性偏差在可证明范围内有界。

**挑战 C3：实时 PHY 仿真的计算预算**

运行 N=4 个并行孪生副本，每个以完整 100 抽头 CIR 卷积在 1ms TTI 速率下运行，需要约 20ms/TTI 的计算量——超出预算 20 倍。朴素实现下，实时孪生根本无法跟上实际速度。

我们的洞见：每次评估步骤所需的精度取决于**决策敏感性**。当信道漂移 D(t) 较低、候选策略趋于一致时，廉价的 MAC 层模型（CQI→MCS 查表，无需 PHY 计算）与全精度模型给出相同的排名结果。只有当 D(t) 突升或策略存在分歧时，孪生才需要完整的 CIR 卷积。基于规则的选择器（后期可引入学习）能以不足 5% 的排名精度损失实现 ≥3 倍的 CPU 节省。

**挑战 C4：无地面真值情况下的统计有效策略排名**

标准 A/B 测试要求将每个候选策略部署给真实用户。我们需要仅凭孪生仿真结果对策略进行排名。难点在于孪生仿真结果存在噪声（来自各副本间的 CIR 扰动）和潜在偏差（来自 C2 的状态滞后）。仅靠奖励的点估计是不够的——我们需要一个同时考虑二者的置信区间。

我们对 N 个副本的奖励样本进行自举重采样（B=1000），得到改进量 $\text{CI}(\pi_i) = [l_i, u_i]$ 的 95% 置信区间。当且仅当 $l_i > 0$（改进量置信下界为正）**且**没有任何副本观测到 BLER > β（安全约束）时，才部署策略 $\pi_i$。这一机制包含并量化了先前的集成安全过滤器。

---

## 三、与现有工作的区别

### 3.1 精确对比表

| 已有工作 | 他们做什么 | 与我们相比缺少什么 |
|---|---|---|
| **EdgeRIC**（Ko 等，NSDI'24） | 实时 RIC + 离线模拟器在环策略训练 | 孪生是**离线**的（replay traces）；无实时信道同步；无漂移检测；无反事实决策引擎 |
| **ColO-RAN**（Bonati 等，2024） | Colosseum FPGA 信道仿真器 + DRL 迁移 | 需要 **FPGA** 硬件；无漂移感知重校准；策略迁移而非反事实评估；无形式化状态等价性 |
| **OWDT**（Iimori 等，arXiv 2503.12177，2025） | 将预计算的 Sionna RT CIR **开环**注入 OAI | CIR 是静态离线数据集；**无来自真实网络的反馈**；无漂移检测；无 MAC 状态同步 |
| **Tiny-Twin**（2025） | 基于 RFsimulator 的 CPU 原生完整 OAI 协议栈（本工作的基础） | 无与真实 gNB 的实时同步；无漂移检测；无调度决策引擎；我们在此之上构建了所有这些 |
| **Simeone 等 PPI**（arXiv 2507.07067，2025） | 可微 RT 校准 + 贝叶斯 DT 集成 + 跨预测驱动推断（PPI）偏差修正 | **离线**校准（需要批量数据集）；无实时适应；PPI 事后修正离线评估中的偏差，我们通过在线校准防止偏差累积；两种方法互补（E2 将加入 PPI 作为对比基线） |
| **Samsung Sim2Real**（2025） | 发现 39% 相似度差距；提出 RL 再训练方案 | 只是描述问题；我们的系统通过 M1+M4 **弥合这一差距**（目标 ≥70% 相似度，E6 验证） |
| **TailO-RAN**（GLOBECOM'25） | 面向尾时延的可编程调度权重 xApp | 调度策略设计，而非评估。我们使用相同的 ZMQ 权重注入接口进行决策查询 |
| **RANBooster**（SIGCOMM'25） | 商用 5G 前传中间件 | 不同问题（前传容量）；作为相关工作引用 |

### 3.2 我们填补的空白

最清晰地表述我们的创新点：

> **目前没有任何已发表的工作提供一个实时、持续校准的 RAN 数字孪生，能同时做到：(a) 在闭环中检测并修正信道漂移；(b) 提供 MAC 状态偏差的形式化上界；(c) 根据下游决策的敏感性自适应计算预算；(d) 利用孪生以自举统计保证对候选调度策略进行排名——并且全部在商用硬件上实现，无需额外基础设施。**

M1–M4 每一个机制在相邻领域都有先例（ML 系统中的漂移检测、形式化时钟同步上界、图形学中的自适应仿真精度、RL 中的反事实评估）。创新性在于**将四者紧密耦合为一个单一的实时 RAN 系统**，其中每个机制对其他机制的正确性都是必要的：

- M2（决策引擎）需要 M4（有界初始状态）来避免有偏的仿真。
- M2 需要 M3（选择性精度）才能在 TTI 预算内完成。
- M3 需要 M1 的 D(t) 来决定何时提升精度。
- M4 需要 M1 的 TTI 序列号标签来计算形式化上界。

---

## 四、系统架构

```
┌─────────────────────────────────────────────────────────────────────────┐
│ Mac（Apple Silicon）— 编排节点                                           │
│                                                                          │
│  dt_sync/                                                                │
│   ├── orchestrator/                                                      │
│   │    ├── channel_converter.py          (M1) CQI→CIR + KL 漂移度量     │
│   │    ├── state_sync.py                 (M1+M3+M4) 双层同步            │
│   │    ├── counterfactual_oracle.py      (M2) 影子仿真 + 置信区间        │
│   │    ├── selective_fidelity.py         (M3) 模式选择器                │
│   │    ├── mac_equivalence.py            (M4) 对账协议                  │
│   │    ├── mac_equivalence_twin_client.py(M4) 孪生侧客户端启动器        │
│   │    ├── e3_rl_eval.py                 (E2) 决策排名评估框架          │
│   │    ├── m3_overhead_eval.py           (E3) CPU/精度扫描              │
│   │    ├── auto_recalibrate.py           (M1) 定时重校准                │
│   │    ├── deploy.py                     POWDER 端到端部署              │
│   │    ├── powder_client.py              POWDER 门户客户端              │
│   │    └── ssh_fanout.py                 并行 SSH                       │
│   ├── proto/                                                             │
│   │    ├── metrics.proto / _pb2.py       (M4) +tti_seq +state_hash      │
│   │    └── reconciliation.proto / _pb2.py(M4) REQ/REP schema           │
│   ├── Tiny_Twin/radio/rfsimulator/                                       │
│   │    └── apply_channelmod.c            (M3) TT_FIDELITY_MODE 钩子     │
│   └── design_plan.md                     英文版设计文档                  │
│                                                                          │
└──────┬──────────────────────────────────────┬───────────────────────────┘
       │ ssh / ZMQ                            │ ssh / ZMQ
       ▼                                      ▼
┌──────────────────────┐          ┌──────────────────────────────────────┐
│ POWDER — 真实侧       │          │ POWDER — 孪生侧                       │
│  gnb-node + B210     │  L1 CIR  │  孪生主机（商用 CPU）：               │
│   nr-softmodem       │  via     │   ├── tt-gnb（Docker，TT_FIDELITY_   │
│   + EdgeRIC :5555    │  命名管道 │   │    MODE env，RFsim）             │
│  ue-node + B210      │ ───────► │   ├── tt-nrue（Docker）              │
│  cn-node（oai-cn）   │  L2 MAC  │   ├── oai-cn（Docker）              │
│                       │  :5555→  │   └── edgeric-twin                  │
│                       │  :5655   │                                      │
│                       │  M4 REP  │  ※ N=4 决策副本（M2）               │
│                       │  :5560   │  ※ M4 客户端 :5560                 │
└──────────────────────┘          └──────────────────────────────────────┘
```

### 三层数据流

```
第一层（PHY / 信道）  — M1 + M3
  真实 gNB → logs/snr.txt（每 RNTI 每 TTI 的上行 CQI、上行 ACK）
      → state_sync._tail_snr_log（SSH tail -F）
      → ChannelCalibrator.observe_drift_sample(real_cqi, real_bler, twin_cqi, twin_bler)
      → 若 D(t) > τ：calibrator.update(EMA) + mark_recalibration [M1]
      → selective_fidelity.RuleBasedSelector.choose_mode(signals) [M3]
      → cqi_to_cir(cqi, calibrator) → [若 sparse_tap 则 sparsify_topk] → 命名管道
  孪生 gNB ← 管道 ← apply_channelmod.c（TT_FIDELITY_MODE：跳过PHY / 稀疏化 / 完整）

第二层（MAC / 调度状态）— M4
  真实 EdgeRIC :5555（Metrics protobuf）→ layer2_mac_sync
      → stamp_metrics(tti_seq, ue_state_hash, global_state_hash, reconcile_epoch)
      → MacReconciler.update(metrics) [在 :5560 绑定 REP socket]
      → 转发带时间戳的 proto → 孪生 EdgeRIC :5655
  孪生侧：MacReconcilerClient 每 K TTIs 发送 REQ → 比较哈希值
      → 若不匹配：应用真实侧完整状态差异，记录实测 Δ [M4]

第三层（反事实决策）— M2
  每隔 K_oracle TTIs（由运算符或调度器触发）：
      oracle.evaluate(candidates=[π_1,...,π_M])
          对每个 π_i：在 N 个孪生副本上异步仿真（M3 选择精度模式）
              → 通过 EdgeRIC ZMQ :5556+10i 注入策略权重
              → 从 :5555+10i 收集水平线 H 的奖励
          bootstrap_improvement_ci(baseline_rewards, candidate_rewards)
          部署 π_i 当且仅当 CI_lower > 0 且 safety_violations == 0
```

---

## 五、M1 — 漂移感知实时校准 ✅

### 算法

滑动窗口 W = 200 个配对样本（real_cqi, real_bler, twin_cqi, twin_bler）。  
将（CQI, BLER）离散化为 4×4 联合直方图（4 个 CQI 区间 × 4 个 BLER 区间）。

$$D(t) = \mathrm{KL}\!\left[\hat{p}_\text{real} \,\Big\|\, \hat{p}_\text{twin}\right] = \sum_{c,b} \hat{p}_\text{real}(c,b) \log \frac{\hat{p}_\text{real}(c,b)}{\hat{p}_\text{twin}(c,b) + \varepsilon}$$

拉普拉斯平滑 ε = 1e-6。当且仅当 D(t) > τ_drift（默认值 0.10）时触发 EMA 更新。

**为什么选用 KL 散度：** 它同时捕获 CQI 和 BLER 的联合分布差异，能检测出孪生 CQI 直方图看起来正确但 BLER 出错的情况（例如噪声校准过时）。单纯的 CQI-MSE 指标会漏掉这类情况。

**为什么是触发式而非始终开启的：** 在信道稳定时持续 EMA 会给 amplitude_scale 估计引入噪声。触发器充当变点检测器：只在存在漂移证据时才重校准。

### τ_coherence（实测精度生命周期）

`coherence_samples` = 距上次触发的 TTI 数量。这是孪生自我报告的精度生命周期：当前校准已有效持续多长时间。在 E1 图中以 τ_coherence 对 信道移动性 的关系呈现。

### 实现

- `channel_converter.py`：`ChannelCalibrator` — `observe_drift_sample`、`kl_drift_metric`、`should_recalibrate`、`mark_recalibration`、`coherence_samples`、`trigger_count`
- `state_sync.py`：`_maybe_drift_observe` + `_maybe_recalibrate` 集成于 `_run_local_fifo` 和 `_run_remote_fifo`；每 RNTI 滚动 BLER 窗口（`_BLER_WINDOW=100`）
- E1 日志：`--drift-log logs/drift.csv`，每 5 秒写入（elapsed_s, D_t, trigger_count, coherence_samples, twin_snr_db）

### 核心声明
- 当真实与孪生分布对齐时 D(t) ≈ 0；信道事件（移动性、干扰）时 D(t) >> 0.10
- τ_coherence 与 UE 速度呈负相关（待 E1 测量）
- 触发式 vs 始终开启 EMA：减少虚假重校准 → amplitude_scale 更稳定

---

## 六、M2 — 反事实调度决策引擎 ✅

### 直觉

在时刻 t，真实 gNB 的调度器正在运行策略 π_real。我们希望在部署候选策略 π_i 之前对其进行评估。经过 M1+M4 与真实信道和可观测 MAC metrics 状态同步的实时孪生，提供了一个虚拟测试平台。当前实现将每个候选策略作为**每个 rollout 固定一次权重的候选策略**来评估：决策引擎从 rollout 内第一条新鲜 metrics 计算一次权重向量，注入后在 H TTI 内冻结该权重，并在 N 个副本上测量奖励分布。

关键洞见，且目前没有任何已有工作加以利用：**EdgeRIC 现有的 ZMQ 接口（:5556 权重注入和 :5555 指标上报）已经支持这一固定权重评估循环。** 无需定制的仿真服务器。完整 per-TTI 因果 oracle 需要 EdgeRIC 侧提供 `action_seq` / `target_tti_seq` 确认，留到下一轮实现。

### 形式化定义

真实侧奖励基线（来自实时 `layer2_mac_sync`）：
$$r_t^\text{real} = \sum_{u \in \text{UE}} \bigl(\text{tpt}_u - \lambda \cdot \text{BLER}_u - \mu \cdot \text{queue\_age}_u\bigr), \quad \lambda=50, \mu=0.01$$

对每个候选策略 π_i，在 N 个副本上执行固定权重 H TTI rollout，得到样本 $\{r^{(j)}_i\}_{j=1}^N$。

**改进量**（正值表示候选优于基线）：
$$\widehat{\Delta}_i = \frac{1}{N}\sum_j r^{(j)}_i - \frac{1}{|\mathcal{B}|}\sum_{b \in \mathcal{B}} r^{(b)}_\text{real}$$

**自举置信区间**（B=1000 次重采样，α=0.05）：
$$\text{CI}_{1-\alpha}(\pi_i) = \bigl[Q_{\alpha/2}(\widehat{\Delta}_i^{(b)}), Q_{1-\alpha/2}(\widehat{\Delta}_i^{(b)})\bigr]$$

**部署门控**：当且仅当 CI_lower > 0 **且** 所有副本 BLER ≤ β（默认 0.10）时，部署 π_i。

### 为什么用自举而非高斯假设

N=4 个副本的样本量对中心极限定理而言太少。自举方法无需分布假设，能正确捕获奖励分布的重尾特性（突发流量、HARQ 重传）。

### 实现

- `counterfactual_oracle.py`：`CounterfactualOracle`、`Policy` 抽象类、`EqualWeightPolicy`、`MaxCQIPolicy`、`EdgeRICPPOPolicy`、`PerturbedPolicy`、`bootstrap_improvement_ci`、`compute_reward`、`PolicyVerdict(rollout_semantics="fixed_weight")`
- `e3_rl_eval.py`（E2 评估框架）：5 候选策略排名、Spearman ρ 对比地面真值、Top-1 准确率、不安全策略拒绝率、CSV + PDF 输出
- 副本端口：指标 SUB 在 5555+10i，权重 PUB 在 5556+10i（i=0..N-1）
- Mock 模式：`--mock` 使用合成奖励表进行所有 E2 逻辑；冒烟测试通过

### 核心声明
- Top-1 准确率 ≥ 80%（决策引擎在 5 个候选策略中选出最优策略）
- 不安全候选策略（σ=1.5 扰动）拒绝率 ≥ 95%
- 决策排名与地面真值之间的 Spearman ρ ≥ 0.8（E2，依赖 OTA）

---

## 七、M3 — 选择性精度孪生 ✅

### 三档精度

| 模式 | 信道处理 | PHY 计算 | 每TTI估计耗时 | 触发条件 |
|---|---|---|---|---|
| `full_iq` | 100 抽头 CIR 完整卷积 | RFsim 完整 PHY | ~5ms | D(t) 高、近期重校准、策略存在分歧 |
| `sparse_tap` | 按功率取前 K=4 个抽头 | RFsim PHY（稀疏抽头） | ~0.5ms | 默认中档 |
| `mac_only` | 跳过 CIR | CQI→MCS 查表，解析式 BLER | ~0.05ms | D(t) < 0.02 且 CQI 稳定 |

**声明：** 相比始终使用 full_iq，平均 CPU 节省 ≥3 倍，决策排名精度损失 <5%（E3 验证）。

### 模式选择器

输入（从实时编排节点状态构建）：
- D(t)：来自 M1 校准器
- cqi_var：滚动 CQI 方差（最近 50 次观测）
- regret_width：最近一次决策的置信区间宽度（ci_upper − ci_lower）
- policies_disagree：各候选策略改进量的（最大值−最小值）/|均值|
- recent_recalibration：M1 触发在最近 30s 内发生
- rnti_count：活跃 UE 数量

**基于规则的选择器**（冷启动；W2–W3 生产环境）：
```
优先级 1 → full_iq:  近期重校准 OR D(t) > 0.20 OR
             strategies_disagree > 0.5 OR regret_width > 2×|regret_mean|
优先级 2 → mac_only: D(t) < 0.02 AND cqi_var < 1.0
默认     → sparse_tap
```

**学习式选择器**（W9+）：小型 MLP，在（信号, 地面真值最小所需模式）离线样本对上训练。若无检查点则退回到基于规则的选择器。

### C 侧补丁（`apply_channelmod.c`）

`TT_FIDELITY_MODE` 环境变量在容器启动时读取一次（每容器生命周期）：
- `mac_only`：在 FIFO 读取之前提前返回——完全不运行 PHY
- `sparse_tap`：读取 CIR，将非前 K 个最大功率抽头归零，继续执行卷积
- `full_iq`（默认）：原始代码路径，不变

模式切换通过带有新环境变量的 docker-compose 优雅重启实现。Python 侧的 FIFO 写入器同步应用相同模式（sparsify_topk 或跳过），确保 FIFO 内容与 C 侧期望始终匹配。

### 关键设计决策：为何采用每容器生命周期而非每 TTI 模式切换

在 OAI C 代码中动态切换环境变量需要锁机制且不安全。每容器模式更简单、正确，且与论文声明一致：模式决策的粒度是分钟级，而非 TTI 级。计算节省来自于在 mac_only 模式下运行 200 TTI 的决策副本，而非在仿真中途切换模式。

### 实现

- `selective_fidelity.py`：`Mode`、`SelectorSignals`、`RuleBasedSelector`、`LearnedSelector`、`sparsify_topk`、`build_signals`、`cir_writer_gate`、`apply_mode_to_replica`
- `apply_channelmod.c`：`TT_FIDELITY_MODE` 钩子、`tt_init_mode`、`tt_select_topk`、mac_only 提前返回、sparse_tap 稀疏化
- `state_sync.py`：M3 选择器集成于 `_run_local_fifo` 和 `_run_remote_fifo`；`_restart_production_twin`（带 TT_FIDELITY_MODE 的 docker-compose up --force-recreate）；每 50 次 CQI 观测重新评估模式
- `m3_overhead_eval.py`（E3）：计时扫描 + 决策排名精度对比 full_iq；输出 CSV + PDF Pareto 图

---

## 八、M4 — 可证明的逐 UE MAC 状态等价性 ✅

### 声明范围

M4 的形式化保证适用于**编排节点的度量层视图**：`layer2_mac_sync` 最近转发的 Metrics protobuf。当 EdgeRIC 填充扩展字段时，这个视图包含逐 UE 的（CQI、BLER proxy、HARQ 状态、BSR、队列深度）。如果旧版或当前 EdgeRIC producer 没有填 HARQ/BSR/queue-age，则保证只覆盖已填充的 schema-visible metrics，而不覆盖隐藏的 OAI 内部调度器状态。

我们**不**声明 OAI 内部 C 语言调度器状态是同步的——那在 Python 层无法访问。度量层视图足以支撑当前固定权重 oracle 的输入与奖励记账；状态偏差引入的唯一偏差是每次 rollout 初始可观测条件的误差，该误差由 Δ 有界，且远小于 H=200 TTI 的仿真水平线。

**这是任何已发表 RAN 数字孪生系统中对度量层状态偏差的首次形式化分析。**

### 定理（M4）

设：
- S = 真实侧 protobuf 序列化 + ZMQ 单向延迟上界（ms）
- R = 对账 REQ/REP 往返时延上界（ms）
- K = 对账间隔（TTI 数）
- T_TTI = 1ms（5G NR 数值 0）
- 哈希函数具有抗碰撞性（SHA-256 标准假设）
- 真实侧每 TTI 推送一次指标（无丢包；丢包扩展见附录）

则对所有墙钟时刻 τ 和所有 UE u，逐 UE 的度量层状态偏差满足：

$$\boxed{\Delta^u(\tau) \le K + \left\lceil \frac{R + S}{T_\text{TTI}} \right\rceil}$$

**证明草图：**
1. 真实侧在墙钟时刻 $t \cdot T_\text{TTI}$ 推送带 tti_seq=t 的 metrics(t)。孪生侧最迟在 $t \cdot T_\text{TTI} + S$ 时接收，对应真实 TTI $t + \lceil S/T_\text{TTI} \rceil$。孪生立即更新视图 → 滞后 = $\lceil S/T_\text{TTI} \rceil$。
2. 每隔 K TTI，孪生发送带 global_state_hash 的 ReconciliationRequest。真实侧回应 hash_matched=true（O(字节)，快速）或 hash_matched=false + 完整状态（O(UE数 × 单UE状态大小)）。往返时延 ≤ R。
3. 不匹配时，孪生应用真实侧差异；残余滞后 ≤ $\lceil (R+S)/T_\text{TTI} \rceil$。
4. 最坏情况：不匹配恰好发生在一次对账刚完成之后。两次对账之间，滞后累积 K TTI，然后降至 $\lceil (R+S)/T_\text{TTI} \rceil$。峰值滞后 = $K + \lceil (R+S)/T_\text{TTI} \rceil$。∎

**POWDER 参数实例化：** S ≈ 0.5ms，R ≈ 2ms，K = 100 → **Δ ≤ 103 TTI = 103ms**。

**推论（M2 关联）：** 对于仿真水平线 H ≫ Δ，初始状态偏差引入的系统性偏差为 O(Δ·r̄/H)，其中 r̄ 是单 TTI 奖励上界。在 H=200、Δ=103 时：偏差 < 单个 TTI 奖励的 52%——被自举置信区间宽度所吸收。

### 协议

```
真实侧（MacReconciler，REP :5560）         孪生侧（MacReconcilerClient，REQ）
─────────────────────────────────         ──────────────────────────────────
每次从真实 EdgeRIC 接收：                  每隔 K TTIs：
  stamp_metrics(tti_seq++, epoch)           发送 ReconciliationRequest{
  reconciler.update(metrics)                    twin_tti_seq, twin_global_hash,
  pub.send(stamped_proto)                       reconcile_epoch, wall_clock_us}
  reconciler.serve_one() [非阻塞]
                                          接收 ReconciliationResponse：
收到 ReconciliationRequest 时：             若 hash_matched: Δ=0
  若 twin_hash == 本侧 hash：               否则：
    回应 hash_matched=True                    应用 full_state 差异
  否则：                                      Δ = real_tti_seq - twin_tti_seq
    回应 hash_matched=False                    记录 Δ, rtt_ms
    + 完整 UE 状态
    epoch++
```

**Protobuf Schema 新增字段**（`metrics.proto`）：
- `UeMetrics`：+`harq_state`（repeated uint32）、+`scheduler_queue_age`、+`bsr_kbytes`、+`ue_state_hash`（SHA-256[:16]）
- `Metrics`：+`tti_seq`（uint64）、+`wall_clock_us`、+`global_state_hash`、+`reconcile_epoch`

新文件：`reconciliation.proto` — `ReconciliationRequest`、`ReconciliationResponse`、`UeState`

### 实现

- `mac_equivalence.py`：`hash_ue_state`、`hash_global_state`、`stamp_metrics`、`ReconcileStats`、`MacReconciler`（真实侧 REP）、`MacReconcilerClient`（孪生侧 REQ）、`derive_delta_bound`
- `mac_equivalence_twin_client.py`：孪生侧启动器 + E5 评估 + mock 回路测试服务器
- `state_sync.py:layer2_mac_sync`：对每个转发的 Metrics 加盖时间戳，非阻塞式处理对账请求，每 500 epoch 记录日志
- `proto/metrics_pb2.py`、`proto/reconciliation_pb2.py`：通过 protoc 重新生成

### 核心声明
- 在 POWDER 有线模式下（K=100），实测 p99(Δ) ≤ 103 TTI（E5 验证）
- 协议开销：K=100 时额外 ZMQ 流量 ≤ 1 KB/s
- 对账延迟不阻塞 layer2_mac_sync（serve_one 通过 RCVTIMEO=0 实现非阻塞）

---

## 九、实验与评估计划

| 实验 | 度量指标 | 涉及机制 | 是否需要 OTA | 脚本 |
|---|---|---|---|---|
| **E1** 漂移动态特性 | D(t) 轨迹、τ_coherence、触发事件相关性 | M1 | 否（有线） | `state_sync.py --drift-log` |
| **E2** 决策排名准确率 | Spearman ρ ≥ 0.8；Top-1 ≥ 80%；不安全拒绝率 ≥ 95% | M2 | **是** | `e3_rl_eval.py` |
| **E3** 选择性精度 Pareto | CPU 节省 ≥3×，排名精度损失 <5% | M3 | 否（有线 trace） | `m3_overhead_eval.py` |
| **E4** 端到端安全性 | 4 小时 OTA 运行，M2 门控激活，0 次不安全部署，CPU < 50% | M1+M2+M3 | **是** | 集成运行 |
| **E5** MAC 等价性上界 | 实测 p99(Δ) vs 理论 103 TTI 上界 | M4 | 否（有线） | `mac_equivalence_twin_client.py` |
| **E6** Samsung 差距弥合 | 孪生↔真实 KPI 相似度 ≥ 70%（对比 Samsung 39%） | M1+M4 | **是** | `state_sync.py` 中的 `fidelity_monitor` |

**有线实验（现可运行）：** E1、E3、E5  
**依赖 OTA（等待 POWDER 批准）：** E2、E4、E6

### 消融实验（W9）
分别禁用每个机制运行决策引擎：
- M1 关闭（无漂移校准）：测量排名相关性随时间的下降
- M3 强制 full_iq：测量 CPU 开销比例
- M4 关闭（无对账）：注入人工 200ms 延迟，测量排名偏差
- M2 替换为启发式（若最大副本奖励 > 阈值则部署）：测量误报率

---

## 十、量化声明（论文表格）

| 声明 | 目标值 | 测量方式 | 对应实验 |
|---|---|---|---|
| 决策引擎 Top-1 准确率 | ≥ 80% | oracle 选出地面真值最优策略的比例 | E2（OTA） |
| 不安全策略拒绝率 | ≥ 95% | σ=1.5 扰动策略被正确拒绝的比例 | E2（OTA） |
| Spearman ρ | ≥ 0.8 | 5 个候选策略：oracle 排名 vs 地面真值 | E2（OTA） |
| CPU 节省（M3） | ≥ 3× | 平均每 TTI 计算量：full_iq vs M3 自适应 | E3（有线） |
| 排名精度损失（M3） | < 5% | 相对于始终 full_iq 的 Spearman ρ 下降 | E3（有线） |
| MAC 状态上界（M4） | p99(Δ) ≤ 103 TTI | K=100 下的实测 p99 | E5（有线） |
| KPI 相似度（M1+M4） | ≥ 70% | 吞吐量/BLER：真实↔孪生匹配度 vs Samsung 39% | E6（OTA） |
| 端到端安全率（M2 门控） | 4小时内0次不安全部署 | 导致真实 KPI 下降的部署次数 | E4（OTA） |

---

## 十一、进度路线图

| 周次 | 工作内容 | 交付物 | 状态 |
|---|---|---|---|
| W1 | M1 漂移度量 + 触发机制 + E1 日志 | `channel_converter.py` ✓，`state_sync.py` M1 集成 ✓ | ✅ 完成 |
| W2 | M3 Tiny_Twin C 补丁 + `selective_fidelity.py` | `apply_channelmod.c` ✓，`selective_fidelity.py` ✓ | ✅ 完成 |
| W3 | M3 state_sync 集成 + E3 评估脚本 | `state_sync.py` M3 集成 ✓，`m3_overhead_eval.py` ✓ | ✅ 完成 |
| W4 | M4 proto 升级 + `mac_equivalence.py` + L2 集成 | proto 重新生成 ✓，`mac_equivalence.py` ✓，`mac_equivalence_twin_client.py` ✓ | ✅ 完成 |
| W5 | M2 决策引擎 + E2 评估框架 | `counterfactual_oracle.py` ✓，`e3_rl_eval.py` ✓ | ✅ 完成 |
| **W6** | **运行有线 E1 + E3 + E5；开始论文写作** | E1/E3/E5 图表；引言 + 系统章节草稿 | ← **下一步** |
| **W7** | **POWDER OTA 干跑**（获批后） | 首次 OTA 日志；排查硬件问题 | 等待 OTA 批准 |
| W8 | E2 + E4 + E6 OTA 完整运行 | 所有评估图表 | 等待 OTA 批准 |
| W9 | 消融实验表；M3 学习预测器；论文精修 | 论文初稿 v1 | — |
| W10 | 内部审阅；修改；投稿 | 提交 | — |

**当前状态：** W1–W5 全部完成（所有代码已实现并通过冒烟测试）。W6 可立即启动。

---

## 十二、风险与应对措施

| 风险 | 可能性 | 影响 | 应对措施 |
|---|---|---|---|
| OTA 批准延迟 > 1 周 | 中 | 阻塞 E2/E4/E6 | E1/E3/E5 有线实验覆盖 6 个实验中的 3 个；足以支撑一篇有线模式论文；OTA 补充其余实验 |
| 有线模式下 D(t) 始终接近 0（无信道事件） | 低 | E1 图表缺乏说服力 | 通过调整真实 gNB 发射功率模拟信道事件；使用跨天多场次 trace |
| M3 CPU 节省在目标硬件上测得 < 3× | 中 | M3 核心声明失效 | mac_only 完全跳过 PHY → 节省取决于 PHY 在总计算中的占比；提前在目标机器上测量；若节省 < 3×，将决策水平线收短至 H=50 |
| M4 实测 Δ > 理论 103 TTI | 低 | 形式化证明失效 | 增大 K_reconcile；检查 POWDER 网络抖动峰值；该上界为最坏情况，p99 应显著低于上界 |
| 审稿人问"为什么不用 Simeone PPI" | 高 | 贡献受质疑 | PPI 是离线方法；我们是在线方法。在 E2 中加入 PPI 对比基线。明确说明两种方法是互补的（E2 讨论部分） |
| 初始状态偏差引起的决策偏差主导了置信区间 | 低 | 排名不可靠 | Δ ≤ 103ms ≪ H=200ms；如有需要可将 H 增加至 400；自举置信区间在偏差存在时会自动变宽 |
| Tiny_Twin C 补丁破坏 OAI 构建 | 低 | 阻塞 M3 | 默认模式 = full_iq（环境变量未设时原代码路径不变）；补丁完全是新增的；不影响现有 CI |

---

## 十三、相关工作详述

### 13.1 为什么 EdgeRIC 不够

EdgeRIC（NSDI'24）是最接近的已有工作。它提供了一个连接实时 gNB 的实时 RIC 和一个用于策略训练的离线模拟器。我们的关键区别：

1. **实时孪生 vs 离线孪生。** EdgeRIC 的模拟器回放预先收集的 trace。我们的孪生与真实网络并行实时运行，每 TTI 同步一次。这使得在当前信道条件（而非上次采集 trace 时的条件）下，对策略进行在线评估成为可能。

2. **无漂移检测。** EdgeRIC 的模拟器不能自校准。如果自上次采集 trace 以来真实信道已经改变，模拟器给出错误的预测。M1 明确检测并修正这一问题。

3. **无形式化状态保证。** EdgeRIC 不分析模拟器状态与真实 gNB 状态之间的偏差。M4 以形式化方式提供了这一保证。

4. **无反事实决策引擎。** EdgeRIC 在模拟器中训练策略并部署。它没有任何机制在部署前以统计保证对多个候选策略进行排名。

### 13.2 为什么 OWDT 不够

OWDT 将预计算的 Sionna 射线追踪 CIR 注入 OAI。这是开环的：CIR 从静态 3D 环境模型离线计算，而非来自实时信道测量。存在两个问题：

1. **静态校准。** 注入的 CIR 无法适应真实信道变化。OWDT 的作者承认这一局限，并将实时校准留作未来工作。M1 正是那个未来工作。

2. **无安全决策引擎。** OWDT 是信道注入的基础设施，而非用于策略评估的工具。它没有对调度策略进行排名的机制。

### 13.3 为什么 Simeone PPI 是互补而非竞争关系

Simeone 等（arXiv 2507.07067）使用预测驱动推断（PPI）来修正孪生 KPI 估计中的离线偏差。他们的方法需要来自真实网络的少量标注数据来校准偏差修正项。这对周期性离线分析非常有效，但不适用于我们的在线场景——我们需要在当前 TTI 窗口内做出实时决策。

两种方法正交：PPI 事后修正聚合偏差；M1 通过保持孪生实时校准来防止偏差累积。在 E2 中，我们将加入 PPI 作为基线，以展示：若无在线校准，决策引擎的排名方差更高；而 M1 的实时校准在在线场景中已经取代了 PPI 式修正的需求。

### 13.4 Tiny-Twin 作为基础

我们的系统直接构建于 Tiny-Twin（2025）之上，后者证明了 CPU 原生全协议栈 OAI 的可行性。我们在 Tiny-Twin 的容器基础设施之上贡献了完整的实时同步层（M1+M4）、计算自适应 PHY 模式（M3）和反事实决策引擎（M2）。若没有 Tiny-Twin 作为基础，我们的工作将需要 FPGA 硬件（如 ColO-RAN）或牺牲全协议栈精度。论文中明确声明："Tiny-Twin 提供了执行基础；dt_sync 提供了实时同步与安全决策引擎层。"

---

## 十四、论文结构

```
1. 引言（2.5 页）
   1.1 调度策略更新问题
   1.2 现有孪生为何失败（漂移、无形式化保证、计算瓶颈）
   1.3 我们的方案：dt_sync — M1–M4 贡献
   1.4 主要结果（表1：E1–E6 核心数据汇总）

2. 背景与相关工作（1.5 页）
   2.1 O-RAN 调度 xApp
   2.2 RAN 数字孪生（EdgeRIC、ColO-RAN、OWDT、Tiny-Twin）
   2.3 反事实评估与自举置信区间
   2.4 形式化时钟同步（NTP、PTP — M4 类比）

3. 系统设计（3 页）
   3.1 架构概述（三层数据流）
   3.2 M1：漂移感知校准
   3.3 M3：选择性精度 PHY
   3.4 M4：形式化 MAC 状态等价性（定理 + 证明）

4. 反事实调度决策引擎（2 页）   [M2]
   4.1 问题形式化（改进量、置信区间）
   4.2 仿真协议（EdgeRIC 原生、N 副本异步）
   4.3 部署门控（CI_lower > 0 且 safety_violations == 0）
   4.4 与 M3 的交互（每次决策调用的模式选择）

5. 评估（4 页）
   5.1 实验设置（POWDER B210、Tiny-Twin、候选策略）
   5.2 E1：漂移动态特性与 τ_coherence
   5.3 E2：决策排名准确率（Spearman ρ、Top-1、拒绝率）
   5.4 E3：选择性精度 Pareto（CPU 节省 vs 精度）
   5.5 E4：端到端安全性运行（4 小时 OTA）
   5.6 E5：MAC 状态上界实证验证
   5.7 E6：Samsung 39% 差距弥合
   5.8 消融实验

6. 结论（0.5 页）
```

---

## 十五、实现文件参考（当前状态）

### 全部已实现 — 冒烟测试通过

| 文件 | 作用 | 涉及机制 | 冒烟测试 |
|---|---|---|---|
| `orchestrator/channel_converter.py` | CIR 生成 + KL 漂移度量 | M1 | `python channel_converter.py` → 对齐时D≈0，漂移时D=18.8 |
| `orchestrator/state_sync.py` | 双层同步编排节点 | M1+M3+M4 | `python -m py_compile` 通过 |
| `orchestrator/counterfactual_oracle.py` | 影子仿真 + 自举置信区间 | M2 | `python counterfactual_oracle.py` → max_cqi 排名第一 |
| `orchestrator/e3_rl_eval.py` | E2 排名评估框架 | M2/E2 | `--mock` → 不安全拒绝率100%，CSV 写入正常 |
| `orchestrator/selective_fidelity.py` | 模式选择器 | M3 | `python selective_fidelity.py` → 全部5项断言通过 |
| `orchestrator/m3_overhead_eval.py` | E3 开销扫描 | M3/E3 | `--mock` → 节省表格打印，CSV 写入正常 |
| `orchestrator/mac_equivalence.py` | 哈希 + 对账协议 | M4 | `python mac_equivalence.py` → 上界=103 TTI 验证通过 |
| `orchestrator/mac_equivalence_twin_client.py` | 孪生侧客户端 + E5 | M4/E5 | `--mock` → REQ/REP 协议运行，epoch 追踪正常 |
| `proto/metrics.proto` + `metrics_pb2.py` | 带时间戳的指标 Schema | M4 | import 测试通过 |
| `proto/reconciliation.proto` + `reconciliation_pb2.py` | 对账 Schema | M4 | import 测试通过 |
| `Tiny_Twin/radio/rfsimulator/apply_channelmod.c` | 模式感知 CIR 应用 | M3 | 编译时检查（需 OAI 构建环境） |

### 有线实验 — 可立即运行

```bash
# E1：漂移动态特性（需要真实 gNB SSH 访问）
python orchestrator/state_sync.py --real-host powder-real --twin-host powder-twin \
    --drift-log logs/e1_drift.csv

# E3：开销扫描（完全本地）
python orchestrator/m3_overhead_eval.py --mock --n-samples 2000 --m-trials 10 \
    --out logs/e3_overhead.csv

# E5：MAC 等价性上界（完全本地回路测试）
python orchestrator/mac_equivalence_twin_client.py --mock --k-reconcile 100 \
    --duration 3600 --out logs/e5_reconcile.csv
```

### OTA 实验 — 等待 POWDER 批准

```bash
# E2：决策排名（需要 POWDER 上的真实 + 孪生节点）
python orchestrator/e3_rl_eval.py --twin-host powder-twin --real-host powder-gnb \
    --collect-ground-truth --gt-duration 300 --out logs/e2_oracle_eval.csv

# E4：端到端安全性（4 小时运行）
python orchestrator/state_sync.py --real-host powder-gnb --twin-host powder-twin \
    --drift-log logs/e4_drift.csv &
# 随后通过 e3_rl_eval.py 定期触发决策评估

# E6：KPI 相似度
python orchestrator/state_sync.py --real-host powder-gnb --twin-host powder-twin \
    --monitor-only  # 使用 fidelity_monitor
```

---

## 十六、旧版贡献映射（v1 → v2）

| v1 贡献 | v2 处置方式 |
|---|---|
| #1 CIR 同步协议 | → **M1**（重新定位为漂移感知闭环校准；KL 漂移度量是新增元素） |
| #2 孪生集成安全过滤器 | → **M2**（泛化为带自举置信区间的量化反事实决策引擎） |
| #3 BC → 孪生引导在线 RL | → **消融基线**（在 E2/E4 中，EdgeRIC-PPO checkpoint 作为5个候选策略之一保留） |
| #4 精度衰减分析 | → **并入 M1**（τ_coherence 是精度生命周期度量；E1 图表替代原 E4） |
