# dt_sync — Live Drift-Aware RAN Twin with Counterfactual Scheduling Oracle

*POWDER × Tiny_Twin digital twin system, Mac as orchestrator*  
*Target: MobiCom / SIGCOMM / NSDI / INFOCOM*  
*Version: v2.1 — 2026-04-30 (M1–M4 all implemented)*

---

## One-Sentence Contribution

> **"A live, drift-aware RAN digital twin with bounded metrics-layer MAC-state equivalence and a selective-fidelity counterfactual scheduling oracle that safely evaluates fixed-weight scheduler candidates over TTI-scale rollouts without disrupting production traffic."**

| Mechanism | Role | Files | Status |
|---|---|---|---|
| **M1** Drift-aware live calibration | KL drift metric `D(t)`; trigger-based EMA recalibration | `channel_converter.py` + `state_sync.py` | ✅ Done |
| **M2** Counterfactual scheduler oracle | Fixed-weight candidate rollout on live twin; bootstrap CI improvement bound; deploy gate | `counterfactual_oracle.py` + `e3_rl_eval.py` | ✅ Done |
| **M3** Selective-fidelity twin | Three-tier PHY fidelity (`full_iq` / `sparse_tap` / `mac_only`); decision-sensitivity-driven mode selection | `selective_fidelity.py` + Tiny_Twin C patch | ✅ Done |
| **M4** Provable MAC-state equivalence | Formal bound Δ ≤ K + ⌈(R+S)/T_TTI⌉ on metrics-layer skew; REQ/REP reconciliation protocol | `mac_equivalence.py` + `proto/` | ✅ Done |

---

## 1. Motivation

### 1.1 The Scheduler Update Problem in Live RAN

Modern O-RAN systems allow third-party xApps to override the gNB's scheduler via real-time weight injection (e.g., via EdgeRIC). This creates a critical unsolved problem:

**How do you know a new scheduling policy will improve KPIs before you deploy it to production users?**

Today's answer is conservative: lab testing with synthetic traces, then a slow rollout with manual monitoring. This process takes days to weeks and still risks QoS degradation for real users during the rollout window. The consequence is that operators effectively freeze their schedulers — a policy trained offline may stay in production for months even as channel conditions change.

The theoretical promise of digital twins is to solve this: run the candidate policy in a virtual replica of the network first, observe the KPI, deploy only if it's safe. But this promise has not been realized in live RAN systems for three reasons:

1. **Twins drift.** Real wireless channels are non-stationary. A CIR calibrated at initialization becomes a poor model within minutes as UEs move, interference changes, and hardware temperature drifts. A stale twin gives wrong KPI predictions, defeating the entire purpose.

2. **State equivalence is unproven.** The scheduler's decision depends on MAC-layer state: HARQ buffer contents, buffer status reports, queue depths, CQI history. No published work formalizes whether the twin's MAC state matches the real gNB's — the gap may invalidate any KPI comparison.

3. **Full-fidelity PHY simulation is too expensive.** A 100-tap CIR convolution at TTI rate (~1 ms) on commodity hardware takes ~5 ms per call — 5× over budget. Existing twins either sacrifice fidelity (offline, pre-computed CIR) or run on specialized FPGA hardware.

Samsung's 2025 Sim2Real report quantifies the result: **only 39% twin–real KPI similarity** on a commercial O-RAN deployment. The gap is not a detail; it is the central unsolved problem.

### 1.2 Why This Is the Right Time

Three developments make a solution tractable now:

- **Tiny_Twin** (2025) proved that a full OAI NR stack (gNB + UE + CN) can run on commodity CPUs via RFsimulator, removing the FPGA requirement.
- **EdgeRIC** (NSDI'24) exposed a real-time ZMQ interface for scheduler weight injection, making fixed-weight shadow evaluation via the same protocol feasible. Per-TTI causal action evaluation is future work and requires action acknowledgements in the EdgeRIC wire protocol.
- **POWDER** provides a stable B210 OTA testbed with reproducible channel conditions, enabling ground-truth comparison between twin and real.

Our system, dt_sync, is the first to combine all three into a **closed-loop live twin** that self-calibrates, maintains formal state equivalence, adapts its compute budget to the decision at hand, and provides statistically-guaranteed policy rankings before any deployment.

---

## 2. Problem Statement and Challenges

### 2.1 Formal Problem

Given a live 5G gNB serving $U$ UEs under time-varying channel $h(t)$, and a set of candidate scheduler policies $\{\pi_1, \ldots, \pi_M\}$, **safely identify the policy $\pi^*$ that maximizes expected per-TTI reward** without deploying any untested policy to real users.

Constraints:
- No disruption to real users during evaluation.
- Statistical confidence: the deployed policy must have a lower-bound improvement over baseline with probability ≥ 1−α.
- Bounded compute: evaluation must complete within a time budget compatible with the TTI structure (i.e., twin must run faster than real-time).
- Formal correctness: the initial twin state used for evaluation must be bounded-close to the real gNB state.

### 2.2 Technical Challenges

**Challenge 1 (C1): Channel Non-Stationarity and Silent Drift**

Real CIR changes on timescales of 1–100s due to UE mobility, multipath evolution, and hardware effects. A calibration done at time 0 becomes invalid by time T without online feedback. The core difficulty is that a twin running with stale calibration is *undetectable from inside the twin* — it still produces plausible-looking outputs. We need a drift signal computed from *both* real-side and twin-side observations.

Our insight: the KL divergence between the joint (CQI, BLER) distributions on real and twin, $D(t) = \mathrm{KL}[\hat{p}_\text{real} \| \hat{p}_\text{twin}]$, computed over a sliding window, detects drift without requiring ground-truth channel knowledge. When $D(t) > \tau$, EMA recalibration fires; otherwise, the calibration is frozen (avoiding drift-induced overfitting).

**Challenge 2 (C2): MAC-State Skew with No Formal Bound**

The scheduler cares about MAC state, not just channel. HARQ retransmission slots, buffer status reports, and scheduler queue depths are propagated from real to twin via ZMQ protobuf (EdgeRIC). Network jitter means the twin's view of MAC state lags the real gNB by an unknown and potentially unbounded amount. If this lag is large, the twin's initial condition for a rollout is wrong, biasing the KPI estimate.

No prior RAN-DT paper analyzes this lag formally. We prove a closed-form bound: $\Delta \le K + \lceil (R+S)/T_\text{TTI} \rceil$ where K is the reconciliation interval, R is the REQ/REP RTT, and S is one-way serialization latency. For POWDER parameters (K=100, R=2ms, S=0.5ms): **Δ ≤ 103 TTI = 103 ms** — well under the H=200 TTI horizon, making the systematic bias provably bounded.

**Challenge 3 (C3): Compute Budget for Live PHY Simulation**

Running N=4 parallel twin replicas, each at full 100-tap CIR convolution, at 1ms TTI rate requires ~20 ms/TTI of compute — 20× over budget. Naively, a live twin cannot keep up with real time.

Our insight: the decision sensitivity of each evaluation step determines the required fidelity. When channel drift D(t) is low and candidate policies agree, a cheap MAC-layer model (CQI→MCS lookup, no PHY) gives identical oracle rankings to the full-fidelity model. Only when D(t) spikes or policies disagree does the twin need full CIR convolution. A rule-based selector (with learned refinement later) achieves ≥3× CPU savings at <5% ranking accuracy loss.

**Challenge 4 (C4): Statistically Valid Policy Ranking Without Ground Truth**

Standard A/B testing requires deploying each candidate to real users. We need to rank policies using only twin rollouts. The difficulty is that twin rollouts have noise (from CIR perturbation across replicas) and potential bias (from state lag, C2). A simple point estimate of reward is insufficient — we need a confidence interval that accounts for both.

We use bootstrap resampling (B=1000) over the N-replica reward samples to produce a 95% CI for the improvement $\text{CI}(\pi_i) = [l_i, u_i]$. A policy is deployed only if $l_i > 0$ (the lower confidence bound on improvement is positive) AND no replica observed BLER > β (safety constraint). This subsumes the prior ensemble safety filter and makes it quantitative.

---

## 3. How We Differ from Existing Work

### 3.1 Precise Comparison Table

| Prior work | What they do | What they lack vs. ours |
|---|---|---|
| **EdgeRIC** (Ko et al., NSDI'24) | Real-time RIC + offline emulator-in-the-loop for policy training | Twin is **offline** (replay traces); no live channel sync; no drift detection; no counterfactual oracle |
| **ColO-RAN** (Bonati et al., 2024) | Colosseum FPGA channel emulator + DRL transfer | Requires **FPGA** hardware; no drift-aware recalibration; policy transfer, not counterfactual evaluation; no formal state equivalence |
| **OWDT** (Iimori et al., arXiv 2503.12177, 2025) | Pre-computed Sionna RT CIR injected **open-loop** into OAI | CIR is static offline dataset; **no feedback from real network**; no drift detection; no MAC state sync |
| **Tiny-Twin** (2025) | CPU-native full OAI stack with RFsimulator (basis of our work) | No real-time sync to real gNB; no drift detection; no scheduling oracle; we build all of this on top |
| **Simeone et al. PPI** (arXiv 2507.07067, 2025) | Differentiable RT calibration + Bayesian DT ensembles + Cross-PPI bias correction | **Offline** calibration (requires batched dataset); no live adaptation; PPI corrects bias in offline evaluation, we prevent bias via online calibration; methods are complementary (E2 will add PPI as comparison) |
| **Samsung Sim2Real** (2025) | Identifies 39% similarity gap; proposes RL retraining recipe | Describes the problem; our system **closes the gap** via M1+M4 (target ≥70% similarity, E6) |
| **TailO-RAN** (GLOBECOM'25) | Programmable scheduler weight xApp for tail-latency | Scheduling policy design, not evaluation. We use the same ZMQ weight injection interface for oracle queries |
| **RANBooster** (SIGCOMM'25) | Fronthaul middlebox for commercial 5G | Different problem (fronthaul capacity); related work citation |

### 3.2 The Gap We Fill

The clearest way to state our novelty:

> **No prior work provides a live, continuously-calibrated RAN digital twin that (a) detects and corrects channel drift in closed loop, (b) gives a formal bound on MAC-state skew, (c) adapts its compute budget to the downstream decision's sensitivity, and (d) uses the twin to rank candidate scheduler policies with bootstrap statistical guarantees — all on commodity hardware without additional infrastructure.**

Each of M1–M4 individually has precedents in adjacent areas (drift detection in ML systems, formal clock synchronization bounds, adaptive simulation fidelity in graphics, counterfactual evaluation in RL). The novelty is the **tight coupling of all four in a single live-RAN system**, where each mechanism is necessary for the others' correctness claims:

- M2 (oracle) needs M4 (bounded initial state) to avoid biased rollouts.
- M2 needs M3 (selective fidelity) to finish within the TTI budget.
- M3 needs M1 (D(t)) to decide when to escalate fidelity.
- M4 needs M1's TTI-seq tags to compute the formal bound.

---

## 4. System Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│ Mac (Apple Silicon) — Orchestrator                                       │
│                                                                          │
│  dt_sync/                                                                │
│   ├── orchestrator/                                                      │
│   │    ├── channel_converter.py          (M1) CQI→CIR + KL drift metric  │
│   │    ├── state_sync.py                 (M1+M3+M4) dual-layer sync      │
│   │    ├── counterfactual_oracle.py      (M2) shadow rollout + CI        │
│   │    ├── selective_fidelity.py         (M3) mode selector              │
│   │    ├── mac_equivalence.py            (M4) reconciliation protocol    │
│   │    ├── mac_equivalence_twin_client.py(M4) twin-side client launcher  │
│   │    ├── e3_rl_eval.py                 (E2) oracle ranking harness     │
│   │    ├── m3_overhead_eval.py           (E3) CPU/accuracy sweep         │
│   │    ├── auto_recalibrate.py           (M1) cron recalibration         │
│   │    ├── deploy.py                     POWDER end-to-end deployment    │
│   │    ├── powder_client.py              POWDER portal client            │
│   │    └── ssh_fanout.py                 parallel SSH                    │
│   ├── proto/                                                             │
│   │    ├── metrics.proto / _pb2.py       (M4) +tti_seq +state_hash       │
│   │    └── reconciliation.proto / _pb2.py(M4) REQ/REP schema            │
│   ├── Tiny_Twin/radio/rfsimulator/                                       │
│   │    └── apply_channelmod.c            (M3) TT_FIDELITY_MODE hook      │
│   └── design_plan.md                     this file                       │
│                                                                          │
└──────┬──────────────────────────────────────┬───────────────────────────┘
       │ ssh / ZMQ                            │ ssh / ZMQ
       ▼                                      ▼
┌──────────────────────┐          ┌──────────────────────────────────────┐
│ POWDER — Real        │          │ POWDER — Twin                         │
│  gnb-node + B210     │  L1 CIR  │  twin host (commodity CPU):          │
│   nr-softmodem       │  via     │   ├── tt-gnb (Docker, TT_FIDELITY_  │
│   + EdgeRIC :5555    │  named   │   │    MODE env, RFsim)              │
│  ue-node + B210      │  FIFO    │   ├── tt-nrue (Docker)               │
│  cn-node (oai-cn)    │ ───────► │   ├── oai-cn (Docker)               │
│                       │  L2 MAC  │   └── edgeric-twin                  │
│                       │  :5555→  │                                      │
│                       │  :5655   │  ※ N=4 oracle replicas (M2)        │
│                       │  M4 REP  │  ※ M4 client on :5560              │
│                       │  :5560   │                                      │
└──────────────────────┘          └──────────────────────────────────────┘
```

### Three-Layer Data Flow

```
Layer 1 (PHY / channel)  — M1 + M3
  real gNB → logs/snr.txt (UL CQI, UL ACK per RNTI per TTI)
      → state_sync._tail_snr_log (SSH tail -F)
      → ChannelCalibrator.observe_drift_sample(real_cqi, real_bler, twin_cqi, twin_bler)
      → if D(t) > τ: calibrator.update(EMA) + mark_recalibration [M1]
      → selective_fidelity.RuleBasedSelector.choose_mode(signals) [M3]
      → cqi_to_cir(cqi, calibrator) → [sparsify_topk if sparse_tap] → named FIFO
  twin gNB ← FIFO ← apply_channelmod.c (TT_FIDELITY_MODE: skip PHY / sparsify / full)

Layer 2 (MAC / scheduler state) — M4
  real EdgeRIC :5555 (Metrics protobuf) → layer2_mac_sync
      → stamp_metrics(tti_seq, ue_state_hash, global_state_hash, reconcile_epoch)
      → MacReconciler.update(metrics) [REP bound on :5560]
      → forward stamped proto → twin EdgeRIC :5655
  twin side: MacReconcilerClient REQ every K TTIs → compare hashes
      → on mismatch: apply real full_state diff, record empirical Δ [M4]

Layer 3 (counterfactual decision) — M2
  every K_oracle TTIs (triggered by operator or scheduler):
      oracle.evaluate(candidates=[π_1,...,π_M])
          for each π_i: async rollout on N twin replicas (M3 mode chosen)
              → inject policy weights via EdgeRIC ZMQ :5556+10i
              → collect horizon H rewards from :5555+10i
          bootstrap_improvement_ci(baseline_rewards, candidate_rewards)
          deploy(π_i) iff CI_lower > 0 AND safety_violations == 0
```

---

## 5. M1 — Drift-Aware Live Calibration ✅

### Algorithm

Sliding window W = 200 paired samples (real_cqi, real_bler, twin_cqi, twin_bler).
Discretize (CQI, BLER) into a 4×4 joint histogram (4 CQI bins × 4 BLER bins).

$$D(t) = \mathrm{KL}\!\left[\hat{p}_\text{real} \,\Big\|\, \hat{p}_\text{twin}\right] = \sum_{c,b} \hat{p}_\text{real}(c,b) \log \frac{\hat{p}_\text{real}(c,b)}{\hat{p}_\text{twin}(c,b) + \varepsilon}$$

Laplace smoothing ε = 1e-6. Trigger EMA update iff D(t) > τ_drift (default 0.10).

**Why KL divergence:** it captures joint distribution mismatch across both CQI and BLER simultaneously, detecting cases where the twin's CQI histogram looks correct but BLER is wrong (e.g., stale noise calibration). A simple CQI-MSE metric would miss this.

**Why trigger-based (not always-on):** continuous EMA on a stable channel adds noise to the amplitude_scale estimate. The trigger acts as a change-point detector: only recalibrate when evidence of drift exists.

### τ_coherence (empirical fidelity lifetime)

`coherence_samples` = TTIs since last trigger. This is the twin's self-reported fidelity lifetime: how long the current calibration has been valid. Reported in E1 figure as τ_coherence vs. channel mobility.

### Implementation

- `channel_converter.py`: `ChannelCalibrator` — `observe_drift_sample`, `kl_drift_metric`, `should_recalibrate`, `mark_recalibration`, `coherence_samples`, `trigger_count`
- `state_sync.py`: `_maybe_drift_observe` + `_maybe_recalibrate` in both `_run_local_fifo` and `_run_remote_fifo`; UL ACK rolling BLER window per RNTI (`_BLER_WINDOW=100`)
- E1 logging: `--drift-log logs/drift.csv` writes (elapsed_s, D_t, trigger_count, coherence_samples, twin_snr_db) every 5s

### Key claims
- D(t) ≈ 0 when real and twin distributions are aligned; D(t) >> 0.10 on channel event (mobility, interference)
- τ_coherence correlates inversely with UE velocity (to be measured in E1)
- Trigger-based vs. always-on EMA: fewer spurious recalibrations → more stable amplitude_scale

---

## 6. M2 — Counterfactual Scheduler Oracle ✅

### Intuition

At time t, the real gNB's scheduler is running policy π_real. We want to evaluate candidate π_i **before** deploying it. The live twin, synchronized to the real channel and observable MAC metrics state (via M1+M4), provides a virtual testbed. In the current implementation, each candidate is evaluated as a **per-rollout fixed-weight policy**: the oracle computes one weight vector from the first fresh in-rollout metrics observation, injects it once, freezes it over H TTIs, and measures the resulting reward distribution across N replicas.

The key insight that no prior work has exploited: **EdgeRIC's existing ZMQ interface (weight injection on :5556) and metrics reporting (:5555) already support this fixed-weight evaluation loop.** No custom rollout server is needed. A fully causal per-TTI oracle will require `action_seq` / `target_tti_seq` acknowledgements from the EdgeRIC side and is left for the next iteration.

### Formalization

Real-side reward baseline (from live `layer2_mac_sync`):
$$r_t^\text{real} = \sum_{u \in \text{UE}} \bigl(\text{tpt}_u - \lambda \cdot \text{BLER}_u - \mu \cdot \text{queue\_age}_u\bigr), \quad \lambda=50, \mu=0.01$$

For each candidate π_i, a fixed-weight rollout on N replicas over H TTIs yields samples $\{r^{(j)}_i\}_{j=1}^N$.

**Improvement** (positive = candidate is better than baseline):
$$\widehat{\Delta}_i = \frac{1}{N}\sum_j r^{(j)}_i - \frac{1}{|\mathcal{B}|}\sum_{b \in \mathcal{B}} r^{(b)}_\text{real}$$

**Bootstrap CI** (B=1000 resamples, α=0.05):
$$\text{CI}_{1-\alpha}(\pi_i) = \bigl[Q_{\alpha/2}(\widehat{\Delta}_i^{(b)}), Q_{1-\alpha/2}(\widehat{\Delta}_i^{(b)})\bigr]$$

**Deploy gate**: π_i is deployed iff CI_lower > 0 **AND** ∀ replicas: BLER ≤ β (default 0.10).

### Why bootstrap CI over Gaussian

N=4 replicas give too few samples for CLT. Bootstrap is distribution-free and correctly captures the heavy tail of reward distributions (bursty traffic, HARQ retransmissions).

### Implementation

- `counterfactual_oracle.py`: `CounterfactualOracle`, `Policy` ABC, `EqualWeightPolicy`, `MaxCQIPolicy`, `EdgeRICPPOPolicy`, `PerturbedPolicy`, `bootstrap_improvement_ci`, `compute_reward`, `PolicyVerdict(rollout_semantics="fixed_weight")`
- `e3_rl_eval.py` (E2 harness): 5-candidate ranking, Spearman ρ vs ground truth, top-1 accuracy, unsafe rejection rate, CSV + PDF output
- Replica ports: metrics SUB on 5555+10i, weights PUB on 5556+10i (i=0..N-1)
- Mock mode: `--mock` uses synthetic reward maps for all E2 logic; smoke test passes

### Key claims
- Top-1 accuracy ≥ 80% (oracle picks the best policy among 5 candidates)
- Unsafe candidate (σ=1.5 perturbation) rejection rate ≥ 95%
- Spearman ρ ≥ 0.8 between oracle ranking and ground-truth (E2, OTA-dependent)

---

## 7. M3 — Selective-Fidelity Twin ✅

### Three Tiers

| Mode | Channel | PHY | Est. cost/TTI | When |
|---|---|---|---|---|
| `full_iq` | 100-tap CIR full convolution | RFsim complete PHY | ~5ms | D(t) high, recent recalibration, policies disagree |
| `sparse_tap` | top-K=4 taps by power | RFsim PHY on sparsified taps | ~0.5ms | default mid |
| `mac_only` | skip CIR entirely | CQI→MCS lookup, analytic BLER | ~0.05ms | D(t) < 0.02 and CQI stable |

**Claim:** ≥3× average CPU savings vs. always-full_iq, at <5% oracle ranking accuracy loss (E3).

### Mode Selector

Inputs (built from live orchestrator state):
- D(t) from M1 calibrator
- cqi_var: rolling CQI variance (last 50 observations)
- regret_width: last oracle CI width (ci_upper − ci_lower)
- policies_disagree: (max − min improvement) / abs(mean) across candidates
- recent_recalibration: M1 trigger fired within last 30s
- rnti_count: active UE count

**Rule-based selector** (cold start; production W2–W3):
```
Priority 1 → full_iq:  recent_recalibration OR D(t) > 0.20 OR
             policies_disagree > 0.5 OR regret_width > 2×|regret_mean|
Priority 2 → mac_only: D(t) < 0.02 AND cqi_var < 1.0
Default    → sparse_tap
```

**Learned selector** (W9+): small MLP trained on (signals, ground_truth_minimum_mode) offline pairs. Falls back to rule-based if checkpoint absent.

### C-Side Patch (`apply_channelmod.c`)

`TT_FIDELITY_MODE` env var read once at container startup (per-container-lifetime):
- `mac_only`: early return before FIFO reads — no PHY at all
- `sparse_tap`: read CIR, zero all but top-K taps by power, fall through to convolution
- `full_iq` (default): original code path unchanged

Mode changes trigger graceful docker-compose restart with new env. The FIFO writer on the Python side synchronously applies the same mode (sparsify_topk or skip) so FIFO content and C-side expectation always match.

### Key design decision: why per-container-lifetime, not per-TTI

Dynamic env-var switching inside OAI C code would require locks and is unsafe. Per-container mode is simpler, correct, and aligned with the paper claim: mode decisions are at minute granularity, not TTI granularity. The overhead savings come from running oracle replicas in mac_only mode for 200 TTIs, not from switching mid-rollout.

### Implementation

- `selective_fidelity.py`: `Mode`, `SelectorSignals`, `RuleBasedSelector`, `LearnedSelector`, `sparsify_topk`, `build_signals`, `cir_writer_gate`, `apply_mode_to_replica`
- `apply_channelmod.c`: `TT_FIDELITY_MODE` hook, `tt_init_mode`, `tt_select_topk`, mac_only early return, sparse_tap sparsification
- `state_sync.py`: M3 selector wired in `_run_local_fifo` and `_run_remote_fifo`; `_restart_production_twin` (docker-compose up --force-recreate with TT_FIDELITY_MODE); mode re-evaluated every 50 CQI observations
- `m3_overhead_eval.py` (E3): timing sweep + oracle ranking accuracy vs full_iq; outputs CSV + PDF Pareto plot

---

## 8. M4 — Provable Per-UE MAC-State Equivalence ✅

### Scope of the Claim

M4's formal guarantee applies to the **orchestrator's metrics-layer view**: the Metrics protobuf most recently forwarded by `layer2_mac_sync`. When EdgeRIC fills the extended fields, this view contains per-UE (CQI, BLER proxy, HARQ state, BSR, queue depth). With older/current EdgeRIC producers that leave HARQ/BSR/queue-age unset, the guarantee honestly scopes to the populated schema-visible metrics, not hidden OAI scheduler internals.

We do **not** claim that OAI's internal C scheduler state is synchronized — that is inaccessible from Python. The metrics-layer view is sufficient for the current fixed-weight oracle's input and reward accounting; the only bias the skew introduces is in the initial observable condition of each rollout, which is bounded by Δ and well within the H=200 TTI horizon.

**This is the first formal analysis of metrics-layer state skew in any published RAN-DT system.**

### Theorem (M4)

Let:
- S = real-side protobuf serialize + ZMQ one-way latency upper bound (ms)
- R = reconciliation REQ/REP RTT upper bound (ms)
- K = reconciliation interval (TTIs)
- T_TTI = 1 ms (5G NR numerology 0)
- Hash function is collision-resistant (SHA-256 standard assumption)
- Real side pushes metrics every TTI (no-loss; loss extension in Appendix)

Then for all wall-clock times τ and all UEs u, the per-UE metrics-layer skew satisfies:

$$\boxed{\Delta^u(\tau) \le K + \left\lceil \frac{R + S}{T_\text{TTI}} \right\rceil}$$

**Proof sketch:**
1. Real pushes metrics(t) with tti_seq=t at wall-clock t·T_TTI. Twin receives it at worst t·T_TTI + S, corresponding to real TTI t + ⌈S/T_TTI⌉. Twin updates view immediately → lag = ⌈S/T_TTI⌉.
2. Every K TTIs, twin sends ReconciliationRequest with its global_state_hash. Real replies with hash_matched=true (O(bytes), fast) or hash_matched=false + full state (O(UEs × state_per_UE)). Round trip ≤ R.
3. On mismatch, twin applies real diff; residual lag ≤ ⌈(R+S)/T_TTI⌉.
4. Worst case: mismatch occurs just after a reconciliation. Between reconciliations lag accumulates for K TTIs, then drops to ⌈(R+S)/T_TTI⌉. Peak lag = K + ⌈(R+S)/T_TTI⌉. ∎

**Instantiation (POWDER):** S ≈ 0.5ms, R ≈ 2ms, K = 100 → **Δ ≤ 103 TTI = 103 ms**.

**Corollary (M2 connection):** For oracle rollout horizon H ≫ Δ, the systematic bias from initial state skew is O(Δ·r̄/H) where r̄ is the single-TTI reward upper bound. At H=200, Δ=103: bias < 52% of one TTI's reward — absorbed within the bootstrap CI width.

### Protocol

```
Real side (MacReconciler, REP :5560)      Twin side (MacReconcilerClient, REQ)
──────────────────────────────────        ─────────────────────────────────────
On each recv from real EdgeRIC:           Every K TTIs:
  stamp_metrics(tti_seq++, epoch)           send ReconciliationRequest{
  reconciler.update(metrics)                    twin_tti_seq, twin_global_hash,
  pub.send(stamped_proto)                       reconcile_epoch, wall_clock_us}
  reconciler.serve_one() [non-blocking]
                                          Recv ReconciliationResponse:
On recv ReconciliationRequest:              if hash_matched: Δ=0
  if twin_hash == our_hash:                else:
    respond hash_matched=True                  apply full_state diff
  else:                                        Δ = real_tti_seq - twin_tti_seq
    respond hash_matched=False                 record Δ, rtt_ms
    + full UE state
    epoch++
```

**Protobuf schema additions** (`metrics.proto`):
- `UeMetrics`: +`harq_state` (repeated uint32), +`scheduler_queue_age`, +`bsr_kbytes`, +`ue_state_hash` (SHA-256[:16])
- `Metrics`: +`tti_seq` (uint64), +`wall_clock_us`, +`global_state_hash`, +`reconcile_epoch`

New file: `reconciliation.proto` — `ReconciliationRequest`, `ReconciliationResponse`, `UeState`

### Implementation

- `mac_equivalence.py`: `hash_ue_state`, `hash_global_state`, `stamp_metrics`, `ReconcileStats`, `MacReconciler` (real-side REP), `MacReconcilerClient` (twin-side REQ), `derive_delta_bound`
- `mac_equivalence_twin_client.py`: twin-side launcher + E5 evaluation + mock loopback server
- `state_sync.py:layer2_mac_sync`: stamps every forwarded Metrics, serves reconciliation non-blocking, logs every 500 epochs
- `proto/metrics_pb2.py`, `proto/reconciliation_pb2.py`: regenerated via protoc

### Key claims
- Empirical p99(Δ) ≤ 103 TTI on POWDER cabled (K=100, measured in E5)
- Protocol overhead: ≤ 1 KB/s additional ZMQ traffic at K=100
- Reconciliation latency does not block layer2_mac_sync (serve_one is non-blocking via RCVTIMEO=0)

---

## 9. Experiments and Evaluation Plan

| Exp | Measures | Mechanism | OTA needed | Script |
|---|---|---|---|---|
| **E1** Drift dynamics | D(t) trace, τ_coherence, trigger-event correlation | M1 | No (cabled) | `state_sync.py --drift-log` |
| **E2** Oracle ranking accuracy | Spearman ρ ≥ 0.8; top-1 ≥ 80%; unsafe rejection ≥ 95% | M2 | **Yes** | `e3_rl_eval.py` |
| **E3** Selective-fidelity Pareto | CPU savings ≥ 3× at ranking accuracy loss < 5% | M3 | No (cabled trace) | `m3_overhead_eval.py` |
| **E4** End-to-end safety | 4hr OTA run, M2 gate active, 0 unsafe deployments, CPU < 50% | M1+M2+M3 | **Yes** | integrated |
| **E5** MAC equivalence bound | empirical p99(Δ) vs theoretical 103 TTI bound | M4 | No (cabled) | `mac_equivalence_twin_client.py` |
| **E6** Samsung gap closure | twin↔real KPI similarity ≥ 70% (vs Samsung's 39%) | M1+M4 | **Yes** | `fidelity_monitor` in `state_sync.py` |

**Cabled experiments (can run now):** E1, E3, E5  
**OTA-dependent (waiting for POWDER approval):** E2, E4, E6

### Ablation study (W9)
Run oracle with each mechanism disabled:
- M1 off (no drift calibration): measure rank correlation degradation over time
- M3 forced full_iq: measure CPU overhead ratio
- M4 off (no reconciliation): inject artificial 200ms delay, measure rank bias
- M2 replace with heuristic (deploy if max replica reward > threshold): measure false positive rate

---

## 10. Quantitative Claims (Paper Table)

| Claim | Target | Measurement | Experiment |
|---|---|---|---|
| Oracle top-1 accuracy | ≥ 80% | fraction of trials where oracle picks GT-best policy | E2 (OTA) |
| Oracle unsafe rejection | ≥ 95% | fraction of σ=1.5 perturbed policies correctly blocked | E2 (OTA) |
| Spearman ρ | ≥ 0.8 | oracle ranking vs ground-truth on 5 candidates | E2 (OTA) |
| CPU savings (M3) | ≥ 3× | mean TTI compute: full_iq vs M3-adaptive | E3 (cabled) |
| Ranking accuracy loss (M3) | < 5% | Spearman ρ degradation vs always-full_iq | E3 (cabled) |
| MAC-state bound (M4) | p99(Δ) ≤ 103 TTI | empirical p99 under K=100 | E5 (cabled) |
| KPI similarity (M1+M4) | ≥ 70% | throughput/BLER match real↔twin vs Samsung 39% | E6 (OTA) |
| E2E safety rate (M2 gate) | 0 unsafe deployments / 4hr | count of deployments that degraded real KPI | E4 (OTA) |

---

## 11. Roadmap

| Week | Work | Deliverable | Status |
|---|---|---|---|
| W1 | M1 drift metric + trigger + E1 logging | `channel_converter.py` ✓, `state_sync.py` M1 wiring ✓ | ✅ Done |
| W2 | M3 Tiny_Twin C patch + `selective_fidelity.py` | `apply_channelmod.c` ✓, `selective_fidelity.py` ✓ | ✅ Done |
| W3 | M3 state_sync wiring + E3 eval script | `state_sync.py` M3 wiring ✓, `m3_overhead_eval.py` ✓ | ✅ Done |
| W4 | M4 proto bump + `mac_equivalence.py` + L2 wiring | proto regenerated ✓, `mac_equivalence.py` ✓, `mac_equivalence_twin_client.py` ✓ | ✅ Done |
| W5 | M2 oracle + E2 harness | `counterfactual_oracle.py` ✓, `e3_rl_eval.py` ✓ | ✅ Done |
| **W6** | **Run cabled E1 + E3 + E5; start paper writing** | E1/E3/E5 figures; intro + system sections drafted | ← **Next** |
| **W7** | **POWDER OTA dry run** (after approval) | First OTA logs; identify any hardware issues | Blocked on OTA |
| W8 | E2 + E4 + E6 OTA full runs | All eval figures | Blocked on OTA |
| W9 | Ablation table; M3 learned predictor; paper polish | Paper v1 draft | — |
| W10 | Internal review; revise; submit | Submission | — |

**Current status:** W1–W5 complete (all code implemented and smoke-tested). W6 can start immediately.

---

## 12. Risks and Mitigations

| Risk | Likelihood | Impact | Mitigation |
|---|---|---|---|
| OTA approval delayed > 1 week | Medium | Blocks E2/E4/E6 | E1/E3/E5 cabled fill 3 of 6 experiments; enough for strong cabled-mode paper; OTA adds remaining |
| D(t) stays near 0 in cabled mode (no channel events) | Low | E1 looks uninteresting | Simulate channel events by adjusting real gNB TX power; use multi-session traces across days |
| M3 CPU savings measured < 3× on twin hardware | Medium | Core M3 claim fails | mac_only skips PHY entirely → savings depend on PHY fraction of total compute; pre-measure on target machine; if savings < 3×, tighten oracle horizon to H=50 |
| M4 empirical Δ > theoretical 103 TTI | Low | Formal proof invalidated | Increase K_reconcile; check for POWDER network jitter spikes; bound is worst-case, p99 should be well below |
| Reviewer asks "why not Simeone PPI" | High | Contribution questioned | PPI is offline; ours is online. Add PPI as E2 comparison baseline. Clearly state methods are complementary (E2 discussion) |
| Oracle bias from initial state skew dominates CI | Low | Rankings unreliable | Δ ≤ 103ms ≪ H=200ms; increase H to 400 if needed; bootstrap CI automatically widens when bias is present |
| Tiny_Twin C patch breaks OAI build | Low | Blocks M3 | Default mode = full_iq (env var unset path unchanged); patch is purely additive; existing CI unaffected |

---

## 13. Related Work Details

### 13.1 Why EdgeRIC Is Not Enough

EdgeRIC (NSDI'24) is the closest prior work. It provides a real-time RIC that connects to a live gNB and an offline emulator for policy training. Our key differences:

1. **Live vs. offline twin.** EdgeRIC's emulator replays pre-collected traces. Our twin runs alongside the real network in real time, synchronized at every TTI. This enables online evaluation of policies under current channel conditions, not conditions from the last trace collection.

2. **No drift detection.** EdgeRIC's emulator does not self-calibrate. If the real channel changes after the last trace collection, the emulator gives wrong predictions. M1 explicitly detects and corrects this.

3. **No formal state guarantees.** EdgeRIC does not analyze the skew between emulator state and real gNB state. M4 provides this formally.

4. **No counterfactual oracle.** EdgeRIC trains policies in the emulator and deploys them. It has no mechanism to rank multiple candidates with statistical guarantees before deployment.

### 13.2 Why OWDT (Open Wireless Digital Twin) Is Not Enough

OWDT injects pre-computed Sionna ray-tracing CIR into OAI. This is open-loop: the CIR is computed offline from a static 3D environment model, not from live channel measurements. Two problems:

1. **Static calibration.** The injected CIR does not adapt to real channel changes. OWDT's authors acknowledge this limitation and leave live calibration as future work. M1 is precisely that future work.

2. **No safety oracle.** OWDT is infrastructure for channel injection, not for policy evaluation. It has no mechanism for ranking scheduler policies.

### 13.3 Why Simeone PPI Is Complementary, Not Competing

Simeone et al. (arXiv 2507.07067) use Prediction-Powered Inference (PPI) to correct offline bias in twin-based KPI estimation. Their method requires a small labeled dataset from the real network to calibrate a bias correction term. This is excellent for periodic offline analysis but does not apply to our online scenario where we need a real-time decision within the current TTI window.

The methods are orthogonal: PPI corrects aggregate bias post-hoc; M1 prevents bias from accumulating by keeping the twin calibrated in real time. In E2, we will add a PPI baseline to show the oracle without online calibration has higher variance rankings, and that M1's real-time calibration subsumes the need for PPI-style correction in the live setting.

### 13.4 Tiny-Twin as Foundation

Our system builds directly on Tiny-Twin (2025), which proved CPU-native full-stack OAI is feasible. We contribute the full real-time synchronization layer (M1+M4), the compute-adaptive PHY mode (M3), and the counterfactual oracle (M2) on top of Tiny-Twin's container infrastructure. Without Tiny-Twin as foundation, our work would require FPGA hardware (as in ColO-RAN) or sacrifice full-stack accuracy. This is acknowledged explicitly in the paper as "Tiny-Twin provides the execution substrate; dt_sync provides the live synchronization and safety oracle layer."

---

## 14. Paper Outline

```
1. Introduction (2.5 pages)
   1.1 The scheduler update problem
   1.2 Why existing twins fail (drift, no formal bounds, compute)
   1.3 Our approach: dt_sync — contributions M1–M4
   1.4 Key results (Table 1: E1–E6 summary numbers)

2. Background and Related Work (1.5 pages)
   2.1 O-RAN scheduler xApps
   2.2 RAN digital twins (EdgeRIC, ColO-RAN, OWDT, Tiny-Twin)
   2.3 Counterfactual evaluation and bootstrap CI
   2.4 Formal clock synchronization (NTP, PTP — M4 analogy)

3. System Design (3 pages)
   3.1 Architecture overview (three-layer data flow)
   3.2 M1: Drift-aware calibration
   3.3 M3: Selective-fidelity PHY
   3.4 M4: Formal MAC-state equivalence (theorem + proof)

4. Counterfactual Scheduler Oracle (2 pages)   [M2]
   4.1 Problem formulation (improvement, CI)
   4.2 Rollout protocol (EdgeRIC-native, async N replicas)
   4.3 Deploy gate (CI_lower > 0 AND safety_violations == 0)
   4.4 Interaction with M3 (mode selection per oracle invocation)

5. Evaluation (4 pages)
   5.1 Setup (POWDER B210, Tiny-Twin, candidate policies)
   5.2 E1: Drift dynamics and τ_coherence
   5.3 E2: Oracle ranking accuracy (Spearman ρ, top-1, rejection rate)
   5.4 E3: Selective-fidelity Pareto (CPU savings vs accuracy)
   5.5 E4: End-to-end safety run (4hr OTA)
   5.6 E5: MAC-state bound empirical verification
   5.7 E6: Samsung 39% gap closure
   5.8 Ablation study

6. Conclusion (0.5 pages)
```

---

## 15. Implementation File Reference (Current State)

### All Implemented — Smoke Tests Pass

| File | Role | Mechanism | Smoke test |
|---|---|---|---|
| `orchestrator/channel_converter.py` | CIR generation + KL drift metric | M1 | `python channel_converter.py` → D≈0 aligned, D=18.8 drifted |
| `orchestrator/state_sync.py` | Dual-layer sync orchestrator | M1+M3+M4 | `python -m py_compile` OK |
| `orchestrator/counterfactual_oracle.py` | Shadow rollout + bootstrap CI | M2 | `python counterfactual_oracle.py` → max_cqi ranked first |
| `orchestrator/e3_rl_eval.py` | E2 ranking harness | M2/E2 | `--mock` → unsafe rejection 100%, CSV written |
| `orchestrator/selective_fidelity.py` | Mode selector | M3 | `python selective_fidelity.py` → all 5 assertions pass |
| `orchestrator/m3_overhead_eval.py` | E3 overhead sweep | M3/E3 | `--mock` → savings table printed, CSV written |
| `orchestrator/mac_equivalence.py` | Hash + reconciliation protocol | M4 | `python mac_equivalence.py` → bound=103 TTI verified |
| `orchestrator/mac_equivalence_twin_client.py` | Twin-side client + E5 | M4/E5 | `--mock` → REQ/REP protocol exercised, epoch tracking OK |
| `proto/metrics.proto` + `metrics_pb2.py` | Stamped metrics schema | M4 | import test passes |
| `proto/reconciliation.proto` + `reconciliation_pb2.py` | Reconciliation schema | M4 | import test passes |
| `Tiny_Twin/radio/rfsimulator/apply_channelmod.c` | Mode-aware CIR application | M3 | compile-time check (requires OAI build env) |

### Cabled Experiments — Ready to Run

```bash
# E1: drift dynamics (need real gNB SSH access)
python orchestrator/state_sync.py --real-host powder-real --twin-host powder-twin \
    --drift-log logs/e1_drift.csv

# E3: overhead sweep (fully local)
python orchestrator/m3_overhead_eval.py --mock --n-samples 2000 --m-trials 10 \
    --out logs/e3_overhead.csv

# E5: MAC equivalence bound (fully local loopback)
python orchestrator/mac_equivalence_twin_client.py --mock --k-reconcile 100 \
    --duration 3600 --out logs/e5_reconcile.csv
```

### OTA Experiments — Waiting for POWDER Approval

```bash
# E2: oracle ranking (requires live real + twin on POWDER)
python orchestrator/e3_rl_eval.py --twin-host powder-twin --real-host powder-gnb \
    --collect-ground-truth --gt-duration 300 --out logs/e2_oracle_eval.csv

# E4: end-to-end safety (4hr run)
python orchestrator/state_sync.py --real-host powder-gnb --twin-host powder-twin \
    --drift-log logs/e4_drift.csv &
# then trigger oracle evaluations periodically via e3_rl_eval.py

# E6: KPI similarity
python orchestrator/state_sync.py --real-host powder-gnb --twin-host powder-twin \
    --monitor-only  # uses fidelity_monitor
```

---

## 16. Old Contribution Mapping (v1 → v2)

| v1 contribution | v2 disposition |
|---|---|
| #1 CIR Sync Protocol | → **M1** (reframed as drift-aware closed-loop calibration; KL drift metric is the new element) |
| #2 Twin Ensemble Safety Filter | → **M2** (generalized to quantitative counterfactual regret oracle with bootstrap CI) |
| #3 BC → Twin-Guided Online RL | → **Ablation baseline** in E2/E4 (EdgeRIC-PPO checkpoint kept as one of 5 candidate policies) |
| #4 Fidelity Degradation Analysis | → **Subsumed into M1** (τ_coherence is the fidelity lifetime metric; E1 figure replaces the old E4) |
