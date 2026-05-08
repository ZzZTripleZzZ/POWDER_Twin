"""
M2 — Counterfactual Scheduler Oracle

EdgeRIC-native shadow policy evaluation: each oracle replica is a complete
OAI+EdgeRIC twin. The oracle injects per-UE scheduling weights via ZMQ into
each replica's EdgeRIC, reads back per-TTI Metrics protobuf, computes rewards,
and returns bootstrap confidence intervals on policy improvement over baseline.

Current live semantics are per-rollout fixed-weight candidate evaluation:
the oracle computes one weight vector from the first fresh in-rollout Metrics
message, injects it once, and freezes it for the H-TTI horizon. Per-TTI causal
actions need EdgeRIC-side action_seq / target_tti_seq acknowledgements and are
intentionally left for the next iteration.

Deployment gate:
    deploy(π_i) iff CI_lower(improvement_i) > 0
                AND safety_violations == 0

No custom rollout server needed — reuses existing EdgeRIC ZMQ control plane.
"""

import asyncio
import sys
import time
from abc import ABC, abstractmethod
from collections import deque
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional, Sequence

import numpy as np
import zmq
import zmq.asyncio

sys.path.insert(0, str(Path(__file__).parent.parent / "proto"))
import metrics_pb2
import control_weights_pb2

# EdgeRIC standard ports (replica_i uses BASE + 10*i)
METRICS_PORT_BASE = 5555
WEIGHTS_PORT_BASE = 5556

# Reward coefficients  (match EdgeRIC RL training)
REWARD_LAMBDA = 50.0    # BLER penalty
REWARD_MU     = 0.01    # queue-age penalty


# ── Reward ─────────────────────────────────────────────────────────────────────

def compute_reward(
    metrics: "metrics_pb2.Metrics",
    lam: float = REWARD_LAMBDA,
    mu: float  = REWARD_MU,
) -> float:
    """
    r_t = Σ_u [ tpt_u  -  λ·bler_proxy_u  -  μ·queue_age_u ]

    tpt         : tx_bytes * 8 / 1e6  (Mbps)
    bler_proxy  : 1 if tx_bytes == 0 else 0  (packet-drop indicator)
    queue_age   : dl_buffer / (tx_bytes + 1)

    Note on plan §六 reward formula
    --------------------------------
    The plan states ``- λ·BLER_u`` with λ=50, but the EdgeRIC Metrics
    proto exposed by upstream (see ``proto/metrics.proto``) does not
    carry a per-UE BLER field — only ``tx_bytes`` and ``dl_buffer``.
    We substitute a packet-drop indicator (1 if no bytes transmitted in
    this TTI). For per-UE drops at modest rates the indicator's
    expectation tracks ``BLER × ack_period`` linearly within a sliding
    window, so the reward gradient direction is preserved even though
    the absolute scale differs from the plan's λ=50·BLER. To restore the
    exact plan formula, add a ``bler`` field to ``UeMetrics`` and
    populate it on the EdgeRIC producer side.
    """
    total = 0.0
    for ue in metrics.ue_metrics:
        tpt        = ue.tx_bytes * 8 / 1e6
        bler_proxy = 1.0 if ue.tx_bytes == 0 else 0.0
        queue_age  = ue.dl_buffer / (ue.tx_bytes + 1)
        total     += tpt - lam * bler_proxy - mu * queue_age
    return total


# ── Bootstrap CI ───────────────────────────────────────────────────────────────

def bootstrap_improvement_ci(
    baseline_rewards: list,
    candidate_rewards: list,
    B: int   = 1000,
    alpha: float = 0.05,
    seed: int = 0,
) -> tuple:
    """
    Bootstrap CI for improvement = E[candidate] - E[baseline].
    Returns (mean_improvement, ci_lower, ci_upper).
    Positive mean_improvement → candidate outperforms baseline.
    CI_lower > 0 → confidently better at (1-alpha) level.
    """
    rng = np.random.default_rng(seed)
    a   = np.asarray(baseline_rewards, dtype=float)
    b   = np.asarray(candidate_rewards, dtype=float)
    na, nb = len(a), len(b)
    diffs = np.array([
        rng.choice(b, size=nb, replace=True).mean()
        - rng.choice(a, size=na, replace=True).mean()
        for _ in range(B)
    ])
    return (
        float(diffs.mean()),
        float(np.quantile(diffs, alpha / 2)),
        float(np.quantile(diffs, 1 - alpha / 2)),
    )


# ── Policies ───────────────────────────────────────────────────────────────────

class Policy(ABC):
    """Scheduler policy: maps per-TTI metrics observation to per-UE weights."""

    @abstractmethod
    def act(self, metrics: "metrics_pb2.Metrics") -> dict:
        """Return {rnti: weight}.  Weights need not sum to 1."""
        ...

    @property
    @abstractmethod
    def name(self) -> str: ...


class EqualWeightPolicy(Policy):
    """Proportional-fair baseline: equal weight to every active UE."""
    @property
    def name(self) -> str: return "equal_weight"

    def act(self, metrics):
        ues = [ue.rnti for ue in metrics.ue_metrics]
        if not ues: return {}
        w = 1.0 / len(ues)
        return {r: w for r in ues}


class MaxCQIPolicy(Policy):
    """Greedy: full resources to the UE with highest CQI."""
    @property
    def name(self) -> str: return "max_cqi"

    def act(self, metrics):
        if not metrics.ue_metrics: return {}
        best = max(metrics.ue_metrics, key=lambda u: u.cqi)
        return {u.rnti: (1.0 if u.rnti == best.rnti else 0.0)
                for u in metrics.ue_metrics}


class EdgeRICPPOPolicy(Policy):
    """Load a PPO policy checkpoint trained via EdgeRIC RL."""

    def __init__(self, checkpoint_path: str):
        self._ckpt  = checkpoint_path
        self._net   = None
        self._ready = False

    @property
    def name(self) -> str:
        return f"ppo({Path(self._ckpt).stem})"

    def _load(self):
        if self._ready:
            return
        try:
            import torch
            from ml.bc_pretrain import PolicyNet
            ckpt    = torch.load(self._ckpt, map_location="cpu")
            n_ue    = ckpt.get("n_ue", 2)
            obs_dim = ckpt.get("obs_dim", n_ue * 5)
            net     = PolicyNet(obs_dim, n_ue)
            net.load_state_dict(ckpt.get("model", ckpt))
            net.eval()
            self._net   = net
            self._ready = True
        except Exception as e:
            print(f"[Oracle] PPO load failed ({e}); using EqualWeight fallback")
            self._net   = EqualWeightPolicy()
            self._ready = True

    def act(self, metrics):
        self._load()
        if isinstance(self._net, EqualWeightPolicy):
            return self._net.act(metrics)
        import torch
        obs = []
        for ue in metrics.ue_metrics:
            obs.extend([
                ue.cqi / 15.0, ue.snr / 30.0,
                ue.tx_bytes / 1e6, ue.dl_buffer / 1e5, ue.ul_buffer / 1e5,
            ])
        obs_t = torch.tensor(obs, dtype=torch.float32).unsqueeze(0)
        with torch.no_grad():
            w = self._net(obs_t)[0].numpy()
        return {u.rnti: float(wi) for u, wi in zip(metrics.ue_metrics, w)}


class PerturbedPolicy(Policy):
    """Add Gaussian noise to a base policy — used to test oracle's safety gate."""

    def __init__(self, base: Policy, sigma: float = 0.3, seed: int = 0):
        self._base  = base
        self._sigma = sigma
        self._rng   = np.random.default_rng(seed)

    @property
    def name(self) -> str:
        return f"perturbed({self._base.name},σ={self._sigma:.2f})"

    def act(self, metrics):
        base_w = self._base.act(metrics)
        return {r: max(0.0, w + float(self._rng.normal(0, self._sigma)))
                for r, w in base_w.items()}


# ── Verdict ────────────────────────────────────────────────────────────────────

@dataclass
class PolicyVerdict:
    policy_id:         str
    mean_improvement:  float   # E[candidate] - E[baseline]; positive = better
    ci_lower:          float   # (1-alpha) lower bound on improvement
    ci_upper:          float
    safety_violations: int     # replicas where BLER > threshold
    deploy:            bool    # ci_lower > 0 AND safety_violations == 0
    n_rollouts:        int     # alive replicas used
    mean_reward:       float   # raw mean candidate reward
    baseline_mean:     float   # raw mean baseline reward
    rollout_semantics: str = "fixed_weight"  # current M2 live/mock semantics


# ── ZMQ helpers ────────────────────────────────────────────────────────────────

def _pack_weights(weights: dict, ran_index: int = 0) -> bytes:
    """Serialize {rnti: weight} dict to SchedulingWeights protobuf bytes."""
    msg = control_weights_pb2.SchedulingWeights()
    msg.ran_index = ran_index
    for rnti, w in weights.items():
        msg.weights.append(float(rnti))
        msg.weights.append(float(w))
    return msg.SerializeToString()


# ── Oracle ─────────────────────────────────────────────────────────────────────

class CounterfactualOracle:
    """
    Live counterfactual evaluation of candidate scheduler policies in twin replicas.

    Usage (mock demo):
        oracle = CounterfactualOracle(mock=True, n_replicas=2, horizon_ttis=50)
        verdicts = oracle.evaluate([EqualWeightPolicy(), MaxCQIPolicy()])
        for v in verdicts:
            print(v.policy_id, v.mean_improvement, v.deploy)

    Usage (real twin):
        oracle = CounterfactualOracle(twin_host="powder-twin", n_replicas=4)
        oracle.update_baseline(reward)   # call once per TTI from layer2_mac_sync consumer
        verdicts = oracle.evaluate(candidates)
    """

    def __init__(
        self,
        twin_host:        str   = "localhost",
        n_replicas:       int   = 4,
        horizon_ttis:     int   = 200,
        alpha:            float = 0.05,
        bootstrap_iters:  int   = 1000,
        bler_threshold:   float = 0.10,
        cir_perturb_sigma: float = 0.5,
        mode_selector            = None,   # M3 hook
        seed:             int   = 0,
        mock:             bool  = False,
        warmup_ttis:      int   = 5,
    ):
        self._host          = twin_host
        self._n_replicas    = n_replicas
        self._horizon       = horizon_ttis
        self._alpha         = alpha
        self._B             = bootstrap_iters
        self._bler_thresh   = bler_threshold
        self._perturb_sigma = cir_perturb_sigma
        self._mode_selector = mode_selector
        self._rng           = np.random.default_rng(seed)
        self._mock          = mock
        # PUB/SUB slow-joiner mitigation: discard this many warmup TTIs after
        # socket settle so we know the twin's metrics PUB is connected and
        # producing fresh data before we count rewards.
        self._warmup_ttis   = warmup_ttis

        # Rolling buffer of real-side per-TTI rewards (populated by caller)
        self._baseline_rewards: deque = deque(maxlen=horizon_ttis)
        self._last_verdicts: list     = []
        self._eval_count              = 0

    # ── Public API ─────────────────────────────────────────────────────────────

    def update_baseline(self, reward: float) -> None:
        """Push one real-side TTI reward. Call from layer2_mac_sync consumer."""
        self._baseline_rewards.append(reward)

    def evaluate(
        self,
        candidates: Sequence[Policy],
        baseline:   Optional[Policy] = None,
    ) -> list:
        """
        Evaluate all candidates. Blocks until all rollouts finish.
        Returns list[PolicyVerdict], one per candidate.
        """
        return asyncio.run(self._evaluate_async(candidates, baseline))

    def latest_verdicts(self) -> list:
        return list(self._last_verdicts)

    def latest_summary(self) -> dict:
        if not self._last_verdicts:
            return {}
        best = max(self._last_verdicts, key=lambda v: v.mean_improvement)
        deployable = [v for v in self._last_verdicts if v.deploy]
        return {
            "eval_count":    self._eval_count,
            "n_candidates":  len(self._last_verdicts),
            "n_deployable":  len(deployable),
            "best_policy":   best.policy_id,
            "best_improvement": best.mean_improvement,
            "baseline_mean": best.baseline_mean,
        }

    # ── Async internals ────────────────────────────────────────────────────────

    async def _evaluate_async(self, candidates, baseline):
        # Collect or generate baseline rewards
        baseline_rewards = list(self._baseline_rewards)
        if len(baseline_rewards) < 10:
            bl = baseline or EqualWeightPolicy()
            print(f"[Oracle] Cold start: running {bl.name} baseline on replica 0 "
                  f"({self._horizon} TTIs)...", flush=True)
            res = await self._rollout_one(0, bl)
            baseline_rewards = res[0]

        verdicts = []
        for policy in candidates:
            t0 = time.time()
            tasks   = [self._rollout_one(i, policy) for i in range(self._n_replicas)]
            results = await asyncio.gather(*tasks, return_exceptions=True)

            all_rewards       = []
            safety_violations = 0
            alive             = 0
            for res in results:
                if isinstance(res, Exception):
                    # Treat replica timeouts/errors conservatively as a
                    # safety violation - we did not observe stable behavior
                    # so we cannot certify the candidate is safe.
                    print(f"[Oracle] Replica error: {res} (counted as unsafe)", flush=True)
                    safety_violations += 1
                    continue
                rewards, max_bler = res
                if rewards:
                    all_rewards.extend(rewards)
                    alive += 1
                    if max_bler > self._bler_thresh:
                        safety_violations += 1
                else:
                    # Replica returned but produced no reward samples -
                    # also treat as unsafe (PUB/SUB slow-joiner, weight
                    # injection silently dropped, or twin crash).
                    safety_violations += 1

            elapsed = time.time() - t0

            if alive < 2:
                verdicts.append(PolicyVerdict(
                    policy_id=policy.name,
                    mean_improvement=float("-inf"),
                    ci_lower=float("-inf"), ci_upper=float("-inf"),
                    safety_violations=safety_violations,
                    deploy=False, n_rollouts=alive,
                    mean_reward=float("nan"),
                    baseline_mean=float(np.mean(baseline_rewards)),
                    rollout_semantics="fixed_weight",
                ))
                print(f"[Oracle] {policy.name}: only {alive} alive replicas — skipped "
                      f"({elapsed:.1f}s)", flush=True)
                continue

            # Per-evaluate seed so successive evaluate() calls don't replay
            # the same bootstrap sample sequence (reproducibility hazard
            # flagged in code review). _eval_count is monotonic per oracle.
            mean_imp, lo, hi = bootstrap_improvement_ci(
                baseline_rewards, all_rewards, self._B, self._alpha,
                seed=self._eval_count,
            )
            deploy = (lo > 0) and (safety_violations == 0)

            v = PolicyVerdict(
                policy_id=policy.name,
                mean_improvement=mean_imp,
                ci_lower=lo, ci_upper=hi,
                safety_violations=safety_violations,
                deploy=deploy,
                n_rollouts=alive,
                mean_reward=float(np.mean(all_rewards)),
                baseline_mean=float(np.mean(baseline_rewards)),
                rollout_semantics="fixed_weight",
            )
            verdicts.append(v)
            sign = "✓ DEPLOY" if deploy else "✗ hold"
            print(
                f"[Oracle] {policy.name:40s}  imp={mean_imp:+.2f}  "
                f"CI=[{lo:+.2f},{hi:+.2f}]  safe_viol={safety_violations}  "
                f"{sign}  ({elapsed:.1f}s)",
                flush=True,
            )

        self._last_verdicts = verdicts
        self._eval_count   += 1
        return verdicts

    async def _rollout_one(self, replica_id: int, policy: Policy):
        """Run policy for horizon_ttis on one replica. Returns (rewards, max_bler)."""
        if self._mock:
            return await self._rollout_mock(replica_id, policy)
        return await self._rollout_live(replica_id, policy)

    async def _rollout_live(self, replica_id: int, policy: Policy):
        """
        Live rollout on one replica. Semantics: per-rollout fixed weights.

        EdgeRIC's PUB/SUB weight channel has no per-TTI ack, so the previous
        per-TTI weight injection had no causal guarantee that weights at TTI t
        actually applied to the reward observed at TTI t (see C2 in the
        modification plan). We therefore inject the policy's act() output once
        per rollout — computed from the first in-rollout metrics observation —
        and freeze it for the whole H-TTI horizon. M2's claim is restated as
        "rank candidate policies by their initial-weight choice"; design plan
        §6.2 documents this shift from per-TTI to per-rollout fixed semantics.
        """
        metrics_port = METRICS_PORT_BASE + 10 * replica_id
        weights_port = WEIGHTS_PORT_BASE + 10 * replica_id

        ctx = zmq.asyncio.Context.instance()
        sub = ctx.socket(zmq.SUB)
        sub.connect(f"tcp://{self._host}:{metrics_port}")
        sub.setsockopt(zmq.SUBSCRIBE, b"")

        pub = ctx.socket(zmq.PUB)
        pub.connect(f"tcp://{self._host}:{weights_port}")
        await asyncio.sleep(0.1)   # let ZMQ connections settle (slow joiner)

        # Warmup: discard the first few metrics so we know the SUB is hooked
        # up and the twin EdgeRIC is producing fresh data before counting.
        last_tti = -1
        warmup_seen = 0
        try:
            while warmup_seen < self._warmup_ttis:
                try:
                    raw = await asyncio.wait_for(sub.recv(), timeout=1.0)
                except asyncio.TimeoutError:
                    break
                m_warm = metrics_pb2.Metrics()
                m_warm.ParseFromString(raw)
                if int(m_warm.tti_seq) > last_tti:
                    last_tti = int(m_warm.tti_seq)
                    warmup_seen += 1
        except Exception as e:
            print(f"[Oracle] replica {replica_id} warmup error: {e}", flush=True)

        rewards       = []
        max_bler      = 0.0
        weights_sent  = False
        try:
            for _ in range(self._horizon):
                try:
                    raw = await asyncio.wait_for(sub.recv(), timeout=1.0)
                except asyncio.TimeoutError:
                    rewards.append(-1000.0)
                    break

                m = metrics_pb2.Metrics()
                m.ParseFromString(raw)

                # Inject weights exactly once. After this point the policy is
                # frozen for the rollout, and observed rewards reflect that
                # fixed weight assignment.
                if not weights_sent:
                    weights = policy.act(m)
                    if weights:
                        await pub.send(_pack_weights(weights))
                    weights_sent = True

                rewards.append(compute_reward(m))

                if m.ue_metrics:
                    n_drop   = sum(1 for u in m.ue_metrics if u.tx_bytes == 0)
                    max_bler = max(max_bler, n_drop / len(m.ue_metrics))
        finally:
            sub.close()
            pub.close()

        return rewards, max_bler

    async def _rollout_mock(self, replica_id: int, policy: Policy):
        """Synthetic rollout for testing without real twin replicas."""
        # Policy-specific base performance (for E2 ground-truth ordering)
        perf_map = {
            "equal_weight":  (18.0, 3.0, 0.04),   # (mean, std, bler)
            "max_cqi":       (22.0, 4.0, 0.03),
            "ppo":           (24.0, 3.5, 0.025),
        }
        key = next((k for k in perf_map if k in policy.name.lower()), None)
        if key is None:
            # Perturbed or unknown: degrade proportionally to sigma
            sigma_val = 0.3
            if "σ=" in policy.name:
                try:
                    sigma_val = float(policy.name.split("σ=")[1].rstrip(")"))
                except Exception:
                    pass
            mean_r = 18.0 - sigma_val * 10
            std_r  = 3.0 + sigma_val * 5
            bler   = 0.04 + sigma_val * 0.2
        else:
            mean_r, std_r, bler = perf_map[key]

        # Add replica-level CIR perturbation noise
        mean_r += self._rng.normal(0, self._perturb_sigma)

        rewards = self._rng.normal(mean_r, std_r, self._horizon).tolist()
        max_bler = float(bler + self._rng.uniform(0, 0.02))

        # Simulate timing (mock is fast)
        await asyncio.sleep(0.001 * self._horizon / self._n_replicas)
        return rewards, max_bler


# ── CLI smoke test ─────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("=== M2 CounterfactualOracle mock demo ===\n")

    oracle = CounterfactualOracle(
        mock=True,
        n_replicas=4,
        horizon_ttis=200,
        bootstrap_iters=1000,
        alpha=0.05,
        bler_threshold=0.10,
    )

    # Inject some baseline rewards (simulating real-side stream)
    rng = np.random.default_rng(0)
    for r in rng.normal(18.0, 3.0, 200):
        oracle.update_baseline(float(r))

    candidates = [
        EqualWeightPolicy(),
        MaxCQIPolicy(),
        PerturbedPolicy(EqualWeightPolicy(), sigma=0.2, seed=1),   # mild noise
        PerturbedPolicy(EqualWeightPolicy(), sigma=1.5, seed=2),   # unsafe
    ]

    verdicts = oracle.evaluate(candidates)

    print("\n─── E2 Oracle Summary ───────────────────────────────────────")
    print(f"{'Policy':<45}  {'Improvement':>11}  {'CI lower':>9}  {'Deploy':>7}")
    for v in verdicts:
        print(f"{v.policy_id:<45}  {v.mean_improvement:>+11.3f}  "
              f"{v.ci_lower:>+9.3f}  {'YES' if v.deploy else 'no':>7}")

    summary = oracle.latest_summary()
    print(f"\nBest: {summary['best_policy']}  "
          f"improvement={summary['best_improvement']:+.3f}")
    print(f"Deployable: {summary['n_deployable']}/{summary['n_candidates']}")

    # Verify mock ordering matches expected ground truth
    ranked = sorted(verdicts, key=lambda v: v.mean_improvement, reverse=True)
    assert ranked[0].policy_id == "max_cqi", \
        f"Expected max_cqi as best, got {ranked[0].policy_id}"
    print("\nSmoke test passed: oracle correctly ranks max_cqi above equal_weight.")
