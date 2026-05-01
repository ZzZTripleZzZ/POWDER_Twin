"""
M2 / E2 — Oracle Ranking Experiment

Evaluates the CounterfactualOracle's ability to correctly rank 5 candidate
scheduler policies without deploying them to the real network.

Experiment design (E2 in paper):
  5 candidates: EqualWeight (PF), MaxCQI, PPO, PF+noise(σ=0.2), MaxCQI+noise(σ=1.5)

  1. [Optional] Ground-truth collection: deploy each policy on real network for
     `--gt-duration` seconds, record mean reward.
  2. Oracle evaluation: use live twin replicas to rank all candidates.
  3. Metrics:
     - Spearman rank correlation between oracle ranking and ground truth
     - Top-1 accuracy (oracle picks the best policy)
     - Safety claim: rejection rate of unsafe candidates (σ=1.5)

Usage:
    # Mock mode (no POWDER required):
    python orchestrator/e3_rl_eval.py --mock --out logs/e2_oracle_eval.csv

    # Real twin, no ground truth:
    python orchestrator/e3_rl_eval.py --twin-host powder-twin \\
        --n-replicas 4 --horizon 200 --out logs/e2_oracle_eval.csv

    # Full E2 (real + ground truth):
    python orchestrator/e3_rl_eval.py --twin-host powder-twin --real-host powder-gnb \\
        --collect-ground-truth --gt-duration 300 --out logs/e2_oracle_eval.csv
"""

import argparse
import csv
import sys
import time
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path(__file__).parent.parent))
sys.path.insert(0, str(Path(__file__).parent.parent / "proto"))

import zmq
import metrics_pb2
import control_weights_pb2

from orchestrator.counterfactual_oracle import (
    CounterfactualOracle, PolicyVerdict,
    EqualWeightPolicy, MaxCQIPolicy, EdgeRICPPOPolicy, PerturbedPolicy,
    compute_reward, bootstrap_improvement_ci,
)

REAL_METRICS_PORT = 5555
REAL_WEIGHTS_PORT = 5556


# ── Real-side utilities (shared with legacy E3) ───────────────────────────────

def collect_metrics(real_host: str, duration_s: float) -> list:
    """Collect raw per-TTI Metrics from real EdgeRIC for `duration_s` seconds."""
    ctx = zmq.Context()
    sub = ctx.socket(zmq.SUB)
    sub.connect(f"tcp://{real_host}:{REAL_METRICS_PORT}")
    sub.setsockopt(zmq.SUBSCRIBE, b"")
    sub.setsockopt(zmq.RCVTIMEO, 2000)
    samples = []
    t0 = time.time()
    while time.time() - t0 < duration_s:
        try:
            raw = sub.recv()
            m   = metrics_pb2.Metrics()
            m.ParseFromString(raw)
            samples.append(m)
        except zmq.Again:
            continue
    sub.close(); ctx.term()
    return samples


def send_weights(real_host: str, weights: dict, ran_index: int = 0):
    ctx = zmq.Context()
    pub = ctx.socket(zmq.PUB)
    pub.connect(f"tcp://{real_host}:{REAL_WEIGHTS_PORT}")
    time.sleep(0.05)
    msg = control_weights_pb2.SchedulingWeights()
    msg.ran_index = ran_index
    for rnti, w in weights.items():
        msg.weights.append(float(rnti)); msg.weights.append(float(w))
    pub.send(msg.SerializeToString())
    pub.close(); ctx.term()


def jain_fairness(tpts: list) -> float:
    if not tpts or sum(tpts) == 0:
        return float("nan")
    n = len(tpts)
    return (sum(tpts) ** 2) / (n * sum(t ** 2 for t in tpts) + 1e-12)


def aggregate_samples(samples: list) -> dict:
    """Aggregate Metrics protobuf list into summary stats."""
    if not samples:
        return {"mean_reward": 0.0, "mean_tpt": 0.0, "bler": 1.0,
                "fairness": float("nan"), "n_samples": 0}
    rewards   = [compute_reward(m) for m in samples]
    all_tpts  = []
    n_drops   = 0
    for m in samples:
        for u in m.ue_metrics:
            all_tpts.append(u.tx_bytes * 8 / 1e6)
            if u.tx_bytes == 0:
                n_drops += 1
    total_ue_obs = sum(len(m.ue_metrics) for m in samples)
    return {
        "mean_reward": float(np.mean(rewards)),
        "mean_tpt":    float(np.mean(all_tpts)) if all_tpts else 0.0,
        "bler":        n_drops / max(total_ue_obs, 1),
        "fairness":    jain_fairness(all_tpts),
        "n_samples":   len(samples),
    }


# ── Ground truth collection ───────────────────────────────────────────────────

def collect_ground_truth(
    real_host: str,
    candidates: list,
    duration_s: float,
    mock: bool,
) -> dict:
    """
    Deploy each candidate on the real network for duration_s, record mean reward.
    Returns {policy_id: mean_reward}.
    """
    gt = {}
    for policy in candidates:
        print(f"\n[GT] Deploying {policy.name} on real for {duration_s:.0f}s ...",
              flush=True)
        if mock:
            # Synthetic ground truth with known ordering
            base = {
                "equal_weight": 18.0,
                "max_cqi":      22.0,
            }
            key = next((k for k in base if k in policy.name.lower()), None)
            mean = base.get(key, 18.0)
            if "σ=0.20" in policy.name:
                mean = 17.0
            elif "σ=1.50" in policy.name:
                mean = 10.0
            gt[policy.name] = float(np.random.default_rng(abs(hash(policy.name)) % 2**31)
                                    .normal(mean, 1.0))
        else:
            # Send weights for first sample, then collect
            rntis = [0x4401, 0x4402]   # placeholder; update after first recv
            weights = policy.act(metrics_pb2.Metrics())   # cold start: empty metrics
            if weights:
                send_weights(real_host, weights)
            samples = collect_metrics(real_host, duration_s)
            agg     = aggregate_samples(samples)
            gt[policy.name] = agg["mean_reward"]
        print(f"[GT] {policy.name}: mean_reward={gt[policy.name]:.3f}", flush=True)
    return gt


# ── Spearman correlation & accuracy ──────────────────────────────────────────

def spearman_correlation(rank_a: list, rank_b: list) -> float:
    """Spearman ρ — pure numpy, no scipy required."""
    a = np.asarray(rank_a, dtype=float)
    b = np.asarray(rank_b, dtype=float)
    n = len(a)
    if n < 2:
        return float("nan")
    ra = np.argsort(np.argsort(a)).astype(float)
    rb = np.argsort(np.argsort(b)).astype(float)
    d2 = ((ra - rb) ** 2).sum()
    return float(1 - 6 * d2 / (n * (n ** 2 - 1)))


def top1_accuracy(oracle_verdicts: list, ground_truth: dict) -> float:
    """1.0 if oracle's best deployable policy matches ground truth best."""
    if not ground_truth:
        return float("nan")
    gt_best    = max(ground_truth, key=ground_truth.get)
    deployable = [v for v in oracle_verdicts if v.deploy]
    if not deployable:
        return 0.0
    oracle_best = max(deployable, key=lambda v: v.mean_improvement).policy_id
    return 1.0 if oracle_best == gt_best else 0.0


def _parse_sigma(policy_id: str) -> float:
    """Extract σ value from a PerturbedPolicy name, or -1 if not perturbed."""
    if "σ=" not in policy_id:
        return -1.0
    try:
        return float(policy_id.split("σ=")[-1].rstrip(")"))
    except ValueError:
        return -1.0


def rejection_rate_unsafe(oracle_verdicts: list, unsafe_sigma: float = 1.0) -> float:
    """Fraction of unsafe (high-σ perturbed) candidates correctly NOT deployed."""
    unsafe = [v for v in oracle_verdicts if _parse_sigma(v.policy_id) >= unsafe_sigma]
    if not unsafe:
        return float("nan")
    rejected = sum(1 for v in unsafe if not v.deploy)
    return rejected / len(unsafe)


# ── Main E2 experiment ────────────────────────────────────────────────────────

def run_e2(
    twin_host: str,
    real_host: str,
    ppo_ckpt: Path | None,
    n_replicas: int,
    horizon: int,
    gt_duration: float,
    collect_gt: bool,
    out_path: Path,
    mock: bool,
):
    candidates = [
        EqualWeightPolicy(),
        MaxCQIPolicy(),
        PerturbedPolicy(EqualWeightPolicy(), sigma=0.2, seed=1),
        PerturbedPolicy(EqualWeightPolicy(), sigma=1.5, seed=2),
    ]
    if ppo_ckpt and (ppo_ckpt.exists() or mock):
        candidates.insert(2, EdgeRICPPOPolicy(str(ppo_ckpt)))

    print(f"[E2] {len(candidates)} candidates, {n_replicas} replicas, "
          f"horizon={horizon} TTIs, mock={mock}", flush=True)

    # Ground truth (optional)
    ground_truth = {}
    if collect_gt:
        ground_truth = collect_ground_truth(real_host, candidates, gt_duration, mock)

    # Oracle evaluation
    oracle = CounterfactualOracle(
        twin_host=twin_host,
        n_replicas=n_replicas,
        horizon_ttis=horizon,
        bler_threshold=0.10,
        mock=mock,
    )

    # Seed baseline from real side (or mock)
    if collect_gt and not mock:
        baseline_samples = collect_metrics(real_host, 30.0)
        for m in baseline_samples:
            oracle.update_baseline(compute_reward(m))
    else:
        rng = np.random.default_rng(99)
        for r in rng.normal(18.0, 3.0, horizon):
            oracle.update_baseline(float(r))

    print("\n[E2] Running oracle evaluation...", flush=True)
    verdicts = oracle.evaluate(candidates)

    # Compute metrics
    oracle_order = sorted(verdicts, key=lambda v: v.mean_improvement, reverse=True)
    oracle_ranks = {v.policy_id: i + 1 for i, v in enumerate(oracle_order)}

    if ground_truth:
        gt_order = sorted(ground_truth, key=ground_truth.get, reverse=True)
        gt_ranks = {pid: i + 1 for i, pid in enumerate(gt_order)}
        common   = [pid for pid in oracle_ranks if pid in gt_ranks]
        spear    = spearman_correlation(
            [oracle_ranks[p] for p in common],
            [gt_ranks[p]     for p in common],
        ) if len(common) >= 2 else float("nan")
        top1     = top1_accuracy(verdicts, ground_truth)
    else:
        spear = float("nan")
        top1  = float("nan")

    rej_rate = rejection_rate_unsafe(verdicts, unsafe_sigma=1.5)

    # Print summary
    print("\n─── E2 Results ─────────────────────────────────────────────────────")
    print(f"{'Policy':<45}  {'Improvement':>11}  {'CI':>18}  {'Deploy':>7}",
          f"{'GT reward':>10}" if ground_truth else "")
    for v in sorted(verdicts, key=lambda v: v.mean_improvement, reverse=True):
        gt_str = f"{ground_truth.get(v.policy_id, float('nan')):>10.3f}" \
                 if ground_truth else ""
        print(f"{v.policy_id:<45}  {v.mean_improvement:>+11.3f}  "
              f"[{v.ci_lower:>+7.3f},{v.ci_upper:>+7.3f}]  "
              f"{'YES' if v.deploy else 'no':>7}  {gt_str}")

    print(f"\nSpearman ρ (oracle vs ground truth): {spear:.3f}")
    print(f"Top-1 accuracy:                      {top1:.1%}")
    print(f"Unsafe candidate rejection rate:     {rej_rate:.1%}")
    print(f"\nTarget: ρ≥0.8, top-1≥80%, rejection≥95%")

    # Write CSV
    out_path.parent.mkdir(parents=True, exist_ok=True)
    rows = []
    for v in verdicts:
        rows.append({
            "policy_id":        v.policy_id,
            "mean_improvement": v.mean_improvement,
            "ci_lower":         v.ci_lower,
            "ci_upper":         v.ci_upper,
            "safety_violations":v.safety_violations,
            "deploy":           int(v.deploy),
            "n_rollouts":       v.n_rollouts,
            "mean_reward":      v.mean_reward,
            "baseline_mean":    v.baseline_mean,
            "oracle_rank":      oracle_ranks.get(v.policy_id, ""),
            "gt_reward":        ground_truth.get(v.policy_id, ""),
            "gt_rank":          gt_ranks.get(v.policy_id, "") if ground_truth else "",
        })
    rows.append({
        "policy_id": "__summary__",
        "mean_improvement": spear,
        "ci_lower": top1,
        "ci_upper": rej_rate,
    })
    fieldnames = list(rows[0].keys())
    with open(out_path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        w.writeheader()
        w.writerows(rows)
    print(f"\n[E2] Results → {out_path}")

    # Plot
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt

        policies = [v.policy_id.split("(")[0][:20] for v in oracle_order]
        imps     = [v.mean_improvement for v in oracle_order]
        errs     = [[v.mean_improvement - v.ci_lower for v in oracle_order],
                    [v.ci_upper - v.mean_improvement for v in oracle_order]]
        colors   = ["#2ca02c" if v.deploy else "#d62728" for v in oracle_order]

        fig, ax = plt.subplots(figsize=(9, 4))
        bars = ax.barh(policies, imps, xerr=errs, color=colors,
                       error_kw={"capsize": 4})
        ax.axvline(0, color="black", linewidth=0.8, linestyle="--")
        ax.set_xlabel("Mean improvement over baseline (reward units)")
        ax.set_title(f"E2: Oracle ranking  "
                     f"(Spearman ρ={spear:.2f}, top-1={top1:.0%})")
        ax.invert_yaxis()
        plt.tight_layout()
        fig.savefig(out_path.with_suffix(".pdf"), dpi=150)
        print(f"[E2] Plot → {out_path.with_suffix('.pdf')}")
    except ImportError:
        pass


def main():
    parser = argparse.ArgumentParser(description="E2 Oracle ranking experiment (M2)")
    parser.add_argument("--twin-host",          default="localhost")
    parser.add_argument("--real-host",          default="powder-gnb")
    parser.add_argument("--ppo-checkpoint",     default="checkpoints/ppo_policy.pt")
    parser.add_argument("--n-replicas",  type=int,   default=4)
    parser.add_argument("--horizon",     type=int,   default=200,
                        help="TTIs per policy per replica rollout")
    parser.add_argument("--collect-ground-truth", action="store_true",
                        help="Deploy each policy on real for ground-truth reward collection")
    parser.add_argument("--gt-duration", type=float, default=300,
                        help="Seconds per policy for ground-truth collection")
    parser.add_argument("--out", default="logs/e2_oracle_eval.csv")
    parser.add_argument("--mock", action="store_true",
                        help="Use synthetic metrics (no real twin required)")
    args = parser.parse_args()

    run_e2(
        twin_host  = args.twin_host,
        real_host  = args.real_host,
        ppo_ckpt   = Path(args.ppo_checkpoint),
        n_replicas = args.n_replicas,
        horizon    = args.horizon,
        gt_duration= args.gt_duration,
        collect_gt = args.collect_ground_truth,
        out_path   = Path(args.out),
        mock       = args.mock,
    )


if __name__ == "__main__":
    main()
