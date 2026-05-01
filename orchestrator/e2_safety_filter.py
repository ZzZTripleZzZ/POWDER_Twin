"""
W9 — E2 Safety Filter Effectiveness Experiment (Contribution 2).

Evaluates how accurately the Twin Ensemble Safety Filter predicts whether
a candidate policy will be safe on the real network.

Procedure:
  1. Generate a sweep of test policies (weight vectors spanning safe→unsafe spectrum)
  2. Run each policy through PolicyArbiter.evaluate() (twin ensemble verdict)
  3. Deploy each policy to the real network briefly, measure actual BLER/throughput
  4. Compute confusion matrix: TP/TN/FP/FN, precision, recall, F1
  5. Sweep N_replicas and sigma to produce ROC-style curves

Usage:
    python orchestrator/e2_safety_filter.py \
        --twin-host powder-twin \
        --real-host powder-gnb \
        --n-replicas 4 \
        --out logs/e2_safety_filter.csv

    # Dry-run (mock twin + mock real) for development:
    python orchestrator/e2_safety_filter.py --mock
"""
import argparse
import csv
import sys
import threading
import time
from dataclasses import dataclass
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path(__file__).parent.parent))
sys.path.insert(0, str(Path(__file__).parent.parent / "proto"))

import metrics_pb2
import zmq
from orchestrator.policy_arbiter import (
    EvalResult, Policy, PolicyArbiter, ReplicaResult, SafetyCriterion,
)

REAL_METRICS_PORT = 5555
REAL_WEIGHTS_PORT = 5556
EVAL_REAL_TTIS    = 200   # TTIs collected on real network per policy test


# ── Ground-truth collection from real network ────────────────────────────────

def measure_real_performance(
    real_host: str,
    policy: Policy,
    n_ttis: int = EVAL_REAL_TTIS,
    criterion: SafetyCriterion | None = None,
) -> tuple[float, float, bool]:
    """
    Deploy policy to real EdgeRIC briefly, collect n_ttis metrics.
    Returns (mean_bler, mean_tpt_mbps, actually_safe).
    """
    if criterion is None:
        criterion = SafetyCriterion()

    ctx = zmq.Context()

    # Send policy
    pub = ctx.socket(zmq.PUB)
    pub.connect(f"tcp://{real_host}:{REAL_WEIGHTS_PORT}")
    time.sleep(0.05)
    pub.send(policy.to_proto())
    pub.close()

    # Collect metrics
    sub = ctx.socket(zmq.SUB)
    sub.connect(f"tcp://{real_host}:{REAL_METRICS_PORT}")
    sub.setsockopt(zmq.SUBSCRIBE, b"")
    sub.setsockopt(zmq.RCVTIMEO, 5000)

    samples = []
    for _ in range(n_ttis):
        try:
            raw = sub.recv()
            m = metrics_pb2.Metrics()
            m.ParseFromString(raw)
            for ue in m.ue_metrics:
                samples.append({"tx_bytes": ue.tx_bytes})
        except zmq.Again:
            break
    sub.close()
    ctx.term()

    if not samples:
        return 1.0, 0.0, False

    n_zero    = sum(1 for s in samples if s["tx_bytes"] == 0)
    mean_bler = n_zero / len(samples)
    mean_tpt  = float(np.mean([s["tx_bytes"] * 8 / 1e6 for s in samples]))
    actually_safe = (mean_bler < criterion.bler_threshold and
                     mean_tpt  > criterion.tpt_threshold)
    return mean_bler, mean_tpt, actually_safe


# ── Mock implementations for dry-run ─────────────────────────────────────────

def _mock_twin_verdict(policy: Policy, n_replicas: int,
                       criterion: SafetyCriterion) -> EvalResult:
    """Simulated arbiter with realistic noise."""
    total_weight = sum(policy.weights.values())
    # Heuristic: unequal weights → potentially unsafe in weak-channel replicas
    imbalance = max(policy.weights.values()) / (total_weight + 1e-9)

    results = []
    rng = np.random.default_rng(seed=int(imbalance * 1000) + n_replicas)
    for i in range(n_replicas):
        bler = float(np.clip(rng.normal(0.04 + 0.10 * imbalance, 0.03), 0, 1))
        tpt  = float(np.clip(rng.normal(25.0 - 15 * imbalance, 5.0), 0, 100))
        passed = bler < criterion.bler_threshold and tpt > criterion.tpt_threshold
        results.append(ReplicaResult(i, bler, tpt, 200, passed))

    pass_ratio = sum(r.passed for r in results) / n_replicas
    safe = pass_ratio >= criterion.min_pass_ratio
    return EvalResult(policy, results, safe, pass_ratio)


def _mock_real_verdict(policy: Policy,
                       criterion: SafetyCriterion) -> tuple[float, float, bool]:
    """Simulated real outcome — higher imbalance → worse real performance."""
    total = sum(policy.weights.values())
    imbalance = max(policy.weights.values()) / (total + 1e-9)
    rng = np.random.default_rng(seed=int(imbalance * 999))
    bler = float(np.clip(rng.normal(0.03 + 0.12 * imbalance, 0.025), 0, 1))
    tpt  = float(np.clip(rng.normal(28.0 - 18 * imbalance, 4.0), 0, 100))
    safe = bler < criterion.bler_threshold and tpt > criterion.tpt_threshold
    return bler, tpt, safe


# ── Policy sweep generator ────────────────────────────────────────────────────

def policy_sweep(n_steps: int = 20, n_ue: int = 2) -> list[Policy]:
    """
    Generate policies spanning from equal weights (safe) to maximally skewed
    (potentially unsafe for weaker UEs). Returns list of Policy objects.
    """
    rntis = [0x4401 + i for i in range(n_ue)]
    policies = []
    for step in range(n_steps):
        alpha = step / max(n_steps - 1, 1)   # 0 = equal, 1 = fully skewed
        weights = np.ones(n_ue)
        weights[0] += alpha * (n_ue - 1)     # give all weight to UE 0
        weights /= weights.sum()
        policies.append(Policy(weights=dict(zip(rntis, weights.tolist()))))
    return policies


# ── Confusion matrix helpers ──────────────────────────────────────────────────

@dataclass
class ConfusionMatrix:
    tp: int = 0; tn: int = 0; fp: int = 0; fn: int = 0

    @property
    def precision(self) -> float:
        return self.tp / (self.tp + self.fp) if (self.tp + self.fp) > 0 else float("nan")

    @property
    def recall(self) -> float:
        return self.tp / (self.tp + self.fn) if (self.tp + self.fn) > 0 else float("nan")

    @property
    def f1(self) -> float:
        p, r = self.precision, self.recall
        if np.isnan(p) or np.isnan(r) or (p + r) == 0:
            return float("nan")
        return 2 * p * r / (p + r)

    @property
    def fpr(self) -> float:
        return self.fp / (self.fp + self.tn) if (self.fp + self.tn) > 0 else float("nan")

    def summary(self) -> str:
        return (f"TP={self.tp} TN={self.tn} FP={self.fp} FN={self.fn}  "
                f"Prec={self.precision:.2f} Rec={self.recall:.2f} "
                f"F1={self.f1:.2f} FPR={self.fpr:.2f}")


# ── Main experiment ───────────────────────────────────────────────────────────

def run(
    twin_host: str,
    real_host: str,
    n_replicas: int,
    sigma: float,
    n_ue: int,
    n_policies: int,
    out_path: Path,
    mock: bool,
):
    criterion = SafetyCriterion(bler_threshold=0.10, tpt_threshold=5.0)

    if not mock:
        arbiter = PolicyArbiter(
            twin_host=twin_host,
            real_host=real_host,
            n_replicas=n_replicas,
            criterion=criterion,
            perturbation_sigma=sigma,
        )
        arbiter.start_replicas(sigma=sigma)
        time.sleep(15)

    policies = policy_sweep(n_steps=n_policies, n_ue=n_ue)
    cm = ConfusionMatrix()
    rows = []

    print(f"E2: evaluating {len(policies)} policies  "
          f"N={n_replicas}  σ={sigma}  {'MOCK' if mock else 'REAL'}")

    for idx, policy in enumerate(policies):
        # Twin verdict
        if mock:
            twin_result = _mock_twin_verdict(policy, n_replicas, criterion)
        else:
            twin_result = arbiter.evaluate(policy)

        # Real verdict
        if mock:
            real_bler, real_tpt, real_safe = _mock_real_verdict(policy, criterion)
        else:
            real_bler, real_tpt, real_safe = measure_real_performance(
                real_host, policy, criterion=criterion)

        twin_safe = twin_result.safe

        if twin_safe and real_safe:
            cm.tp += 1; verdict = "TP"
        elif not twin_safe and not real_safe:
            cm.tn += 1; verdict = "TN"
        elif twin_safe and not real_safe:
            cm.fp += 1; verdict = "FP"
        else:
            cm.fn += 1; verdict = "FN"

        row = {
            "policy_idx":    idx,
            "max_weight":    max(policy.weights.values()),
            "twin_safe":     int(twin_safe),
            "twin_pass_ratio": twin_result.pass_ratio,
            "real_bler":     f"{real_bler:.4f}",
            "real_tpt":      f"{real_tpt:.3f}",
            "real_safe":     int(real_safe),
            "verdict":       verdict,
        }
        rows.append(row)
        print(f"  [{idx:2d}] wmax={max(policy.weights.values()):.2f}  "
              f"twin={'✓' if twin_safe else '✗'}  "
              f"real={'✓' if real_safe else '✗'}  → {verdict}")

    # Write CSV
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)

    print(f"\n[E2] Results → {out_path}")
    print(f"[E2] {cm.summary()}")

    # Plot ROC-style curve
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt

        max_ws = [r["max_weight"] for r in rows]
        verdicts = [r["verdict"] for r in rows]
        colors = {"TP": "green", "TN": "blue", "FP": "red", "FN": "orange"}

        fig, ax = plt.subplots(figsize=(6, 4))
        for r in rows:
            ax.scatter(r["max_weight"], float(r["real_bler"]),
                       c=colors[r["verdict"]], s=40, zorder=3)

        ax.axhline(criterion.bler_threshold, color="gray", linestyle="--",
                   label=f"BLER threshold={criterion.bler_threshold}")
        ax.set_xlabel("Max scheduling weight (policy skewness)")
        ax.set_ylabel("Real network BLER")
        ax.set_title(f"E2: Safety Filter  N={n_replicas}  "
                     f"Prec={cm.precision:.2f}  Rec={cm.recall:.2f}")
        ax.legend()
        from matplotlib.patches import Patch
        ax.legend(handles=[Patch(color=c, label=v) for v, c in colors.items()] +
                  [ax.axhline(criterion.bler_threshold, color="gray",
                              linestyle="--", label=f"BLER<{criterion.bler_threshold}")],
                  fontsize=8)
        plt.tight_layout()
        plt.savefig(out_path.with_suffix(".pdf"), dpi=150)
        print(f"[E2] Plot → {out_path.with_suffix('.pdf')}")
    except ImportError:
        pass

    if not mock:
        arbiter.stop_replicas()

    return cm


def main():
    parser = argparse.ArgumentParser(description="E2 safety filter effectiveness")
    parser.add_argument("--twin-host",   default="powder-twin")
    parser.add_argument("--real-host",   default="powder-gnb")
    parser.add_argument("--n-replicas",  type=int, default=4)
    parser.add_argument("--sigma",       type=float, default=0.15,
                        help="CIR perturbation sigma for twin replicas")
    parser.add_argument("--n-ue",        type=int, default=2)
    parser.add_argument("--n-policies",  type=int, default=20,
                        help="Number of test policies to sweep")
    parser.add_argument("--out",         default="logs/e2_safety_filter.csv")
    parser.add_argument("--mock",        action="store_true")
    args = parser.parse_args()

    run(
        twin_host=args.twin_host,
        real_host=args.real_host,
        n_replicas=args.n_replicas,
        sigma=args.sigma,
        n_ue=args.n_ue,
        n_policies=args.n_policies,
        out_path=Path(args.out),
        mock=args.mock,
    )


if __name__ == "__main__":
    main()
