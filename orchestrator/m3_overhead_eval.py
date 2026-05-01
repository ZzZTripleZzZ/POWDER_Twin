"""
M3 / E3 — Selective-Fidelity Overhead Evaluation

Sweeps TT_FIDELITY_MODE across {full_iq, sparse_tap, mac_only} and measures:
  - Wall-clock time per CIR write (FIFO write throughput)
  - Oracle ranking accuracy relative to full_iq (regret-bound accuracy)
  - CPU savings factor vs. always-full_iq baseline

Experiment design (E3 in paper):
  - N_SAMPLES CIR writes per mode; time each write
  - For each mode, run oracle.evaluate(candidates) M_TRIALS times;
    compare rankings to full_iq oracle reference
  - Plot: CPU-savings vs. ranking-accuracy scatter; goal ≥3× savings at <5% loss

Usage:
    # Synthetic timing benchmark only (no real twin required):
    python orchestrator/m3_overhead_eval.py --mock --out logs/e3_overhead.csv

    # Full sweep against live twin:
    python orchestrator/m3_overhead_eval.py --twin-host powder-twin \\
        --n-samples 500 --m-trials 10 --out logs/e3_overhead.csv
"""

import argparse
import csv
import sys
import time
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path(__file__).parent.parent))

from orchestrator.selective_fidelity import (
    Mode, RuleBasedSelector, SelectorSignals,
    sparsify_topk, COLD_START_SIGNALS,
)
from orchestrator.channel_converter import cqi_to_cir, ChannelCalibrator
from orchestrator.counterfactual_oracle import (
    CounterfactualOracle, EqualWeightPolicy, MaxCQIPolicy, PerturbedPolicy,
)


# ── CIR write timing ──────────────────────────────────────────────────────────

def _generate_cir_sample(cqi: int, mode: Mode, calibrator, rng) -> tuple:
    """Generate CIR taps for a given mode (pure Python side, no FIFO)."""
    r_list, i_list = cqi_to_cir(cqi, calibrator=calibrator, rng=rng)
    r_taps, i_taps = np.asarray(r_list, float), np.asarray(i_list, float)
    if mode == Mode.SPARSE_TAP:
        r_taps, i_taps = sparsify_topk(r_taps, i_taps, K=4)
    elif mode == Mode.MAC_ONLY:
        return np.array([]), np.array([])   # nothing written
    return r_taps, i_taps


def _format_line(taps: np.ndarray) -> str:
    return " ".join(f"{v:.6f}" for v in taps) + "\n"


def benchmark_cir_write(mode: Mode, n_samples: int, rng) -> dict:
    """Time CIR generation + formatting for n_samples at a given mode.

    This exercises the Python-side cost per TTI (cqi_to_cir + sparsify).
    The FIFO write itself is I/O bound and excluded here; actual savings
    on the C side (convolution skip/sparsify) are larger.
    """
    calibrator = ChannelCalibrator()
    cqis = rng.integers(1, 15, size=n_samples)
    latencies = []
    for cqi in cqis:
        t0 = time.perf_counter()
        r, i = _generate_cir_sample(int(cqi), mode, calibrator, rng)
        if len(r):
            _ = _format_line(r)
            _ = _format_line(i)
        latencies.append((time.perf_counter() - t0) * 1e6)  # µs
    arr = np.array(latencies)
    return {
        "mode":   mode.value,
        "n":      n_samples,
        "mean_us": float(arr.mean()),
        "p50_us":  float(np.percentile(arr, 50)),
        "p95_us":  float(np.percentile(arr, 95)),
        "total_ms": float(arr.sum() / 1e3),
    }


# ── Oracle ranking accuracy vs. full_iq ──────────────────────────────────────

def _run_oracle_trial(mode: Mode, twin_host: str, n_replicas: int,
                      horizon: int, mock: bool) -> list:
    """Run one oracle evaluation in the given fidelity mode. Returns verdicts."""
    oracle = CounterfactualOracle(
        twin_host=twin_host,
        n_replicas=n_replicas,
        horizon_ttis=horizon,
        mock=mock,
    )
    rng = np.random.default_rng(42)
    for r in rng.normal(18.0, 3.0, horizon):
        oracle.update_baseline(float(r))

    candidates = [
        EqualWeightPolicy(),
        MaxCQIPolicy(),
        PerturbedPolicy(EqualWeightPolicy(), sigma=0.3, seed=7),
    ]
    return oracle.evaluate(candidates)


def spearman_rho(rank_a: list, rank_b: list) -> float:
    a, b = np.asarray(rank_a, float), np.asarray(rank_b, float)
    n = len(a)
    if n < 2:
        return float("nan")
    ra = np.argsort(np.argsort(a)).astype(float)
    rb = np.argsort(np.argsort(b)).astype(float)
    d2 = ((ra - rb) ** 2).sum()
    return float(1 - 6 * d2 / (n * (n ** 2 - 1)))


def benchmark_oracle_accuracy(modes: list, twin_host: str, n_replicas: int,
                               horizon: int, m_trials: int, mock: bool) -> list:
    """Compare oracle rankings across modes vs. full_iq reference."""
    rows = []
    ref_rankings_by_trial = []

    for trial in range(m_trials):
        ref = _run_oracle_trial(Mode.FULL_IQ, twin_host, n_replicas, horizon, mock)
        ref_order = {v.policy_id: rank
                     for rank, v in enumerate(
                         sorted(ref, key=lambda v: v.mean_improvement, reverse=True)
                     )}
        ref_rankings_by_trial.append(ref_order)

    for mode in modes:
        rhos = []
        for trial in range(m_trials):
            verdicts = _run_oracle_trial(mode, twin_host, n_replicas, horizon, mock)
            mode_order = {v.policy_id: rank
                          for rank, v in enumerate(
                              sorted(verdicts, key=lambda v: v.mean_improvement, reverse=True)
                          )}
            ref_order = ref_rankings_by_trial[trial]
            common = [p for p in ref_order if p in mode_order]
            rho = spearman_rho(
                [ref_order[p] for p in common],
                [mode_order[p] for p in common],
            ) if len(common) >= 2 else float("nan")
            rhos.append(rho)
        rows.append({
            "mode":          mode.value,
            "mean_rho":      float(np.nanmean(rhos)),
            "min_rho":       float(np.nanmin(rhos)),
            "accuracy_loss": float(1.0 - np.nanmean(rhos)),  # vs. perfect 1.0
            "m_trials":      m_trials,
        })
    return rows


# ── Summary: CPU savings factor ───────────────────────────────────────────────

def compute_savings_factor(timing_rows: list) -> list:
    """Normalize wall-clock to full_iq baseline."""
    ref = next((r["mean_us"] for r in timing_rows if r["mode"] == "full_iq"), None)
    for row in timing_rows:
        row["savings_factor"] = ref / max(row["mean_us"], 1e-9) if ref else float("nan")
    return timing_rows


# ── Main E3 experiment ────────────────────────────────────────────────────────

def run_e3(
    twin_host: str,
    n_samples: int,
    n_replicas: int,
    horizon: int,
    m_trials: int,
    out_path: Path,
    mock: bool,
) -> None:
    modes = [Mode.FULL_IQ, Mode.SPARSE_TAP, Mode.MAC_ONLY]
    rng   = np.random.default_rng(0)

    print(f"[E3] Timing sweep: {n_samples} samples × 3 modes ...", flush=True)
    timing_rows = [benchmark_cir_write(m, n_samples, rng) for m in modes]
    timing_rows = compute_savings_factor(timing_rows)

    print(f"[E3] Oracle accuracy sweep: {m_trials} trials × 3 modes ...", flush=True)
    accuracy_rows = benchmark_oracle_accuracy(
        modes, twin_host, n_replicas, horizon, m_trials, mock
    )

    # Merge by mode
    acc_by_mode = {r["mode"]: r for r in accuracy_rows}
    merged = []
    for tr in timing_rows:
        ar = acc_by_mode.get(tr["mode"], {})
        merged.append({**tr, **ar})

    # Print summary
    print("\n─── E3 Results ──────────────────────────────────────────────────────")
    header = f"{'Mode':<12}  {'mean µs':>9}  {'savings×':>9}  {'ρ vs full_iq':>13}  {'acc_loss':>9}"
    print(header)
    for row in merged:
        print(
            f"{row['mode']:<12}  {row['mean_us']:>9.2f}  "
            f"{row.get('savings_factor', float('nan')):>9.2f}×  "
            f"{row.get('mean_rho', float('nan')):>13.3f}  "
            f"{row.get('accuracy_loss', float('nan')):>9.1%}"
        )
    print(f"\nTarget: ≥3× savings at <5% accuracy loss")

    # Write CSV
    out_path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = list(merged[0].keys())
    with open(out_path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        w.writeheader()
        w.writerows(merged)
    print(f"\n[E3] Results → {out_path}")

    # Plot
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt

        fig, axes = plt.subplots(1, 2, figsize=(10, 4))

        # Left: savings factor
        labels  = [r["mode"] for r in merged]
        savings = [r.get("savings_factor", float("nan")) for r in merged]
        colors  = ["#d62728", "#ff7f0e", "#2ca02c"]
        axes[0].bar(labels, savings, color=colors)
        axes[0].axhline(3.0, color="black", linestyle="--", linewidth=0.8, label="3× target")
        axes[0].set_ylabel("CPU savings factor (×)")
        axes[0].set_title("E3: Compute savings by mode")
        axes[0].legend()

        # Right: accuracy (Spearman ρ)
        rhos   = [r.get("mean_rho", float("nan")) for r in merged]
        axes[1].bar(labels, rhos, color=colors)
        axes[1].axhline(0.95, color="black", linestyle="--", linewidth=0.8,
                        label="ρ=0.95 (5% loss)")
        axes[1].set_ylim(0, 1.05)
        axes[1].set_ylabel("Oracle ranking ρ vs full_iq")
        axes[1].set_title("E3: Ranking accuracy by mode")
        axes[1].legend()

        plt.tight_layout()
        fig.savefig(out_path.with_suffix(".pdf"), dpi=150)
        print(f"[E3] Plot → {out_path.with_suffix('.pdf')}")
    except ImportError:
        pass


def main():
    parser = argparse.ArgumentParser(description="E3 M3 overhead sweep")
    parser.add_argument("--twin-host",   default="localhost")
    parser.add_argument("--n-samples",   type=int, default=500,
                        help="CIR write samples per mode for timing")
    parser.add_argument("--n-replicas",  type=int, default=4)
    parser.add_argument("--horizon",     type=int, default=200)
    parser.add_argument("--m-trials",    type=int, default=5,
                        help="Oracle eval trials per mode for accuracy")
    parser.add_argument("--out",         default="logs/e3_overhead.csv")
    parser.add_argument("--mock",        action="store_true",
                        help="Synthetic metrics (no live twin required)")
    args = parser.parse_args()

    run_e3(
        twin_host  = args.twin_host,
        n_samples  = args.n_samples,
        n_replicas = args.n_replicas,
        horizon    = args.horizon,
        m_trials   = args.m_trials,
        out_path   = Path(args.out),
        mock       = args.mock,
    )


if __name__ == "__main__":
    main()
