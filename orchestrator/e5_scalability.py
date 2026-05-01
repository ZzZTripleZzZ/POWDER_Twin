"""
W9 — E5 Scalability Experiment (Contribution 2).

Measures how policy evaluation latency and safety coverage scale with the
number of twin replicas N ∈ {1, 2, 4, 8, 16}.

Procedure per N:
  1. Start N replicas (or skip if --mock)
  2. Submit a test policy to PolicyArbiter
  3. Time the full evaluate() call wall-clock
  4. Record (N, latency_s, pass_ratio)

Output: logs/e5_scalability.csv and a latency-vs-N plot.

Usage:
    # With real twins (requires twin_host):
    python orchestrator/e5_scalability.py \
        --twin-host powder-twin \
        --real-host powder-gnb \
        --replica-counts 1 2 4 8 \
        --trials 5

    # Mock mode (no network, measures orchestration overhead only):
    python orchestrator/e5_scalability.py --mock --replica-counts 1 2 4 8 16
"""
import argparse
import csv
import sys
import time
from pathlib import Path
from unittest.mock import MagicMock, patch

import numpy as np

sys.path.insert(0, str(Path(__file__).parent.parent))
from orchestrator.policy_arbiter import (
    Policy, PolicyArbiter, ReplicaResult, EvalResult, SafetyCriterion,
)


# ── Mock replica evaluator for dry-run / CI ──────────────────────────────────

def _mock_eval_replica(replica_id: int, policy: Policy,
                       base_latency_s: float = 0.20) -> ReplicaResult:
    """Simulates one replica evaluation with configurable base latency."""
    time.sleep(base_latency_s + np.random.exponential(0.03))
    mean_bler = np.random.uniform(0.01, 0.08)
    mean_tpt  = np.random.uniform(12.0, 40.0)
    passed = mean_bler < 0.10 and mean_tpt > 10.0
    return ReplicaResult(replica_id, mean_bler, mean_tpt, 200, passed)


def run_mock(
    replica_counts: list[int],
    trials: int,
    out_path: Path,
) -> list[dict]:
    """
    Mock mode: no real twins, simulates evaluate() latency.
    Each replica runs in its own thread (same threading model as real arbiter).
    """
    import threading

    policy = Policy(weights={0x4401: 0.5, 0x4402: 0.5})
    rows = []

    for n in replica_counts:
        latencies = []
        pass_ratios = []

        for trial in range(trials):
            results: list[ReplicaResult] = [None] * n  # type: ignore

            def _worker(i, res_list=results):
                res_list[i] = _mock_eval_replica(i, policy)

            t0 = time.perf_counter()
            threads = [threading.Thread(target=_worker, args=(i,)) for i in range(n)]
            for t in threads:
                t.start()
            for t in threads:
                t.join()
            elapsed = time.perf_counter() - t0

            pass_ratio = sum(r.passed for r in results) / n
            latencies.append(elapsed)
            pass_ratios.append(pass_ratio)
            print(f"  N={n:2d}  trial={trial+1}/{trials}  "
                  f"latency={elapsed:.3f}s  pass={pass_ratio*100:.0f}%")

        row = {
            "n_replicas":    n,
            "latency_mean":  float(np.mean(latencies)),
            "latency_std":   float(np.std(latencies)),
            "latency_p95":   float(np.percentile(latencies, 95)),
            "pass_ratio_mean": float(np.mean(pass_ratios)),
        }
        rows.append(row)

    return rows


def run_real(
    twin_host: str,
    real_host: str,
    replica_counts: list[int],
    trials: int,
    cir_real: Path,
    cir_imag: Path,
    out_path: Path,
) -> list[dict]:
    """
    Real mode: spin up N replicas, call PolicyArbiter.evaluate(), tear down.
    """
    policy = Policy(weights={0x4401: 0.5, 0x4402: 0.5})
    rows = []

    for n in replica_counts:
        arbiter = PolicyArbiter(
            twin_host=twin_host,
            real_host=real_host,
            n_replicas=n,
            base_cir_real=cir_real,
            base_cir_imag=cir_imag,
            criterion=SafetyCriterion(bler_threshold=0.10, tpt_threshold=5.0),
        )

        print(f"\n[E5] Starting {n} replicas ...")
        arbiter.start_replicas()
        time.sleep(15)  # wait for containers to be ready

        latencies, pass_ratios = [], []
        for trial in range(trials):
            t0 = time.perf_counter()
            result = arbiter.evaluate(policy)
            elapsed = time.perf_counter() - t0

            latencies.append(elapsed)
            pass_ratios.append(result.pass_ratio)
            print(f"  N={n:2d}  trial={trial+1}/{trials}  "
                  f"latency={elapsed:.3f}s  pass={result.pass_ratio*100:.0f}%")

        arbiter.stop_replicas()
        time.sleep(5)

        row = {
            "n_replicas":      n,
            "latency_mean":    float(np.mean(latencies)),
            "latency_std":     float(np.std(latencies)),
            "latency_p95":     float(np.percentile(latencies, 95)),
            "pass_ratio_mean": float(np.mean(pass_ratios)),
        }
        rows.append(row)

    return rows


def save_and_plot(rows: list[dict], out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)
    print(f"\n[E5] Results saved → {out_path}")

    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt

        ns   = [r["n_replicas"]   for r in rows]
        lat  = [r["latency_mean"] for r in rows]
        err  = [r["latency_std"]  for r in rows]

        fig, ax1 = plt.subplots(figsize=(6, 4))
        ax1.errorbar(ns, lat, yerr=err, marker="o", capsize=4, label="Eval latency")
        ax1.set_xlabel("Number of twin replicas (N)")
        ax1.set_ylabel("Evaluation latency (s)")
        ax1.set_xticks(ns)

        ax2 = ax1.twinx()
        ax2.plot(ns, [r["pass_ratio_mean"] * 100 for r in rows],
                 marker="s", linestyle="--", color="tab:orange", label="Pass ratio %")
        ax2.set_ylabel("Safety pass ratio (%)")
        ax2.set_ylim(0, 105)

        lines1, labels1 = ax1.get_legend_handles_labels()
        lines2, labels2 = ax2.get_legend_handles_labels()
        ax1.legend(lines1 + lines2, labels1 + labels2, loc="upper left")

        ax1.set_title("E5: Scalability of Twin Ensemble Safety Filter")
        plt.tight_layout()
        plot_path = out_path.with_suffix(".pdf")
        plt.savefig(plot_path, dpi=150)
        print(f"[E5] Plot saved → {plot_path}")
    except ImportError:
        print("[E5] matplotlib not available, skipping plot.")


def main():
    parser = argparse.ArgumentParser(description="E5 scalability experiment")
    parser.add_argument("--twin-host",       default="powder-twin")
    parser.add_argument("--real-host",       default="powder-gnb")
    parser.add_argument("--replica-counts",  type=int, nargs="+",
                        default=[1, 2, 4, 8],
                        help="List of N values to sweep")
    parser.add_argument("--trials",          type=int, default=5,
                        help="Repeat each N this many times")
    parser.add_argument("--cir-real",        default="logs/cir_offline/cir_ue4401_real.txt")
    parser.add_argument("--cir-imag",        default="logs/cir_offline/cir_ue4401_imag.txt")
    parser.add_argument("--out",             default="logs/e5_scalability.csv")
    parser.add_argument("--mock",            action="store_true",
                        help="Run without real network (simulates latency)")
    args = parser.parse_args()

    out_path = Path(args.out)
    counts   = sorted(set(args.replica_counts))

    print(f"[E5] Scalability sweep: N={counts}, trials={args.trials}, "
          f"{'MOCK' if args.mock else 'REAL'} mode")

    if args.mock:
        rows = run_mock(counts, args.trials, out_path)
    else:
        rows = run_real(
            args.twin_host, args.real_host,
            counts, args.trials,
            Path(args.cir_real), Path(args.cir_imag),
            out_path,
        )

    save_and_plot(rows, out_path)

    # Print summary table
    print("\n─── E5 Summary ───────────────────────────────────────")
    print(f"{'N':>4}  {'lat_mean':>10}  {'lat_p95':>10}  {'pass%':>7}")
    for r in rows:
        print(f"{r['n_replicas']:>4}  {r['latency_mean']:>10.3f}  "
              f"{r['latency_p95']:>10.3f}  {r['pass_ratio_mean']*100:>6.1f}%")


if __name__ == "__main__":
    main()
