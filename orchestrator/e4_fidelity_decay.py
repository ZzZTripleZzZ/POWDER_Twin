"""
W9 — E4 Fidelity Decay Experiment (Contribution 4).

Procedure:
  1. At t=0: run offline_replay.py to generate CIR files from real logs (calibration)
  2. Start twin with calibrated CIR
  3. Every Δt seconds: collect metrics from both real and twin EdgeRIC,
     compute fidelity (RMSE of CQI/throughput), write to CSV
  4. After T_total seconds: plot fidelity-vs-time curve, fit exp decay τ

Usage:
    python orchestrator/e4_fidelity_decay.py \
        --real-host powder-gnb \
        --twin-host powder-twin \
        --duration 3600 \
        --interval 30 \
        --out logs/e4_fidelity_decay.csv
"""
import argparse
import csv
import sys
import time
from pathlib import Path

import numpy as np
import zmq

sys.path.insert(0, str(Path(__file__).parent.parent))
sys.path.insert(0, str(Path(__file__).parent.parent / "proto"))
import metrics_pb2

REAL_METRICS_PORT = 5555
TWIN_METRICS_PORT = 5555


def get_snapshot(sock: zmq.Socket, n_samples: int = 50) -> dict[int, dict]:
    """Collect n_samples TTIs and return mean per-UE metrics."""
    from collections import defaultdict
    accum: dict[int, list] = defaultdict(list)
    for _ in range(n_samples):
        try:
            raw = sock.recv()
            m = metrics_pb2.Metrics()
            m.ParseFromString(raw)
            for ue in m.ue_metrics:
                accum[ue.rnti].append({
                    "cqi": ue.cqi, "snr": ue.snr,
                    "tx_bytes": ue.tx_bytes, "dl_buf": ue.dl_buffer,
                })
        except zmq.Again:
            break

    return {
        rnti: {k: np.mean([d[k] for d in vals]) for k in vals[0]}
        for rnti, vals in accum.items() if vals
    }


def fidelity_metrics(real: dict, twin: dict) -> dict[str, float]:
    """Compute per-metric fidelity between real and twin snapshots."""
    rntis = set(real) & set(twin)
    if not rntis:
        return {"cqi_rmse": float("nan"), "tpt_rmse": float("nan"), "n_ue": 0}

    cqi_errs, tpt_errs = [], []
    for rnti in rntis:
        r, t = real[rnti], twin[rnti]
        cqi_errs.append((r["cqi"] - t["cqi"]) ** 2)
        tpt_errs.append((r["tx_bytes"] - t["tx_bytes"]) ** 2)

    return {
        "cqi_rmse": float(np.sqrt(np.mean(cqi_errs))),
        "tpt_rmse": float(np.sqrt(np.mean(tpt_errs))),
        "n_ue":     len(rntis),
    }


def fit_exp_decay(times: list[float], fidelity: list[float]) -> tuple[float, float]:
    """
    Fit fidelity(t) = A * exp(-t / τ).
    Returns (A, τ) — τ is the coherence time in seconds.
    """
    from scipy.optimize import curve_fit

    def model(t, A, tau):
        return A * np.exp(-np.array(t) / tau)

    try:
        popt, _ = curve_fit(model, times, fidelity, p0=[fidelity[0], 300],
                            bounds=([0, 1e-3], [1e6, 1e6]), maxfev=5000)
        return float(popt[0]), float(popt[1])
    except Exception:
        return float("nan"), float("nan")


def run(
    real_host: str,
    twin_host: str,
    duration_s: float,
    interval_s: float,
    out_path: Path,
    samples_per_snap: int = 50,
):
    ctx = zmq.Context()

    real_sub = ctx.socket(zmq.SUB)
    real_sub.connect(f"tcp://{real_host}:{REAL_METRICS_PORT}")
    real_sub.setsockopt(zmq.SUBSCRIBE, b"")
    real_sub.setsockopt(zmq.CONFLATE, 1)
    real_sub.setsockopt(zmq.RCVTIMEO, 3000)

    twin_sub = ctx.socket(zmq.SUB)
    twin_sub.connect(f"tcp://{twin_host}:{TWIN_METRICS_PORT}")
    twin_sub.setsockopt(zmq.SUBSCRIBE, b"")
    twin_sub.setsockopt(zmq.CONFLATE, 1)
    twin_sub.setsockopt(zmq.RCVTIMEO, 3000)

    out_path.parent.mkdir(parents=True, exist_ok=True)
    times, cqi_fidelities = [], []

    with open(out_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["elapsed_s", "cqi_rmse", "tpt_rmse", "n_ue"])

        t0 = time.time()
        print(f"E4: Collecting fidelity for {duration_s}s, interval={interval_s}s → {out_path}")
        while (elapsed := time.time() - t0) < duration_s:
            real_snap = get_snapshot(real_sub, samples_per_snap)
            twin_snap = get_snapshot(twin_sub, samples_per_snap)
            fm = fidelity_metrics(real_snap, twin_snap)

            writer.writerow([f"{elapsed:.1f}", f"{fm['cqi_rmse']:.4f}",
                             f"{fm['tpt_rmse']:.4f}", fm["n_ue"]])
            f.flush()

            print(f"  t={elapsed:6.0f}s  CQI_RMSE={fm['cqi_rmse']:.3f}  "
                  f"TPT_RMSE={fm['tpt_rmse']:.3f}  UEs={fm['n_ue']}")

            times.append(elapsed)
            if not np.isnan(fm["cqi_rmse"]):
                cqi_fidelities.append(fm["cqi_rmse"])

            time.sleep(interval_s)

    # Fit decay model and report
    if len(times) >= 3 and cqi_fidelities:
        # fidelity = reciprocal of RMSE for fitting (higher = better)
        inv_fid = [1.0 / (v + 1e-6) for v in cqi_fidelities]
        A, tau = fit_exp_decay(times[:len(inv_fid)], inv_fid)
        opt_interval = tau / 3   # heuristic: recalibrate at 1/3 coherence time
        print(f"\nExp decay fit: A={A:.3f}, τ={tau:.1f}s")
        print(f"Recommended recalibration interval: Δt* ≈ {opt_interval:.0f}s")

    real_sub.close(); twin_sub.close(); ctx.term()


def main():
    parser = argparse.ArgumentParser(description="E4 fidelity decay experiment")
    parser.add_argument("--real-host",  required=True)
    parser.add_argument("--twin-host",  required=True)
    parser.add_argument("--duration",   type=float, default=3600,
                        help="Total duration in seconds")
    parser.add_argument("--interval",   type=float, default=30,
                        help="Measurement interval in seconds")
    parser.add_argument("--out",        default="logs/e4_fidelity_decay.csv")
    args = parser.parse_args()
    run(args.real_host, args.twin_host,
        args.duration, args.interval, Path(args.out))


if __name__ == "__main__":
    main()
