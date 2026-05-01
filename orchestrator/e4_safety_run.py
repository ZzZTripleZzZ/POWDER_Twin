"""
E4 — 4-Hour M2-Gated Safety Run

Continuously proposes candidate scheduler policy updates, gates each on the
M2 counterfactual oracle, and deploys only approved updates to the real gNB.
Tracks: quarantine events, CPU usage, throughput vs PF baseline.

Pass criteria (from design_plan.md):
  - Zero quarantine events (oracle never blocks a safe update)
  - CPU on twin server < 50%
  - Throughput ≥ static PF baseline throughout

Usage:
    # OTA run (4 hours):
    python orchestrator/e4_safety_run.py \
        --real-host powder-gnb --twin-host powder-twin \
        --duration 14400 --swap-interval 300 \
        --n-replicas 4 --out logs/e4_safety_run.csv

    # Short smoke test (mock, 60 s):
    python orchestrator/e4_safety_run.py --mock --duration 60 --out logs/e4_smoke.csv
"""

from __future__ import annotations

import argparse
import csv
import sys
import time
from pathlib import Path

import zmq

sys.path.insert(0, str(Path(__file__).parent.parent))
sys.path.insert(0, str(Path(__file__).parent.parent / "proto"))
import metrics_pb2

METRICS_PORT = 5555


# ── Candidate policy generator ────────────────────────────────────────────────

def _next_candidate(step: int) -> dict[int, float]:
    """Cycle through a small set of candidate scheduler weight vectors."""
    candidates = [
        {0x4401: 0.50, 0x4402: 0.50},   # equal weight (safe baseline)
        {0x4401: 0.60, 0x4402: 0.40},   # slightly favour ue1
        {0x4401: 0.40, 0x4402: 0.60},   # slightly favour ue2
        {0x4401: 0.70, 0x4402: 0.30},   # max-CQI-biased
    ]
    return candidates[step % len(candidates)]


# ── Throughput snapshot helper ────────────────────────────────────────────────

def _tpt_snapshot(sock: zmq.Socket, n: int = 30) -> float:
    """Return mean per-UE tx_bytes over n TTIs (proxy for throughput)."""
    total_bytes = 0
    count = 0
    for _ in range(n):
        try:
            raw = sock.recv()
            m = metrics_pb2.Metrics()
            m.ParseFromString(raw)
            for ue in m.ue_metrics:
                total_bytes += ue.tx_bytes
                count += 1
        except zmq.Again:
            break
    return total_bytes / count if count > 0 else 0.0


# ── CPU poller (remote via SSH) ───────────────────────────────────────────────

def _cpu_pct_remote(twin_host: str) -> float:
    """One-shot SSH cpu check; returns load average (1 min) as a % proxy."""
    import subprocess
    try:
        out = subprocess.check_output(
            ["ssh", twin_host, "cat /proc/loadavg"],
            timeout=5, stderr=subprocess.DEVNULL
        ).decode()
        load1 = float(out.split()[0])
        # Rough conversion: 1 core fully busy = 100%; divide by nproc estimate (8 cores)
        return min(load1 / 8.0 * 100, 100.0)
    except Exception:
        return float("nan")


# ── Mock oracle (for smoke test) ──────────────────────────────────────────────

class _MockOracle:
    """Always approves; simulates oracle verdict for loopback testing."""

    def evaluate_weights(self, weights: dict[int, float]):
        import random
        class _V:
            safe = True
            mean_improvement = round(random.uniform(0.01, 0.15), 3)
            def summary(self): return f"[MOCK] approved weights={weights} improvement={self.mean_improvement:.3f}"
        return _V()

    def deploy_weights(self, weights): pass


# ── Main run loop ─────────────────────────────────────────────────────────────

def run(
    real_host: str,
    twin_host: str,
    duration_s: float,
    swap_interval_s: float,
    n_replicas: int,
    out_path: Path,
    mock: bool,
) -> None:
    if mock:
        oracle = _MockOracle()
        real_sub = None
        print(f"[E4][MOCK] Running mock safety run for {duration_s}s", flush=True)
    else:
        from orchestrator.counterfactual_oracle import CounterfactualOracle
        oracle = CounterfactualOracle(
            twin_host=twin_host,
            real_host=real_host,
            n_replicas=n_replicas,
        )
        ctx = zmq.Context.instance()
        real_sub = ctx.socket(zmq.SUB)
        real_sub.connect(f"tcp://{real_host}:{METRICS_PORT}")
        real_sub.setsockopt(zmq.SUBSCRIBE, b"")
        real_sub.setsockopt(zmq.RCVTIMEO, 3000)

    out_path.parent.mkdir(parents=True, exist_ok=True)
    t0 = time.time()
    step = 0
    quarantine_count = 0
    pf_tpt = None  # established on first swap

    print(f"[E4] Starting {duration_s/3600:.1f}hr safety run  swap_interval={swap_interval_s}s", flush=True)
    print(f"[E4] Writing to {out_path}", flush=True)

    with open(out_path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow([
            "elapsed_s", "step", "weights",
            "oracle_approved", "mean_improvement",
            "cpu_pct", "tpt_proxy", "quarantine_total",
        ])

        while (elapsed := time.time() - t0) < duration_s:
            weights = _next_candidate(step)

            # M2 oracle gate
            verdict = oracle.evaluate_weights(weights) if not mock else oracle.evaluate_weights(weights)
            approved = verdict.safe
            improvement = getattr(verdict, "mean_improvement", float("nan"))

            if not approved:
                quarantine_count += 1
                print(f"[E4] t={elapsed:7.0f}s  step={step:3d}  REJECTED  quarantines={quarantine_count}",
                      flush=True)
            else:
                if not mock:
                    oracle.deploy_weights(weights)
                print(f"[E4] t={elapsed:7.0f}s  step={step:3d}  deployed={weights}  Δ={improvement:.3f}",
                      flush=True)

            # Throughput proxy
            tpt = _tpt_snapshot(real_sub, n=30) if real_sub else round(100 + step * 0.5, 1)
            if pf_tpt is None:
                pf_tpt = tpt

            # CPU on twin
            cpu = _cpu_pct_remote(twin_host) if not mock else round(20 + step % 10, 1)

            w.writerow([
                f"{elapsed:.1f}", step, str(weights),
                int(approved), f"{improvement:.4f}",
                f"{cpu:.1f}", f"{tpt:.1f}", quarantine_count,
            ])
            f.flush()

            step += 1
            time.sleep(swap_interval_s)

    print(f"\n[E4] Done.  steps={step}  quarantine_events={quarantine_count}", flush=True)
    if pf_tpt and pf_tpt > 0:
        pass  # throughput tracking is per-row in CSV; summary printed by acceptance script


def main():
    parser = argparse.ArgumentParser(description="E4 M2-gated 4hr safety run")
    parser.add_argument("--real-host",      default="powder-gnb")
    parser.add_argument("--twin-host",      default="powder-twin")
    parser.add_argument("--duration",       type=float, default=14400,
                        help="Run duration in seconds (default: 4hr)")
    parser.add_argument("--swap-interval",  type=float, default=300,
                        help="Seconds between policy swap attempts")
    parser.add_argument("--n-replicas",     type=int,   default=4)
    parser.add_argument("--out",            default="logs/e4_safety_run.csv")
    parser.add_argument("--mock",           action="store_true",
                        help="Mock mode: no real ZMQ/SSH, loopback test only")
    args = parser.parse_args()

    run(
        real_host       = args.real_host,
        twin_host       = args.twin_host,
        duration_s      = args.duration,
        swap_interval_s = args.swap_interval,
        n_replicas      = args.n_replicas,
        out_path        = Path(args.out),
        mock            = args.mock,
    )


if __name__ == "__main__":
    main()
