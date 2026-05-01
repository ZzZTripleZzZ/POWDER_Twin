"""
W10 — Adaptive Recalibration Daemon (Contribution 4, operational component).

Continuously monitors twin fidelity. When fidelity drops below a threshold
(or after the computed optimal interval Δt* = τ/3 elapses), automatically:
  1. Re-runs offline_replay.py to regenerate CIR files from fresh real logs
  2. Uploads updated CIR files to the twin node via SCP
  3. Restarts the twin container with the new CIR
  4. Logs each recalibration event with timestamp and fidelity delta

This closes the loop on Contribution 4: the theoretical Δt* derived from
the exp-decay fit becomes an operational policy.

Usage:
    # Continuous adaptive recalibration (uses computed τ to set Δt*):
    python orchestrator/auto_recalibrate.py \
        --real-host powder-gnb \
        --twin-host powder-twin \
        --snr-log-remote /tmp/edgeric/snr.txt \
        --cir-dir logs/cir_online \
        --fidelity-threshold 2.0 \
        --tau 300 \
        --recal-log logs/recalibration_events.csv

    # Fixed-interval mode (bypass adaptive, recalibrate every Δt seconds):
    python orchestrator/auto_recalibrate.py ... --fixed-interval 100

    # Dry-run (mock, no real network):
    python orchestrator/auto_recalibrate.py --mock --tau 60 --duration 600
"""
import argparse
import asyncio
import csv
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path

import numpy as np
import zmq

sys.path.insert(0, str(Path(__file__).parent.parent))
sys.path.insert(0, str(Path(__file__).parent.parent / "proto"))

import metrics_pb2
from orchestrator.channel_converter import parse_snr_log, generate_cir_files
from orchestrator.e4_fidelity_decay import fidelity_metrics, fit_exp_decay

REAL_METRICS_PORT = 5555
TWIN_METRICS_PORT = 5555
FIDELITY_SAMPLES  = 50   # TTIs per snapshot for fidelity measurement


# ── Fidelity snapshot ─────────────────────────────────────────────────────────

def snapshot(host: str, port: int, n_samples: int, ctx: zmq.Context) -> dict[int, dict]:
    """Collect n_samples TTIs → mean per-UE metrics."""
    from collections import defaultdict
    sub = ctx.socket(zmq.SUB)
    sub.connect(f"tcp://{host}:{port}")
    sub.setsockopt(zmq.SUBSCRIBE, b"")
    sub.setsockopt(zmq.CONFLATE, 1)
    sub.setsockopt(zmq.RCVTIMEO, 3000)

    accum: dict[int, list] = defaultdict(list)
    for _ in range(n_samples):
        try:
            raw = sub.recv()
            m = metrics_pb2.Metrics()
            m.ParseFromString(raw)
            for ue in m.ue_metrics:
                accum[ue.rnti].append({
                    "cqi": ue.cqi, "snr": ue.snr,
                    "tx_bytes": ue.tx_bytes, "dl_buf": ue.dl_buffer,
                })
        except zmq.Again:
            break
    sub.close()

    return {
        rnti: {k: np.mean([d[k] for d in vals]) for k in vals[0]}
        for rnti, vals in accum.items() if vals
    }


# ── Recalibration action ──────────────────────────────────────────────────────

async def fetch_snr_log(real_host: str, remote_path: str, local_path: Path) -> bool:
    """Pull the latest snr.txt from the real node."""
    try:
        import asyncssh
        async with asyncssh.connect(real_host, username="zifanzhang",
                                    known_hosts=None) as conn:
            await asyncssh.scp(f"{real_host}:{remote_path}", str(local_path))
        return True
    except Exception as e:
        print(f"  [Recal] SCP fetch failed: {e}")
        return False


async def upload_cir(twin_host: str, cir_real: Path, cir_imag: Path,
                     remote_dir: str = "/tmp/dt_cir_latest") -> bool:
    """Upload updated CIR files to the twin node."""
    try:
        import asyncssh
        async with asyncssh.connect(twin_host, username="zifanzhang",
                                    known_hosts=None) as conn:
            await conn.run(f"mkdir -p {remote_dir}")
            await asyncssh.scp(str(cir_real), f"{twin_host}:{remote_dir}/cir_real.txt")
            await asyncssh.scp(str(cir_imag), f"{twin_host}:{remote_dir}/cir_imag.txt")
        return True
    except Exception as e:
        print(f"  [Recal] SCP upload failed: {e}")
        return False


async def restart_twin(twin_host: str, compose_file: str,
                       cir_real_path: str, cir_imag_path: str) -> bool:
    """Restart tt-gnb container with updated CIR environment variables."""
    try:
        import asyncssh
        cmd = (
            f"TT_CHANNEL_FILE_REAL={cir_real_path} "
            f"TT_CHANNEL_FILE_IMAG={cir_imag_path} "
            f"sudo docker compose -f {compose_file} up -d tt-gnb"
        )
        async with asyncssh.connect(twin_host, username="zifanzhang",
                                    known_hosts=None) as conn:
            result = await conn.run(cmd)
            if result.exit_status != 0:
                print(f"  [Recal] docker compose restart failed: {result.stderr}")
                return False
        return True
    except Exception as e:
        print(f"  [Recal] Restart failed: {e}")
        return False


async def do_recalibrate(
    real_host: str,
    twin_host: str,
    snr_log_remote: str,
    local_snr_log: Path,
    cir_dir: Path,
    compose_file: str,
    mock: bool,
) -> bool:
    """Full recalibration pipeline. Returns True on success."""
    cir_dir.mkdir(parents=True, exist_ok=True)
    cir_real = cir_dir / "cir_latest_real.txt"
    cir_imag = cir_dir / "cir_latest_imag.txt"

    if mock:
        # Simulate new CIR files with slight variation
        rng = np.random.default_rng(seed=int(time.time()))
        base = np.ones((100, 10)) * 0.5
        noise = rng.normal(0, 0.05, base.shape)
        np.savetxt(str(cir_real), base + noise, fmt="%.6f")
        np.savetxt(str(cir_imag), noise, fmt="%.6f")
        print("  [Recal] Mock CIR files generated.")
        return True

    # Step 1: fetch fresh SNR log
    print("  [Recal] Fetching snr.txt from real node ...")
    ok = await fetch_snr_log(real_host, snr_log_remote, local_snr_log)
    if not ok:
        return False

    # Step 2: regenerate CIR files
    print("  [Recal] Regenerating CIR from SNR log ...")
    try:
        records = parse_snr_log(local_snr_log)
        if not records:
            print("  [Recal] Empty SNR log.")
            return False
        # Use the last 500 TTIs for freshest estimate
        trimmed = {rnti: recs[-500:] for rnti, recs in records.items()}
        for rnti, recs in trimmed.items():
            generate_cir_files(recs,
                               cir_dir / f"cir_ue{rnti:04x}_real.txt",
                               cir_dir / f"cir_ue{rnti:04x}_imag.txt")
            # Use first RNTI as the "latest" symlink
            import shutil
            shutil.copy(str(cir_dir / f"cir_ue{rnti:04x}_real.txt"), str(cir_real))
            shutil.copy(str(cir_dir / f"cir_ue{rnti:04x}_imag.txt"), str(cir_imag))
            break
    except Exception as e:
        print(f"  [Recal] CIR generation failed: {e}")
        return False

    # Step 3: upload CIR to twin
    print("  [Recal] Uploading CIR to twin ...")
    remote_dir = "/tmp/dt_cir_latest"
    ok = await upload_cir(twin_host, cir_real, cir_imag, remote_dir)
    if not ok:
        return False

    # Step 4: restart twin with new CIR
    print("  [Recal] Restarting twin ...")
    ok = await restart_twin(
        twin_host, compose_file,
        f"{remote_dir}/cir_real.txt",
        f"{remote_dir}/cir_imag.txt",
    )
    return ok


# ── Event logger ──────────────────────────────────────────────────────────────

class RecalibrationLog:
    def __init__(self, path: Path):
        path.parent.mkdir(parents=True, exist_ok=True)
        self._f = open(path, "w", newline="")
        self._w = csv.writer(self._f)
        self._w.writerow(["timestamp", "elapsed_s", "cqi_rmse_before",
                          "cqi_rmse_after", "trigger", "success"])
        self._f.flush()

    def record(self, elapsed_s: float, rmse_before: float, rmse_after: float,
               trigger: str, success: bool):
        self._w.writerow([
            time.strftime("%Y-%m-%dT%H:%M:%S"),
            f"{elapsed_s:.1f}",
            f"{rmse_before:.4f}",
            f"{rmse_after:.4f}",
            trigger,
            int(success),
        ])
        self._f.flush()
        print(f"  [Recal] Logged: t={elapsed_s:.0f}s  "
              f"RMSE {rmse_before:.3f}→{rmse_after:.3f}  "
              f"trigger={trigger}  ok={success}")

    def close(self):
        self._f.close()


# ── Adaptive interval logic ───────────────────────────────────────────────────

def next_interval(tau: float, fixed_interval: float | None) -> float:
    """
    Compute next recalibration interval.
    If tau is known: Δt* = τ/3 (minimizes overhead × fidelity loss).
    If fixed_interval provided: use that instead.
    """
    if fixed_interval is not None:
        return fixed_interval
    return max(tau / 3.0, 10.0)   # at least 10s


# ── Main daemon loop ──────────────────────────────────────────────────────────

def run(
    real_host: str,
    twin_host: str,
    snr_log_remote: str,
    cir_dir: Path,
    recal_log_path: Path,
    fidelity_threshold: float,
    tau: float,
    fixed_interval: float | None,
    duration_s: float,
    compose_file: str,
    mock: bool,
):
    local_snr_log = cir_dir / "snr_latest.txt"
    ctx = zmq.Context()
    log = RecalibrationLog(recal_log_path)

    t0 = time.time()
    last_recal_t = t0
    recal_count  = 0
    times_since_cal: list[float] = []
    rmse_since_cal: list[float]  = []

    interval = next_interval(tau, fixed_interval)
    print(f"[AutoRecal] Starting. τ={tau:.0f}s  Δt*={interval:.0f}s  "
          f"threshold={fidelity_threshold}  duration={duration_s:.0f}s  "
          f"{'MOCK' if mock else 'REAL'}")

    while (elapsed := time.time() - t0) < duration_s:
        # ── Measure current fidelity ──
        if mock:
            # Simulate fidelity decay following exp(-t/τ) with noise
            t_since = time.time() - last_recal_t
            base_rmse = 0.3 * np.exp(t_since / tau) + np.random.normal(0, 0.05)
            cqi_rmse = max(0.1, float(base_rmse))
            real_snap = twin_snap = {}   # unused in mock
        else:
            real_snap = snapshot(real_host, REAL_METRICS_PORT, FIDELITY_SAMPLES, ctx)
            twin_snap = snapshot(twin_host, TWIN_METRICS_PORT, FIDELITY_SAMPLES, ctx)
            fm = fidelity_metrics(real_snap, twin_snap)
            cqi_rmse = fm["cqi_rmse"]

        times_since_cal.append(time.time() - last_recal_t)
        if not np.isnan(cqi_rmse):
            rmse_since_cal.append(cqi_rmse)

        t_since_recal = time.time() - last_recal_t
        print(f"[AutoRecal] t={elapsed:6.0f}s  since_recal={t_since_recal:5.0f}s  "
              f"CQI_RMSE={cqi_rmse:.3f}  "
              f"(threshold={fidelity_threshold}  Δt*={interval:.0f}s)")

        # ── Decide whether to recalibrate ──
        fidelity_trigger = (not np.isnan(cqi_rmse) and
                            cqi_rmse > fidelity_threshold)
        interval_trigger = t_since_recal >= interval
        trigger = ("fidelity" if fidelity_trigger else
                   "interval" if interval_trigger else None)

        if trigger:
            print(f"\n[AutoRecal] RECALIBRATING (trigger={trigger}) ...")
            rmse_before = cqi_rmse

            success = asyncio.run(do_recalibrate(
                real_host, twin_host, snr_log_remote,
                local_snr_log, cir_dir, compose_file, mock,
            ))

            # Re-measure fidelity after recalibration
            if mock:
                rmse_after = 0.3 + np.random.normal(0, 0.03)
            else:
                time.sleep(5)  # wait for twin to restart
                real_snap = snapshot(real_host, REAL_METRICS_PORT, FIDELITY_SAMPLES, ctx)
                twin_snap = snapshot(twin_host, TWIN_METRICS_PORT, FIDELITY_SAMPLES, ctx)
                fm_after  = fidelity_metrics(real_snap, twin_snap)
                rmse_after = fm_after["cqi_rmse"]

            log.record(elapsed, rmse_before, rmse_after, trigger, success)
            recal_count += 1
            last_recal_t = time.time()

            # Update τ estimate from observed decay (if enough data)
            if len(rmse_since_cal) >= 5:
                A, tau_est = fit_exp_decay(times_since_cal[:len(rmse_since_cal)],
                                           rmse_since_cal)
                if not np.isnan(tau_est) and tau_est > 0:
                    tau = tau_est
                    interval = next_interval(tau, fixed_interval)
                    print(f"  [AutoRecal] Updated τ={tau:.1f}s → Δt*={interval:.0f}s")

            times_since_cal.clear()
            rmse_since_cal.clear()
            print()

        time.sleep(min(10.0, interval / 6))   # check every Δt*/6 at most

    ctx.term()
    log.close()

    print(f"\n[AutoRecal] Done. {recal_count} recalibrations in {duration_s:.0f}s. "
          f"Log → {recal_log_path}")


def main():
    parser = argparse.ArgumentParser(description="Adaptive twin recalibration daemon")
    parser.add_argument("--real-host",         default="powder-gnb")
    parser.add_argument("--twin-host",         default="powder-twin")
    parser.add_argument("--snr-log-remote",    default="/tmp/edgeric/snr.txt",
                        help="Path to snr.txt on the real node")
    parser.add_argument("--cir-dir",           default="logs/cir_online",
                        help="Local directory for generated CIR files")
    parser.add_argument("--recal-log",         default="logs/recalibration_events.csv")
    parser.add_argument("--fidelity-threshold", type=float, default=2.0,
                        help="CQI RMSE above which recalibration is triggered")
    parser.add_argument("--tau",               type=float, default=300.0,
                        help="Initial coherence time estimate τ (s); updated adaptively")
    parser.add_argument("--fixed-interval",    type=float, default=None,
                        help="Use fixed recalibration interval (s) instead of τ/3")
    parser.add_argument("--duration",          type=float, default=3600,
                        help="Total run duration (s)")
    parser.add_argument("--compose-file",      default="sims/docker-compose.twin.yaml",
                        help="Docker compose file on the twin node")
    parser.add_argument("--mock",              action="store_true",
                        help="Simulate without real network")
    args = parser.parse_args()

    run(
        real_host=args.real_host,
        twin_host=args.twin_host,
        snr_log_remote=args.snr_log_remote,
        cir_dir=Path(args.cir_dir),
        recal_log_path=Path(args.recal_log),
        fidelity_threshold=args.fidelity_threshold,
        tau=args.tau,
        fixed_interval=args.fixed_interval,
        duration_s=args.duration,
        compose_file=args.compose_file,
        mock=args.mock,
    )


if __name__ == "__main__":
    main()
