"""
Real-Time Dual-Layer State Sync: Real → Twin.

Implements M1 (drift-aware live calibration) and provides the sync substrate
for M3 (selective-fidelity FIFO writes) and M4 (MAC-state reconciliation).

Layer 1 (PHY / channel) — M1:
  SSH-tail real-side logs/snr.txt → parse (UL CQI, UL ACK) per UE per TTI
  → ChannelCalibrator drift metric D(t) → trigger-based EMA recalibration
  → cqi_to_cir() → named FIFO → Tiny_Twin apply_channelmod.c

  Background calibration:
  Poll twin gNB docker logs for UL SNR → feed (real_cqi, twin_snr) to
  ChannelCalibrator.update() only when D(t) > tau_drift (M1 gate).

  E1 logging (--drift-log):
  Every 5 s write (timestamp, D(t), trigger_count, coherence_samples, twin_snr)
  to CSV for paper figure E1.

Layer 2 (MAC / scheduler state):
  ZMQ SUB real EdgeRIC metrics (protobuf, port 5555)
  → stamp with (tti_seq, state_hash) [M4]
  → re-publish to twin EdgeRIC (port 5655)
  → MacReconciler serves reconciliation requests on port 5560 [M4]

Usage:
    python orchestrator/state_sync.py \\
        --real-host powder-real \\
        --twin-host powder-twin \\
        [--rnti 0x4401] \\
        [--cal-path logs/calibration.npy] \\
        [--drift-log logs/drift.csv]
"""
import argparse
import asyncio
import csv
import os
import re
import sys
import threading
import time
from collections import defaultdict, deque
from pathlib import Path
from typing import AsyncIterator

import asyncssh
import zmq

sys.path.insert(0, str(Path(__file__).parent.parent))
from orchestrator.channel_converter import (
    cqi_to_cir, cqi_to_snr_db, snr_db_to_cqi, bler_from_snr, ChannelCalibrator
)
from orchestrator.selective_fidelity import (
    Mode, RuleBasedSelector, SelectorSignals,
    build_signals, sparsify_topk, cir_writer_gate,
    COLD_START_SIGNALS,
)

# ZMQ ports -- must match edgeric.cpp hardcoded values
REAL_METRICS_PORT  = 5555   # EdgeRIC PUB (metrics)
TWIN_WEIGHTS_PORT  = 5556   # EdgeRIC SUB (scheduling weights)
TWIN_MCS_PORT      = 5557   # EdgeRIC SUB (MCS)

PIPE_REAL_TMPL = "/tmp/tt_cir_real.fifo"
PIPE_IMAG_TMPL = "/tmp/tt_cir_imag.fifo"

USERNAME = "zifan716"

# Regex to extract UL SNR from twin gNB docker logs
_UL_SNR_RE = re.compile(r"ulsch_rounds.*?SNR ([\d.]+) dB")
# Calibration poll interval (seconds)
CAL_POLL_INTERVAL = 10.0
# How often to log calibrator summary
CAL_LOG_INTERVAL = 60.0
# M1: rolling window for real-side BLER computed from UL ACK stream (per RNTI)
_BLER_WINDOW = 100
# M1: minimum ACK samples before real_bler is considered usable
_BLER_MIN_SAMPLES = 8


# ── Layer 1: Channel sync ─────────────────────────────────────────────────────

async def _tail_snr_log(host: str, log_path: str = "~/Tiny_Twin/logs/snr.txt") -> AsyncIterator[str]:
    """SSH tail -f the real-side snr.txt, yield lines."""
    async with asyncssh.connect(host, username=USERNAME, known_hosts=None, config=[Path.home() / ".ssh" / "config"]) as conn:
        async with conn.create_process(f"tail -F {log_path}") as proc:
            async for line in proc.stdout:
                yield line.strip()


async def _poll_twin_snr(conn, snr_box: list, interval: float = CAL_POLL_INTERVAL):
    """
    Background coroutine: periodically fetch latest UL SNR from twin gNB logs.
    Reuses an existing asyncssh connection. Stores float in snr_box[0].
    """
    cmd = "sudo docker logs tt-gnb --tail 200 2>/dev/null | grep 'ulsch_rounds.*SNR' | tail -1"
    while True:
        try:
            result = await conn.run(cmd, check=False)
            m = _UL_SNR_RE.search(result.stdout or "")
            snr_box[0] = float(m.group(1)) if m else None
        except Exception as e:
            print(f"[Cal] SNR poll error: {e}", flush=True)
            snr_box[0] = None
        await asyncio.sleep(interval)


async def _restart_production_twin(mode: Mode, twin_host: str = None) -> None:
    """Gracefully restart the production tt-gnb container with a new TT_FIDELITY_MODE.

    Stops the running container, then recreates it with TT_FIDELITY_MODE updated
    via docker-compose (preferred) or a plain docker stop/start sequence.
    The FIFO writer continues on the Python side; the 2-3 s gap is acceptable
    because mode decisions are made at ≥60 s cadence (M3 claim is minute-level).
    """
    print(f"[M3] Restarting production twin with mode={mode.value}", flush=True)
    if twin_host is None:
        # Local mode: update env via docker-compose with env-override, or plain stop+start.
        # docker-compose reads TT_FIDELITY_MODE from the shell env when recreating.
        try:
            env = {**os.environ, "TT_FIDELITY_MODE": mode.value,
                   "TT_SPARSE_K": "4"}
            # Try docker-compose first (preferred: preserves volumes/networks)
            proc = await asyncio.create_subprocess_exec(
                "docker-compose", "up", "-d", "--force-recreate", "tt-gnb",
                env=env,
                stdout=asyncio.subprocess.DEVNULL,
                stderr=asyncio.subprocess.PIPE,
            )
            _, stderr = await asyncio.wait_for(proc.communicate(), timeout=30.0)
            if proc.returncode != 0:
                # Fallback: stop + start with env passed via docker update (best-effort)
                stop = await asyncio.create_subprocess_exec(
                    "docker", "stop", "tt-gnb",
                    stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.DEVNULL,
                )
                await stop.wait()
                start = await asyncio.create_subprocess_exec(
                    "docker", "start", "tt-gnb",
                    stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.DEVNULL,
                )
                await start.wait()
                print(f"[M3] Fallback stop/start done (env must be set in compose file)", flush=True)
        except Exception as e:
            print(f"[M3] Restart failed: {e}", flush=True)
    # Remote restart goes through asyncssh; left as extension point.


async def layer1_channel_sync(
    real_host: str,
    twin_host: str,
    rnti_filter=None,
    calibrator=None,
    local_fifo: bool = False,
    drift_log_path: str = None,
) -> None:
    """
    Tail real SNR log → CQI → CIR (M1 trigger-based calibration) → named FIFO.

    local_fifo=True  : write directly to /tmp FIFOs (run ON the twin node).
    local_fifo=False : push CIR to twin over asyncssh cat tunnel (Mac-as-orchestrator).
    drift_log_path   : if set, write D(t) time-series CSV every 5 s (E1 figure data).
    """
    tti_re = re.compile(r"TTI (?:Index|Count): (\d+)")
    cqi_re = re.compile(r"UL CQI: (\d+) RNTI: ([0-9a-fA-F]+)")
    ack_re = re.compile(r"UL ACK: (\d+) RNTI: ([0-9a-fA-F]+)")
    snr_box = [None]
    import numpy as np
    rng = np.random.default_rng()
    last_cal_log   = [time.time()]
    last_drift_log = [time.time()]
    t0 = time.time()
    # M1: per-RNTI rolling window of UL ACK outcomes for empirical real_bler
    rolling_acks: "defaultdict[int, deque[int]]" = defaultdict(
        lambda: deque(maxlen=_BLER_WINDOW)
    )

    # M3: mode selector state
    _m3_selector      = RuleBasedSelector()
    _m3_last_mode     = Mode.FULL_IQ
    _m3_last_recal_t  = 0.0       # time.time() of last M1 trigger
    _m3_cqi_window: list          = []
    _m3_last_verdicts: list       = []   # updated by oracle callback if wired
    _m3_cqi_count     = 0
    K_MODE_CHECK      = 50        # re-evaluate mode every N CQI observations

    # E1 figure: D(t) CSV writer (None if no path given)
    _drift_csv_file   = None
    _drift_csv_writer = None
    if drift_log_path and calibrator is not None:
        Path(drift_log_path).parent.mkdir(parents=True, exist_ok=True)
        _drift_csv_file   = open(drift_log_path, "w", newline="", buffering=1)
        _drift_csv_writer = csv.writer(_drift_csv_file)
        _drift_csv_writer.writerow(
            ["elapsed_s", "D_t", "trigger_count", "coherence_samples", "twin_snr_db"]
        )
        print(f"[M1/E1] Drift log → {drift_log_path}", flush=True)

    def _log_drift_if_due() -> None:
        if _drift_csv_writer is None or calibrator is None:
            return
        now = time.time()
        if now - last_drift_log[0] < 5.0:
            return
        last_drift_log[0] = now
        d = calibrator.kl_drift_metric()
        _drift_csv_writer.writerow([
            f"{now - t0:.1f}",
            f"{d:.6f}" if d is not None else "",
            calibrator.trigger_count(),
            calibrator.coherence_samples(),
            f"{snr_box[0]:.2f}" if snr_box[0] is not None else "",
        ])

    def _real_bler(rnti: int) -> "float | None":
        acks = rolling_acks.get(rnti)
        if acks is None or len(acks) < _BLER_MIN_SAMPLES:
            return None
        # OAI logs UL ACK: 1 = success, 0 = NAK; BLER = fraction of NAKs.
        return 1.0 - (sum(acks) / len(acks))

    def _maybe_drift_observe(real_cqi: int, rnti: int) -> None:
        """M1: feed one paired (real, twin) sample to the calibrator's drift buffer.
        Only fires when we have a fresh twin SNR and enough real ACKs."""
        if calibrator is None or snr_box[0] is None:
            return
        rb = _real_bler(rnti)
        if rb is None:
            return
        twin_snr = snr_box[0]
        twin_cqi = snr_db_to_cqi(twin_snr)
        twin_bler = bler_from_snr(twin_snr)
        calibrator.observe_drift_sample(
            real_cqi=real_cqi, real_bler=rb,
            twin_cqi=twin_cqi, twin_bler=twin_bler,
        )

    def _maybe_recalibrate(real_cqi: int) -> bool:
        """M1: gate the EMA update on D(t) > tau_drift. Returns True if applied."""
        nonlocal _m3_last_recal_t
        if calibrator is None or snr_box[0] is None:
            return False
        if not calibrator.should_recalibrate():
            return False
        calibrator.update(real_cqi=real_cqi, twin_snr_db=snr_box[0])
        calibrator.mark_recalibration()
        _m3_last_recal_t = time.time()
        return True

    async def _run_local_fifo():
        """Write CIR directly to local FIFOs (no SSH to twin)."""
        nonlocal _m3_cqi_count, _m3_last_mode
        import os
        for p in (PIPE_REAL_TMPL, PIPE_IMAG_TMPL):
            if not os.path.exists(p):
                os.mkfifo(p)

        # Open FIFOs in a thread (open blocks until reader connects)
        loop = asyncio.get_event_loop()
        fr, fi = await asyncio.gather(
            loop.run_in_executor(None, lambda: open(PIPE_REAL_TMPL, "w", buffering=1)),
            loop.run_in_executor(None, lambda: open(PIPE_IMAG_TMPL, "w", buffering=1)),
        )
        print(f"[L1] Local FIFOs open: {PIPE_REAL_TMPL}", flush=True)

        # Calibration: poll twin gNB docker logs locally (non-blocking asyncio subprocess)
        async def poll_snr_local():
            while True:
                try:
                    proc = await asyncio.create_subprocess_exec(
                        "sudo", "docker", "logs", "tt-gnb", "--tail", "200",
                        stdout=asyncio.subprocess.PIPE,
                        stderr=asyncio.subprocess.PIPE,
                    )
                    stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=5.0)
                    text = (stdout + stderr).decode("utf-8", errors="ignore")
                    m = _UL_SNR_RE.search(text)
                    snr_box[0] = float(m.group(1)) if m else None
                except Exception as e:
                    snr_box[0] = None
                await asyncio.sleep(CAL_POLL_INTERVAL)

        snr_task = asyncio.ensure_future(poll_snr_local())
        try:
            async for line in _tail_snr_log(real_host):
                if m := tti_re.match(line):
                    pass
                elif m := ack_re.match(line):
                    ack  = int(m.group(1))
                    rnti = int(m.group(2), 16)
                    if rnti_filter is not None and rnti != rnti_filter:
                        continue
                    rolling_acks[rnti].append(ack)
                elif m := cqi_re.match(line):
                    cqi  = int(m.group(1))
                    rnti = int(m.group(2), 16)
                    if rnti_filter is not None and rnti != rnti_filter:
                        continue
                    # M1: observe drift, then gate EMA update on D(t) > tau
                    _maybe_drift_observe(cqi, rnti)
                    _maybe_recalibrate(cqi)

                    # M3: maintain CQI window + re-evaluate mode every K TTIs
                    _m3_cqi_window.append(cqi)
                    if len(_m3_cqi_window) > 50:
                        _m3_cqi_window.pop(0)
                    _m3_cqi_count += 1
                    if _m3_cqi_count % K_MODE_CHECK == 0:
                        sig  = build_signals(
                            calibrator, _m3_cqi_window, _m3_last_verdicts,
                            _m3_last_recal_t, rnti_count=len(rolling_acks),
                        )
                        new_mode = _m3_selector.choose_mode(sig)
                        if new_mode != _m3_last_mode:
                            d_str = f"  D(t)={sig.D_t:.3f}" if sig.D_t is not None else ""
                            print(f"[M3] mode {_m3_last_mode.value} → {new_mode.value}{d_str}",
                                  flush=True)
                            _m3_last_mode = new_mode
                            asyncio.ensure_future(
                                _restart_production_twin(new_mode, twin_host=None)
                            )

                    # M3: gate FIFO write; apply sparsify if sparse_tap
                    with cir_writer_gate(_m3_last_mode) as should_write:
                        if should_write:
                            r_taps, i_taps = cqi_to_cir(cqi, calibrator=calibrator, rng=rng)
                            if _m3_last_mode == Mode.SPARSE_TAP:
                                r_taps, i_taps = sparsify_topk(r_taps, i_taps, K=4)
                            fr.write(" ".join(f"{v:.6f}" for v in r_taps) + "\n")
                            fi.write(" ".join(f"{v:.6f}" for v in i_taps) + "\n")

                    _log_drift_if_due()
                    now = time.time()
                    if calibrator is not None and now - last_cal_log[0] > CAL_LOG_INTERVAL:
                        print(
                            f"[L1] {calibrator.summary()} | twin_snr={snr_box[0]} dB"
                            f" | M3_mode={_m3_last_mode.value}",
                            flush=True,
                        )
                        last_cal_log[0] = now
        finally:
            snr_task.cancel()
            fr.close(); fi.close()
            if _drift_csv_file:
                _drift_csv_file.close()

    async def _run_remote_fifo():
        """Push CIR to twin via asyncssh cat tunnel (original Mac-as-orchestrator mode)."""
        nonlocal _m3_cqi_count, _m3_last_mode
        async with asyncssh.connect(twin_host, username=USERNAME, known_hosts=None, config=[Path.home() / ".ssh" / "config"]) as twin_conn:
            rnti_pipes = {}
            _pipes_ready = False

            async def ensure_pipe(rnti: int):
                nonlocal _pipes_ready
                if _pipes_ready:
                    return
                rp, ip = PIPE_REAL_TMPL, PIPE_IMAG_TMPL
                await twin_conn.run(f"mkfifo {rp} {ip} 2>/dev/null; true")
                real_proc = await twin_conn.create_process(f"cat > {rp}")
                imag_proc = await twin_conn.create_process(f"cat > {ip}")
                rnti_pipes[rnti] = (real_proc.stdin, imag_proc.stdin)
                _pipes_ready = True
                print(f"[L1] Named pipes ready (SSH) for RNTI 0x{rnti:04x}", flush=True)

            cal_task = asyncio.ensure_future(
                _poll_twin_snr(twin_conn, snr_box, CAL_POLL_INTERVAL)
            )
            try:
                async for line in _tail_snr_log(real_host):
                    if m := tti_re.match(line):
                        pass
                    elif m := ack_re.match(line):
                        ack  = int(m.group(1))
                        rnti = int(m.group(2), 16)
                        if rnti_filter is not None and rnti != rnti_filter:
                            continue
                        rolling_acks[rnti].append(ack)
                    elif m := cqi_re.match(line):
                        cqi  = int(m.group(1))
                        rnti = int(m.group(2), 16)
                        if rnti_filter is not None and rnti != rnti_filter:
                            continue
                        # M1: observe drift, then gate EMA update on D(t) > tau
                        _maybe_drift_observe(cqi, rnti)
                        _maybe_recalibrate(cqi)

                        # M3: maintain CQI window + re-evaluate mode every K TTIs
                        _m3_cqi_window.append(cqi)
                        if len(_m3_cqi_window) > 50:
                            _m3_cqi_window.pop(0)
                        _m3_cqi_count += 1
                        if _m3_cqi_count % K_MODE_CHECK == 0:
                            sig  = build_signals(
                                calibrator, _m3_cqi_window, _m3_last_verdicts,
                                _m3_last_recal_t, rnti_count=len(rolling_acks),
                            )
                            new_mode = _m3_selector.choose_mode(sig)
                            if new_mode != _m3_last_mode:
                                d_str = f"  D(t)={sig.D_t:.3f}" if sig.D_t is not None else ""
                                print(f"[M3] mode {_m3_last_mode.value} → {new_mode.value}{d_str}",
                                      flush=True)
                                _m3_last_mode = new_mode

                        # M3: gate FIFO write; apply sparsify if sparse_tap
                        with cir_writer_gate(_m3_last_mode) as should_write:
                            if should_write:
                                await ensure_pipe(rnti)
                                r_taps, i_taps = cqi_to_cir(cqi, calibrator=calibrator, rng=rng)
                                if _m3_last_mode == Mode.SPARSE_TAP:
                                    r_taps, i_taps = sparsify_topk(r_taps, i_taps, K=4)
                                r_line = " ".join(f"{v:.6f}" for v in r_taps) + "\n"
                                i_line = " ".join(f"{v:.6f}" for v in i_taps) + "\n"
                                rw, iw = rnti_pipes[rnti]
                                rw.write(r_line)
                                iw.write(i_line)

                        _log_drift_if_due()
                        now = time.time()
                        if calibrator is not None and now - last_cal_log[0] > CAL_LOG_INTERVAL:
                            print(
                                f"[L1] {calibrator.summary()} | twin_snr={snr_box[0]} dB"
                                f" | M3_mode={_m3_last_mode.value}",
                                flush=True,
                            )
                            last_cal_log[0] = now
            finally:
                cal_task.cancel()
                if _drift_csv_file:
                    _drift_csv_file.close()

    if local_fifo:
        await _run_local_fifo()
    else:
        await _run_remote_fifo()

    if calibrator is not None:
        calibrator.save()
        print(f"[L1] Calibration saved: {calibrator.summary()}", flush=True)


# ── Layer 2: MAC state sync ───────────────────────────────────────────────────

def layer2_mac_sync(
    real_host: str,
    twin_host: str,
    K_reconcile: int = 100,
) -> None:
    """
    M4-augmented MAC sync:
      - Subscribe to real EdgeRIC metrics (ZMQ PUB, port 5555)
      - Stamp each forwarded message with tti_seq + per-UE + global state hash
      - Forward stamped protobuf to twin EdgeRIC (port 5655)
      - Bind MacReconciler REP socket on port 5560 for twin-side equivalence checks

    K_reconcile: reconciliation interval in forwarded-TTI units (default 100).
    Reconciliation requests are served non-blocking once per receive loop.
    """
    sys.path.insert(0, str(Path(__file__).parent.parent / "proto"))
    import metrics_pb2
    from orchestrator.mac_equivalence import MacReconciler, stamp_metrics

    ctx = zmq.Context()

    sub = ctx.socket(zmq.SUB)
    sub.connect(f"tcp://{real_host}:{REAL_METRICS_PORT}")
    sub.setsockopt(zmq.SUBSCRIBE, b"")
    sub.setsockopt(zmq.CONFLATE, 1)
    sub.setsockopt(zmq.RCVTIMEO, 200)  # 200ms timeout so reconciliation loop runs

    pub = ctx.socket(zmq.PUB)
    pub.bind(f"tcp://0.0.0.0:{REAL_METRICS_PORT + 100}")  # port 5655

    reconciler = MacReconciler(bind_endpoint="tcp://*:5560")
    tti_seq = 0

    print(
        f"[L2] M4 reconciler bound on :5560; "
        f"forwarding metrics {real_host}:{REAL_METRICS_PORT} → local:{REAL_METRICS_PORT + 100}",
        flush=True,
    )
    while True:
        try:
            raw = sub.recv()
            metrics = metrics_pb2.Metrics()
            metrics.ParseFromString(raw)
            tti_seq += 1
            # epoch tracks reconciliation events (managed inside MacReconciler)
            stamp_metrics(metrics, tti_seq=tti_seq, epoch=reconciler.epoch)
            reconciler.update(metrics)
            pub.send(metrics.SerializeToString())
        except zmq.Again:
            pass  # timeout: still run reconciler below
        except zmq.ZMQError as e:
            print(f"[L2] ZMQ error: {e}", flush=True)
            time.sleep(0.1)

        # Non-blocking: serve at most one reconciliation request per iteration
        reconciler.serve_one()
        if reconciler.stats.epochs > 0 and reconciler.stats.epochs % 500 == 0:
            print(f"[L2/M4] {reconciler.stats.summary()}", flush=True)


# ── Fidelity monitor (E4) ─────────────────────────────────────────────────────

def fidelity_monitor(
    real_host: str,
    twin_host: str,
    out_path: str = "logs/fidelity_over_time.csv",
    interval_s: float = 5.0,
) -> None:
    """
    Every interval_s seconds, collect latest SNR/CQI from both real and twin
    EdgeRIC and write to CSV for fidelity-vs-time analysis.
    """
    import csv
    sys.path.insert(0, str(Path(__file__).parent.parent / "proto"))
    import metrics_pb2

    ctx = zmq.Context()
    real_sub = ctx.socket(zmq.SUB)
    real_sub.connect(f"tcp://{real_host}:{REAL_METRICS_PORT}")
    real_sub.setsockopt(zmq.SUBSCRIBE, b"")
    real_sub.setsockopt(zmq.CONFLATE, 1)
    real_sub.setsockopt(zmq.RCVTIMEO, 2000)

    twin_sub = ctx.socket(zmq.SUB)
    twin_sub.connect(f"tcp://{twin_host}:{REAL_METRICS_PORT}")
    twin_sub.setsockopt(zmq.SUBSCRIBE, b"")
    twin_sub.setsockopt(zmq.CONFLATE, 1)
    twin_sub.setsockopt(zmq.RCVTIMEO, 2000)

    def get_metrics(sock) -> dict:
        try:
            raw = sock.recv()
            m = metrics_pb2.Metrics()
            m.ParseFromString(raw)
            return {
                ue.rnti: {"cqi": ue.cqi, "snr": ue.snr,
                           "tx_bytes": ue.tx_bytes, "dl_buffer": ue.dl_buffer}
                for ue in m.ue_metrics
            }
        except zmq.Again:
            return {}

    Path(out_path).parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["timestamp", "rnti", "real_cqi", "twin_cqi",
                         "real_snr", "twin_snr", "real_tpt", "twin_tpt", "cqi_err"])
        t0 = time.time()
        print(f"[Monitor] Writing fidelity to {out_path} every {interval_s}s", flush=True)
        while True:
            real_m = get_metrics(real_sub)
            twin_m = get_metrics(twin_sub)
            ts = time.time() - t0
            for rnti in set(real_m) | set(twin_m):
                r = real_m.get(rnti, {})
                t = twin_m.get(rnti, {})
                cqi_err = abs(r.get("cqi", 0) - t.get("cqi", 0))
                writer.writerow([
                    f"{ts:.1f}", f"0x{rnti:04x}",
                    r.get("cqi", ""), t.get("cqi", ""),
                    r.get("snr", ""), t.get("snr", ""),
                    r.get("tx_bytes", ""), t.get("tx_bytes", ""),
                    cqi_err,
                ])
            f.flush()
            time.sleep(interval_s)


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Real-time dual-layer state sync")
    parser.add_argument("--real-host", required=True)
    parser.add_argument("--twin-host", required=True)
    parser.add_argument("--rnti", default=None, help="Filter to single RNTI (hex)")
    parser.add_argument("--cal-path", default="logs/calibration.npy",
                        help="Path for calibration state file")
    parser.add_argument("--no-calibration", action="store_true",
                        help="Disable data-driven channel calibration")
    parser.add_argument("--local-fifo", action="store_true",
                        help="Write CIR to local FIFOs (use when running ON the twin node)")
    parser.add_argument("--monitor-only", action="store_true",
                        help="Only run fidelity monitor (no CIR sync)")
    parser.add_argument("--drift-log", default=None,
                        help="CSV path for E1 D(t) time-series (e.g. logs/drift.csv)")
    args = parser.parse_args()

    rnti = int(args.rnti, 16) if args.rnti else None

    if args.monitor_only:
        fidelity_monitor(args.real_host, args.twin_host)
        return

    calibrator = None if args.no_calibration else ChannelCalibrator(cal_path=args.cal_path)
    if calibrator is not None:
        print(f"[L1] Calibrator initialized: {calibrator.summary()}", flush=True)

    # Layer 2 in a background thread
    t2 = threading.Thread(
        target=layer2_mac_sync,
        args=(args.real_host, args.twin_host),
        daemon=True,
    )
    t2.start()

    # Layer 1 in asyncio event loop (main thread)
    try:
        asyncio.run(layer1_channel_sync(
            args.real_host, args.twin_host, rnti,
            calibrator=calibrator, local_fifo=args.local_fifo,
            drift_log_path=args.drift_log,
        ))
    except KeyboardInterrupt:
        print("\nstate_sync stopped.", flush=True)


if __name__ == "__main__":
    main()
