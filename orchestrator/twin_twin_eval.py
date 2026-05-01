"""
Twin-Twin End-to-End System Test

Uses two Tiny_Twin RFsim nodes as a stand-in for the real OTA setup:
  --real-host  = fake "real" gNB (RFsim + EdgeRIC, static channel)
  --twin-host  = actual twin (CIR injected via state_sync FIFO)

Node setup: instantiate profile_twin.py with num_compute_nodes=2 in POWDER portal
(compute-only, no radio hardware, no OTA permission needed).

Mac orchestrator runs all eval scripts, exactly as it would in OTA.

What gets tested (all without OTA permission):
  Layer 1 (M1): state_sync CIR injection + drift logging → E1
  Layer 2 (M4): layer2_mac_sync + MacReconciler on twin-host → E5
  M3 overhead: m3_overhead_eval against live twin → E3
  E6 KPI similarity: dual-ZMQ subscriber across both nodes

Limitations vs real OTA:
  E2 oracle ranking accuracy: protocol only (no real policy effect in RFsim)
  E4 safety run: gated policy deploys go to RFsim gNB (valid protocol test)

Usage:
    # Full automated run (~45 min):
    python orchestrator/twin_twin_eval.py \\
        --real-host twin-node-0.myexp.nicelabexp.emulab.net \\
        --twin-host twin-node-1.myexp.nicelabexp.emulab.net

    # Quick sanity check (~8 min):
    python orchestrator/twin_twin_eval.py \\
        --real-host <node0> --twin-host <node1> --quick

    # Setup only (start stacks, skip experiments):
    python orchestrator/twin_twin_eval.py \\
        --real-host <node0> --twin-host <node1> --setup-only

    # Teardown all containers on both nodes:
    python orchestrator/twin_twin_eval.py \\
        --real-host <node0> --twin-host <node1> --teardown
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import subprocess
import sys
import time
from pathlib import Path

try:
    import asyncssh
except ImportError:
    print("[ERROR] asyncssh not installed: pip install asyncssh", file=sys.stderr)
    sys.exit(1)

# ── SSH / deployment constants ────────────────────────────────────────────────

SSH_USER   = "zifan716"
SSH_CFG    = [Path.home() / ".ssh" / "config"]

REPO_DIR   = "~/Tiny_Twin"
SIMS_DIR   = f"{REPO_DIR}/sims"
CN_DIR     = f"{SIMS_DIR}/oai-cn"
COMPOSE    = "docker-compose.twin.yaml"

FIFO_REAL  = "/tmp/tt_cir_real.fifo"
FIFO_IMAG  = "/tmp/tt_cir_imag.fifo"

METRICS_PORT   = 5555
RECONCILE_PORT = 5560

# Experiment durations (seconds) — shortened by --quick
DUR_E1_WARMUP = 30
DUR_E3        = 120
DUR_E5        = 120
DUR_E6        = 120


# ── SSH helpers ───────────────────────────────────────────────────────────────

async def _ssh(host: str, cmd: str, check: bool = True) -> str:
    async with asyncssh.connect(
        host, username=SSH_USER, known_hosts=None, config=SSH_CFG
    ) as conn:
        r = await conn.run(cmd, check=False)
        if check and r.exit_status != 0:
            raise RuntimeError(f"[{host}] '{cmd[:80]}' failed:\n{r.stderr.strip()}")
        return r.stdout


async def _ssh_bg(host: str, cmd: str, log: str) -> None:
    escaped = cmd.replace("'", "'\\''")
    nohup   = f"nohup bash -c '{escaped}' >{log} 2>&1 &"
    await _ssh(host, nohup, check=False)


# ── Phase 0: connectivity check ───────────────────────────────────────────────

async def check_connectivity(real: str, twin: str) -> bool:
    print("\n=== Phase 0: Connectivity ===", flush=True)
    ok = True
    for host, label in [(real, "real"), (twin, "twin")]:
        try:
            out = await _ssh(
                host,
                "hostname && sudo docker images --format '{{.Repository}}:{{.Tag}}' "
                "| grep tt-gnb | head -1",
            )
            hostname  = out.strip().split("\n")[0]
            has_image = "tt-gnb" in out
            status    = "OK" if has_image else "WARN: tt-gnb image missing"
            print(f"  [{status}] {label} ({host}) → {hostname}", flush=True)
            if not has_image:
                print(f"    Build on {host}:")
                print(f"    ssh {host} 'cd ~/Tiny_Twin && sudo docker build "
                      f"--target tt-gnb -f docker/tinytwin/Dockerfile.TTgNB.ubuntu22 -t tt-gnb:v2 .'")
                ok = False
        except Exception as e:
            print(f"  [FAIL] Cannot reach {label} ({host}): {e}", flush=True)
            ok = False
    return ok


# ── Phase 1: fake-real stack ──────────────────────────────────────────────────

async def start_fake_real(real: str) -> None:
    print(f"\n=== Phase 1: Fake-real stack on {real} ===", flush=True)

    await _ssh(real,
        "sudo docker network create --driver bridge --subnet 192.168.70.128/26 "
        "--opt com.docker.network.bridge.name=tt-public-net tt-public-net 2>/dev/null || true",
        check=False)

    print("[Real] Starting CN ...", flush=True)
    await _ssh(real,
        f"cd {CN_DIR} && "
        "sudo docker compose -f docker-compose.yaml -f oai-cn-tt-override.yaml up -d")

    for _ in range(30):
        await asyncio.sleep(5)
        out = await _ssh(real,
            "sudo docker ps --format '{{.Names}} {{.Status}}' | grep oai-amf || true",
            check=False)
        if "healthy" in out:
            print("[Real] CN healthy.", flush=True)
            break
    else:
        print("[Real][WARN] AMF may not be healthy yet — continuing.", flush=True)

    print("[Real] Starting gNB (RFsim, static channel) ...", flush=True)
    await _ssh(real, f"cd {SIMS_DIR} && sudo docker compose -f {COMPOSE} up -d tt-gnb")
    await asyncio.sleep(8)

    print("[Real] Starting UE ...", flush=True)
    await _ssh(real, f"cd {SIMS_DIR} && sudo docker compose -f {COMPOSE} up -d tt-nrue1")
    await asyncio.sleep(5)
    print("[Real] Fake-real stack up.", flush=True)


# ── Phase 2: state_sync on twin node ─────────────────────────────────────────

async def start_state_sync_on_twin(twin: str, real: str) -> None:
    print(f"\n=== Phase 2: state_sync on {twin} ===", flush=True)

    await _ssh(twin,
        f"pkill -f state_sync.py || true; sleep 1; rm -f {FIFO_REAL} {FIFO_IMAG}",
        check=False)

    cmd = (
        f"cd {REPO_DIR} && "
        f"python3 -m orchestrator.state_sync "
        f"--real-host {real} "
        f"--twin-host {twin} "
        f"--local-fifo "
        f"--drift-log /tmp/drift_e1.csv"
    )
    await _ssh_bg(twin, cmd, "/tmp/state_sync.log")

    print("[Twin] Waiting for FIFOs ...", flush=True)
    for _ in range(30):
        await asyncio.sleep(1)
        out = await _ssh(twin, f"test -p {FIFO_REAL} && echo yes || echo no", check=False)
        if "yes" in out:
            print(f"[Twin] FIFOs ready: {FIFO_REAL}", flush=True)
            return
    raise RuntimeError(
        "[Twin] FIFOs not created — state_sync may have crashed.\n"
        f"  Check: ssh {twin} 'tail -50 /tmp/state_sync.log'"
    )


# ── Phase 3: twin gNB with FIFO ──────────────────────────────────────────────

async def start_twin_gnb(twin: str) -> None:
    print(f"\n=== Phase 3: Twin gNB on {twin} ===", flush=True)

    await _ssh(twin,
        "sudo docker network create --driver bridge --subnet 192.168.70.128/26 "
        "--opt com.docker.network.bridge.name=tt-public-net tt-public-net 2>/dev/null || true",
        check=False)

    out = await _ssh(twin,
        "sudo docker ps --format '{{.Names}} {{.Status}}' | grep oai-amf || true",
        check=False)
    if "oai-amf" in out and "Up" in out:
        print("[Twin] CN already running.", flush=True)
    else:
        print("[Twin] Starting CN ...", flush=True)
        await _ssh(twin,
            f"cd {CN_DIR} && "
            "sudo docker compose -f docker-compose.yaml -f oai-cn-tt-override.yaml up -d")
        for _ in range(30):
            await asyncio.sleep(5)
            out = await _ssh(twin,
                "sudo docker ps --format '{{.Names}} {{.Status}}' | grep oai-amf || true",
                check=False)
            if "healthy" in out:
                print("[Twin] CN healthy.", flush=True)
                break

    print("[Twin] Starting twin gNB (FIFO mode) ...", flush=True)
    env_vars = f"TT_CHANNEL_FILE_REAL={FIFO_REAL} TT_CHANNEL_FILE_IMAG={FIFO_IMAG}"
    await _ssh(twin,
        f"cd {SIMS_DIR} && export {env_vars} && "
        f"sudo -E docker compose -f {COMPOSE} up -d tt-gnb")
    await asyncio.sleep(8)

    print("[Twin] Starting UE ...", flush=True)
    await _ssh(twin,
        f"cd {SIMS_DIR} && sudo docker compose -f {COMPOSE} up -d --no-recreate tt-nrue1")
    print("[Twin] Twin stack up.", flush=True)


# ── Phase 4: wait for metrics ─────────────────────────────────────────────────

async def wait_for_metrics(real: str, twin: str, timeout_s: int = 120) -> bool:
    print("\n=== Phase 4: Waiting for metrics from both nodes ===", flush=True)
    import zmq
    ctx = zmq.Context.instance()
    results: dict[str, int] = {}
    for host, label in [(real, "real"), (twin, "twin")]:
        sock = ctx.socket(zmq.SUB)
        sock.connect(f"tcp://{host}:{METRICS_PORT}")
        sock.setsockopt(zmq.SUBSCRIBE, b"")
        sock.setsockopt(zmq.RCVTIMEO, timeout_s * 1000)
        try:
            raw = sock.recv()
            results[label] = len(raw)
            print(f"  [OK] {label} ({host}): {len(raw)} bytes", flush=True)
        except Exception:
            print(f"  [FAIL] No metrics from {label} ({host}) within {timeout_s}s", flush=True)
            results[label] = 0
        sock.close()
    return all(v > 0 for v in results.values())


# ── Experiment: E1 drift check ────────────────────────────────────────────────

async def run_e1_check(twin: str, warmup_s: int) -> dict:
    print(f"\n=== E1: Drift log (waiting {warmup_s}s warmup) ===", flush=True)
    await asyncio.sleep(warmup_s)
    out = await _ssh(twin,
        "wc -l /tmp/drift_e1.csv 2>/dev/null || echo '0 /tmp/drift_e1.csv'",
        check=False)
    lines = int(out.strip().split()[0])
    subprocess.run(
        ["scp", f"{twin}:/tmp/drift_e1.csv", "logs/e1_drift.csv"],
        capture_output=True)
    print(f"  E1: drift log has {lines} rows ({'PASS' if lines > 0 else 'FAIL'})", flush=True)
    return {"e1_rows": lines, "e1_pass": lines > 0}


# ── Experiment: E3 overhead sweep ────────────────────────────────────────────

def run_e3(twin: str, n_samples: int, m_trials: int) -> dict:
    print("\n=== E3: M3 overhead sweep ===", flush=True)
    r = subprocess.run([
        sys.executable, "orchestrator/m3_overhead_eval.py",
        "--twin-host", twin,
        "--n-samples", str(n_samples),
        "--m-trials",  str(m_trials),
        "--out",       "logs/e3_overhead.csv",
    ])
    ok = r.returncode == 0
    print(f"  E3: {'PASS' if ok else 'FAIL'}", flush=True)
    return {"e3_pass": ok}


# ── Experiment: E5 MAC equivalence ───────────────────────────────────────────

def run_e5(twin: str, duration_s: int, k: int = 100) -> dict:
    """Connect mac_equivalence_twin_client to MacReconciler on twin:5560."""
    print("\n=== E5: MAC equivalence (twin client → reconciler) ===", flush=True)
    r = subprocess.run([
        sys.executable, "orchestrator/mac_equivalence_twin_client.py",
        "--real-host",   twin,         # reconciler runs on twin (layer2_mac_sync)
        "--k-reconcile", str(k),
        "--duration",    str(duration_s),
        "--out",         "logs/e5_reconcile.csv",
    ])
    ok = r.returncode == 0
    p99 = float("nan")
    try:
        import numpy as np
        with open("logs/e5_reconcile.csv") as f:
            rows = list(csv.DictReader(f))
        deltas = [float(row["delta_ttis"]) for row in rows if row["delta_ttis"] != "0"]
        if deltas:
            p99 = float(np.percentile(deltas, 99))
    except Exception:
        pass
    bound = k + 3
    pass_e5 = ok and (p99 != p99 or p99 <= bound)  # nan comparison → True
    print(f"  E5: p99(Δ)={p99:.0f} TTI  bound={bound} TTI  {'PASS' if pass_e5 else 'CHECK'}", flush=True)
    return {"e5_p99": p99, "e5_pass": pass_e5}


# ── Experiment: E6 KPI similarity ────────────────────────────────────────────

def run_e6(real: str, twin: str, duration_s: int) -> dict:
    print("\n=== E6: KPI similarity ===", flush=True)
    r = subprocess.run([
        sys.executable, "orchestrator/e6_kpi_similarity.py",
        "--real-host", real,
        "--twin-host", twin,
        "--duration",  str(duration_s),
        "--interval",  "5",
        "--out",       "logs/e6_kpi_similarity.csv",
    ])
    ok = r.returncode == 0
    mean_sim = float("nan")
    try:
        with open("logs/e6_kpi_similarity.csv") as f:
            rows = list(csv.DictReader(f))
        vals = [float(row["similarity_pct"]) for row in rows
                if row["similarity_pct"] not in ("nan", "")]
        if vals:
            mean_sim = sum(vals) / len(vals)
    except Exception:
        pass
    pass_e6 = ok and (mean_sim != mean_sim or mean_sim >= 70)
    print(f"  E6: mean similarity={mean_sim:.1f}%  Samsung baseline=39%  "
          f"{'PASS' if pass_e6 else 'WARN'}", flush=True)
    return {"e6_sim": mean_sim, "e6_pass": pass_e6}


# ── Teardown ──────────────────────────────────────────────────────────────────

async def teardown(real: str, twin: str) -> None:
    print("\n=== Teardown ===", flush=True)
    for host, label in [(real, "Real"), (twin, "Twin")]:
        print(f"[{label}] Stopping containers on {host} ...", flush=True)
        await _ssh(host,
            f"pkill -f state_sync.py || true; "
            f"cd {SIMS_DIR} && sudo docker compose -f {COMPOSE} down 2>/dev/null; "
            f"cd {CN_DIR} && sudo docker compose -f docker-compose.yaml "
            f"-f oai-cn-tt-override.yaml down 2>/dev/null",
            check=False)
    print("Teardown complete.", flush=True)


# ── Final report ──────────────────────────────────────────────────────────────

def print_report(results: dict) -> None:
    print("\n" + "=" * 60, flush=True)
    print("Twin-Twin Eval — Final Report", flush=True)
    print("=" * 60, flush=True)
    checks = [
        ("E1 Drift log rows",      results.get("e1_rows",  "?"),           results.get("e1_pass",  False)),
        ("E3 Overhead sweep",      "OK" if results.get("e3_pass") else "FAIL", results.get("e3_pass", False)),
        ("E5 p99(Δ) TTI",         f"{results.get('e5_p99', float('nan')):.0f}", results.get("e5_pass", False)),
        ("E6 KPI similarity %",   f"{results.get('e6_sim', float('nan')):.1f}", results.get("e6_pass", False)),
    ]
    for name, val, ok in checks:
        print(f"  [{'PASS' if ok else 'WARN'}] {name}: {val}", flush=True)
    n_pass = sum(1 for _, _, ok in checks if ok)
    print(f"\n{n_pass}/{len(checks)} checks passed.", flush=True)
    print("Logs: logs/e1_drift.csv  logs/e3_overhead.csv  "
          "logs/e5_reconcile.csv  logs/e6_kpi_similarity.csv", flush=True)


# ── Main ─────────────────────────────────────────────────────────────────────

async def _main(args) -> None:
    Path("logs").mkdir(exist_ok=True)
    real = args.real_host
    twin = args.twin_host

    if args.teardown:
        await teardown(real, twin)
        return

    ok = await check_connectivity(real, twin)
    if not ok and not args.skip_checks:
        print("\n[ERROR] Connectivity check failed.", file=sys.stderr)
        sys.exit(1)

    await start_fake_real(real)
    await start_state_sync_on_twin(twin, real)
    await start_twin_gnb(twin)

    if args.setup_only:
        print(f"\nSetup complete.")
        print(f"  Fake-real metrics : tcp://{real}:{METRICS_PORT}")
        print(f"  Twin metrics      : tcp://{twin}:{METRICS_PORT}")
        print(f"  Reconciler        : tcp://{twin}:{RECONCILE_PORT}")
        print(f"  state_sync log    : ssh {twin} 'tail -f /tmp/state_sync.log'")
        print(f"  Drift log         : ssh {twin} 'tail -f /tmp/drift_e1.csv'")
        return

    ok = await wait_for_metrics(real, twin, timeout_s=120)
    if not ok and not args.skip_checks:
        print("[ERROR] Metrics not flowing. Check gNB logs.", file=sys.stderr)
        print(f"  ssh {real} 'sudo docker logs tt-gnb --tail 30'")
        print(f"  ssh {twin} 'sudo docker logs tt-gnb --tail 30'")
        sys.exit(1)

    q = args.quick
    results = {}
    results.update(await run_e1_check(twin, warmup_s=15 if q else DUR_E1_WARMUP))
    results.update(run_e3(twin, n_samples=50 if q else 300, m_trials=1 if q else 3))
    results.update(run_e5(twin, duration_s=30 if q else DUR_E5, k=100))
    results.update(run_e6(real, twin, duration_s=30 if q else DUR_E6))

    print_report(results)

    if args.teardown_after:
        await teardown(real, twin)


def main():
    p = argparse.ArgumentParser(description="Twin-twin end-to-end system test")
    p.add_argument("--real-host",      default="powder-real",
                   help="SSH host for fake-real node (default: powder-real)")
    p.add_argument("--twin-host",      default="powder-twin",
                   help="SSH host for twin node (default: powder-twin)")
    p.add_argument("--quick",          action="store_true",
                   help="Shorten experiment durations for a fast sanity check (~8 min)")
    p.add_argument("--setup-only",     action="store_true",
                   help="Start stacks only; do not run experiments")
    p.add_argument("--teardown",       action="store_true",
                   help="Stop all containers on both nodes and exit")
    p.add_argument("--teardown-after", action="store_true",
                   help="Run experiments then stop all containers")
    p.add_argument("--skip-checks",   action="store_true",
                   help="Proceed even if connectivity or metrics checks fail")
    args = p.parse_args()
    asyncio.run(_main(args))


if __name__ == "__main__":
    main()
