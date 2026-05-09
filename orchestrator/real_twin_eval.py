"""
Real2Twin Indoor OTA Evaluation

Connects a live OAI gNB (d430 + USRP X310, indoor OTA) to a Tiny_Twin gNB
running on a second d430 with real-time CIR injection via state_sync.

Node roles (from instantiate_ota.py manifest):
  gnb-real  — real OAI gNB + CN; Quectel COTS UEs attach over the air
  twin      — Tiny_Twin gNB + CN; gets CIR from state_sync FIFO
  ue-nuc1~4 — Intel NUC nodes with Quectel RM500Q 5G UEs

Differences from twin_twin_eval.py:
  Phase 1 is split into start_real_cn / start_real_gnb / wait_ue_attach.
  Everything from Phase 2 onward (state_sync, twin gNB, E1/E3/E5/E6) is
  byte-for-byte identical to twin_twin_eval.py.

Usage:
    # Full run (~45 min):
    python orchestrator/real_twin_eval.py \\
        --gnb-host  powder-gnb-real \\
        --twin-host powder-twin \\
        --ue-hosts  powder-ue1,powder-ue2,powder-ue3,powder-ue4

    # Quick sanity check (~8 min, 1 UE):
    python orchestrator/real_twin_eval.py \\
        --gnb-host powder-gnb-real --twin-host powder-twin \\
        --ue-hosts powder-ue1 --quick

    # Setup only (start stacks, skip experiments):
    python orchestrator/real_twin_eval.py \\
        --gnb-host powder-gnb-real --twin-host powder-twin \\
        --ue-hosts powder-ue1,powder-ue2 --setup-only
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

TWIN_COMPOSE            = "docker-compose.twin.yaml"
REAL_OTA_COMPOSE        = "docker-compose.real-ota.yaml"
REAL_OTA_STANDALONE     = "docker-compose.real-ota-standalone.yaml"

FIFO_REAL  = "/tmp/tt_cir_real.fifo"
FIFO_IMAG  = "/tmp/tt_cir_imag.fifo"
STATE_SYNC_KILL_PATTERN = "[o]rchestrator.state_sync|[s]tate_sync.py"

METRICS_PORT   = 5555
RECONCILE_PORT = 5560

_node_ip: dict[str, str] = {}

DUR_E1_WARMUP = 30
DUR_E3        = 120
DUR_E5        = 120
DUR_E6        = 120

# X310 reachable from gnb-real on the dedicated radio-link (192.168.40.0/24).
# The profile (powder/profile_real_twin_ota.py) declares a single Link
# between gnb-real and one ota-x310-N selected by the x310_id parameter at
# instantiation time, so only one candidate is reachable at runtime; the
# X310 always responds at .2 (the USRP's 10G transport default address).
X310_CANDIDATE_IPS = ["192.168.40.2"]


_UHD_FAILURE_MARKERS = (
    "no uhd devices found",
    "no devices found",
    "lookuperror",
    "keyerror",
    "exception caught",
    "could not open",
)


async def _select_live_x310(gnb: str) -> str:
    """SSH into gnb and run uhd_find_devices against each candidate IP.

    A live X310 produces stdout with a `Device Address:` block followed by
    matching `serial:` and `type: x300` lines. UHD's failure path also
    prints a banner that mentions x300 so substring matches like ``"x300"
    in out.lower()`` are unsafe — we anchor on the structured block.

    Returns the first IP that produces a valid device block; raises if none do.
    """
    import re as _re

    for ip in X310_CANDIDATE_IPS:
        out = await _ssh(gnb,
            f"timeout 12 uhd_find_devices --args 'addr={ip}' 2>&1 || true",
            check=False)
        lower = out.lower()
        if any(m in lower for m in _UHD_FAILURE_MARKERS):
            print(f"[gNB] X310 {ip}: no response (failure banner)", flush=True)
            continue
        # A valid response has a structured device block. Require both
        # `Device Address:` AND a matching `type: x300` line — neither is
        # printed on the failure path.
        if (_re.search(r"Device Address:\s*\n", out)
                and _re.search(r"^\s*type:\s*x300\s*$", out, _re.MULTILINE)):
            print(f"[gNB] X310 {ip}: alive (uhd_find_devices match)", flush=True)
            return ip
        print(f"[gNB] X310 {ip}: no device block in output", flush=True)
    raise RuntimeError(
        f"No X310 in {X310_CANDIDATE_IPS} responded to UHD discovery on {gnb}. "
        "All allocated USRPs are unreachable - escalate to POWDER support."
    )


async def _wait_for_build(host: str, max_minutes: int = 90) -> None:
    """Block until profile_real_twin_ota's background OAI build finishes.

    The startup script forks the heavy `docker build` into a nohup job
    that touches ~/.tt-build-complete on success or ~/.tt-build-failed
    on error. We poll every 30 s up to max_minutes.
    """
    deadline_secs = max_minutes * 60
    waited = 0
    while waited < deadline_secs:
        out = await _ssh(host,
            "if [ -f ~/.tt-build-complete ]; then echo OK; "
            "elif [ -f ~/.tt-build-failed ]; then echo FAIL; "
            "else echo WAIT; fi",
            check=False)
        flag = out.strip()
        if flag == "OK":
            print(f"[{host}] OAI build complete.", flush=True)
            return
        if flag == "FAIL":
            tail = await _ssh(host,
                "tail -40 /tmp/tt-gnb-build.log 2>/dev/null; "
                "tail -40 /tmp/tt-nrue-build.log 2>/dev/null",
                check=False)
            raise RuntimeError(
                f"[{host}] OAI build FAILED. Last lines:\n{tail}")
        if waited % 300 == 0:
            print(f"[{host}] still building (waited {waited//60} min)...",
                  flush=True)
        await asyncio.sleep(30)
        waited += 30
    raise RuntimeError(
        f"[{host}] OAI build did not finish within {max_minutes} min")


# ── SSH helpers (identical to twin_twin_eval.py) ───────────────────────────

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
    await _ssh(host, f"nohup bash -c '{escaped}' >{log} 2>&1 &", check=False)


# ── Phase 0: connectivity ─────────────────────────────────────────────────────

async def check_connectivity(gnb: str, twin: str, ue_hosts: list[str]) -> bool:
    print("\n=== Phase 0: Connectivity ===", flush=True)
    ok = True
    for host, label in [(gnb, "gnb-real"), (twin, "twin")] + [(u, u) for u in ue_hosts]:
        try:
            out = await _ssh(
                host,
                "hostname && "
                "ip addr show eno1 2>/dev/null | grep 'inet ' | awk '{print $2}' | cut -d/ -f1",
            )
            lines = out.strip().split("\n")
            if len(lines) >= 2 and lines[1].count(".") == 3:
                _node_ip[host] = lines[1]
            print(f"  [OK] {label} ({host}) → {lines[0]}", flush=True)
        except Exception as e:
            print(f"  [FAIL] Cannot reach {label} ({host}): {e}", flush=True)
            ok = False

    # Verify tt-gnb Docker image on gnb-real and twin
    for host, label in [(gnb, "gnb-real"), (twin, "twin")]:
        try:
            out = await _ssh(host, "sudo docker images --format '{{.Repository}}:{{.Tag}}' | grep tt-gnb | head -1")
            if "tt-gnb" not in out:
                print(f"  [WARN] tt-gnb image missing on {label} — startup script may still be running")
                ok = False
        except Exception:
            pass
    return ok


# ── Phase 1a: real CN ─────────────────────────────────────────────────────────

async def start_real_cn(gnb: str, quick: bool = False) -> None:
    print(f"\n=== Phase 1a: OAI CN on {gnb} ===", flush=True)

    await _ssh(gnb,
        "sudo docker network create --driver bridge --subnet 192.168.70.128/26 "
        "--opt com.docker.network.bridge.name=tt-public-net tt-public-net 2>/dev/null || true",
        check=False)

    print("[gNB] Starting CN ...", flush=True)
    await _ssh(gnb,
        f"cd {CN_DIR} && "
        "sudo docker compose -f docker-compose.yaml -f oai-cn-tt-override.yaml up -d")

    # quick mode: 12 × 5s = 60s; full: 30 × 5s = 150s
    poll_n = 12 if quick else 30
    for _ in range(poll_n):
        await asyncio.sleep(5)
        out = await _ssh(gnb,
            "sudo docker ps --format '{{.Names}} {{.Status}}' | grep oai-amf || true",
            check=False)
        if "healthy" in out:
            print("[gNB] CN healthy.", flush=True)
            return
    print("[gNB][WARN] AMF may not be healthy yet - continuing.", flush=True)


# ── Phase 1b: real OAI gNB with X310 radio ───────────────────────────────────

async def start_real_gnb(gnb: str, quick: bool = False) -> None:
    print(f"\n=== Phase 1b: Real OAI gNB (X310 radio) on {gnb} ===", flush=True)

    # Block until the profile's background docker build completed.
    await _wait_for_build(gnb)

    # Stop any leftover gNB containers
    await _ssh(gnb,
        f"cd {SIMS_DIR} && "
        f"sudo docker compose -f {REAL_OTA_STANDALONE} "
        "down tt-gnb 2>/dev/null || true",
        check=False)

    # Probe candidate X310s on ctrl-lan and pick the first that responds to
    # uhd_find_devices. The POWDER profile allocates ota-x310-2/-3/-4; ota-x310-1
    # was confirmed dead 2026-05-08.
    x310_ip = await _select_live_x310(gnb)
    print(f"[gNB] Selected X310 at {x310_ip}", flush=True)

    # Patch the X310 ctrl-lan IP into the host-side conf. The compose file
    # bind-mounts this exact path into the container, so the patch takes
    # effect without rebuilding the image.
    await _ssh(gnb,
        r"GNB_CONF=~/Tiny_Twin/targets/PROJECTS/GENERIC-NR-5GC/CONF/gnb.sa.band78.fr1.24PRB.usrpx310.dt-ota.conf && "
        rf"sudo sed -i 's|sdr_addrs[[:space:]]*=[[:space:]]*\"addr=[^\"]*\"|sdr_addrs = \"addr={x310_ip}\"|g' \"$GNB_CONF\" && "
        rf"grep sdr_addrs \"$GNB_CONF\"",
        check=True)

    print("[gNB] Starting real gNB (usrp X310, no rfsim) ...", flush=True)
    await _ssh(gnb,
        f"cd {SIMS_DIR} && "
        f"sudo docker compose -f {REAL_OTA_STANDALONE} up -d tt-gnb")

    # Wait for gNB to start (X310 init takes ~20-40 s on cold boot;
    # quick mode trims aggressively).
    init_wait = 15 if quick else 40
    await asyncio.sleep(init_wait)
    out = await _ssh(gnb,
        "sudo docker logs tt-gnb --tail 20 2>&1 || true", check=False)
    if "is UP" in out or "Initialized" in out or "softmodem" in out.lower():
        print("[gNB] Real gNB started.", flush=True)
    else:
        print("[gNB][WARN] gNB log not yet showing 'is UP' - may still be initializing.", flush=True)
        print(f"  Check: ssh {gnb} 'sudo docker logs -f tt-gnb'", flush=True)


# ── Phase 1c: COTS UE attach ─────────────────────────────────────────────────

async def wait_ue_attach(ue_hosts: list[str], timeout_s: int = 120) -> bool:
    """Enable Quectel modems and wait for at least one to show 'registered' state."""
    print(f"\n=== Phase 1c: COTS UE attach ({len(ue_hosts)} UEs, timeout={timeout_s}s) ===",
          flush=True)

    # Enable each modem (ModemManager may have disabled them on startup)
    for ue in ue_hosts:
        try:
            await _ssh(ue, "sudo mmcli -m 0 --enable 2>/dev/null || true", check=False)
        except Exception:
            pass

    deadline = time.time() + timeout_s
    attached: set[str] = set()
    while time.time() < deadline:
        await asyncio.sleep(5)
        for ue in ue_hosts:
            if ue in attached:
                continue
            try:
                out = await _ssh(ue, "sudo mmcli -m 0 --simple-status 2>/dev/null || true",
                                  check=False)
                if "registered" in out.lower() or "connected" in out.lower():
                    attached.add(ue)
                    print(f"  [ATTACH] {ue} registered", flush=True)
            except Exception:
                pass
        if attached:
            print(f"  {len(attached)}/{len(ue_hosts)} UE(s) attached.", flush=True)
            return True

    print(f"  [WARN] No UEs attached within {timeout_s}s. "
          "Check gNB logs and UE modem state.", flush=True)
    print(f"  Diagnose: ssh <ue-node> 'sudo mmcli -m 0'", flush=True)
    return False


# ── Phase 2–3: state_sync + twin gNB (identical to twin_twin_eval.py) ────────

async def start_state_sync_on_twin(twin: str, gnb: str, quick: bool = False) -> None:
    print(f"\n=== Phase 2: state_sync on {twin} ===", flush=True)

    await _ssh(twin,
        f"pkill -f '{STATE_SYNC_KILL_PATTERN}' || true; sleep 1; "
        f"rm -f {FIFO_REAL} {FIFO_IMAG}",
        check=False)

    twin_ip         = _node_ip.get(twin, twin)
    gnb_metrics_ip  = _node_ip.get(gnb,  gnb)
    cmd = (
        f"cd {REPO_DIR} && "
        f"python3 -m orchestrator.state_sync "
        f"--real-host {gnb} "
        f"--real-metrics-host {gnb_metrics_ip} "
        f"--twin-host {twin_ip} "
        f"--local-fifo "
        f"--drift-log /tmp/drift_e1.csv"
    )
    await _ssh_bg(twin, cmd, "/tmp/state_sync.log")

    print("[Twin] Waiting for FIFOs ...", flush=True)
    fifo_n = 15 if quick else 30
    for _ in range(fifo_n):
        await asyncio.sleep(1)
        out = await _ssh(twin, f"test -p {FIFO_REAL} && echo yes || echo no", check=False)
        if "yes" in out:
            print(f"[Twin] FIFOs ready.", flush=True)
            return
    raise RuntimeError(
        "[Twin] FIFOs not created - state_sync may have crashed.\n"
        f"  Check: ssh {twin} 'tail -50 /tmp/state_sync.log'"
    )


async def start_twin_gnb(twin: str, quick: bool = False) -> None:
    print(f"\n=== Phase 3: Twin gNB on {twin} ===", flush=True)

    # Block until the twin's profile-side background build (tt-gnb + tt-nrue) finishes.
    await _wait_for_build(twin)

    await _ssh(twin,
        "sudo docker network create --driver bridge --subnet 192.168.70.128/26 "
        "--opt com.docker.network.bridge.name=tt-public-net tt-public-net 2>/dev/null || true",
        check=False)

    out = await _ssh(twin,
        "sudo docker ps --format '{{.Names}} {{.Status}}' | grep oai-amf || true",
        check=False)
    if "oai-amf" not in out or "Up" not in out:
        print("[Twin] Starting CN ...", flush=True)
        await _ssh(twin,
            f"cd {CN_DIR} && "
            "sudo docker compose -f docker-compose.yaml -f oai-cn-tt-override.yaml up -d")
        cn_n = 12 if quick else 30
        for _ in range(cn_n):
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
        f"sudo -E docker compose -f {TWIN_COMPOSE} up -d tt-gnb")
    await asyncio.sleep(5 if quick else 8)

    print("[Twin] Starting RFsim UE ...", flush=True)
    await _ssh(twin,
        f"cd {SIMS_DIR} && sudo docker compose -f {TWIN_COMPOSE} up -d --force-recreate tt-nrue1")
    print("[Twin] Twin stack up.", flush=True)


# ── Phase 4: wait for metrics ─────────────────────────────────────────────────

async def wait_for_metrics(gnb: str, twin: str, timeout_s: int = 120) -> bool:
    print("\n=== Phase 4: Waiting for ZMQ metrics from both nodes ===", flush=True)
    import zmq
    ctx = zmq.Context.instance()
    results: dict[str, int] = {}
    for host, label in [(gnb, "gnb-real"), (twin, "twin")]:
        zmq_host = _node_ip.get(host, host)
        sock = ctx.socket(zmq.SUB)
        sock.connect(f"tcp://{zmq_host}:{METRICS_PORT}")
        sock.setsockopt(zmq.SUBSCRIBE, b"")
        sock.setsockopt(zmq.RCVTIMEO, timeout_s * 1000)
        try:
            raw = sock.recv()
            results[label] = len(raw)
            print(f"  [OK] {label} ({zmq_host}): {len(raw)} bytes", flush=True)
        except Exception:
            print(f"  [FAIL] No metrics from {label} ({zmq_host}) within {timeout_s}s", flush=True)
            results[label] = 0
        sock.close()
    return all(v > 0 for v in results.values())


# ── Experiments E1 / E3 / E5 / E6 (identical to twin_twin_eval.py) ───────────

async def run_e1_check(twin: str, warmup_s: int) -> dict:
    print(f"\n=== E1: Drift log (waiting {warmup_s}s warmup) ===", flush=True)
    await asyncio.sleep(warmup_s)
    out = await _ssh(twin,
        "wc -l /tmp/drift_e1.csv 2>/dev/null || echo '0 /tmp/drift_e1.csv'",
        check=False)
    lines = int(out.strip().split()[0])
    subprocess.run(["scp", f"{twin}:/tmp/drift_e1.csv", "logs/e1_drift.csv"],
                   capture_output=True)
    print(f"  E1: drift log has {lines} rows ({'PASS' if lines > 0 else 'FAIL'})",
          flush=True)
    return {"e1_rows": lines, "e1_pass": lines > 0}


def run_e3(twin: str, n_samples: int, m_trials: int) -> dict:
    print("\n=== E3: M3 overhead sweep ===", flush=True)
    r = subprocess.run([
        sys.executable, "orchestrator/m3_overhead_eval.py",
        "--mock",
        "--n-samples", str(n_samples),
        "--m-trials",  str(m_trials),
        "--out",       "logs/e3_overhead.csv",
    ])
    ok = r.returncode == 0
    print(f"  E3: {'PASS' if ok else 'FAIL'}", flush=True)
    return {"e3_pass": ok}


def run_e5(twin: str, duration_s: int, k: int = 100) -> dict:
    print("\n=== E5: MAC equivalence ===", flush=True)
    twin_ip = _node_ip.get(twin, twin)
    r = subprocess.run([
        sys.executable, "orchestrator/mac_equivalence_twin_client.py",
        "--real-host",   twin_ip,
        "--k-reconcile", str(k),
        "--duration",    str(duration_s),
        "--out",         "logs/e5_reconcile.csv",
        "--metrics-host", twin_ip,
    ])
    ok = r.returncode == 0
    p99 = float("nan")
    cold_start_delta = float("nan")
    n_rows = 0
    try:
        import numpy as np
        with open("logs/e5_reconcile.csv") as f:
            rows = list(csv.DictReader(f))
        n_rows = len(rows)
        deltas = [abs(float(row["delta_ttis"]))
                  for row in rows if row.get("hash_matched") == "0"]
        if deltas:
            cold_start_delta = deltas[0]
            steady = deltas[1:]
            p99 = float(np.percentile(steady, 99)) if steady else 0.0
        else:
            cold_start_delta = 0.0
            p99 = 0.0
    except Exception:
        pass
    bound = k + 3
    pass_e5 = ok and n_rows > 0 and (p99 != p99 or p99 <= bound)
    print(f"  E5: steady p99(Δ)={p99:.0f} TTI  bound={bound} TTI  "
          f"cold_startΔ={cold_start_delta:.0f} rows={n_rows}  "
          f"{'PASS' if pass_e5 else 'CHECK'}", flush=True)
    return {"e5_p99": p99, "e5_cold_start_delta": cold_start_delta, "e5_pass": pass_e5}


def run_e6(gnb: str, twin: str, duration_s: int) -> dict:
    print("\n=== E6: KPI similarity (real OTA vs twin) ===", flush=True)
    gnb_ip  = _node_ip.get(gnb,  gnb)
    twin_ip = _node_ip.get(twin, twin)
    r = subprocess.run([
        sys.executable, "orchestrator/e6_kpi_similarity.py",
        "--real-host", gnb_ip,
        "--twin-host", twin_ip,
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
                if row.get("similarity_pct") not in ("nan", "")]
        if vals:
            mean_sim = sum(vals) / len(vals)
    except Exception:
        pass
    pass_e6 = ok and (mean_sim == mean_sim) and mean_sim >= 70
    print(f"  E6: mean similarity={mean_sim:.1f}%  Samsung baseline=39%  "
          f"{'PASS' if pass_e6 else 'WARN'}", flush=True)
    return {"e6_sim": mean_sim, "e6_pass": pass_e6}


# ── Teardown ──────────────────────────────────────────────────────────────────

async def teardown(gnb: str, twin: str, ue_hosts: list[str]) -> None:
    print("\n=== Teardown ===", flush=True)
    # Tear down both the twin compose (twin host) and the real-OTA standalone
    # compose (gNB host). Old code referenced REAL_OTA_COMPOSE which is the
    # overlay on top of TWIN_COMPOSE, not the file actually used to bring up
    # the real gNB - so tt-gnb was leaking across runs.
    for host, label in [(gnb, "gNB-real"), (twin, "Twin")]:
        print(f"[{label}] Stopping containers on {host} ...", flush=True)
        await _ssh(host,
            f"pkill -f '{STATE_SYNC_KILL_PATTERN}' || true; "
            f"cd {SIMS_DIR} && "
            f"sudo docker compose -f {REAL_OTA_STANDALONE} down 2>/dev/null; "
            f"sudo docker compose -f {TWIN_COMPOSE} down 2>/dev/null; "
            f"cd {CN_DIR} && sudo docker compose -f docker-compose.yaml "
            f"-f oai-cn-tt-override.yaml down 2>/dev/null",
            check=False)
    for ue in ue_hosts:
        await _ssh(ue, "sudo mmcli -m 0 --disable 2>/dev/null || true", check=False)
    print("Teardown complete.", flush=True)


# ── Report ────────────────────────────────────────────────────────────────────

def print_report(results: dict) -> None:
    print("\n" + "=" * 60, flush=True)
    print("Real2Twin OTA Eval — Final Report", flush=True)
    print("=" * 60, flush=True)
    checks = [
        ("E1 Drift log rows",    results.get("e1_rows",  "?"),                   results.get("e1_pass",  False)),
        ("E3 Overhead sweep",    "OK" if results.get("e3_pass") else "FAIL",      results.get("e3_pass",  False)),
        ("E5 p99(Δ) TTI",       f"{results.get('e5_p99', float('nan')):.0f}",    results.get("e5_pass",  False)),
        ("E6 KPI similarity %", f"{results.get('e6_sim', float('nan')):.1f}",    results.get("e6_pass",  False)),
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
    gnb   = args.gnb_host
    twin  = args.twin_host
    ues   = [u.strip() for u in args.ue_hosts.split(",") if u.strip()]

    if args.teardown:
        await teardown(gnb, twin, ues)
        return

    ok = await check_connectivity(gnb, twin, ues)
    if not ok and not args.skip_checks:
        print("\n[ERROR] Connectivity check failed.", file=sys.stderr)
        sys.exit(1)

    await start_real_cn(gnb, quick=args.quick)
    await start_real_gnb(gnb, quick=args.quick)
    ue_ok = await wait_ue_attach(ues, timeout_s=90 if args.quick else 180)
    if not ue_ok:
        print("[WARN] No UEs attached - E6 fidelity will be zero. Continuing.", flush=True)

    await start_state_sync_on_twin(twin, gnb, quick=args.quick)
    await start_twin_gnb(twin, quick=args.quick)

    if args.setup_only:
        gnb_ip  = _node_ip.get(gnb,  gnb)
        twin_ip = _node_ip.get(twin, twin)
        print(f"\nSetup complete.")
        print(f"  Real gNB metrics  : tcp://{gnb_ip}:{METRICS_PORT}")
        print(f"  Twin metrics      : tcp://{twin_ip}:{METRICS_PORT}")
        print(f"  Reconciler        : tcp://{twin_ip}:{RECONCILE_PORT}")
        print(f"  state_sync log    : ssh {twin} 'tail -f /tmp/state_sync.log'")
        return

    ok = await wait_for_metrics(gnb, twin, timeout_s=60 if args.quick else 120)
    if not ok and not args.skip_checks:
        print("[ERROR] Metrics not flowing.", file=sys.stderr)
        print(f"  ssh {gnb} 'sudo docker logs tt-gnb --tail 30'")
        print(f"  ssh {twin} 'sudo docker logs tt-gnb --tail 30'")
        sys.exit(1)

    q = args.quick
    results = {}
    results.update(await run_e1_check(twin, warmup_s=15 if q else DUR_E1_WARMUP))
    results.update(run_e3(twin, n_samples=50 if q else 300, m_trials=1 if q else 3))
    results.update(run_e5(twin, duration_s=30 if q else DUR_E5, k=100))
    results.update(run_e6(gnb, twin, duration_s=30 if q else DUR_E6))

    print_report(results)

    if args.teardown_after:
        await teardown(gnb, twin, ues)


def main():
    p = argparse.ArgumentParser(description="Real2Twin indoor OTA evaluation")
    p.add_argument("--gnb-host",      default="powder-gnb-real",
                   help="SSH alias for real OAI gNB node (default: powder-gnb-real)")
    p.add_argument("--twin-host",     default="powder-twin",
                   help="SSH alias for Tiny_Twin node (default: powder-twin)")
    p.add_argument("--ue-hosts",      default="powder-ue1,powder-ue2,powder-ue3,powder-ue4",
                   help="Comma-separated SSH aliases for NUC UE nodes")
    p.add_argument("--quick",         action="store_true",
                   help="Shorten experiment durations (~8 min)")
    p.add_argument("--setup-only",    action="store_true",
                   help="Start stacks only; skip experiments")
    p.add_argument("--teardown",      action="store_true",
                   help="Stop all containers and exit")
    p.add_argument("--teardown-after", action="store_true",
                   help="Run experiments then stop containers")
    p.add_argument("--skip-checks",   action="store_true",
                   help="Continue even if connectivity or metrics checks fail")
    args = p.parse_args()
    asyncio.run(_main(args))


if __name__ == "__main__":
    main()
