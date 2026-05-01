"""
deploy.py -- Full end-to-end deployment of Real + Twin 5G stacks.

Phases:
  1. Real side: find free B210 cluster via portal → instantiate/Submit
                → poll until READY → SSH start CN / gNB / UE
  2. Twin side: SSH powder-twin → start CN → state_sync (FIFO) → gNB → UE
  3. Handoff:  print status, optionally launch fidelity monitor

Usage:
    # Full deployment (real + twin):
    python3 -m orchestrator.deploy --password <pw>

    # Twin side only (real already running):
    python3 -m orchestrator.deploy --twin-only \
        --real-gnb-host gnb-node.dt-real-auto.nicelabexp.emulab.net

    # Dry-run (find B210 cluster but don't create experiment):
    python3 -m orchestrator.deploy --dry-run --password <pw>

Setup (one-time):
    Register powder/profile_real_b210.py on the POWDER portal:
        https://www.powderwireless.net/manage_profile.php
    Copy the UUID and set POWDER_PROFILE_UUID env var or PROFILE_UUID below.
"""

from __future__ import annotations

import argparse
import asyncio
import http.cookiejar
import json
import os
import re
import ssl
import sys
import time
import urllib.parse
import urllib.request
from dataclasses import dataclass
from datetime import datetime
from html import unescape
from pathlib import Path

import asyncssh

# ── Constants ────────────────────────────────────────────────────────────────

POWDER_USER    = "zifan716"
PROJECT        = "NICELabExp"
EXP_NAME       = "dt-real-auto"
PORTAL_BASE    = "https://www.powderwireless.net"
SERVER_AJAX    = f"{PORTAL_BASE}/server-ajax.php"
OAI_BRANCH     = "main"

# Profile "dt-real-b210-new" registered on POWDER portal
PROFILE_UUID   = os.environ.get("POWDER_PROFILE_UUID", "b90eceab-62b9-4658-83e3-ce545a6f2050")

# Campus B210 clusters preferred over bus-stop clusters (health=100 vs 50)
PREFERRED_CLUSTERS = [
    "humanities", "law73", "sagepoint", "ebc", "guesthouse",
    "madsen", "cpg", "moran", "web",
]

TWIN_HOST      = "powder-twin"          # SSH alias in ~/.ssh/config
SSH_USER       = "zifan716"
SSH_CFG        = [Path.home() / ".ssh" / "config"]

TWIN_REPO_DIR  = "~/Tiny_Twin"
TWIN_SIMS_DIR  = f"{TWIN_REPO_DIR}/sims"
TWIN_CN_DIR    = f"{TWIN_SIMS_DIR}/oai-cn"

REAL_GNB_LOG   = f"{TWIN_REPO_DIR}/logs/snr.txt"
FIFO_REAL      = "/tmp/tt_cir_real.fifo"
FIFO_IMAG      = "/tmp/tt_cir_imag.fifo"


# ── POWDER Portal API client ──────────────────────────────────────────────────

class PowderAPI:
    """
    Thin wrapper around the POWDER portal server-ajax.php AJAX API.

    Authentication: uid + password (session cookie-based).
    Node discovery:  amstatus-json embedded in landing.php → B210 cluster status.
    Experiment start: instantiate/Submit with profile UUID + node URNs.
    """

    def __init__(self, uid: str, password: str):
        self.uid = uid
        self.password = password
        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        self._cj = http.cookiejar.CookieJar()
        self._opener = urllib.request.build_opener(
            urllib.request.HTTPSHandler(context=ctx),
            urllib.request.HTTPCookieProcessor(self._cj),
        )
        self._opener.addheaders = [("User-Agent", "dt_sync/deploy")]

    def login(self) -> bool:
        r = self._ajax("login", "DoLogin",
                       {"uid": self.uid, "password": self.password})
        if r.get("code") != 0:
            print(f"  [ERROR] Portal login failed: {r.get('value')}", file=sys.stderr)
            return False
        print("  [OK] Logged in to POWDER portal")
        return True

    def _ajax(self, route: str, method: str, args: dict | None = None) -> dict:
        fields = [("ajax_route", route), ("ajax_method", method)]
        if args:
            for k, v in args.items():
                fields.append((f"ajax_args[{k}]", str(v)))
        else:
            fields.append(("ajax_args[noargs]", "noargs"))
        data = urllib.parse.urlencode(fields).encode()
        req  = urllib.request.Request(
            SERVER_AJAX, data=data,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        )
        resp = self._opener.open(req, timeout=20)
        raw  = resp.read()
        return json.loads(raw) if raw else {"code": -1, "value": "empty"}

    # ── node discovery ───────────────────────────────────────────────────────

    def find_free_b210_cluster(self) -> str | None:
        """
        Return the first B210 cluster name where both nuc1 and nuc2 are free,
        or None if all clusters are busy.
        """
        resp = self._opener.open(f"{PORTAL_BASE}/landing.php", timeout=20)
        content = resp.read(600000).decode("utf-8", errors="ignore")
        m = re.search(
            r"<script[^>]+id=['\"]amstatus-json['\"][^>]*>(.*?)</script>",
            content, re.DOTALL,
        )
        if not m:
            print("  [WARN] amstatus-json not found in landing.php", file=sys.stderr)
            return None
        try:
            amstatus = json.loads(unescape(m.group(1).strip()))
        except Exception as e:
            print(f"  [WARN] amstatus-json parse error: {e}", file=sys.stderr)
            return None

        free: dict[str, str] = {}
        for urn, status in amstatus.items():
            if "powderwireless" not in urn or "emulab" in urn:
                continue
            cluster = urn.split("+")[1].replace(".powderwireless.net", "")
            if (status.get("rawPCsAvailable") == "2"
                    and status.get("rawPCsTotal") == "2"):
                free[cluster] = urn

        if not free:
            print("  [INFO] No free B210 clusters found")
            return None

        for name in PREFERRED_CLUSTERS:
            if name in free:
                print(f"  [OK] Free campus B210 cluster: {name}")
                return name
        name = sorted(free)[0]
        print(f"  [OK] Free B210 cluster: {name}")
        return name

    # ── experiment lifecycle ─────────────────────────────────────────────────

    def _compile_rspec(self, cluster: str) -> str | None:
        """
        Compile profile_real_b210.py with the given cluster's B210 URNs baked in
        as default parameter values, returning the RSpec XML string.

        The portal stores no script for this profile (raw-RSpec profile), so
        parameter formfields are ignored on instantiation.  Passing xmlcode= in
        the Submit call overrides the stored RSpec with this freshly-compiled one,
        ensuring the radio nodes carry valid component_ids.
        """
        script_path = Path(__file__).parent.parent / "powder" / "profile_real_b210.py"
        try:
            script = script_path.read_text()
        except OSError as e:
            print(f"  [ERROR] Cannot read profile script: {e}", file=sys.stderr)
            return None

        gnb_urn = f"urn:publicid:IDN+{cluster}.powderwireless.net+node+nuc1"
        ue_urn  = f"urn:publicid:IDN+{cluster}.powderwireless.net+node+nuc2"

        script = re.sub(
            r'(defineParameter\(\s*\n?\s*"gnb_b210_id".*?STRING,\s*)"[^"]*"',
            r'\1"' + gnb_urn + '"', script, flags=re.DOTALL,
        )
        script = re.sub(
            r'(defineParameter\(\s*\n?\s*"ue_b210_id".*?STRING,\s*)"[^"]*"',
            r'\1"' + ue_urn + '"', script, flags=re.DOTALL,
        )

        result = self._ajax("manage_profile", "CheckScript",
                            {"uuid": PROFILE_UUID, "script": script})
        if result.get("code") != 0 or not isinstance(result.get("value"), dict):
            print(f"  [ERROR] CheckScript failed: {result.get('value')}", file=sys.stderr)
            return None
        rspec = result["value"].get("rspec", "")
        if not rspec:
            print("  [ERROR] CheckScript returned empty rspec", file=sys.stderr)
            return None
        print(f"  [OK] Compiled RSpec for cluster '{cluster}' ({len(rspec)} bytes)")
        return rspec

    def start_experiment(
        self,
        cluster: str,
        dry_run: bool = False,
    ) -> bool:
        """Instantiate profile_real_b210 using nuc1 + nuc2 from the given cluster."""
        gnb_urn = f"urn:publicid:IDN+{cluster}.powderwireless.net+node+nuc1"
        ue_urn  = f"urn:publicid:IDN+{cluster}.powderwireless.net+node+nuc2"

        if dry_run:
            print(f"  [DRY-RUN] Would start experiment:")
            print(f"    profile  : {PROFILE_UUID or '(not set)'}")
            print(f"    project  : {PROJECT}")
            print(f"    name     : {EXP_NAME}")
            print(f"    gnb_urn  : {gnb_urn}")
            print(f"    ue_urn   : {ue_urn}")
            return True

        if not PROFILE_UUID:
            print(
                "  [ERROR] PROFILE_UUID not set.\n"
                "    Register powder/profile_real_b210.py at:\n"
                "    https://www.powderwireless.net/manage_profile.php\n"
                "    then set POWDER_PROFILE_UUID env var.",
                file=sys.stderr,
            )
            return False

        baked_rspec = self._compile_rspec(cluster)
        if not baked_rspec:
            return False

        fields = [
            ("ajax_route",  "instantiate"),
            ("ajax_method", "Submit"),
        ]
        for k, v in {
            "profile":  PROFILE_UUID,
            "pid":      PROJECT,
            "gid":      PROJECT,
            "name":     EXP_NAME,
            "where":    "Emulab",
            "duration": "16",
            "xmlcode":  baked_rspec,
        }.items():
            fields.append((f"ajax_args[formfields][{k}]", v))

        data = urllib.parse.urlencode(fields).encode()
        req  = urllib.request.Request(
            SERVER_AJAX, data=data,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        )
        resp   = self._opener.open(req, timeout=30)
        result = json.loads(resp.read())
        code   = result.get("code", -1)
        val    = result.get("value", "")

        if code == 0:
            print(f"  [OK] Experiment started: {val}")
            return True
        if isinstance(val, str) and "already exists" in val.lower():
            print(f"  [INFO] Experiment {EXP_NAME!r} already running")
            return True
        print(f"  [ERROR] Submit failed (code={code}): {val}", file=sys.stderr)
        return False

    def experiment_status(self) -> str | None:
        """Return state string ('ready', 'active', 'provisioning', 'failed', …) or None."""
        # The portal status page embeds state in the URL query string response.
        # Use the status/GetInstanceStatus AJAX API with uuid if known.
        # Fallback: check experiments.php for experiment name match.
        # For now, return None (caller uses SSH connectivity as proxy for readiness).
        return None

    def wait_until_ready(self, timeout: int = 1200, poll: int = 30) -> bool:
        """Block until experiment nodes are SSH-accessible or timeout."""
        gnb_host = self.node_hostname("gnb-node")
        deadline = time.time() + timeout
        print(f"  Waiting up to {timeout}s for {gnb_host} to be SSH-accessible ...", flush=True)
        import socket
        while time.time() < deadline:
            try:
                socket.setdefaulttimeout(5)
                s = socket.create_connection((gnb_host, 22), timeout=5)
                s.close()
                print(f"  [OK] {gnb_host} is up.")
                return True
            except OSError:
                ts = datetime.now().strftime("%H:%M:%S")
                print(f"  [{ts}] waiting ...", flush=True)
                time.sleep(poll)
        print("  [ERROR] Timeout waiting for experiment.")
        return False

    @staticmethod
    def node_hostname(node_name: str) -> str:
        """Deterministic POWDER hostname: {node}.{eid}.{pid_lower}.emulab.net"""
        return f"{node_name}.{EXP_NAME}.{PROJECT.lower()}.emulab.net"


# ── Async SSH helpers ────────────────────────────────────────────────────────

async def _ssh_run(host: str, cmd: str, check: bool = True) -> str:
    """Run a command on host via SSH, return stdout. Raises on non-zero if check=True."""
    async with asyncssh.connect(
        host, username=SSH_USER, known_hosts=None, config=SSH_CFG
    ) as conn:
        result = await conn.run(cmd, check=False)
        if check and result.exit_status != 0:
            raise RuntimeError(
                f"[{host}] command failed (exit {result.exit_status}):\n"
                f"  cmd:    {cmd}\n"
                f"  stderr: {result.stderr.strip()}"
            )
        return result.stdout


async def _ssh_bg(host: str, cmd: str, log: str) -> None:
    """Launch cmd in background on host, redirect to log."""
    nohup = f"nohup bash -c {_q(cmd)} > {log} 2>&1 &"
    await _ssh_run(host, nohup, check=False)


def _q(s: str) -> str:
    """Single-quote a shell string."""
    return "'" + s.replace("'", "'\\''") + "'"


# ── Twin side deployer ────────────────────────────────────────────────────────

class TwinDeployer:
    """Start the full twin stack on powder-twin."""

    def __init__(self, real_gnb_host: str, cal_path: str = "logs/calibration.npy"):
        self.host       = TWIN_HOST
        self.real_gnb   = real_gnb_host
        self.cal_path   = cal_path

    async def ensure_cn_running(self) -> None:
        print("[Twin] Checking CN ...", flush=True)
        out = await _ssh_run(
            self.host,
            "sudo docker ps --format '{{.Names}} {{.Status}}' | grep oai-amf || true",
            check=False,
        )
        if "oai-amf" in out and "Up" in out:
            print("[Twin] CN already running.")
            return
        print("[Twin] Starting CN ...", flush=True)
        await _ssh_run(
            self.host,
            f"cd {TWIN_CN_DIR} && "
            "sudo docker compose -f docker-compose.yaml "
            "-f oai-cn-tt-override.yaml up -d",
        )
        # Wait for AMF to become healthy
        for _ in range(24):
            await asyncio.sleep(5)
            out = await _ssh_run(
                self.host,
                "sudo docker ps --format '{{.Names}} {{.Status}}' | grep oai-amf",
                check=False,
            )
            if "healthy" in out:
                print("[Twin] CN healthy.")
                return
        print("[Twin][WARN] CN may not be healthy yet — continuing.")

    async def start_state_sync(self) -> None:
        print("[Twin] Starting state_sync ...", flush=True)
        await _ssh_run(
            self.host,
            "pkill -f state_sync.py || true; sleep 1; "
            f"rm -f {FIFO_REAL} {FIFO_IMAG}",
            check=False,
        )
        cmd = (
            f"cd {TWIN_REPO_DIR} && "
            f"python3 -m orchestrator.state_sync "
            f"--real-host {self.real_gnb} "
            f"--twin-host {TWIN_HOST} "
            f"--local-fifo "
            f"--cal-path {self.cal_path} "
            f"--drift-log /tmp/drift_e1.csv"
        )
        await _ssh_bg(self.host, cmd, "/tmp/state_sync.log")

        # Wait until FIFOs exist (state_sync created them)
        print("[Twin] Waiting for FIFOs ...", flush=True)
        for _ in range(20):
            await asyncio.sleep(0.5)
            out = await _ssh_run(
                self.host,
                f"test -p {FIFO_REAL} && echo yes || echo no",
                check=False,
            )
            if "yes" in out:
                print(f"[Twin] FIFOs ready: {FIFO_REAL}")
                return
        raise RuntimeError("[Twin] FIFOs not created within 10s — state_sync may have crashed.")

    async def start_gnb(self) -> None:
        print("[Twin] Starting twin gNB ...", flush=True)
        env = (
            f"TT_CHANNEL_FILE_REAL={FIFO_REAL} "
            f"TT_CHANNEL_FILE_IMAG={FIFO_IMAG}"
        )
        await _ssh_run(
            self.host,
            f"cd {TWIN_SIMS_DIR} && "
            f"export {env} && "
            "sudo -E docker compose -f docker-compose.twin.yaml up -d tt-gnb",
        )
        await asyncio.sleep(8)
        print("[Twin] gNB started.")

    async def start_ue(self) -> None:
        print("[Twin] Starting twin UE ...", flush=True)
        await _ssh_run(
            self.host,
            f"cd {TWIN_SIMS_DIR} && "
            "sudo docker compose -f docker-compose.twin.yaml up -d --no-recreate tt-nrue1",
        )
        print("[Twin] UE started.")

    async def deploy(self) -> None:
        await self.ensure_cn_running()
        await self.start_state_sync()
        await self.start_gnb()
        await self.start_ue()
        print("[Twin] Full twin stack deployed.", flush=True)


# ── Real side deployer ────────────────────────────────────────────────────────

# Docker run commands for real B210 mode (no --rfsim, real radio hardware).
# The UHD driver auto-detects the connected B210 via USB.
_REAL_GNB_CMD = (
    "sudo docker run -d --rm --privileged --name real-gnb "
    "--network host "
    "-v /dev/bus/usb:/dev/bus/usb "
    "-v ~/Tiny_Twin/logs:/tt-ran/logs "
    "tt-gnb:v2 "
    "bash -c '"
    "cd /tt-ran/cmake_targets/ran_build/build/ && "
    "exec ./nr-softmodem "
    "-O ../../../targets/PROJECTS/GENERIC-NR-5GC/CONF/gnb.sa.band78.fr1.106PRB.usrpb210.conf "
    "--sa -E "
    "--TAP 1 --TTI 1 --SNR 1 --CQI 1 --TPT 1"
    "'"
)

_REAL_UE_CMD = (
    "sudo docker run -d --rm --privileged --name real-ue "
    "--network host "
    "-v /dev/bus/usb:/dev/bus/usb "
    "tt-nrue:v2 "
    "bash -c '"
    "cd /opt/tt-ran/cmake_targets/ran_build/build/ && "
    "exec ./nr-uesoftmodem "
    "--uicc0.imsi 001010000000001 "
    "-C 3619200000 -r 106 --numerology 1 --ssb 516 "
    "-E --sa "
    "--TAP 1 "
    "-O ../../../ci-scripts/conf_files/nrue.uicc.conf"
    "'"
)

_REAL_CN_CMD = (
    f"cd {TWIN_REPO_DIR}/sims/oai-cn && "
    "sudo docker compose -f docker-compose.yaml "
    "-f oai-cn-tt-override.yaml up -d"
)


@dataclass
class RealHosts:
    gnb: str   # gnb-node.dt-real-auto.nicelabexp.emulab.net
    ue:  str
    cn:  str


class RealDeployer:
    """Wait for real nodes to be SSH-accessible then start OAI processes."""

    def __init__(self, hosts: RealHosts):
        self.hosts = hosts

    async def _wait_ssh(self, host: str, timeout: int = 900) -> None:
        deadline = time.time() + timeout
        print(f"[Real] Waiting for SSH on {host} ...", flush=True)
        while time.time() < deadline:
            try:
                await _ssh_run(host, "echo ok", check=True)
                print(f"[Real] {host} SSH ready.")
                return
            except Exception:
                await asyncio.sleep(15)
        raise RuntimeError(f"[Real] SSH timeout on {host}")

    async def _wait_startup_done(self, host: str, marker: str, timeout: int = 1800) -> None:
        """Block until startup script writes marker string to /var/log/startup.log."""
        deadline = time.time() + timeout
        print(f"[Real] Waiting for startup on {host} (marker: '{marker}') ...", flush=True)
        while time.time() < deadline:
            out = await _ssh_run(
                host,
                f"grep -q {_q(marker)} /var/log/startup.log 2>/dev/null && echo yes || echo no",
                check=False,
            )
            if "yes" in out:
                print(f"[Real] {host} startup complete.")
                return
            await asyncio.sleep(20)
        print(f"[Real][WARN] Startup marker not found on {host} — continuing anyway.")

    async def deploy(self) -> None:
        h = self.hosts
        # Wait for SSH on all three nodes in parallel
        await asyncio.gather(
            self._wait_ssh(h.gnb),
            self._wait_ssh(h.ue),
            self._wait_ssh(h.cn),
        )

        # Wait for startup scripts (profile wrote "node ready." at the end)
        await asyncio.gather(
            self._wait_startup_done(h.gnb, "gNB node ready."),
            self._wait_startup_done(h.ue,  "UE node ready."),
            self._wait_startup_done(h.cn,  "CN node ready."),
        )

        # Start CN first
        print("[Real] Starting CN ...", flush=True)
        await _ssh_run(h.cn, _REAL_CN_CMD)
        await asyncio.sleep(15)   # give AMF/SMF/UPF time to start

        # Start gNB
        print("[Real] Starting gNB (B210) ...", flush=True)
        await _ssh_run(h.gnb, _REAL_GNB_CMD)
        await asyncio.sleep(5)

        # Start UE
        print("[Real] Starting UE (B210) ...", flush=True)
        await _ssh_run(h.ue, _REAL_UE_CMD)

        print("[Real] All real-side processes started.", flush=True)


# ── Main orchestration ────────────────────────────────────────────────────────

async def _full_deploy(args) -> None:
    # ── Phase 1: Real side ────────────────────────────────────────────────
    if not args.twin_only:
        api = PowderAPI(uid=args.uid, password=args.password)
        if not api.login():
            sys.exit(1)

        print("\n=== Phase 1: Real side — POWDER experiment ===", flush=True)
        cluster = None
        while cluster is None:
            cluster = api.find_free_b210_cluster()
            if cluster is None:
                if args.once:
                    print("No free B210 cluster available. Use --poll-interval to keep retrying.")
                    return
                print(f"  Retrying in {args.poll_interval}s ...", flush=True)
                await asyncio.sleep(args.poll_interval)

        ok = api.start_experiment(cluster, dry_run=args.dry_run)
        if not ok:
            sys.exit(1)

        if not args.dry_run:
            ready = api.wait_until_ready(timeout=args.startup_timeout)
            if not ready:
                sys.exit(1)

        real_hosts = RealHosts(
            gnb=api.node_hostname("gnb-node"),
            ue= api.node_hostname("ue-node"),
            cn= api.node_hostname("cn-node"),
        )
        print(f"\n  Node hostnames:")
        print(f"    gNB : {real_hosts.gnb}")
        print(f"    UE  : {real_hosts.ue}")
        print(f"    CN  : {real_hosts.cn}")
    else:
        real_hosts = RealHosts(
            gnb=args.real_gnb_host,
            ue= args.real_ue_host  or args.real_gnb_host.replace("gnb-node", "ue-node"),
            cn= args.real_cn_host  or args.real_gnb_host.replace("gnb-node", "cn-node"),
        )

    # ── Phase 2: Twin side ────────────────────────────────────────────────
    print("\n=== Phase 2: Twin side — powder-twin ===", flush=True)
    twin = TwinDeployer(real_gnb_host=real_hosts.gnb)
    await twin.deploy()

    # ── Phase 3: Real side process startup ───────────────────────────────
    if not args.twin_only and not args.dry_run:
        print("\n=== Phase 3: Real side — OAI processes ===", flush=True)
        real = RealDeployer(real_hosts)
        await real.deploy()

    # ── Done ──────────────────────────────────────────────────────────────
    print("\n=== Deployment complete ===", flush=True)
    print(f"  Twin state_sync log : ssh {TWIN_HOST} 'tail -f /tmp/state_sync.log'")
    if not args.twin_only:
        print(f"  Real gNB logs       : ssh {real_hosts.gnb} 'tail -f ~/Tiny_Twin/logs/snr.txt'")
    print()


def main():
    parser = argparse.ArgumentParser(description="Deploy Real + Twin 5G DT stacks")
    parser.add_argument("--uid",      default=os.environ.get("POWDER_USER", POWDER_USER),
                        help="POWDER portal username (default: $POWDER_USER or zifan716)")
    parser.add_argument("--password", default=os.environ.get("POWDER_PASS", ""),
                        help="POWDER portal password (or set $POWDER_PASS)")

    # Real side control
    parser.add_argument("--twin-only", action="store_true",
                        help="Skip real-side POWDER instantiation (twin side only)")
    parser.add_argument("--real-gnb-host", default="",
                        help="Real gNB hostname (required with --twin-only)")
    parser.add_argument("--real-ue-host",  default="",
                        help="Real UE hostname (default: derived from gnb hostname)")
    parser.add_argument("--real-cn-host",  default="",
                        help="Real CN hostname (default: derived from gnb hostname)")

    # Timing
    parser.add_argument("--poll-interval",   type=int, default=300,
                        help="Seconds between B210 availability checks (default: 300)")
    parser.add_argument("--startup-timeout", type=int, default=1200,
                        help="Max seconds to wait for POWDER experiment READY (default: 1200)")
    parser.add_argument("--once", action="store_true",
                        help="Check B210 availability once and exit if unavailable")

    # Debug
    parser.add_argument("--dry-run", action="store_true",
                        help="Find B210 pair and show plan, but do not start anything")

    args = parser.parse_args()

    if args.twin_only and not args.real_gnb_host:
        parser.error("--real-gnb-host is required with --twin-only")

    if not args.twin_only and not args.password:
        import getpass
        args.password = getpass.getpass("POWDER portal password: ")

    asyncio.run(_full_deploy(args))


if __name__ == "__main__":
    main()
