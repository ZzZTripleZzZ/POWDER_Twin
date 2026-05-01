#!/usr/bin/env python3
"""
auto_schedule.py -- Poll POWDER for available same-sector B210 pairs and
automatically start the real-side experiment when resources are free.

Usage:
    python3 powder/auto_schedule.py [--once] [--dry-run] [--poll-interval 300]

Setup (one-time):
    1. Register profile_real_b210.py on the POWDER portal:
       https://www.powderwireless.net/manage_profile.php
       → Copy the UUID shown after saving → paste into PROFILE_UUID below.
    2. Ensure ~/.ssl/emulab.pem or cloudlab.pem is available.

How it works:
    1. Login to POWDER portal via session cookie (uid/password).
    2. Fetch amstatus-json from landing.php to find free B210 clusters.
    3. Pick a cluster where both nuc1 and nuc2 are free (rawPCsAvailable==2).
    4. Construct node URNs and call instantiate/Submit to start the experiment.
    5. If nothing is free, sleep --poll-interval seconds and retry.
"""

import argparse
import http.cookiejar
import json
import os
import ssl
import sys
import time
import urllib.parse
import urllib.request
from datetime import datetime
from html import unescape

PORTAL_BASE    = "https://www.powderwireless.net"
SERVER_AJAX    = f"{PORTAL_BASE}/server-ajax.php"
PROJECT        = "NICELabExp"
EXP_NAME       = "dt-real-auto"
OAI_BRANCH     = "main"

# Profile "dt-real-b210-new" registered on POWDER portal
PROFILE_UUID   = os.environ.get("POWDER_PROFILE_UUID", "b90eceab-62b9-4658-83e3-ce545a6f2050")
# ─────────────────────────────────────────────────────────────────────────────

# Clusters known to have 2 B210 NUC nodes (nuc1 + nuc2).
# "campus" clusters (humanities, law73, etc.) have health=100 and are preferred.
# "bus" clusters (bus-4208, etc.) have health=50 (intermittent).
PREFERRED_CLUSTERS = [
    "humanities", "law73", "sagepoint", "ebc", "guesthouse",
    "madsen", "cpg", "moran", "web",
]


class PowderPortal:
    """Thin wrapper around the POWDER portal AJAX API."""

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
        self._opener.addheaders = [("User-Agent", "dt_sync/auto_schedule")]

    def login(self) -> bool:
        result = self._ajax("login", "DoLogin",
                            {"uid": self.uid, "password": self.password})
        if result.get("code") != 0:
            print(f"[ERROR] Login failed: {result.get('value')}", file=sys.stderr)
            return False
        print("[OK] Logged in to POWDER portal")
        return True

    def _ajax(self, route: str, method: str, args: dict | None = None) -> dict:
        fields = [("ajax_route", route), ("ajax_method", method)]
        if args:
            for k, v in args.items():
                fields.append((f"ajax_args[{k}]", str(v)))
        else:
            fields.append(("ajax_args[noargs]", "noargs"))
        data = urllib.parse.urlencode(fields).encode()
        req = urllib.request.Request(
            SERVER_AJAX, data=data,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        )
        resp = self._opener.open(req, timeout=20)
        raw = resp.read()
        return json.loads(raw) if raw else {"code": -1, "value": "empty"}

    def get_free_b210_cluster(self) -> str | None:
        """
        Return the name of the first B210 cluster (e.g. 'humanities') where
        both nuc1 and nuc2 are currently free, or None if nothing is available.

        Prefers campus clusters (health=100) over bus-stop clusters (health=50).
        """
        # Fetch landing page and extract amstatus-json
        resp = self._opener.open(f"{PORTAL_BASE}/landing.php", timeout=20)
        content = resp.read(600000).decode("utf-8", errors="ignore")

        import re
        m = re.search(
            r"<script[^>]+id=['\"]amstatus-json['\"][^>]*>(.*?)</script>",
            content, re.DOTALL,
        )
        if not m:
            print("[WARN] Could not find amstatus-json in landing.php", file=sys.stderr)
            return None

        try:
            amstatus = json.loads(unescape(m.group(1).strip()))
        except Exception as e:
            print(f"[WARN] amstatus-json parse error: {e}", file=sys.stderr)
            return None

        # Collect free POWDER B210 clusters (rawPCsAvailable == rawPCsTotal == '2')
        free: dict[str, str] = {}   # cluster_name → URN
        for urn, status in amstatus.items():
            if "powderwireless" not in urn or "emulab" in urn:
                continue
            cluster = urn.split("+")[1].replace(".powderwireless.net", "")
            if (status.get("rawPCsAvailable") == "2"
                    and status.get("rawPCsTotal") == "2"):
                free[cluster] = urn

        if not free:
            print("[INFO] No free B210 clusters found")
            return None

        # Prefer campus clusters
        for name in PREFERRED_CLUSTERS:
            if name in free:
                print(f"[OK] Free campus cluster: {name}")
                return name

        # Fall back to any bus-stop cluster
        for name in sorted(free):
            print(f"[OK] Free bus cluster: {name}")
            return name

        return None

    def _compile_rspec(self, cluster: str, profile_uuid: str) -> str | None:
        """
        Compile the geni-lib profile script with the given cluster's B210 URNs
        baked in as default parameter values.  Returns the RSpec XML string, or
        None on failure.

        The portal stores only a bare RSpec for this profile (no script), so we
        bake the URNs into a local copy of the script and call CheckScript to get
        a fully-resolved RSpec.  The 'xmlcode' field in instantiate/Submit then
        overrides the stored RSpec, making the radio-node component_ids valid.
        """
        import re as _re
        script_path = os.path.join(os.path.dirname(__file__), "profile_real_b210.py")
        try:
            with open(script_path) as f:
                script = f.read()
        except OSError as e:
            print(f"[ERROR] Cannot read profile script: {e}", file=sys.stderr)
            return None

        gnb_urn = f"urn:publicid:IDN+{cluster}.powderwireless.net+node+nuc1"
        ue_urn  = f"urn:publicid:IDN+{cluster}.powderwireless.net+node+nuc2"

        # Substitute URNs as default values in defineParameter() calls
        script = _re.sub(
            r'(defineParameter\(\s*\n?\s*"gnb_b210_id".*?STRING,\s*)"[^"]*"',
            r'\1"' + gnb_urn + '"', script, flags=_re.DOTALL,
        )
        script = _re.sub(
            r'(defineParameter\(\s*\n?\s*"ue_b210_id".*?STRING,\s*)"[^"]*"',
            r'\1"' + ue_urn + '"', script, flags=_re.DOTALL,
        )

        result = self._ajax("manage_profile", "CheckScript",
                            {"uuid": profile_uuid, "script": script})
        if result.get("code") != 0 or not isinstance(result.get("value"), dict):
            print(f"[ERROR] CheckScript failed: {result.get('value')}", file=sys.stderr)
            return None

        rspec = result["value"].get("rspec", "")
        if not rspec:
            print("[ERROR] CheckScript returned empty rspec", file=sys.stderr)
            return None

        print(f"[OK] Compiled RSpec for cluster '{cluster}' ({len(rspec)} bytes)")
        return rspec

    def start_experiment(
        self,
        cluster: str,
        profile_uuid: str,
        dry_run: bool = False,
    ) -> bool:
        """
        Instantiate profile_real_b210 using nuc1 and nuc2 from the given cluster.

        cluster      -- e.g. 'humanities'
        profile_uuid -- UUID from the POWDER portal profile page
        """
        gnb_urn = f"urn:publicid:IDN+{cluster}.powderwireless.net+node+nuc1"
        ue_urn  = f"urn:publicid:IDN+{cluster}.powderwireless.net+node+nuc2"

        if dry_run:
            print("[DRY-RUN] Would start experiment:")
            print(f"  profile  : {profile_uuid}")
            print(f"  project  : {PROJECT}")
            print(f"  name     : {EXP_NAME}")
            print(f"  gnb_urn  : {gnb_urn}")
            print(f"  ue_urn   : {ue_urn}")
            return True

        # Compile a cluster-specific RSpec with component_ids baked in.
        # The portal's profile has no stored script, so parameter values passed
        # via formfields are ignored.  Passing xmlcode= overrides the stored RSpec.
        baked_rspec = self._compile_rspec(cluster, profile_uuid)
        if not baked_rspec:
            return False

        fields = [
            ("ajax_route",  "instantiate"),
            ("ajax_method", "Submit"),
        ]
        for k, v in {
            "profile":  profile_uuid,
            "pid":      PROJECT,
            "gid":      PROJECT,
            "name":     EXP_NAME,
            "where":    "Emulab",
            "duration": "16",
            "xmlcode":  baked_rspec,
        }.items():
            fields.append((f"ajax_args[formfields][{k}]", v))

        data = urllib.parse.urlencode(fields).encode()
        req = urllib.request.Request(
            SERVER_AJAX, data=data,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        )
        resp = self._opener.open(req, timeout=30)
        result = json.loads(resp.read())

        code = result.get("code", -1)
        val  = result.get("value", "")

        if code == 0:
            print(f"[OK] Experiment started! UUID: {val}")
            print(f"[OK] Status: {PORTAL_BASE}/status.php?pid={PROJECT}&eid={EXP_NAME}")
            return True
        elif isinstance(val, str) and "already exists" in val.lower():
            print(f"[INFO] Experiment {EXP_NAME!r} already running")
            return True
        else:
            print(f"[ERROR] Submit failed (code={code}): {val}", file=sys.stderr)
            return False


def poll_and_start(
    uid: str,
    password: str,
    poll_interval: int = 300,
    dry_run: bool = False,
) -> None:
    portal = PowderPortal(uid, password)

    if not portal.login():
        sys.exit(1)

    if not PROFILE_UUID:
        print(
            "[ERROR] PROFILE_UUID is not set.\n"
            "  1. Go to https://www.powderwireless.net/manage_profile.php\n"
            "  2. Create a new profile using powder/profile_real_b210.py\n"
            "  3. Copy the UUID and set POWDER_PROFILE_UUID env var or\n"
            "     edit PROFILE_UUID in this script.",
            file=sys.stderr,
        )
        sys.exit(1)

    print(f"[auto_schedule] Polling every {poll_interval}s for a free B210 cluster ...")
    attempt = 0
    while True:
        attempt += 1
        ts = datetime.now().strftime("%H:%M:%S")
        print(f"\n[{ts}] Attempt #{attempt}")

        cluster = portal.get_free_b210_cluster()
        if cluster is not None:
            ok = portal.start_experiment(cluster, PROFILE_UUID, dry_run=dry_run)
            if ok:
                print("[auto_schedule] Done.")
                return
            print("[WARN] Experiment start failed — will retry")

        print(f"Sleeping {poll_interval}s ...")
        time.sleep(poll_interval)


def main():
    ap = argparse.ArgumentParser(description="Auto-schedule POWDER B210 experiment")
    ap.add_argument("--uid",      default=os.environ.get("POWDER_USER", "zifan716"))
    ap.add_argument("--password", default=os.environ.get("POWDER_PASS", ""))
    ap.add_argument("--poll-interval", type=int, default=300,
                    help="Seconds between availability checks (default: 300)")
    ap.add_argument("--dry-run", action="store_true",
                    help="Find a free cluster but do not start the experiment")
    ap.add_argument("--once", action="store_true",
                    help="Check once and exit (no polling loop)")
    args = ap.parse_args()

    if not args.password:
        args.password = input("POWDER password: ")

    portal = PowderPortal(args.uid, args.password)
    if not portal.login():
        sys.exit(1)

    cluster = portal.get_free_b210_cluster()
    if args.once or args.dry_run:
        if cluster:
            portal.start_experiment(cluster, PROFILE_UUID or "PROFILE_UUID_HERE",
                                    dry_run=args.dry_run)
        else:
            print("No free B210 cluster available right now.")
        return

    poll_and_start(args.uid, args.password, args.poll_interval, args.dry_run)


if __name__ == "__main__":
    main()
