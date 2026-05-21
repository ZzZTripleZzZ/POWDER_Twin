#!/usr/bin/env python3
"""
auto_schedule.py -- Poll POWDER for available same-sector B210 pairs and
automatically start the real-side experiment when resources are free.

Usage:
    python3 powder/auto_schedule.py [--once] [--dry-run] [--poll-interval 300]

Setup (one-time):
    1. Register profile_real_b210_2node.py on the POWDER portal:
       https://www.powderwireless.net/manage_profile.php
       → Copy the UUID shown after saving → paste into PROFILE_UUID below.
    2. Ensure ~/.ssl/emulab.pem or cloudlab.pem is available.

How it works:
    1. Login to POWDER portal via session cookie (uid/password).
    2. Fetch amstatus-json and radioinfo-json from instantiate.php.
    3. Pick a cluster only if nuc1 and nuc2 are both available B210 radios
       and each has at least one TX/RX frontend covering the requested band.
    4. Construct node URNs and call instantiate/Submit to start the experiment.
    5. If nothing is safe to schedule, sleep --poll-interval seconds and retry.
"""

from __future__ import annotations

import argparse
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

PORTAL_BASE    = "https://www.powderwireless.net"
SERVER_AJAX    = f"{PORTAL_BASE}/server-ajax.php"
PROJECT        = "NICELabExp"
EXP_NAME       = "dt-real-auto"
OAI_BRANCH     = "main"

# Profile "dt-real-b210" registered on POWDER portal. Prefer the latest
# version UUID; old version UUIDs can silently instantiate stale RSpecs.
PROFILE_UUID   = os.environ.get("POWDER_PROFILE_UUID", "07f2e171-464f-11f1-90d9-e4434b2381fc")
KNOWN_BAD_PROFILE_UUIDS = {
    # Stale profile that maps the real side back to generic d430 nodes instead
    # of Indoor OTA B210 hosts. Leaving this in the environment caused a false
    # successful instantiate with the wrong topology.
    "c623ac4e-edd2-45a6-b200-e4c5c45d821e": "stale d430-mapped B210 profile",
    # Older 2-node Indoor OTA profile version that placed spectrum on a
    # synthetic rf0 interface and triggered a POWDER CM internal error.
    "e137185c-45ed-11f1-90d9-e4434b2381fc": "stale rf0-interface spectrum profile",
}
# ─────────────────────────────────────────────────────────────────────────────

# Clusters known to have 2 B210 NUC nodes (nuc1 + nuc2).
# "campus" clusters (humanities, law73, etc.) have health=100 and are preferred.
# "bus" clusters (bus-4208, etc.) have health=50 (intermittent).
PREFERRED_CLUSTERS = [
    "humanities", "law73", "sagepoint", "ebc", "guesthouse",
    "madsen", "cpg", "moran", "web",
]


@dataclass(frozen=True)
class B210Pair:
    label: str
    gnb_urn: str
    ue_urn: str


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
        self._pair_profile_cache: dict[str, str] = {}

    def login(self) -> bool:
        result = self._ajax("login", "DoLogin",
                            {"uid": self.uid, "password": self.password})
        if result.get("code") != 0:
            print(f"[ERROR] Login failed: {result.get('value')}", file=sys.stderr)
            return False
        print("[OK] Logged in to POWDER portal", file=sys.stderr)
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

    def _ajax_form(
        self,
        route: str,
        method: str,
        formfields: dict,
        checkonly: bool,
        timeout: int = 60,
    ) -> dict:
        fields = [
            ("ajax_route", route),
            ("ajax_method", method),
            ("ajax_args[checkonly]", str(int(checkonly))),
            ("ajax_args[embedded]", "0"),
        ]
        for key, value in formfields.items():
            if isinstance(value, (dict, list)):
                value = json.dumps(value)
            fields.append((f"ajax_args[formfields][{key}]", str(value)))
        data = urllib.parse.urlencode(fields).encode()
        req = urllib.request.Request(
            SERVER_AJAX,
            data=data,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        )
        resp = self._opener.open(req, timeout=timeout)
        raw = resp.read()
        return json.loads(raw) if raw else {"code": -1, "value": "empty"}

    @staticmethod
    def _cluster_of(urn: str) -> str:
        parts = urn.split("+")
        if len(parts) < 2:
            return urn
        return parts[1].replace(".powderwireless.net", "")

    @staticmethod
    def _node_urn(cluster_urn: str, node_name: str) -> str:
        parts = cluster_urn.split("+")
        if len(parts) < 2:
            return node_name
        return f"urn:publicid:IDN+{parts[1]}+node+{node_name}"

    @staticmethod
    def _covers_band(freqs: str, lo_mhz: float, hi_mhz: float) -> bool:
        for part in str(freqs or "").split(","):
            match = re.match(r"\s*(\d+(?:\.\d+)?)\s*-\s*(\d+(?:\.\d+)?)\s*$", part)
            if match and float(match.group(1)) <= lo_mhz and float(match.group(2)) >= hi_mhz:
                return True
        return False

    def _resource_snapshot(self) -> tuple[dict, dict] | None:
        resp = self._opener.open(f"{PORTAL_BASE}/instantiate.php", timeout=20)
        content = resp.read().decode("utf-8", errors="ignore")

        def extract(script_id: str) -> dict | None:
            match = re.search(
                rf"<script[^>]+id=['\"]{script_id}['\"][^>]*>(.*?)</script>",
                content,
                re.DOTALL,
            )
            if not match:
                return None
            return json.loads(unescape(match.group(1).strip()))

        try:
            amstatus = extract("amstatus-json")
            radioinfo = extract("radioinfo-json")
        except Exception as e:
            print(f"[WARN] POWDER resource JSON parse error: {e}", file=sys.stderr)
            return None

        if amstatus is None:
            print("[WARN] Could not find amstatus-json in instantiate.php", file=sys.stderr)
            return None
        if radioinfo is None:
            print("[WARN] Could not find radioinfo-json in instantiate.php", file=sys.stderr)
            return None
        return amstatus, radioinfo

    def _schedulable_b210_pairs(
        self,
        amstatus: dict,
        radioinfo: dict,
        lo_mhz: float = 3550.0,
        hi_mhz: float = 3700.0,
    ) -> list[B210Pair]:
        pairs: list[B210Pair] = []

        def is_txrx_b210(node: dict) -> bool:
            return bool(
                self._txrx_capable_frontends(
                    node, lo_mhz, hi_mhz, "NI/Ettus B210"
                )
            )

        def natural_node_key(name: str) -> tuple[str, int]:
            match = re.search(r"(\d+)$", name)
            return (re.sub(r"\d+$", "", name), int(match.group(1)) if match else -1)

        # Indoor OTA B210 nodes live under emulab.net as ota-nuc*. These are
        # compute hosts with attached B210s and are the right target for a
        # two-radio OTA experiment when the Indoor OTA Lab has capacity.
        for urn, nodes in radioinfo.items():
            if self._cluster_of(urn) != "emulab.net":
                continue
            ota_nodes = sorted(
                (
                    node_name
                    for node_name, node in (nodes or {}).items()
                    if node_name.startswith("ota-nuc") and is_txrx_b210(node)
                ),
                key=natural_node_key,
            )
            for idx in range(0, len(ota_nodes) - 1, 2):
                gnb_node = ota_nodes[idx]
                ue_node = ota_nodes[idx + 1]
                pairs.append(
                    B210Pair(
                        label=f"emulab.net:{gnb_node}+{ue_node}",
                        gnb_urn=self._node_urn(urn, gnb_node),
                        ue_urn=self._node_urn(urn, ue_node),
                    )
                )

        # Campus fixed endpoints use nuc1/nuc2 naming. These are only valid for
        # NR attach if both endpoints are TX/RX on the requested band; many
        # current campus clusters have nuc1 as RX-only.
        campus_pairs: dict[str, B210Pair] = {}
        for urn, status in amstatus.items():
            if "powderwireless" not in urn or "emulab" in urn:
                continue
            if status.get("rawPCsAvailable") != "2" or status.get("rawPCsTotal") != "2":
                continue
            nodes = radioinfo.get(urn, {})
            if all(is_txrx_b210(nodes.get(node_name, {})) for node_name in ("nuc1", "nuc2")):
                cluster = self._cluster_of(urn)
                campus_pairs[cluster] = B210Pair(
                    label=f"{cluster}:nuc1+nuc2",
                    gnb_urn=self._node_urn(urn, "nuc1"),
                    ue_urn=self._node_urn(urn, "nuc2"),
                )

        for name in PREFERRED_CLUSTERS:
            if name in campus_pairs:
                pairs.append(campus_pairs.pop(name))
        for name in sorted(campus_pairs):
            pairs.append(campus_pairs[name])

        return pairs

    def _txrx_capable_frontends(
        self,
        node: dict,
        lo_mhz: float,
        hi_mhz: float,
        radio_type: str | None = None,
    ) -> list[str]:
        if not node.get("available"):
            return []
        if radio_type is not None and node.get("radio_type") != radio_type:
            return []

        capable = []
        for fe_name, frontend in (node.get("frontends") or {}).items():
            tx_ok = self._covers_band(
                frontend.get("transmit_frequencies", ""), lo_mhz, hi_mhz
            )
            rx_ok = self._covers_band(
                frontend.get("receive_frequencies", ""), lo_mhz, hi_mhz
            )
            if tx_ok and rx_ok:
                capable.append(fe_name)
        return capable

    def get_free_b210_pair(self) -> B210Pair | None:
        """
        Return the first concrete B210 gNB/UE pair that is currently available
        and TX/RX-capable across the requested band, or None if nothing is safe
        to schedule.

        Prefers Indoor OTA emulab.net ota-nuc* pairs, then campus nuc1/nuc2
        pairs in PREFERRED_CLUSTERS order.
        """
        snapshot = self._resource_snapshot()
        if snapshot is None:
            return None
        amstatus, radioinfo = snapshot
        pairs = self._schedulable_b210_pairs(amstatus, radioinfo)
        if not pairs:
            print("[INFO] No free two-radio TX/RX B210 pairs found")
            return None
        pair = pairs[0]
        print(f"[OK] Free B210 pair: {pair.label}")
        return pair

    def get_free_b210_pairs(self) -> list[B210Pair]:
        snapshot = self._resource_snapshot()
        if snapshot is None:
            return []
        amstatus, radioinfo = snapshot
        pairs = self._schedulable_b210_pairs(amstatus, radioinfo)
        if not pairs:
            print("[INFO] No free two-radio TX/RX B210 pairs found")
        else:
            print("[OK] Free B210 pairs: " + ", ".join(pair.label for pair in pairs))
        return pairs

    def get_free_b210_cluster(self) -> str | None:
        """Legacy wrapper; prefer get_free_b210_pair()."""
        pair = self.get_free_b210_pair()
        if pair is None:
            return None
        return pair.label.split(":", 1)[0]

    def audit_radios(self, lo_mhz: float = 3550.0, hi_mhz: float = 3700.0) -> int:
        """
        Read-only audit: dump every fixed-endpoint radio's TX/RX frequency
        coverage and availability to stdout (TSV), plus a summary on stderr
        of which clusters have at least one bidirectional-capable node on
        [lo_mhz, hi_mhz].  No experiment is created.

        Use to find replacements for humanities (whose nuc1 is RX-only) when
        the auto-scheduler reports no free 2x TX/RX B210 cluster.
        """
        snapshot = self._resource_snapshot()
        if snapshot is None:
            return 1
        amstatus, radioinfo = snapshot

        header = ["cluster", "node", "radio_type", "available",
                  "tx_freqs_mhz", "rx_freqs_mhz",
                  "rawPCsAvail", "rawPCsTotal"]
        print("\t".join(header))

        for urn in sorted(radioinfo.keys()):
            cluster = self._cluster_of(urn)
            am = amstatus.get(urn, {})
            raw_avail = am.get("rawPCsAvailable", "")
            raw_total = am.get("rawPCsTotal", "")
            for node_name, node in (radioinfo[urn] or {}).items():
                radio_type = node.get("radio_type", "")
                available = node.get("available")
                frontends = node.get("frontends") or {}
                if not frontends:
                    print("\t".join(map(str, [cluster, node_name, radio_type,
                                              available, "", "", raw_avail, raw_total])))
                    continue
                for fe_name, fe in frontends.items():
                    print("\t".join(map(str, [
                        cluster, f"{node_name}/{fe_name}", radio_type, available,
                        fe.get("transmit_frequencies", "") or "",
                        fe.get("receive_frequencies", "") or "",
                        raw_avail, raw_total,
                    ])))

        print(f"\n# --- Bidirectional {lo_mhz:g}-{hi_mhz:g} MHz capable nodes ---",
              file=sys.stderr)
        capable: dict[str, list[str]] = {}
        schedulable = self._schedulable_b210_pairs(
            amstatus, radioinfo, lo_mhz=lo_mhz, hi_mhz=hi_mhz
        )
        for urn, nodes in radioinfo.items():
            cluster = self._cluster_of(urn)
            for node_name, node in (nodes or {}).items():
                radio_type = node.get("radio_type", "") or ""
                if "B210" not in radio_type and "X310" not in radio_type:
                    continue
                frontends = self._txrx_capable_frontends(node, lo_mhz, hi_mhz)
                if frontends:
                    for fe_name in frontends:
                        capable.setdefault(cluster, []).append(
                            f"{node_name}/{fe_name} ({radio_type})"
                        )
        if not capable:
            print("# (none)", file=sys.stderr)
        else:
            for cluster, nodes in sorted(capable.items()):
                print(f"# {cluster}: {', '.join(nodes)}", file=sys.stderr)

        print(f"\n# --- Auto-schedulable B210 pairs ---", file=sys.stderr)
        if not schedulable:
            print("# (none)", file=sys.stderr)
        else:
            for pair in schedulable:
                print(f"# {pair.label}: gNB={pair.gnb_urn}, UE={pair.ue_urn}",
                      file=sys.stderr)

        return 0

    def _compile_rspec(self, cluster: str, profile_uuid: str) -> str | None:
        """
        Compile the geni-lib profile script with the given cluster's B210 URNs
        baked in as default parameter values.  Returns the RSpec XML string, or
        None on failure.

        This is a debug/validation helper. instantiate/Submit does not reliably
        honor ad hoc xmlcode, so production scheduling passes portal profile
        parameters instead of trying to override the stored profile RSpec.
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

        cm_urn = f"urn:publicid:IDN+{cluster}.powderwireless.net+authority+cm"
        required = [gnb_urn, ue_urn, cm_urn]
        missing = [item for item in required if item not in rspec]
        if missing:
            print(
                "[ERROR] Compiled RSpec is missing fixed-endpoint binding(s): "
                + ", ".join(missing),
                file=sys.stderr,
            )
            print(
                "[ERROR] Refusing to instantiate; otherwise POWDER may map "
                "radio roles to ordinary Emulab compute nodes.",
                file=sys.stderr,
            )
            return None

        print(f"[OK] Compiled RSpec for cluster '{cluster}' ({len(rspec)} bytes)")
        return rspec

    def _pair_profile_version(self, pair: B210Pair, profile_uuid: str) -> str | None:
        """
        Create a profile version whose default B210 node parameters match pair.

        POWDER's instantiate/Submit path has not reliably honored ad hoc
        gnb_b210_id/ue_b210_id form overrides for this profile, so bake the
        selected pair into a fresh profile version before Submit.
        """
        cache_key = f"{profile_uuid}:{pair.label}:{pair.gnb_urn}:{pair.ue_urn}"
        if cache_key in self._pair_profile_cache:
            return self._pair_profile_cache[cache_key]

        script_path = os.path.join(os.path.dirname(__file__), "profile_real_b210_2node.py")
        try:
            with open(script_path) as f:
                script = f.read()
        except OSError as e:
            print(f"[ERROR] Cannot read profile script: {e}", file=sys.stderr)
            return None

        script = re.sub(
            r'(defineParameter\(\s*\n?\s*"gnb_b210_id".*?STRING,\s*)"[^"]*"',
            r'\1"' + pair.gnb_urn + '"',
            script,
            flags=re.DOTALL,
        )
        script = re.sub(
            r'(defineParameter\(\s*\n?\s*"ue_b210_id".*?STRING,\s*)"[^"]*"',
            r'\1"' + pair.ue_urn + '"',
            script,
            flags=re.DOTALL,
        )

        checked = self._ajax(
            "manage_profile", "CheckScript", {"uuid": profile_uuid, "script": script}
        )
        if checked.get("code") != 0 or not isinstance(checked.get("value"), dict):
            print(f"[ERROR] CheckScript failed: {checked.get('value')}", file=sys.stderr)
            return None
        rspec = checked["value"].get("rspec", "")
        if not rspec:
            print("[ERROR] CheckScript returned empty rspec", file=sys.stderr)
            return None
        if "rf0" in rspec:
            print("[ERROR] Refusing RSpec with synthetic rf0 spectrum interface",
                  file=sys.stderr)
            return None

        page = self._opener.open(
            f"{PORTAL_BASE}/manage_profile.php?action=edit&uuid={profile_uuid}",
            timeout=30,
        ).read().decode("utf-8", errors="ignore")
        match = re.search(
            r"<script[^>]+id=['\"]form-json['\"][^>]*>(.*?)</script>",
            page,
            re.DOTALL,
        )
        if not match:
            print("[ERROR] Could not load profile edit form-json", file=sys.stderr)
            return None
        formfields = json.loads(unescape(match.group(1).strip()))
        formfields.update({
            "action": "edit",
            "uuid": profile_uuid,
            "profile_script": script,
            "profile_rspec": rspec,
            "portal_converted": "no",
            "profile_pid": formfields.get("profile_pid", PROJECT),
            "profile_name": formfields.get("profile_name", "dt-real-b210"),
            "profile_who": formfields.get("profile_who", "private"),
            "profile_listed": formfields.get("profile_listed", ""),
            "profile_disabled": formfields.get("profile_disabled", ""),
            "profile_nodelete": formfields.get("profile_nodelete", ""),
            "profile_project_write": formfields.get("profile_project_write", ""),
            "profile_topdog": formfields.get("profile_topdog", ""),
            "examples_portals": formfields.get("examples_portals", ""),
        })

        checked_form = self._ajax_form(
            "manage_profile", "Create", formfields, checkonly=True
        )
        if checked_form.get("code") != 0:
            print(f"[ERROR] Profile form check failed: {checked_form.get('value')}",
                  file=sys.stderr)
            return None

        submitted = self._ajax_form(
            "manage_profile", "Create", formfields, checkonly=False
        )
        if submitted.get("code") != 0:
            print(f"[ERROR] Profile version create failed: {submitted.get('value')}",
                  file=sys.stderr)
            return None
        value = str(submitted.get("value", ""))
        match = re.search(r"uuid=([0-9a-f-]+)", value)
        if not match:
            print(f"[ERROR] Could not parse new profile UUID from: {value}",
                  file=sys.stderr)
            return None
        new_uuid = match.group(1)
        self._pair_profile_cache[cache_key] = new_uuid
        print(f"[OK] Created pair-specific profile version: {new_uuid}")
        return new_uuid

    def start_experiment(
        self,
        pair: B210Pair | str,
        profile_uuid: str,
        dry_run: bool = False,
    ) -> bool:
        """
        Instantiate profile_real_b210 using a concrete B210 pair.

        pair         -- B210Pair, or a legacy cluster string for nuc1/nuc2
        profile_uuid -- UUID from the POWDER portal profile page
        """
        if isinstance(pair, str):
            label = f"{pair}:nuc1+nuc2"
            gnb_urn = f"urn:publicid:IDN+{pair}.powderwireless.net+node+nuc1"
            ue_urn  = f"urn:publicid:IDN+{pair}.powderwireless.net+node+nuc2"
        else:
            label = pair.label
            gnb_urn = pair.gnb_urn
            ue_urn = pair.ue_urn

        if dry_run:
            print("[DRY-RUN] Would start experiment:")
            print(f"  pair     : {label}")
            print(f"  profile  : {profile_uuid}")
            print(f"  project  : {PROJECT}")
            print(f"  name     : {EXP_NAME}")
            print(f"  gnb_urn  : {gnb_urn}")
            print(f"  ue_urn   : {ue_urn}")
            return True

        if isinstance(pair, B210Pair):
            pair_profile_uuid = self._pair_profile_version(pair, profile_uuid)
            if pair_profile_uuid is None:
                return False
            profile_uuid = pair_profile_uuid

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
            # These are profile parameters. The portal binds them into the
            # stored geni-lib profile during instantiate/Submit. Passing a
            # precompiled xmlcode field is ignored by Submit and can leave the
            # radio roles mapped to ordinary d430 compute nodes.
            "gnb_b210_id": gnb_urn,
            "ue_b210_id": ue_urn,
            "freq_low_mhz": "3550.0",
            "freq_high_mhz": "3700.0",
            "max_power_dbm": "30.0",
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

        pairs = portal.get_free_b210_pairs()
        for pair in pairs:
            ok = portal.start_experiment(pair, PROFILE_UUID, dry_run=dry_run)
            if ok:
                print("[auto_schedule] Done.")
                return
            print(f"[WARN] Experiment start failed for {pair.label} — trying next pair")

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
    ap.add_argument("--audit", action="store_true",
                    help="Dump TX/RX frequency coverage of every fixed-endpoint "
                         "radio; no experiment created. Use to find replacements "
                         "for clusters whose nuc1 is RX-only.")
    ap.add_argument("--audit-band", default="3550-3700",
                    help="Band MHz used by --audit summary (default: 3550-3700)")
    args = ap.parse_args()

    if not args.password:
        args.password = input("POWDER password: ")

    portal = PowderPortal(args.uid, args.password)
    if not portal.login():
        sys.exit(1)

    if args.audit:
        try:
            lo_str, hi_str = args.audit_band.split("-")
            lo_mhz, hi_mhz = float(lo_str), float(hi_str)
        except ValueError:
            print(f"[ERROR] --audit-band must be 'LO-HI' MHz, got {args.audit_band!r}",
                  file=sys.stderr)
            sys.exit(2)
        sys.exit(portal.audit_radios(lo_mhz=lo_mhz, hi_mhz=hi_mhz))

    if PROFILE_UUID in KNOWN_BAD_PROFILE_UUIDS:
        print(
            f"[ERROR] Refusing profile {PROFILE_UUID}: "
            f"{KNOWN_BAD_PROFILE_UUIDS[PROFILE_UUID]}. "
            "Unset POWDER_PROFILE_UUID or set it to the registered "
            "profile_real_b210_2node UUID.",
            file=sys.stderr,
        )
        sys.exit(2)

    pairs = portal.get_free_b210_pairs()
    if args.once or args.dry_run:
        if pairs:
            ok = False
            for pair in pairs:
                ok = portal.start_experiment(pair, PROFILE_UUID or "PROFILE_UUID_HERE",
                                             dry_run=args.dry_run)
                if ok or args.dry_run:
                    break
            if not ok and not args.dry_run:
                print("No B210 pair could be instantiated right now.")
        else:
            print("No free B210 pair available right now.")
        return

    poll_and_start(args.uid, args.password, args.poll_interval, args.dry_run)


if __name__ == "__main__":
    main()
