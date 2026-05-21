#!/usr/bin/env python3
"""
find_b210.py -- DEPRECATED. List available B210 fixed endpoints on POWDER/Emulab.

DEPRECATED: this script only filters by node status/name and cannot tell whether
a B210 endpoint is RX-only (e.g. humanities nuc1) or full TX/RX, nor whether it
covers a given band. Use the richer audit instead:

    python3 powder/auto_schedule.py --audit
    python3 powder/auto_schedule.py --audit --audit-band 3550-3700

The --audit subcommand reads POWDER's radioinfo JSON and dumps per-frontend
transmit_frequencies / receive_frequencies, which is what you actually need to
find a replacement for humanities (whose nuc1 has empty transmit_frequencies).

Usage (legacy):
    python3 powder/find_b210.py --user zifan716 --password <your_password>

Or set env vars:
    export POWDER_USER=zifan716
    export POWDER_PASS=<your_password>
    python3 powder/find_b210.py

Output: two available B210 node URNs (ready to paste into profile parameters).
Requires: standard library only (xmlrpc.client, ssl).
"""

import os
import sys
import ssl
import xmlrpc.client
import argparse

XMLRPC_URL = "https://www.emulab.net/xmlrpc/"
PROJECT = "NICELabExp"


def get_server(user: str, password: str):
    ctx = ssl.create_default_context()
    transport = xmlrpc.client.SafeTransport(context=ctx)
    server = xmlrpc.client.ServerProxy(XMLRPC_URL, transport=transport)
    return server, {"uid": user, "password": password}


def list_free_b210(user: str, password: str) -> list[str]:
    server, cred = get_server(user, password)
    try:
        # GetNodes returns all nodes; filter by type and status
        result = server.GetNodes(cred, {"type": "FixedEndpoint"})
    except Exception as e:
        print(f"[WARN] GetNodes failed: {e}. Trying ListNodes...", file=sys.stderr)
        try:
            result = server.ListNodes(cred, {})
        except Exception as e2:
            print(f"[ERROR] Both API calls failed: {e2}", file=sys.stderr)
            print("Try logging in to powderwireless.net -> Resources -> Map", file=sys.stderr)
            return []

    if result.get("code", -1) != 0:
        print(f"[ERROR] API error: {result.get('output', result)}", file=sys.stderr)
        return []

    free_b210 = []
    nodes = result.get("value", [])
    for node in nodes:
        node_id = node.get("node_id", "")
        node_type = node.get("type", "")
        status = node.get("status", "")
        # B210 fixed endpoints typically have "b210" or "ffe" in name
        if ("b210" in node_type.lower() or "ffe" in node_id.lower()):
            if status in ("free", "available", "idle"):
                urn = f"urn:publicid:IDN+emulab.net+node+{node_id}"
                free_b210.append(urn)

    return free_b210


def main():
    parser = argparse.ArgumentParser(description="List available POWDER B210 nodes")
    parser.add_argument("--user", default=os.environ.get("POWDER_USER", "zifan716"))
    parser.add_argument("--password", default=os.environ.get("POWDER_PASS", ""))
    args = parser.parse_args()

    if not args.password:
        args.password = input("POWDER password: ")

    print("Querying POWDER for available B210 fixed endpoints...\n")
    available = list_free_b210(args.user, args.password)

    if not available:
        print("No available B210 nodes found (or API unavailable).")
        print("Fallback: go to powderwireless.net -> Resources -> Map")
        print("  Filter: 'Fixed Endpoint B210 (X/R) broadband antenna'")
        print("  Click a green node -> copy its component_id")
        return

    print(f"Found {len(available)} available B210 node(s):\n")
    for urn in available:
        print(f"  {urn}")

    if len(available) >= 2:
        print("\nReady to use in profile_real_b210.py:")
        print(f"  gnb_b210_id = \"{available[0]}\"")
        print(f"  ue_b210_id  = \"{available[1]}\"")
        print("\nOr paste these as parameters when instantiating in portal.")


if __name__ == "__main__":
    main()
