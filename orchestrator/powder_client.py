"""
POWDER Portal client: parse manifest XML to extract node hostnames.
First version: manual experiment creation via web UI, this script only
reads the downloaded manifest.xml and returns SSH-accessible hostnames.
"""
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from pathlib import Path


@dataclass
class Node:
    name: str
    hostname: str
    ip: str


def parse_manifest(manifest_path: str) -> list[Node]:
    tree = ET.parse(manifest_path)
    root = tree.getroot()
    ns = {
        "rspec": "http://www.geni.net/resources/rspec/3",
        "emulab": "http://www.emulab.net/resources/rspec/1",
    }
    nodes = []
    for node in root.findall("rspec:node", ns):
        name = node.get("client_id", "")
        host_el = node.find("emulab:host", ns)
        hostname = host_el.get("name", "") if host_el is not None else ""
        iface = node.find("rspec:interface", ns)
        ip_el = iface.find("rspec:ip", ns) if iface is not None else None
        ip = ip_el.get("address", "") if ip_el is not None else ""
        nodes.append(Node(name=name, hostname=hostname, ip=ip))
    return nodes


if __name__ == "__main__":
    import sys
    path = sys.argv[1] if len(sys.argv) > 1 else "manifest.xml"
    for n in parse_manifest(path):
        print(f"{n.name:20s}  {n.hostname:50s}  {n.ip}")
