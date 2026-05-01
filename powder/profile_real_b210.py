# -*- coding: utf-8 -*-
"""
POWDER Profile - Real Side with B210 Fixed Endpoints
=====================================================
Topology:
  gnb-node  (d430) <-> gnb-radio (B210 Fixed Endpoint)
  ue-node   (d430) <-> ue-radio  (B210 Fixed Endpoint)
  cn-node   (d430, Docker CN)

HOW TO USE:
  In the POWDER portal, the gnb/ue radio parameters show a node-picker map.
  Click two GREEN (available) nodes in the SAME sector (same rooftop/building).
  gNB and UE radios must be physically adjacent for the RF link to work.
  Rule of thumb: pick nodes whose IDs share the same sector prefix
  (e.g., ffe-s1-comp21 + ffe-s1-comp22 are both in sector s1).
"""

import geni.portal as portal
import geni.rspec.pg as pg
import geni.rspec.emulab as emulab
import geni.rspec.emulab.powder as powder

TINY_TWIN_REPO = "https://github.com/ZzZTripleZzZ/POWDER_Twin.git"

pc = portal.Context()

pc.defineParameter(
    "oai_branch",
    "POWDER_Twin git branch",
    portal.ParameterType.STRING,
    "main",
)

pc.defineParameter(
    "gnb_b210_id",
    "gNB B210 Fixed Endpoint URN",
    portal.ParameterType.STRING,
    "",
    longDescription=(
        "Full URN of the gNB B210 fixed endpoint node, e.g.: "
        "urn:publicid:IDN+humanities.powderwireless.net+node+nuc1 . "
        "Check the POWDER status page for available clusters. "
        "Both gNB and UE must be from the SAME cluster (same prefix before +node+)."
    ),
)

pc.defineParameter(
    "ue_b210_id",
    "UE B210 Fixed Endpoint URN (same cluster as gNB)",
    portal.ParameterType.STRING,
    "",
    longDescription=(
        "Full URN of the UE B210 fixed endpoint node, e.g.: "
        "urn:publicid:IDN+humanities.powderwireless.net+node+nuc2 . "
        "Must be from the SAME cluster as the gNB URN above (nuc2 of the same cluster)."
    ),
)

params = pc.bindParameters()

if params.gnb_b210_id and params.ue_b210_id and params.gnb_b210_id == params.ue_b210_id:
    pc.reportError(portal.ParameterError(
        "gNB and UE must be different B210 nodes.", ["gnb_b210_id", "ue_b210_id"]
    ))

pc.verifyParameters()

request = pc.makeRequestRSpec()

# ── Spectrum / frequency request ─────────────────────────────────────────────
# Required for OTA (Over The Air) permission.
# 5G NR band n78 / CBRS: 3550–3700 MHz.
# OAI default center: 3619.2 MHz, 100 MHz bandwidth.
request.requestSpectrum(3550, 3700, 0)

GNB_STARTUP = """#!/bin/bash
set -e
export DEBIAN_FRONTEND=noninteractive
curl -fsSL https://get.docker.com | sh
sudo systemctl enable docker
sudo systemctl start docker
sudo apt-get install -y uhd-host
sudo uhd_images_downloader
git clone --branch {branch} {repo} ~/Tiny_Twin
cd ~/Tiny_Twin
sudo docker build --target tt-gnb --file docker/tinytwin/Dockerfile.TTgNB.ubuntu22 -t tt-gnb:v2 .
sudo docker network create --driver bridge --subnet 192.168.70.128/26 --opt com.docker.network.bridge.name=tt-public-net tt-public-net 2>/dev/null || true
mkdir -p ~/Tiny_Twin/logs
echo "gNB node ready."
""".format(branch=params.oai_branch, repo=TINY_TWIN_REPO)

UE_STARTUP = """#!/bin/bash
set -e
export DEBIAN_FRONTEND=noninteractive
sudo apt-get install -y uhd-host
sudo uhd_images_downloader
git clone --branch {branch} {repo} ~/Tiny_Twin
cd ~/Tiny_Twin
sudo docker build --target tt-nrue --file docker/tinytwin/Dockerfile.TTnrUE.ubuntu22 -t tt-nrue:v2 .
mkdir -p ~/Tiny_Twin/logs
echo "UE node ready."
""".format(branch=params.oai_branch, repo=TINY_TWIN_REPO)

CN_STARTUP = """#!/bin/bash
set -e
export DEBIAN_FRONTEND=noninteractive
curl -fsSL https://get.docker.com | sh
sudo systemctl enable docker
sudo systemctl start docker
git clone --branch {branch} {repo} ~/Tiny_Twin
sudo sysctl -w net.ipv4.ip_forward=1
echo 'net.ipv4.ip_forward=1' | sudo tee -a /etc/sysctl.conf
sudo iptables -t nat -A POSTROUTING -s 192.168.1.0/24 ! -d 192.168.1.0/24 -j MASQUERADE
echo "CN node ready."
""".format(branch=params.oai_branch, repo=TINY_TWIN_REPO)

# --- compute nodes (no hardware_type: Emulab picks any available node) ---
gnb_node = request.RawPC("gnb-node")
gnb_node.disk_image = "urn:publicid:IDN+emulab.net+image+emulab-ops:UBUNTU22-64-STD"

ue_node = request.RawPC("ue-node")
ue_node.disk_image = "urn:publicid:IDN+emulab.net+image+emulab-ops:UBUNTU22-64-STD"

cn_node = request.RawPC("cn-node")
cn_node.disk_image = "urn:publicid:IDN+emulab.net+image+emulab-ops:UBUNTU22-64-STD"

# --- B210 radio fixed endpoint nodes ---
gnb_radio = request.RawPC("gnb-radio")
gnb_radio.component_id = params.gnb_b210_id

ue_radio = request.RawPC("ue-radio")
ue_radio.component_id = params.ue_b210_id

# --- compute <-> radio links ---
gnb_rf_link = request.Link("gnb-rf-link")
gnb_rf_link.addInterface(gnb_node.addInterface("gnb-rf-if"))
gnb_rf_link.addInterface(gnb_radio.addInterface("gnb-rf-if"))
gnb_rf_link.bandwidth = 10000000

ue_rf_link = request.Link("ue-rf-link")
ue_rf_link.addInterface(ue_node.addInterface("ue-rf-if"))
ue_rf_link.addInterface(ue_radio.addInterface("ue-rf-if"))
ue_rf_link.bandwidth = 10000000

# --- control LAN ---
lan = request.LAN("ctrl-lan")
for node, ip in [(gnb_node, "192.168.1.1"),
                 (ue_node,  "192.168.1.2"),
                 (cn_node,  "192.168.1.3")]:
    iface = node.addInterface("ctrl-iface")
    iface.addAddress(pg.IPv4Address(ip, "255.255.255.0"))
    lan.addInterface(iface)

# --- startup ---
gnb_node.addService(pg.Execute(shell="bash", command=GNB_STARTUP))
ue_node.addService(pg.Execute(shell="bash", command=UE_STARTUP))
cn_node.addService(pg.Execute(shell="bash", command=CN_STARTUP))

pc.printRequestRSpec(request)
