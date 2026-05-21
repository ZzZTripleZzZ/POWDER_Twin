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

import re

import geni.portal as portal
import geni.rspec.pg as pg
import geni.rspec.emulab as emulab
import geni.rspec.emulab.spectrum as spectrum

TINY_TWIN_REPO = "https://github.com/ZzZTripleZzZ/POWDER_Twin.git"

pc = portal.Context()

pc.defineParameter(
    "oai_branch",
    "POWDER_Twin git branch",
    portal.ParameterType.STRING,
    "main",
)

pc.defineParameter(
    "freq_low_mhz",
    "Requested RF lower bound (MHz)",
    portal.ParameterType.STRING,
    "3550.0",
    longDescription=(
        "Lower edge of the allowed transmit band in MHz. Must match your "
        "POWDER/FCC authorization before any OTA transmission."
    ),
)

pc.defineParameter(
    "freq_high_mhz",
    "Requested RF upper bound (MHz)",
    portal.ParameterType.STRING,
    "3700.0",
    longDescription=(
        "Upper edge of the allowed transmit band in MHz. For the current "
        "OAI n78 config centered at 3619.2 MHz with 100 MHz bandwidth, the "
        "occupied band is roughly 3569.2-3669.2 MHz; 3550-3700 MHz gives guard."
    ),
)

pc.defineParameter(
    "max_power_dbm",
    "Requested max TX power (dBm)",
    portal.ParameterType.STRING,
    "30.0",
    longDescription=(
        "Maximum allowed transmit power in dBm for POWDER RF monitoring. "
        "Set this to the value granted by your permission/reservation."
    ),
)

pc.defineParameter(
    "gnb_b210_id",
    "gNB B210 Fixed Endpoint URN",
    portal.ParameterType.STRING,
    "urn:publicid:IDN+humanities.powderwireless.net+node+nuc1",
    longDescription=(
        "Full URN of a TX/RX-capable B210 endpoint for the gNB, e.g.: "
        "urn:publicid:IDN+emulab.net+node+ota-nuc2 . "
        "Do not use campus nuc1 endpoints marked receive-only; the gNB must transmit."
    ),
)

pc.defineParameter(
    "ue_b210_id",
    "UE B210 Fixed Endpoint URN (same cluster as gNB)",
    portal.ParameterType.STRING,
    "urn:publicid:IDN+humanities.powderwireless.net+node+nuc2",
    longDescription=(
        "Full URN of a second TX/RX-capable B210 endpoint for the UE, e.g.: "
        "urn:publicid:IDN+emulab.net+node+ota-nuc3 . "
        "The UE must also transmit uplink, so RX-only endpoints are not sufficient."
    ),
)

params = pc.bindParameters()

def fixed_endpoint_cm_urn(node_urn):
    m = re.match(r"^urn:publicid:IDN\+([^+]+)\+node\+[^+]+$", node_urn or "")
    if not m:
        return None
    return "urn:publicid:IDN+{}+authority+cm".format(m.group(1))

gnb_radio_cm = fixed_endpoint_cm_urn(params.gnb_b210_id)
ue_radio_cm = fixed_endpoint_cm_urn(params.ue_b210_id)

if not params.gnb_b210_id or not gnb_radio_cm:
    pc.reportError(portal.ParameterError(
        "gnb_b210_id must be a full fixed-endpoint node URN, e.g. "
        "urn:publicid:IDN+humanities.powderwireless.net+node+nuc1.",
        ["gnb_b210_id"],
    ))

if not params.ue_b210_id or not ue_radio_cm:
    pc.reportError(portal.ParameterError(
        "ue_b210_id must be a full fixed-endpoint node URN, e.g. "
        "urn:publicid:IDN+humanities.powderwireless.net+node+nuc2.",
        ["ue_b210_id"],
    ))

if params.gnb_b210_id and params.ue_b210_id and params.gnb_b210_id == params.ue_b210_id:
    pc.reportError(portal.ParameterError(
        "gNB and UE must be different B210 nodes.", ["gnb_b210_id", "ue_b210_id"]
    ))

if gnb_radio_cm and ue_radio_cm and gnb_radio_cm != ue_radio_cm:
    pc.reportError(portal.ParameterError(
        "gNB and UE B210 fixed endpoints must be in the same POWDER aggregate.",
        ["gnb_b210_id", "ue_b210_id"],
    ))

try:
    freq_low_mhz = float(params.freq_low_mhz)
    freq_high_mhz = float(params.freq_high_mhz)
    max_power_dbm = float(params.max_power_dbm)
except ValueError:
    pc.reportError(portal.ParameterError(
        "freq_low_mhz, freq_high_mhz, and max_power_dbm must be numeric.",
        ["freq_low_mhz", "freq_high_mhz", "max_power_dbm"],
    ))
    freq_low_mhz = 0.0
    freq_high_mhz = 0.0
    max_power_dbm = 0.0

if freq_low_mhz >= freq_high_mhz:
    pc.reportError(portal.ParameterError(
        "freq_low_mhz must be less than freq_high_mhz.",
        ["freq_low_mhz", "freq_high_mhz"],
    ))

pc.verifyParameters()

request = pc.makeRequestRSpec()

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
curl -fsSL https://get.docker.com | sh
sudo systemctl enable docker
sudo systemctl start docker
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
gnb_radio.component_manager_id = gnb_radio_cm

ue_radio = request.RawPC("ue-radio")
ue_radio.component_id = params.ue_b210_id
ue_radio.component_manager_id = ue_radio_cm

# --- compute <-> radio links ---
# GENI requires explicit IP/MASK on every interface when a link crosses
# aggregate boundaries.  The fixed-endpoint B210 nodes live in POWDER campus
# clusters while the compute RawPCs may be allocated from a different aggregate,
# so keep these as small, isolated /30 point-to-point subnets.
gnb_rf_link = request.Link("gnb-rf-link")
gnb_compute_if = gnb_node.addInterface("gnb-rf-if")
gnb_compute_if.addAddress(pg.IPv4Address("192.168.40.1", "255.255.255.252"))
gnb_radio_if = gnb_radio.addInterface("gnb-rf-if")
gnb_radio_if.addAddress(pg.IPv4Address("192.168.40.2", "255.255.255.252"))
# Request spectrum on the actual radio interface, not globally.  Some POWDER
# fixed endpoints are RX-only, and global spectrum declarations are checked
# against every radio-like node in the experiment.
gnb_radio_if.requestSpectrum(freq_low_mhz, freq_high_mhz, max_power_dbm)
gnb_rf_link.addInterface(gnb_compute_if)
gnb_rf_link.addInterface(gnb_radio_if)
gnb_rf_link.bandwidth = 10000000

ue_rf_link = request.Link("ue-rf-link")
ue_compute_if = ue_node.addInterface("ue-rf-if")
ue_compute_if.addAddress(pg.IPv4Address("192.168.41.1", "255.255.255.252"))
ue_radio_if = ue_radio.addInterface("ue-rf-if")
ue_radio_if.addAddress(pg.IPv4Address("192.168.41.2", "255.255.255.252"))
ue_radio_if.requestSpectrum(freq_low_mhz, freq_high_mhz, max_power_dbm)
ue_rf_link.addInterface(ue_compute_if)
ue_rf_link.addInterface(ue_radio_if)
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
