# -*- coding: utf-8 -*-
"""
POWDER Profile - Real Side
===========================
Topology: 1 gNB node (d430 + B210 via USB)
          1 UE node  (d430 + B210 via USB)
          1 CN node  (d430, runs oai-cn via Docker)

The B210 USRP is USB-attached to the d430 compute node in POWDER's
indoor paired-workbench testbed. No separate USRP RawPC is needed.
Upload this file at powderwireless.net -> Experiments -> Create Profile.
"""

import geni.portal as portal
import geni.rspec.pg as pg
import geni.rspec.emulab as emulab

TINY_TWIN_REPO = "https://github.com/ZzZTripleZzZ/POWDER_Twin.git"

pc = portal.Context()

pc.defineParameter(
    "oai_branch",
    "POWDER_Twin git branch",
    portal.ParameterType.STRING,
    "main",
)

params = pc.bindParameters()

request = pc.makeRequestRSpec()

GNB_STARTUP = """#!/bin/bash
set -e
export DEBIAN_FRONTEND=noninteractive
curl -fsSL https://get.docker.com | sh
sudo usermod -aG docker $USER
sudo systemctl enable docker
sudo systemctl start docker
sudo apt-get install -y uhd-host
sudo uhd_images_downloader
git clone --branch {branch} {repo} ~/Tiny_Twin
cd ~/Tiny_Twin/cmake_targets
./build_oai -C
./build_oai -I --gNB
mkdir -p ~/Tiny_Twin/logs
echo "gNB node ready."
"""

UE_STARTUP = """#!/bin/bash
set -e
export DEBIAN_FRONTEND=noninteractive
sudo apt-get install -y uhd-host
sudo uhd_images_downloader
git clone --branch {branch} {repo} ~/Tiny_Twin
cd ~/Tiny_Twin/cmake_targets
./build_oai -C
./build_oai -I --nrUE
mkdir -p ~/Tiny_Twin/logs
echo "UE node ready."
"""

CN_STARTUP = """#!/bin/bash
set -e
export DEBIAN_FRONTEND=noninteractive
curl -fsSL https://get.docker.com | sh
sudo usermod -aG docker $USER
sudo systemctl enable docker
sudo systemctl start docker
git clone --branch {branch} {repo} ~/Tiny_Twin
sudo sysctl -w net.ipv4.ip_forward=1
echo 'net.ipv4.ip_forward=1' | sudo tee -a /etc/sysctl.conf
sudo iptables -t nat -A POSTROUTING -s 192.168.1.0/24 ! -d 192.168.1.0/24 -j MASQUERADE
echo "CN node ready."
"""

# --- nodes ---
gnb_node = request.RawPC("gnb-node")
gnb_node.hardware_type = "d430"
gnb_node.disk_image = "urn:publicid:IDN+emulab.net+image+emulab-ops:UBUNTU22-64-STD"

ue_node = request.RawPC("ue-node")
ue_node.hardware_type = "d430"
ue_node.disk_image = "urn:publicid:IDN+emulab.net+image+emulab-ops:UBUNTU22-64-STD"

cn_node = request.RawPC("cn-node")
cn_node.hardware_type = "d430"
cn_node.disk_image = "urn:publicid:IDN+emulab.net+image+emulab-ops:UBUNTU22-64-STD"

# --- control LAN ---
lan = request.LAN("ctrl-lan")
for node, ip in [(gnb_node, "192.168.1.1"),
                 (ue_node,  "192.168.1.2"),
                 (cn_node,  "192.168.1.3")]:
    iface = node.addInterface("ctrl-iface")
    iface.addAddress(pg.IPv4Address(ip, "255.255.255.0"))
    lan.addInterface(iface)

# --- startup ---
gnb_node.addService(pg.Execute(shell="bash",
    command=GNB_STARTUP.format(branch=params.oai_branch, repo=TINY_TWIN_REPO)))
ue_node.addService(pg.Execute(shell="bash",
    command=UE_STARTUP.format(branch=params.oai_branch, repo=TINY_TWIN_REPO)))
cn_node.addService(pg.Execute(shell="bash",
    command=CN_STARTUP.format(branch=params.oai_branch, repo=TINY_TWIN_REPO)))

pc.printRequestRSpec(request)
