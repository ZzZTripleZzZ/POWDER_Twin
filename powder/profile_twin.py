# -*- coding: utf-8 -*-
"""
POWDER Profile - Twin Side
===========================
Topology: 1 d740 compute node (high CPU, no SDR)
          Runs tt-gnb + tt-nrue via Docker (RFsim, no USRP needed)

Upload this file at powderwireless.net -> Experiments -> Create Profile.
"""

import geni.portal as portal
import geni.rspec.pg as pg
import geni.rspec.emulab as emulab

TINY_TWIN_REPO = "https://github.com/ZzZTripleZzZ/POWDER_Twin.git"

pc = portal.Context()

pc.defineParameter(
    "num_compute_nodes",
    "Number of d740 compute nodes",
    portal.ParameterType.INTEGER,
    1,
    [1, 2],
)

pc.defineParameter(
    "oai_branch",
    "POWDER_Twin git branch",
    portal.ParameterType.STRING,
    "main",
)

params = pc.bindParameters()

request = pc.makeRequestRSpec()

# Startup script for each compute node
STARTUP_TMPL = """#!/bin/bash
set -e
export DEBIAN_FRONTEND=noninteractive

# Install Docker
curl -fsSL https://get.docker.com | sh
sudo usermod -aG docker $USER
sudo systemctl enable docker
sudo systemctl start docker

# Clone POWDER_Twin
git clone --branch {branch} {repo} ~/Tiny_Twin

# Build Docker images in parallel (takes ~20 min)
cd ~/Tiny_Twin
docker build --target tt-gnb \\
    --file docker/tinytwin/Dockerfile.TTgNB.ubuntu22 \\
    -t tt-gnb:v2 . &
docker build --target tt-nrue \\
    --file docker/tinytwin/Dockerfile.TTnrUE.ubuntu22 \\
    -t tt-nrue:v2 . &
wait

# Create shared Docker network for gNB + UE + CN
docker network create \\
    --driver bridge \\
    --subnet 192.168.70.128/26 \\
    --opt com.docker.network.bridge.name=tt-public-net \\
    tt-public-net 2>/dev/null || true

mkdir -p ~/Tiny_Twin/logs_gnb ~/Tiny_Twin/logs_ue ~/Tiny_Twin/logs

echo "Twin node {idx} ready."
"""

for i in range(params.num_compute_nodes):
    node = request.RawPC("twin-node-" + str(i))
    node.hardware_type = "d740"
    node.disk_image = "urn:publicid:IDN+emulab.net+image+emulab-ops:UBUNTU22-64-STD"
    startup = STARTUP_TMPL.format(
        branch=params.oai_branch,
        repo=TINY_TWIN_REPO,
        idx=i,
    )
    node.addService(pg.Execute(shell="bash", command=startup))

pc.printRequestRSpec(request)
