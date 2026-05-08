# -*- coding: utf-8 -*-
"""
POWDER Profile - Real2Twin Indoor OTA
======================================
Topology:
  gnb-real  (d430)         -- OAI gNB (Docker) + OAI CN (Docker)
                              accesses X310 radio over Ethernet via UHD
  twin      (d430)         -- Tiny_Twin gNB + CN (Docker, RFsim + CIR FIFO)
  gnb-x310-2 (ota-x310-2) }
  gnb-x310-3 (ota-x310-3) } X310 SDR pool — orchestrator probes each at run
  gnb-x310-4 (ota-x310-4) } time and selects whichever responds to UHD.
  ue-nuc1~4 (ota-nuc1~4)  -- Intel NUC nodes with Quectel RM500Q COTS 5G UEs

Spectrum: 3430-3450 MHz (n78, matches approved NICELabExp reservation)

ctrl-lan IPs (192.168.1.0/24):
  gnb-real    192.168.1.1
  twin        192.168.1.2
  gnb-x310-2  192.168.1.12
  gnb-x310-3  192.168.1.13
  gnb-x310-4  192.168.1.14
  ue-nuc1     192.168.1.21 ... ue-nuc4 192.168.1.24

Upload at powderwireless.net -> My Profiles -> Create Profile.
Copy the UUID into register_and_instantiate_ota.py.
"""

import base64
import geni.portal as portal
import geni.rspec.pg as pg
import geni.rspec.emulab as emulab
import geni.rspec.emulab.spectrum as spectrum

TINY_TWIN_REPO = "https://github.com/ZzZTripleZzZ/POWDER_Twin.git"
EMULAB_CM      = "urn:publicid:IDN+emulab.net+authority+cm"

# ota-x310-1 was confirmed dead (no UHD/ARP/ICMP response after portal reboot
# on 2026-05-08). Allocating -2/-3/-4 instead so the orchestrator can probe
# each at runtime and pick whichever USRP actually responds.
FIXED_NODES = {
    "gnb-x310-2": "urn:publicid:IDN+emulab.net+node+ota-x310-2",
    "gnb-x310-3": "urn:publicid:IDN+emulab.net+node+ota-x310-3",
    "gnb-x310-4": "urn:publicid:IDN+emulab.net+node+ota-x310-4",
    "ue-nuc1":    "urn:publicid:IDN+emulab.net+node+ota-nuc1",
    "ue-nuc2":    "urn:publicid:IDN+emulab.net+node+ota-nuc2",
    "ue-nuc3":    "urn:publicid:IDN+emulab.net+node+ota-nuc3",
    "ue-nuc4":    "urn:publicid:IDN+emulab.net+node+ota-nuc4",
}

CTRL_LAN_IPS = {
    "gnb-real":   "192.168.1.1",
    "twin":       "192.168.1.2",
    "gnb-x310-2": "192.168.1.12",
    "gnb-x310-3": "192.168.1.13",
    "gnb-x310-4": "192.168.1.14",
    "ue-nuc1":    "192.168.1.21",
    "ue-nuc2":    "192.168.1.22",
    "ue-nuc3":    "192.168.1.23",
    "ue-nuc4":    "192.168.1.24",
}

# Candidates the orchestrator probes via uhd_find_devices; first responder wins.
X310_CANDIDATE_IPS = ["192.168.1.12", "192.168.1.13", "192.168.1.14"]

# -- Portal parameters ------------------------------------------------------

pc = portal.Context()

pc.defineParameter(
    "oai_branch", "POWDER_Twin git branch",
    portal.ParameterType.STRING, "main",
)
pc.defineParameter(
    "freq_low_mhz", "Spectrum low edge (MHz)",
    portal.ParameterType.STRING, "3430.0",
)
pc.defineParameter(
    "freq_high_mhz", "Spectrum high edge (MHz)",
    portal.ParameterType.STRING, "3450.0",
)
pc.defineParameter(
    "max_power_dbm", "Max TX power (dBm)",
    portal.ParameterType.STRING, "30.0",
)

params = pc.bindParameters()

try:
    freq_low  = float(params.freq_low_mhz)
    freq_high = float(params.freq_high_mhz)
    max_power = float(params.max_power_dbm)
except ValueError:
    pc.reportError(portal.ParameterError(
        "freq_low_mhz, freq_high_mhz, max_power_dbm must be numeric.",
        ["freq_low_mhz", "freq_high_mhz", "max_power_dbm"],
    ))
    freq_low  = 0.0
    freq_high = 0.0
    max_power = 0.0

if freq_low >= freq_high:
    pc.reportError(portal.ParameterError(
        "freq_low_mhz must be less than freq_high_mhz.",
        ["freq_low_mhz", "freq_high_mhz"],
    ))

pc.verifyParameters()
request = pc.makeRequestRSpec()

# -- Startup scripts --------------------------------------------------------
#
# POWDER wraps each startup command in:   /bin/bash -c "SCRIPT_CONTENT"
# at the /bin/sh level.  Any double-quote inside SCRIPT_CONTENT terminates
# that outer "..." early, causing a syntax error.  The fix: base64-encode
# the real inner script; the thin outer wrapper (zero double-quotes) decodes
# and executes it.

def _b64_wrap(inner_script):
    b64 = base64.b64encode(inner_script.encode("utf-8")).decode("ascii")
    return "#!/bin/bash\nset -e\necho " + b64 + " | base64 -d > /tmp/_node_setup.sh\nbash /tmp/_node_setup.sh\n"


_GNB_REAL_INNER = (
    "#!/bin/bash\n"
    "set -ex\n"
    "export DEBIAN_FRONTEND=noninteractive\n"
    "\n"
    "curl -fsSL https://get.docker.com | sh\n"
    "sudo usermod -aG docker $USER\n"
    "sudo systemctl enable docker\n"
    "sudo systemctl start docker\n"
    "\n"
    "sudo apt-get update -q\n"
    "sudo apt-get install -y -q uhd-host python3-uhd\n"
    "sudo uhd_images_downloader -t x3xx\n"
    "\n"
    "git clone --branch " + params.oai_branch + " " + TINY_TWIN_REPO + " ~/Tiny_Twin\n"
    "cd ~/Tiny_Twin\n"
    "sudo docker build --target tt-gnb \\\n"
    "    --file docker/tinytwin/Dockerfile.TTgNB.ubuntu22 \\\n"
    "    -t tt-gnb:v2 .\n"
    "\n"
    "sudo docker network create \\\n"
    "    --driver bridge --subnet 192.168.70.128/26 \\\n"
    "    --opt com.docker.network.bridge.name=tt-public-net \\\n"
    "    tt-public-net 2>/dev/null || true\n"
    "\n"
    "# X310 selection deferred to orchestrator: real_twin_eval.py probes\n"
    "# X310_CANDIDATE_IPS at runtime and patches gnb conf with the working IP.\n"
    "\n"
    "mkdir -p ~/Tiny_Twin/logs\n"
    "echo \"gnb-real node ready\"\n"
)

_TWIN_INNER = (
    "#!/bin/bash\n"
    "set -ex\n"
    "export DEBIAN_FRONTEND=noninteractive\n"
    "\n"
    "curl -fsSL https://get.docker.com | sh\n"
    "sudo usermod -aG docker $USER\n"
    "sudo systemctl enable docker\n"
    "sudo systemctl start docker\n"
    "\n"
    "git clone --branch " + params.oai_branch + " " + TINY_TWIN_REPO + " ~/Tiny_Twin\n"
    "cd ~/Tiny_Twin\n"
    "sudo docker build --target tt-gnb \\\n"
    "    --file docker/tinytwin/Dockerfile.TTgNB.ubuntu22 \\\n"
    "    -t tt-gnb:v2 . &\n"
    "sudo docker build --target tt-nrue \\\n"
    "    --file docker/tinytwin/Dockerfile.TTnrUE.ubuntu22 \\\n"
    "    -t tt-nrue:v2 . &\n"
    "wait\n"
    "\n"
    "sudo docker network create \\\n"
    "    --driver bridge --subnet 192.168.70.128/26 \\\n"
    "    --opt com.docker.network.bridge.name=tt-public-net \\\n"
    "    tt-public-net 2>/dev/null || true\n"
    "\n"
    "mkdir -p ~/Tiny_Twin/logs\n"
    "echo \"twin node ready\"\n"
)

_NUC_INNER = (
    "#!/bin/bash\n"
    "set -ex\n"
    "export DEBIAN_FRONTEND=noninteractive\n"
    "sudo apt-get update -q\n"
    "sudo apt-get install -y -q modemmanager mmcli libqmi-utils\n"
    "sudo systemctl enable ModemManager\n"
    "sudo systemctl start ModemManager\n"
    "sudo mmcli -m 0 --disable 2>/dev/null || true\n"
    "echo \"UE NUC node ready\"\n"
)

GNB_REAL_STARTUP = _b64_wrap(_GNB_REAL_INNER)
TWIN_STARTUP     = _b64_wrap(_TWIN_INNER)
NUC_STARTUP      = _b64_wrap(_NUC_INNER)

# -- Compute nodes (generic d430) -------------------------------------------

gnb_real = request.RawPC("gnb-real")
gnb_real.hardware_type = "d430"
gnb_real.disk_image = "urn:publicid:IDN+emulab.net+image+emulab-ops:UBUNTU22-64-STD"
gnb_real.addService(pg.Execute(shell="bash", command=GNB_REAL_STARTUP))

twin = request.RawPC("twin")
twin.hardware_type = "d430"
twin.disk_image = "urn:publicid:IDN+emulab.net+image+emulab-ops:UBUNTU22-64-STD"
twin.addService(pg.Execute(shell="bash", command=TWIN_STARTUP))

# -- Fixed indoor OTA nodes -------------------------------------------------

fixed_nodes = {}
for name, component_id in FIXED_NODES.items():
    n = request.RawPC(name)
    n.component_id = component_id
    n.component_manager_id = EMULAB_CM
    if name.startswith("ue-nuc"):
        n.addService(pg.Execute(shell="bash", command=NUC_STARTUP))
    fixed_nodes[name] = n

# -- Spectrum allocation on all radio nodes ---------------------------------

for name in FIXED_NODES:
    fixed_nodes[name].requestSpectrum(freq_low, freq_high, max_power)

# -- Control LAN ------------------------------------------------------------

ctrl_lan = request.LAN("ctrl-lan")
all_nodes = {"gnb-real": gnb_real, "twin": twin}
all_nodes.update(fixed_nodes)

for name, node in all_nodes.items():
    ip = CTRL_LAN_IPS[name]
    iface = node.addInterface("ctrl-if-" + name.replace("-", ""))
    iface.addAddress(pg.IPv4Address(ip, "255.255.255.0"))
    ctrl_lan.addInterface(iface)

pc.printRequestRSpec(request)
