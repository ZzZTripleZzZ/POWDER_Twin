# -*- coding: utf-8 -*-
"""
POWDER Profile - Real2Twin Indoor OTA
======================================

Topology (matches POWDER's official oai-indoor-ota reference pattern):

    gnb-real (d430) <-- 10G fiber radio-link --> gnb-x310 (ota-x310-N)
        |                                           |
        |                                          (ctrl-lan reachability
        |                                           NOT used for X310 - the
        |                                           USRP only speaks UHD on
        |                                           the dedicated radio link)
        |
        +--- ctrl-lan (192.168.1.0/24) ---+
                                          |
        twin (d430) ----------------------+
                                          |
        ue-nuc1..4 (ota-nucN, COTS UE) ---+

Why a dedicated radio-link instead of joining X310 to ctrl-lan:
  POWDER's switch fabric only provisions an X310's 10G fiber when the
  profile declares `request.Link()` between the X310 and a paired compute
  node. Putting the X310 on a shared LAN does not bring up the fiber and
  the ctrl-lan IP for the X310 is unreachable. See
  https://github.com/sayaz/OAI-Indoor-OTA-5G-NR profile_x310_b210.py
  and the official PowderTeam/oai-indoor-ota profile.

Spectrum: 3430-3450 MHz (n78, NICELabExp reservation).

IP scheme:
  ctrl-lan    192.168.1.0/24    - gnb-real / twin / ue-nucs
  radio-link  192.168.40.0/24   - gnb-real (192.168.40.1) <-> X310 (192.168.40.2)
                                   X310 default UHD addr is 192.168.40.2

Upload at powderwireless.net -> My Profiles -> Create Profile, or pull
from https://github.com/ZzZTripleZzZ/POWDER_Twin (powder/profile_real_twin_ota.py).
"""

import base64
import geni.portal as portal
import geni.rspec.pg as pg
import geni.rspec.emulab as emulab
import geni.rspec.emulab.spectrum as spectrum

TINY_TWIN_REPO = "https://github.com/ZzZTripleZzZ/POWDER_Twin.git"
EMULAB_CM      = "urn:publicid:IDN+emulab.net+authority+cm"

# UE NUC nodes only. The X310 is allocated separately and connected to
# gnb-real via a dedicated 10G fiber Link (see X310_RADIO + radio-link below)
# rather than joining ctrl-lan.
NUC_FIXED_NODES = {
    "ue-nuc1": "urn:publicid:IDN+emulab.net+node+ota-nuc1",
    "ue-nuc2": "urn:publicid:IDN+emulab.net+node+ota-nuc2",
    "ue-nuc3": "urn:publicid:IDN+emulab.net+node+ota-nuc3",
    "ue-nuc4": "urn:publicid:IDN+emulab.net+node+ota-nuc4",
}

# ctrl-lan: gnb-real + twin + ue-nucs (NOT the X310).
CTRL_LAN_IPS = {
    "gnb-real": "192.168.1.1",
    "twin":     "192.168.1.2",
    "ue-nuc1":  "192.168.1.21",
    "ue-nuc2":  "192.168.1.22",
    "ue-nuc3":  "192.168.1.23",
    "ue-nuc4":  "192.168.1.24",
}

# X310 reachable from gnb-real on the dedicated radio-link.
# UHD discovery uses the X310's default 10G transport address.
X310_RADIO_LINK_IP_GNB = "192.168.40.1"
X310_RADIO_LINK_IP_X310 = "192.168.40.2"
X310_CANDIDATE_IPS = [X310_RADIO_LINK_IP_X310]

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
pc.defineParameter(
    "x310_id",
    "X310 SDR component (ota-x310-1 dead 2026-05-08; pick -2/-3/-4)",
    portal.ParameterType.STRING, "ota-x310-2",
    [
        ("ota-x310-2", "ota-x310-2"),
        ("ota-x310-3", "ota-x310-3"),
        ("ota-x310-4", "ota-x310-4"),
        ("ota-x310-1", "ota-x310-1 (likely broken)"),
    ],
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


# OAI USRP build (-w USRP) takes ~45-60 min on a d430. POWDER's startup
# command runs *synchronously* before marking the node ready, and several
# clusters timeout the startup at 600 s. We do the fast prerequisites
# (Docker, UHD, repo clone, network) inline, then fork the heavy docker
# build into a nohup'd background job that writes ~/.tt-build-complete
# when it finishes. Orchestrator waits on that sentinel before bringing
# up containers.

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
    "git clone --branch " + params.oai_branch + " " + TINY_TWIN_REPO + " ~/Tiny_Twin || (cd ~/Tiny_Twin && git fetch origin && git checkout " + params.oai_branch + " && git pull)\n"
    "mkdir -p ~/Tiny_Twin/logs\n"
    "\n"
    "sudo docker network create \\\n"
    "    --driver bridge --subnet 192.168.70.128/26 \\\n"
    "    --opt com.docker.network.bridge.name=tt-public-net \\\n"
    "    tt-public-net 2>/dev/null || true\n"
    "\n"
    "# Fork the heavy OAI USRP build to background (~45 min).\n"
    "rm -f ~/.tt-build-complete ~/.tt-build-failed\n"
    "( cd ~/Tiny_Twin && \\\n"
    "  sudo docker build --target tt-gnb \\\n"
    "      --file docker/tinytwin/Dockerfile.TTgNB.ubuntu22 \\\n"
    "      -t tt-gnb:v2 . > /tmp/tt-gnb-build.log 2>&1 \\\n"
    "  && touch ~/.tt-build-complete \\\n"
    "  || touch ~/.tt-build-failed \\\n"
    ") </dev/null >/dev/null 2>&1 &\n"
    "disown\n"
    "\n"
    "echo \"gnb-real node ready (build running in background; tail /tmp/tt-gnb-build.log)\"\n"
)

# Twin builds gnb + nrue *serially* - d430 (64 GB RAM) was OOM-killing the
# linker when both ran in parallel. Total wall time is ~80 min serial vs.
# crash-and-restart parallel.
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
    "git clone --branch " + params.oai_branch + " " + TINY_TWIN_REPO + " ~/Tiny_Twin || (cd ~/Tiny_Twin && git fetch origin && git checkout " + params.oai_branch + " && git pull)\n"
    "mkdir -p ~/Tiny_Twin/logs\n"
    "\n"
    "sudo docker network create \\\n"
    "    --driver bridge --subnet 192.168.70.128/26 \\\n"
    "    --opt com.docker.network.bridge.name=tt-public-net \\\n"
    "    tt-public-net 2>/dev/null || true\n"
    "\n"
    "rm -f ~/.tt-build-complete ~/.tt-build-failed\n"
    "( cd ~/Tiny_Twin && \\\n"
    "  sudo docker build --target tt-gnb \\\n"
    "      --file docker/tinytwin/Dockerfile.TTgNB.ubuntu22 \\\n"
    "      -t tt-gnb:v2 . > /tmp/tt-gnb-build.log 2>&1 && \\\n"
    "  sudo docker build --target tt-nrue \\\n"
    "      --file docker/tinytwin/Dockerfile.TTnrUE.ubuntu22 \\\n"
    "      -t tt-nrue:v2 . > /tmp/tt-nrue-build.log 2>&1 \\\n"
    "  && touch ~/.tt-build-complete \\\n"
    "  || touch ~/.tt-build-failed \\\n"
    ") </dev/null >/dev/null 2>&1 &\n"
    "disown\n"
    "\n"
    "echo \"twin node ready (builds running in background; tail /tmp/tt-{gnb,nrue}-build.log)\"\n"
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

# -- X310 paired with gnb-real via dedicated 10G fiber radio-link -----------
#
# This is the POWDER OAI Indoor OTA pattern. The X310 is NOT joined to
# ctrl-lan; instead a `request.Link()` declares the gnb-real <-> X310 fiber
# so POWDER's switch fabric provisions the multi-Gbps path. Without this
# link declaration the X310 is unreachable regardless of any IP scheme.

gnb_x310 = request.RawPC("gnb-x310")
gnb_x310.component_id = "urn:publicid:IDN+emulab.net+node+" + params.x310_id
gnb_x310.component_manager_id = EMULAB_CM
gnb_x310.requestSpectrum(freq_low, freq_high, max_power)

usrp_if = gnb_real.addInterface("usrp-if")
usrp_if.addAddress(pg.IPv4Address(X310_RADIO_LINK_IP_GNB, "255.255.255.0"))

radio_link = request.Link("radio-link")
radio_link.bandwidth = 10 * 1000 * 1000  # 10 Gbps fiber to OTA lab
radio_link.addInterface(usrp_if)
radio_link.addNode(gnb_x310)

# -- COTS UE NUCs (ota-nuc1..4) ---------------------------------------------

ue_nodes = {}
for name, component_id in NUC_FIXED_NODES.items():
    n = request.RawPC(name)
    n.component_id = component_id
    n.component_manager_id = EMULAB_CM
    n.addService(pg.Execute(shell="bash", command=NUC_STARTUP))
    n.requestSpectrum(freq_low, freq_high, max_power)
    ue_nodes[name] = n

# -- Control LAN: gnb-real + twin + ue-nucs (no X310) -----------------------

ctrl_lan = request.LAN("ctrl-lan")
ctrl_lan_nodes = {"gnb-real": gnb_real, "twin": twin}
ctrl_lan_nodes.update(ue_nodes)

for name, node in ctrl_lan_nodes.items():
    ip = CTRL_LAN_IPS[name]
    iface = node.addInterface("ctrl-if-" + name.replace("-", ""))
    iface.addAddress(pg.IPv4Address(ip, "255.255.255.0"))
    ctrl_lan.addInterface(iface)

pc.printRequestRSpec(request)
