# Phase A Smoke Test — Real Side

Run these commands manually on POWDER nodes after the experiment is ready.
Get hostnames first: `python orchestrator/powder_client.py manifest.xml`

---

## 0. SSH Config (~/.ssh/config on Mac)

```
Host powder-gnb
    HostName <gnb-node hostname from manifest>
    User zifanzhang
    IdentityFile ~/.ssh/id_ed25519

Host powder-ue
    HostName <ue-node hostname from manifest>
    User zifanzhang
    IdentityFile ~/.ssh/id_ed25519

Host powder-cn
    HostName <cn-node hostname from manifest>
    User zifanzhang
    IdentityFile ~/.ssh/id_ed25519
```

---

## 1. CN Node — Start Core Network

The profile startup already:
- Cloned `~/Tiny_Twin`
- Enabled `net.ipv4.ip_forward=1`
- Added iptables MASQUERADE for ctrl-lan → Docker traffic

```bash
ssh powder-cn
cd ~/Tiny_Twin/sims/oai-cn
sudo docker compose up -d
sudo docker ps   # verify: mysql, oai-amf, oai-smf, oai-upf, oai-ext-dn all running
```

No override file needed here — the real CN node creates its own Docker network
(`oai-cn5g-public-net`) with no conflict.

---

## 2. Routing Setup

The gNB and UE nodes need a route to reach AMF (192.168.70.132) and UPF
inside Docker on the CN node. Run on **both** powder-gnb and powder-ue:

```bash
# On powder-gnb:
ssh powder-gnb
sudo ip route add 192.168.70.128/26 via 192.168.1.3
# Verify:
ping -c1 192.168.70.132 && echo "AMF reachable"

# On powder-ue:
ssh powder-ue
sudo ip route add 192.168.70.128/26 via 192.168.1.3
```

---

## 3. gNB Node — Start gNB (paired workbench, Band 78, 106 PRB)

```bash
ssh powder-gnb
cd ~/Tiny_Twin/cmake_targets/ran_build/build

# Verify X310 is visible and note its address
uhd_find_devices
# Expected output includes: addr=192.168.40.2 (or similar)
# If the address differs from 192.168.40.2, update --usrp-args below.

# Start gNB — uses powder_real.conf (ctrl-lan IPs, not Docker IPs)
sudo ./nr-softmodem \
  -O ../../../targets/PROJECTS/GENERIC-NR-5GC/CONF/gnb.sa.band78.fr1.106PRB.powder_real.conf \
  --sa \
  --usrp-args "type=x300,addr=192.168.40.2"
```

Expected output: `[NR_RRC] Waiting for UE...`

---

## 4. UE Node — Start nrUE

```bash
ssh powder-ue
cd ~/Tiny_Twin/cmake_targets/ran_build/build

sudo ./nr-uesoftmodem \
  --uicc0.imsi 001010000000001 \
  -C 3619200000 -r 106 --numerology 1 --ssb 516 \
  --sa \
  -O ../../../ci-scripts/conf_files/nrue.uicc.conf \
  --usrp-args "type=x300,addr=192.168.40.2"
```

Expected: UE gets IP `10.0.0.2`, visible in `ip addr show oaitun_ue1`

---

## 5. Verify Connectivity

```bash
# On UE node
ping 192.168.70.135   # oai-ext-dn IP

# iperf3 DL (server on ext-dn, client on UE)
# CN node:
sudo docker exec -it oai-ext-dn iperf3 -s

# UE node:
iperf3 -c 192.168.70.135 -t 30 -i 1
```

---

## 6. Verify EdgeRIC Logs (gNB node)

```bash
# While gNB + UE are running, check log files:
tail -f ~/Tiny_Twin/logs/snr.txt    # UL SNR per UE per TTI
tail -f ~/Tiny_Twin/logs/rsrp.txt   # RSRP
tail -f ~/Tiny_Twin/logs/gnb-mimo.txt  # DL throughput
```

Phase A passes when:
- [ ] UE registered (IMSI 001010000000001, IP 10.0.0.2)
- [ ] iperf3 DL > 50 Mbps
- [ ] `snr.txt` has continuous output (EdgeRIC logging active)

---

## 7. Collect Logs to Mac

```bash
# On Mac (run after Phase A passes)
scp powder-gnb:~/Tiny_Twin/logs/snr.txt logs/real_snr.txt
scp powder-gnb:~/Tiny_Twin/logs/rsrp.txt logs/real_rsrp.txt
scp powder-gnb:~/Tiny_Twin/logs/gnb-mimo.txt logs/real_tpt.txt
```

These files feed into `channel_converter.py` for W4 (offline CIR replay).
