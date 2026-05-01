# Phase B / B2 Smoke Test — Twin Side

Run on the POWDER twin node (powder-twin) after `profile_twin.py` experiment is ready.

---

## 0. SSH into Twin Node

```bash
ssh powder-twin   # alias in ~/.ssh/config
```

---

## Phase B — Static Channel (channel_clean.txt)

### 1. Bring Up Core Network

The profile startup script already created `tt-public-net`.  Use the override
file so oai-cn joins that network instead of trying to create its own
`oai-cn5g-public-net` on the same subnet (would conflict).

```bash
cd ~/Tiny_Twin/sims/oai-cn
sudo docker compose \
    -f docker-compose.yaml \
    -f oai-cn-tt-override.yaml \
    up -d
sudo docker ps   # wait until all healthy: mysql, oai-amf, oai-smf, oai-upf, oai-ext-dn
```

### 2. Verify Docker Network (profile creates it; this is a sanity check only)

```bash
sudo docker network inspect tt-public-net | grep Subnet
# Expected: "Subnet": "192.168.70.128/26"
```

### 3. Start gNB + 1 UE (default channel_clean.txt)

```bash
cd ~/Tiny_Twin/sims
sudo docker compose -f docker-compose.twin.yaml up -d tt-gnb
# Wait ~5s for gNB to be ready, then:
sudo docker compose -f docker-compose.twin.yaml up -d tt-nrue1
```

### 4. Verify UE Connected

```bash
# Check UE got IP
sudo docker exec tt-ue1 ip addr show oaitun_ue1
# Expected: 10.0.0.X assigned

# iperf3 DL
sudo docker exec -it oai-ext-dn iperf3 -s &
sudo docker exec tt-ue1 iperf3 -c 192.168.70.135 -t 20 -i 1
```

### 5. Check EdgeRIC Logs (gNB node)

```bash
sudo docker exec tt-gnb tail -f /tt-ran/logs/snr.txt
sudo docker exec tt-gnb tail -f /tt-ran/logs/gnb-mimo.txt
```

**Phase B passes when:**
- [ ] `tt-ue1` gets IP on `oaitun_ue1`
- [ ] iperf3 DL completes without errors
- [ ] `snr.txt` shows per-TTI SNR values

---

## Phase B2 — Dynamic CIR File (channel_gradual.txt)

This verifies that our W2 modification (`TT_CHANNEL_FILE_REAL` env var) works correctly.
The channel degrades over time → BLER should rise and throughput should drop.

### 1. Stop Previous Containers

```bash
cd ~/Tiny_Twin/sims
sudo docker compose -f docker-compose.twin.yaml down
```

### 2. Start with Gradual Channel

```bash
cd ~/Tiny_Twin/sims
export TT_CHANNEL_FILE_REAL=/tt-ran/channel/channel_gradual.txt
export TT_CHANNEL_FILE_IMAG=/tt-ran/channel/channel_gradual.txt

sudo -E docker compose -f docker-compose.twin.yaml up -d tt-gnb
sleep 5
sudo -E docker compose -f docker-compose.twin.yaml up -d tt-nrue1
```

### 3. Monitor BLER over Time

```bash
# Watch SNR drop as channel degrades
sudo docker exec tt-gnb bash -c "tail -f /tt-ran/logs/snr.txt" | \
    awk '{sum+=$1; n++; if(n%100==0) print "TTI "n": avg_SNR="sum/100; if(n%100==0) sum=0}'

# Watch throughput drop
sudo docker exec tt-gnb tail -f /tt-ran/logs/gnb-mimo.txt
```

**Phase B2 passes when:**
- [ ] At t=0: SNR is high, throughput ~= Phase B baseline
- [ ] At t=30s+: SNR visibly lower, throughput drops (channel_gradual.txt is decaying)
- [ ] This confirms CIR file injection works end-to-end

---

## Collect Logs to Mac

```bash
# From Mac, after Phase B/B2 pass
scp powder-twin:~/Tiny_Twin/logs/snr.txt logs/twin_static_snr.txt
scp powder-twin:~/Tiny_Twin/logs/gnb-mimo.txt logs/twin_static_tpt.txt
```

---

## Teardown

```bash
cd ~/Tiny_Twin/sims
sudo docker compose -f docker-compose.twin.yaml down
cd ~/Tiny_Twin/sims/oai-cn && sudo docker compose \
    -f docker-compose.yaml \
    -f oai-cn-tt-override.yaml \
    down
```
