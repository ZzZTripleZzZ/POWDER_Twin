# Twin-Twin End-to-End Test (No OTA Required)

**Goal**: Validate M1–M4 system with two compute-only POWDER nodes (no radio hardware, no OTA permission).

```
powder-real (node 0) = fake "real" gNB — RFsim + EdgeRIC, static channel
powder-twin (node 1) = actual twin   — CIR injected from state_sync FIFO
Mac orchestrator     = state_sync + all eval scripts
```

Covers: E1 (drift), E3 (overhead), E5 (MAC equivalence), E6 (KPI similarity).
Does NOT cover: E2 oracle ground-truth ranking, E4 sustained safety run (these need OTA).

---

## Step 0: Start the POWDER experiment

1. Go to [powderwireless.net](https://www.powderwireless.net) → **Experiments** → **Create Experiment**
2. Select profile: **profile_twin** (upload `powder/profile_twin.py` if not already there)
3. Set **num_compute_nodes = 2**
4. Give the experiment a name, e.g., `dt-twin-twin`
5. Click **Instantiate** → wait ~25 min for Docker images to build on both nodes

Check startup progress:
```bash
# After SSH is up on each node (usually 5-10 min after instantiation)
ssh twin-node-0.dt-twin-twin.nicelabexp.emulab.net 'tail -20 /var/log/startup.log'
ssh twin-node-1.dt-twin-twin.nicelabexp.emulab.net 'tail -20 /var/log/startup.log'
# Wait until: "Twin node 0 ready." / "Twin node 1 ready."
```

---

## Step 1: Update SSH config

Add to `~/.ssh/config`:
```
Host powder-real
    HostName twin-node-0.dt-twin-twin.nicelabexp.emulab.net
    User zifan716
    IdentityFile ~/.ssh/id_ed25519
    ServerAliveInterval 60

Host powder-twin
    HostName twin-node-1.dt-twin-twin.nicelabexp.emulab.net
    User zifan716
    IdentityFile ~/.ssh/id_ed25519
    ServerAliveInterval 60
```

Replace `dt-twin-twin.nicelabexp.emulab.net` with the actual POWDER experiment hostname
(shown in the portal under Experiment → List Nodes).

Verify:
```bash
ssh powder-real hostname
ssh powder-twin hostname
```

---

## Step 2: Quick sanity check (~8 min)

```bash
cd ~/Research/dt_sync
conda activate dt_sync

python orchestrator/twin_twin_eval.py --quick
```

Expected output:
```
=== Phase 0: Connectivity ===
  [OK] real (powder-real) → twin-node-0
  [OK] twin (powder-twin) → twin-node-1

=== Phase 1: Fake-real stack on powder-real ===
[Real] CN healthy.
[Real] Fake-real stack up.

=== Phase 2: state_sync on powder-twin ===
[Twin] FIFOs ready: /tmp/tt_cir_real.fifo

=== Phase 3: Twin gNB on powder-twin ===
[Twin] Twin stack up.

=== Phase 4: Waiting for metrics from both nodes ===
  [OK] real (powder-real): 128 bytes
  [OK] twin (powder-twin): 128 bytes

[E1] ... drift_log has N rows
[E3] ... timing table printed
[E5] ... p99(Δ) TTI  bound=103 TTI
[E6] ... mean similarity=XX.X%

==========================================
Twin-Twin Eval — Final Report
[PASS] E1 Drift log rows: N
[PASS] E3 Overhead sweep: OK
[PASS] E5 p99(Δ) TTI: ...
[PASS] E6 KPI similarity %: ...
4/4 checks passed.
```

---

## Step 3: Full run (~45 min)

```bash
python orchestrator/twin_twin_eval.py
```

Full durations: E1 warmup=30s, E3=120s, E5=120s, E6=120s.
Results in `logs/` as CSVs.

---

## Step 4: Manual experiment trigger (if you already have both stacks up)

If stacks are already running (e.g. after `--setup-only`), run experiments individually:

```bash
# E1: check drift log on powder-twin
ssh powder-twin 'wc -l /tmp/drift_e1.csv && tail -5 /tmp/drift_e1.csv'
scp powder-twin:/tmp/drift_e1.csv logs/e1_drift.csv

# E3: overhead sweep
python orchestrator/m3_overhead_eval.py \
    --twin-host powder-twin --n-samples 300 --m-trials 3 \
    --out logs/e3_overhead.csv

# E5: MAC equivalence (connects to MacReconciler on powder-twin:5560)
python orchestrator/mac_equivalence_twin_client.py \
    --real-host powder-twin --k-reconcile 100 \
    --duration 120 --out logs/e5_reconcile.csv

# E6: KPI similarity
python orchestrator/e6_kpi_similarity.py \
    --real-host powder-real --twin-host powder-twin \
    --duration 120 --interval 5 --out logs/e6_kpi_similarity.csv
```

---

## Step 5: Teardown

```bash
python orchestrator/twin_twin_eval.py --teardown
```

Or manually:
```bash
for host in powder-real powder-twin; do
    ssh $host "cd ~/Tiny_Twin/sims && sudo docker compose -f docker-compose.twin.yaml down"
    ssh $host "cd ~/Tiny_Twin/sims/oai-cn && sudo docker compose -f docker-compose.yaml -f oai-cn-tt-override.yaml down"
    ssh $host "pkill -f state_sync.py || true"
done
```

Then terminate the POWDER experiment in the portal to release the nodes.

---

## Troubleshooting

**FIFOs not created**:
```bash
ssh powder-twin 'tail -30 /tmp/state_sync.log'
```
Likely cause: state_sync cannot reach powder-real:5555. Check gNB is running on powder-real:
```bash
ssh powder-real 'sudo docker ps | grep tt-gnb'
ssh powder-real 'sudo docker logs tt-gnb --tail 20'
```

**No metrics from powder-real**:
```bash
ssh powder-real 'sudo docker logs tt-gnb --tail 20'
# Check EdgeRIC is logging: look for "[EdgeRIC] Publishing metrics on :5555"
```

**CN not healthy after 150s**:
```bash
ssh powder-real 'sudo docker ps'
# If mysql is not up yet, give it more time (mysql takes 90-120s on d740)
ssh powder-real 'sudo docker logs mysql --tail 20'
```

**E5 timeout (no reconciler response)**:
layer2_mac_sync on powder-twin binds MacReconciler on :5560. Check it's running:
```bash
ssh powder-twin 'ss -tlnp | grep 5560'
ssh powder-twin 'tail -20 /tmp/state_sync.log'
```
