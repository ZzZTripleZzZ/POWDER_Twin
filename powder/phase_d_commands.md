# Phase D — OTA End-to-End Experiments (M1–M4)

**Prerequisites**: Phase A/B/C passed. POWDER OTA permission approved for 3550–3700 MHz.  
**Goal**: Run all OTA-dependent experiments (E2, E4, E6) and validate the full M1–M4 system.

**Current resource gate (2026-05-02)**: do not instantiate the B210 OTA
profile unless `auto_schedule.py --audit` reports a non-empty
`Auto-schedulable B210 pairs` section. POWDER campus B210 pairs currently show
`nuc1` as RX-only and `nuc2` as TX/RX, so they cannot support a full NR attach
where both gNB and UE transmit. Indoor OTA `emulab.net:ota-nuc*` pairs are the
preferred B210 real-side target when available.

Experiment mapping:
- **E1** (cabled/OTA): drift dynamics — `state_sync.py --drift-log`
- **E2** (OTA): oracle ranking accuracy — `e3_rl_eval.py --collect-ground-truth`
- **E3** (cabled): overhead sweep — `m3_overhead_eval.py` (already runnable without OTA)
- **E4** (OTA): 4hr M2-gated safety run — `e4_safety_run.py`
- **E5** (cabled): MAC equivalence — `mac_equivalence_twin_client.py --mock` (already runnable)
- **E6** (OTA): Samsung gap KPI similarity — `e6_kpi_similarity.py`

---

## Step D-0A: Resource gate before any real-side instantiate

```bash
python3 powder/auto_schedule.py --audit --audit-band 3550-3700 \
    > /tmp/powder_audit.tsv \
    2> /tmp/powder_audit.summary

cat /tmp/powder_audit.summary
```

Proceed with the B210 OTA profile only if the summary contains an actual pair
under `Auto-schedulable B210 pairs`. A summary of `(none)` means the B210 branch
is blocked by radio capability, not just reservations.

Fallback for real-SDR real2twin smoke tests if Indoor OTA is reserved:

```bash
# Portal profile: powder/profile_real_x310_workbench.py
# This is cabled X310, not OTA, but exercises real OAI/USRP behavior.
```

---

## Step D-0: Confirm all nodes online

```bash
REAL=powder-gnb
TWIN=powder-twin

# Real-side EdgeRIC metrics stream
timeout 5 python -c "
import zmq; ctx = zmq.Context()
s = ctx.socket(zmq.SUB)
s.connect('tcp://$REAL:5555')
s.setsockopt(zmq.SUBSCRIBE, b'')
s.setsockopt(zmq.RCVTIMEO, 4000)
raw = s.recv(); print(f'Real EdgeRIC OK: {len(raw)} bytes')
"

# Twin-side EdgeRIC metrics stream
timeout 5 python -c "
import zmq; ctx = zmq.Context()
s = ctx.socket(zmq.SUB)
s.connect('tcp://$TWIN:5555')
s.setsockopt(zmq.SUBSCRIBE, b'')
s.setsockopt(zmq.RCVTIMEO, 4000)
raw = s.recv(); print(f'Twin EdgeRIC OK: {len(raw)} bytes')
"
```

---

## Step D-1: Start dual-layer state sync (background, runs all of Phase D)

```bash
# On Mac — runs layer1 (CIR) + layer2 (MAC) sync + M4 reconciler + E1 drift logging
conda run -n dt_sync python orchestrator/state_sync.py \
    --real-host powder-gnb \
    --twin-host powder-twin \
    --local-fifo \
    --drift-log logs/e1_drift.csv \
    &

SYNC_PID=$!
echo "state_sync PID: $SYNC_PID"
sleep 5
echo "--- Drift log so far ---"
head -5 logs/e1_drift.csv 2>/dev/null || echo "(not yet created)"
```

Verify E1 drift data is flowing after ~30s:
```bash
tail -5 logs/e1_drift.csv
```

---

## Step D-2: Start N twin replicas for M2 oracle

```bash
conda run -n dt_sync python -c "
from orchestrator.policy_arbiter import PolicyArbiter
from pathlib import Path
arbiter = PolicyArbiter(
    twin_host='powder-twin',
    real_host='powder-gnb',
    n_replicas=4,
    base_cir_real=Path('logs/cir_offline/cir_ue4401_real.txt'),
    base_cir_imag=Path('logs/cir_offline/cir_ue4401_imag.txt'),
)
arbiter.start_replicas(sigma=0.15)
print('Replicas started. Waiting 15s...')
import time; time.sleep(15)
print('Ready.')
"
```

---

## Step D-3: Smoke test — single policy eval

```bash
conda run -n dt_sync python -c "
from orchestrator.policy_arbiter import PolicyArbiter, Policy
from pathlib import Path
arbiter = PolicyArbiter(
    twin_host='powder-twin', real_host='powder-gnb', n_replicas=4,
    base_cir_real=Path('logs/cir_offline/cir_ue4401_real.txt'),
    base_cir_imag=Path('logs/cir_offline/cir_ue4401_imag.txt'),
)
safe_policy = Policy(weights={0x4401: 0.5, 0x4402: 0.5})
result = arbiter.evaluate(safe_policy)
print(result.summary())
if result.safe:
    arbiter.deploy(safe_policy)
    print('Policy deployed to real network.')
else:
    print('Policy REJECTED.')
"
```

**Pass criterion**: 3/4 replicas show safe; equal-weight policy deployed.

---

## Step D-4: E2 — Oracle ranking accuracy (~50 min)

```bash
conda run -n dt_sync python orchestrator/e3_rl_eval.py \
    --real-host powder-gnb \
    --twin-host powder-twin \
    --collect-ground-truth \
    --gt-duration 300 \
    --n-replicas 4 \
    --horizon 200 \
    --out logs/e2_oracle_eval.csv
```

**Pass criterion**: top-1 oracle accuracy ≥ 80% (twin correctly identifies the best policy).

---

## Step D-5: E4 — 4-hour M2-gated safety run

```bash
conda run -n dt_sync python orchestrator/e4_safety_run.py \
    --real-host powder-gnb \
    --twin-host powder-twin \
    --duration 14400 \
    --swap-interval 300 \
    --n-replicas 4 \
    --out logs/e4_safety_run.csv
```

**Pass criteria**:
- Zero quarantine events (oracle never blocks a safe policy update)
- CPU usage stays < 50% on twin server (`sar` or `htop` on powder-twin)
- Policy improves or maintains throughput vs initial PF baseline

---

## Step D-6: E6 — Samsung gap KPI similarity (~30 min)

```bash
conda run -n dt_sync python orchestrator/e6_kpi_similarity.py \
    --real-host powder-gnb \
    --twin-host powder-twin \
    --duration 1800 \
    --interval 5 \
    --out logs/e6_kpi_similarity.csv
```

**Pass criterion**: overall KPI similarity > 70% (vs Samsung 2025 baseline of 39%).

---

## Step D-7: Stop replicas and sync process

```bash
# Stop state_sync
kill $SYNC_PID

# Stop N replicas
conda run -n dt_sync python -c "
from orchestrator.policy_arbiter import PolicyArbiter
from pathlib import Path
arbiter = PolicyArbiter('powder-twin', 'powder-gnb', n_replicas=4,
    base_cir_real=Path('logs/cir_offline/cir_ue4401_real.txt'),
    base_cir_imag=Path('logs/cir_offline/cir_ue4401_imag.txt'))
arbiter.stop_replicas()
print('Replicas stopped.')
"
```

---

## Step D-8: Acceptance checks

```bash
python -c "
import csv

# E2: oracle top-1 accuracy >= 80%
rows = list(csv.DictReader(open('logs/e2_oracle_eval.csv')))
correct = sum(1 for r in rows if r.get('top1_correct','0') == '1')
total   = len(rows)
print(f'E2 Oracle accuracy: {correct}/{total} = {correct/total*100:.1f}%')
print('E2 PASS' if correct/total >= 0.8 else 'E2 WARN: below 80% target')

# E4: zero quarantine events
rows4 = list(csv.DictReader(open('logs/e4_safety_run.csv')))
quarantined = sum(1 for r in rows4 if r.get('oracle_approved','1') == '0')
print(f'E4 Quarantine events: {quarantined}')
print('E4 PASS' if quarantined == 0 else f'E4 WARN: {quarantined} blocks')

# E6: similarity > 70%
rows6 = list(csv.DictReader(open('logs/e6_kpi_similarity.csv')))
sim_vals = [float(r['similarity_pct']) for r in rows6 if r['similarity_pct'] != 'nan']
mean_sim = sum(sim_vals) / len(sim_vals)
print(f'E6 KPI similarity: {mean_sim:.1f}%  (Samsung baseline: 39%)')
print('E6 PASS' if mean_sim >= 70 else f'E6 WARN: {mean_sim:.1f}% below 70% target')
"
```

---

## Step D-9: Archive data

```bash
mkdir -p logs/phase_d

# Pull logs from nodes
scp powder-gnb:/tmp/state_sync.log   logs/phase_d/ 2>/dev/null || true
scp powder-twin:/tmp/state_sync.log  logs/phase_d/twin_state_sync.log 2>/dev/null || true

# Verify all required files
ls -lh logs/e1_drift.csv \
        logs/e2_oracle_eval.csv \
        logs/e4_safety_run.csv \
        logs/e6_kpi_similarity.csv
```
