# Phase C — Offline CIR Replay Smoke Test

**前提**：Phase A（真实侧）和 Phase B（孪生侧静态信道）均已验证通过。  
**目标**：用从真实实验收集的 `snr.txt` 生成 CIR 文件，注入孪生，验证 E1 保真度基线。

---

## Step C-0：从 Phase A 日志生成 CIR 文件

```bash
# 在 Mac 上运行（确保 snr.txt 已 scp 回本地）
cd ~/Research/dt_sync

# 单 UE（RNTI 0x4401）
conda run -n dt_sync python orchestrator/offline_replay.py \
    --snr-log logs/real_snr.txt \
    --out-dir logs/cir_offline \
    --rnti 0x4401

# 检查输出文件
ls -lh logs/cir_offline/
# 期望：cir_ue4401_real.txt  cir_ue4401_imag.txt  各约几十KB
wc -l logs/cir_offline/cir_ue4401_real.txt
# 行数 ≈ TTI 总数（每行一个 tap 向量）
```

---

## Step C-1：上传 CIR 文件到 twin 节点

```bash
TWIN=powder-twin    # 替换为实际节点 hostname

scp logs/cir_offline/cir_ue4401_real.txt \
    logs/cir_offline/cir_ue4401_imag.txt \
    $TWIN:/tmp/cir_ue4401/

ssh $TWIN "ls -lh /tmp/cir_ue4401/"
```

---

## Step C-2：用 CIR 文件启动孪生

在 `twin 节点` 上运行：

```bash
TWIN=powder-twin
ssh $TWIN bash <<'EOF'
cd /opt/dt_sync     # 或 Tiny_Twin compose 目录

# 停止当前静态信道孪生
docker compose -f sims/docker-compose.twin.yaml down

# 用 CIR 文件启动（Mode C：offline replay）
TT_CHANNEL_FILE_REAL=/tmp/cir_ue4401/cir_ue4401_real.txt \
TT_CHANNEL_FILE_IMAG=/tmp/cir_ue4401/cir_ue4401_imag.txt \
docker compose -f sims/docker-compose.twin.yaml up -d

docker compose logs -f tt-gnb | head -30
EOF
```

---

## Step C-3：同步启动真实侧 iperf3（或复用之前的 traffic replay）

```bash
REAL=powder-gnb
ssh $REAL "iperf3 -c 10.0.0.2 -b 20M -t 120 &"
```

孪生侧同时跑流量：

```bash
ssh $TWIN "docker exec tt-nrue iperf3 -c 192.168.70.140 -b 20M -t 120 &"
```

---

## Step C-4：采集 E1 数据

在 Mac 上运行（实时从真实侧和孪生侧同时拉指标，写 CSV）：

```bash
conda run -n dt_sync python orchestrator/e4_fidelity_decay.py \
    --real-host powder-gnb \
    --twin-host powder-twin \
    --duration 120 \
    --interval 5 \
    --out logs/e1_cir_replay_fidelity.csv
```

---

## Step C-5：验收标准

```bash
python -c "
import csv
rows = list(csv.DictReader(open('logs/e1_cir_replay_fidelity.csv')))
cqi_rmse_vals = [float(r['cqi_rmse']) for r in rows if r['cqi_rmse'] != 'nan']
print(f'Samples: {len(rows)}')
print(f'CQI RMSE mean: {sum(cqi_rmse_vals)/len(cqi_rmse_vals):.3f}')
print(f'CQI RMSE min:  {min(cqi_rmse_vals):.3f}')
"
```

**通过标准**：
- `n_ue > 0`（至少一个 UE RNTI 在两侧都匹配）
- CQI RMSE 平均值 < 2.0（physics-based baseline；LSTM 预期 < 1.0）
- 数据点 ≥ 20（120s / 5s interval = 24 点）

---

## Step C-6：（可选）LSTM CIR 对比

如已训练 LSTM 模型：

```bash
# 用 LSTM 生成 CIR 文件
conda run -n dt_sync python ml/compare.py \
    --snr-log logs/real_snr.txt \
    --cir-dir logs/cir_offline \
    --checkpoint checkpoints/cir_lstm.pt \
    --out logs/e1_comparison.pdf
```

---

## 完成后 → Phase C2（named pipe 在线同步）

Phase C 完成表明离线 CIR 回放工作正常。  
下一步：`powder/phase_d_commands.md` → 闭环实验。
