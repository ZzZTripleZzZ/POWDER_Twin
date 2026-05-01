"""
W8 — Behavior Cloning Pretraining (Contribution 3, Step 1).

Extracts (state, action) pairs from Phase A real-side logs and trains
a policy network to imitate the default PF scheduler behavior.
This gives a warm-start policy for online RL fine-tuning.

"Action" in the default scheduler: equal weights (1/N_UE each) for all UEs.
We treat the observed MCS as the implicit action for the MCS head.

Usage:
    python ml/bc_pretrain.py \
        --snr-log logs/real_snr.txt \
        --out checkpoints/bc_policy.pt \
        [--n-ue 1]
"""
import argparse
import sys
from pathlib import Path

import numpy as np
import torch
import torch.nn as nn
from torch.utils.data import TensorDataset, DataLoader, random_split

sys.path.insert(0, str(Path(__file__).parent.parent))
from orchestrator.channel_converter import parse_snr_log, cqi_to_snr_db


# ── Policy network (shared with RL) ──────────────────────────────────────────

class PolicyNet(nn.Module):
    """
    MLP policy: obs → action (scheduling weights).
    Same architecture used for both BC and RL (actor head of PPO).
    """
    def __init__(self, obs_dim: int, n_ue: int, hidden: int = 128):
        super().__init__()
        self.net = nn.Sequential(
            nn.Linear(obs_dim, hidden), nn.ReLU(),
            nn.Linear(hidden, hidden),  nn.ReLU(),
            nn.Linear(hidden, n_ue),    nn.Softmax(dim=-1),
        )

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        return self.net(x)


# ── Dataset builder ───────────────────────────────────────────────────────────

def build_bc_dataset(snr_log: Path, n_ue: int = 1):
    """
    Build (obs, action) tensors from real logs.
    obs:    [CQI_norm, SNR_norm, UL_TPT_norm, UL_MCS_norm, BLER_proxy] × N_UE
    action: equal weight vector (1/N_UE for each UE) — imitating default PF
    """
    data = parse_snr_log(snr_log)
    if not data:
        raise ValueError(f"No records in {snr_log}")

    rntis = sorted(data.keys())[:n_ue]
    min_ttis = min(len(data[r]) for r in rntis)

    obs_list, act_list = [], []
    for t in range(min_ttis):
        obs_row = []
        for rnti in rntis:
            rec = data[rnti][t]
            snr  = cqi_to_snr_db(int(rec.cqi))
            bler = 1.0 if rec.ul_tpt == 0 else 0.0
            obs_row.extend([
                rec.cqi / 15.0,
                np.clip(snr / 30.0, 0, 1),
                np.clip(rec.ul_tpt / 1e6, 0, 1),
                rec.ul_mcs / 28.0,
                bler,
            ])
        obs_list.append(obs_row)
        act_list.append([1.0 / n_ue] * n_ue)   # default: equal weights

    obs = torch.tensor(obs_list, dtype=torch.float32)
    act = torch.tensor(act_list, dtype=torch.float32)
    return obs, act


# ── Training ──────────────────────────────────────────────────────────────────

def train_bc(
    snr_log: Path,
    out_path: Path,
    n_ue: int = 1,
    epochs: int = 30,
    batch_size: int = 256,
    lr: float = 1e-3,
    val_ratio: float = 0.1,
    device_str: str = "cpu",
):
    device = torch.device(device_str)
    obs, act = build_bc_dataset(snr_log, n_ue)
    print(f"BC dataset: {len(obs)} samples, obs_dim={obs.shape[1]}, n_ue={n_ue}")

    dataset = TensorDataset(obs, act)
    val_n   = max(1, int(len(dataset) * val_ratio))
    trn_n   = len(dataset) - val_n
    trn_ds, val_ds = random_split(dataset, [trn_n, val_n])
    trn_dl  = DataLoader(trn_ds, batch_size=batch_size, shuffle=True)
    val_dl  = DataLoader(val_ds, batch_size=batch_size)

    model   = PolicyNet(obs.shape[1], n_ue).to(device)
    opt     = torch.optim.Adam(model.parameters(), lr=lr)
    loss_fn = nn.MSELoss()

    best_val = float("inf")
    out_path.parent.mkdir(parents=True, exist_ok=True)

    for ep in range(1, epochs + 1):
        model.train()
        trn_loss = sum(
            loss_fn(model(x.to(device)), y.to(device)).item() * len(x)
            for x, y in trn_dl
        ) / trn_n

        model.eval()
        with torch.no_grad():
            val_loss = sum(
                loss_fn(model(x.to(device)), y.to(device)).item() * len(x)
                for x, y in val_dl
            ) / val_n

        if ep % 10 == 0 or ep == 1:
            print(f"Epoch {ep:3d}/{epochs}  train={trn_loss:.5f}  val={val_loss:.5f}")

        if val_loss < best_val:
            best_val = val_loss
            torch.save({"model": model.state_dict(), "obs_dim": obs.shape[1], "n_ue": n_ue},
                       out_path)

    print(f"\nBC pretraining done. Best val={best_val:.5f} → {out_path}")
    return model


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--snr-log", default="logs/real_snr.txt")
    parser.add_argument("--out",     default="checkpoints/bc_policy.pt")
    parser.add_argument("--n-ue",    type=int, default=1)
    parser.add_argument("--epochs",  type=int, default=30)
    parser.add_argument("--device",  default="cpu")
    args = parser.parse_args()
    train_bc(Path(args.snr_log), Path(args.out),
             n_ue=args.n_ue, epochs=args.epochs, device_str=args.device)


if __name__ == "__main__":
    main()
