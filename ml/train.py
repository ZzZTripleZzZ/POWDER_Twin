"""
W6 — LSTM CIR Estimator Training Script.

Training data format (collected during W4 Phase C):
  logs/real_snr.txt       → metrics (CQI, SNR, UL_TPT, UL_MCS) per UE per TTI
  logs/cir_offline/cir_<rnti>_real.txt  → ground-truth CIR real taps
  logs/cir_offline/cir_<rnti>_imag.txt  → ground-truth CIR imag taps

Usage:
    python ml/train.py \
        --snr-log logs/real_snr.txt \
        --cir-dir logs/cir_offline \
        --rnti 0x4401 \
        --out checkpoints/cir_lstm.pt
"""
import argparse
import sys
from pathlib import Path

import numpy as np
import torch
import torch.nn as nn
from torch.utils.data import DataLoader, random_split

sys.path.insert(0, str(Path(__file__).parent.parent))
from ml.cir_lstm import CirLSTM, CirDataset, LSTMConfig, normalize_metrics
from orchestrator.channel_converter import parse_snr_log


def build_dataset(
    snr_log: Path,
    cir_dir: Path,
    rnti: int,
    window: int = 16,
) -> CirDataset:
    # Load metrics from real snr log
    all_records = parse_snr_log(snr_log)
    if rnti not in all_records:
        print(f"RNTI 0x{rnti:04x} not in log. Available: {[hex(r) for r in all_records]}")
        sys.exit(1)

    records = all_records[rnti]
    ttis = [r.tti for r in records]
    metrics = np.array([[r.cqi, 0.0, r.ul_tpt, r.ul_mcs] for r in records], dtype=np.float32)
    # SNR column will be filled from physics model until real CIR is collected
    from orchestrator.channel_converter import cqi_to_snr_db
    metrics[:, 1] = [cqi_to_snr_db(int(r.cqi)) for r in records]

    # Load CIR ground truth
    real_file = cir_dir / f"cir_ue{rnti:04x}_real.txt"
    imag_file = cir_dir / f"cir_ue{rnti:04x}_imag.txt"

    cir_real = np.loadtxt(real_file, dtype=np.float32)  # [T, N]
    cir_imag = np.loadtxt(imag_file, dtype=np.float32)

    min_len = min(len(metrics), len(cir_real))
    metrics  = metrics[:min_len]
    cir_real = cir_real[:min_len]
    cir_imag = cir_imag[:min_len]

    metrics_norm, _, _ = normalize_metrics(metrics)
    return CirDataset(metrics_norm, cir_real, cir_imag, window=window)


def train(
    snr_log: Path,
    cir_dir: Path,
    rnti: int,
    out_path: Path,
    epochs: int = 50,
    batch_size: int = 256,
    lr: float = 1e-3,
    window: int = 16,
    val_ratio: float = 0.1,
    device_str: str = "cpu",
):
    device = torch.device(device_str)
    dataset = build_dataset(snr_log, cir_dir, rnti, window)
    print(f"Dataset: {len(dataset)} samples, window={window}")

    val_size = max(1, int(len(dataset) * val_ratio))
    train_size = len(dataset) - val_size
    train_ds, val_ds = random_split(dataset, [train_size, val_size])

    train_loader = DataLoader(train_ds, batch_size=batch_size, shuffle=True)
    val_loader   = DataLoader(val_ds,   batch_size=batch_size)

    cfg = LSTMConfig(window=window)
    model = CirLSTM(cfg).to(device)
    optimizer = torch.optim.Adam(model.parameters(), lr=lr)
    loss_fn = nn.MSELoss()

    best_val_loss = float("inf")
    out_path.parent.mkdir(parents=True, exist_ok=True)

    for epoch in range(1, epochs + 1):
        model.train()
        train_loss = 0.0
        for x, y in train_loader:
            x, y = x.to(device), y.to(device)
            pred = model(x)
            loss = loss_fn(pred, y)
            optimizer.zero_grad()
            loss.backward()
            optimizer.step()
            train_loss += loss.item() * len(x)
        train_loss /= train_size

        model.eval()
        val_loss = 0.0
        with torch.no_grad():
            for x, y in val_loader:
                x, y = x.to(device), y.to(device)
                val_loss += loss_fn(model(x), y).item() * len(x)
        val_loss /= val_size

        if epoch % 10 == 0 or epoch == 1:
            print(f"Epoch {epoch:3d}/{epochs}  train={train_loss:.4f}  val={val_loss:.4f}")

        if val_loss < best_val_loss:
            best_val_loss = val_loss
            torch.save({"model": model.state_dict(), "cfg": cfg}, out_path)

    print(f"\nBest val loss: {best_val_loss:.4f} → saved to {out_path}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--snr-log",  default="logs/real_snr.txt")
    parser.add_argument("--cir-dir",  default="logs/cir_offline")
    parser.add_argument("--rnti",     default=None, help="RNTI in hex")
    parser.add_argument("--out",      default="checkpoints/cir_lstm.pt")
    parser.add_argument("--epochs",   type=int, default=50)
    parser.add_argument("--batch",    type=int, default=256)
    parser.add_argument("--lr",       type=float, default=1e-3)
    parser.add_argument("--window",   type=int, default=16)
    parser.add_argument("--device",   default="cpu")
    args = parser.parse_args()

    snr_log = Path(args.snr_log)
    cir_dir = Path(args.cir_dir)

    # Auto-detect first RNTI if not given
    if args.rnti is None:
        from orchestrator.channel_converter import parse_snr_log as _p
        data = _p(snr_log)
        rnti = next(iter(data))
        print(f"Auto-selected RNTI: 0x{rnti:04x}")
    else:
        rnti = int(args.rnti, 16)

    train(snr_log, cir_dir, rnti, Path(args.out),
          epochs=args.epochs, batch_size=args.batch,
          lr=args.lr, window=args.window, device_str=args.device)


if __name__ == "__main__":
    main()
