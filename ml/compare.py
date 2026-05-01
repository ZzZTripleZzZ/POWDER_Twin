"""
W6 — E1 Fidelity Comparison: Physics-based vs LSTM CIR Estimator.

Loads real logs + twin logs (both static and CIR-replay runs),
computes per-TTI fidelity metrics, produces the E1 paper figure.

Usage:
    python ml/compare.py \
        --real-snr   logs/real_snr.txt \
        --twin-static-tpt  logs/twin_static_tpt.txt \
        --twin-replay-tpt  logs/twin_replay_tpt.txt \
        --real-tpt   logs/real_tpt.txt \
        [--rnti 0x4401] \
        [--checkpoint checkpoints/cir_lstm.pt]
"""
import argparse
import sys
from pathlib import Path

import numpy as np
import matplotlib.pyplot as plt

sys.path.insert(0, str(Path(__file__).parent.parent))


def parse_tpt_log(path: Path) -> np.ndarray:
    """
    Parse gnb-mimo.txt (DL TPT log).
    Format: one float per line (throughput in Mbps).
    Returns array of throughput values.
    """
    values = []
    with open(path) as f:
        for line in f:
            line = line.strip()
            try:
                values.append(float(line.split()[0]))
            except (ValueError, IndexError):
                pass
    return np.array(values, dtype=np.float32)


def rmse(a: np.ndarray, b: np.ndarray) -> float:
    n = min(len(a), len(b))
    return float(np.sqrt(np.mean((a[:n] - b[:n]) ** 2)))


def mae(a: np.ndarray, b: np.ndarray) -> float:
    n = min(len(a), len(b))
    return float(np.mean(np.abs(a[:n] - b[:n])))


def compare(
    real_tpt: np.ndarray,
    twin_static_tpt: np.ndarray,
    twin_physics_tpt: np.ndarray,
    twin_lstm_tpt: np.ndarray | None,
    out_fig: Path = Path("logs/e1_fidelity.pdf"),
):
    n = min(len(real_tpt), len(twin_static_tpt),
            len(twin_physics_tpt),
            len(twin_lstm_tpt) if twin_lstm_tpt is not None else 10**9)

    t = np.arange(n) * 0.001   # TTI = 1 ms

    fig, axes = plt.subplots(2, 1, figsize=(10, 6), sharex=True)

    # ── Top: throughput time series ──
    ax = axes[0]
    ax.plot(t, real_tpt[:n],         label="Real",          linewidth=1.5)
    ax.plot(t, twin_static_tpt[:n],  label="Twin (static)",  linestyle="--", alpha=0.8)
    ax.plot(t, twin_physics_tpt[:n], label="Twin (physics)", linestyle="-.", alpha=0.8)
    if twin_lstm_tpt is not None:
        ax.plot(t, twin_lstm_tpt[:n], label="Twin (LSTM)", linestyle=":", alpha=0.8)
    ax.set_ylabel("DL Throughput (Mbps)")
    ax.legend(fontsize=9)
    ax.grid(True, alpha=0.3)

    # ── Bottom: absolute error ──
    ax = axes[1]
    ax.plot(t, np.abs(real_tpt[:n] - twin_static_tpt[:n]),
            label=f"Static  RMSE={rmse(real_tpt, twin_static_tpt):.2f}", alpha=0.8)
    ax.plot(t, np.abs(real_tpt[:n] - twin_physics_tpt[:n]),
            label=f"Physics RMSE={rmse(real_tpt, twin_physics_tpt):.2f}", alpha=0.8)
    if twin_lstm_tpt is not None:
        ax.plot(t, np.abs(real_tpt[:n] - twin_lstm_tpt[:n]),
                label=f"LSTM    RMSE={rmse(real_tpt, twin_lstm_tpt):.2f}", alpha=0.8)
    ax.set_xlabel("Time (s)")
    ax.set_ylabel("|Error| (Mbps)")
    ax.legend(fontsize=9)
    ax.grid(True, alpha=0.3)

    fig.suptitle("E1: Twin Fidelity — Real vs Static / Physics / LSTM CIR", fontsize=12)
    plt.tight_layout()
    out_fig.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(out_fig, dpi=150)
    print(f"Saved {out_fig}")

    # Print summary table
    print("\n── E1 Fidelity Summary ─────────────────────────────")
    header = f"{'Method':<16} {'RMSE (Mbps)':>12} {'MAE (Mbps)':>12}"
    print(header)
    print("-" * len(header))
    for name, tpt in [("Static", twin_static_tpt),
                       ("Physics", twin_physics_tpt),
                       ("LSTM", twin_lstm_tpt)]:
        if tpt is None:
            continue
        print(f"{name:<16} {rmse(real_tpt, tpt):>12.3f} {mae(real_tpt, tpt):>12.3f}")


def main():
    parser = argparse.ArgumentParser(description="E1 fidelity comparison")
    parser.add_argument("--real-tpt",         required=True)
    parser.add_argument("--twin-static-tpt",  required=True)
    parser.add_argument("--twin-physics-tpt", required=True)
    parser.add_argument("--twin-lstm-tpt",    default=None)
    parser.add_argument("--out",              default="logs/e1_fidelity.pdf")
    args = parser.parse_args()

    real_tpt          = parse_tpt_log(Path(args.real_tpt))
    twin_static_tpt   = parse_tpt_log(Path(args.twin_static_tpt))
    twin_physics_tpt  = parse_tpt_log(Path(args.twin_physics_tpt))
    twin_lstm_tpt     = (parse_tpt_log(Path(args.twin_lstm_tpt))
                         if args.twin_lstm_tpt else None)

    compare(real_tpt, twin_static_tpt, twin_physics_tpt, twin_lstm_tpt,
            out_fig=Path(args.out))


if __name__ == "__main__":
    main()
