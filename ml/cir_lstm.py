"""
W6 — LSTM CIR Estimator (Contribution 1, Phase B).

Maps a sliding window of per-UE EdgeRIC metrics
  (CQI_t, SNR_t, UL_TPT_t, UL_MCS_t) × T timesteps
to the next-TTI CIR tap coefficients (real + imag).

Input:  [B, T, 4]  — batch × window × features
Output: [B, 2*N]   — real_taps concatenated with imag_taps (N taps each)

Training data comes from simultaneous real + twin experiments where
we know both the EdgeRIC metrics AND the CIR file being replayed.
"""
import torch
import torch.nn as nn
from dataclasses import dataclass


@dataclass
class LSTMConfig:
    input_dim: int = 4          # CQI, SNR, UL_TPT, UL_MCS
    hidden_dim: int = 64
    num_layers: int = 2
    num_taps: int = 10
    window: int = 16            # history length in TTIs
    dropout: float = 0.1


class CirLSTM(nn.Module):
    """LSTM that estimates CIR tap coefficients from metrics history."""

    def __init__(self, cfg: LSTMConfig = LSTMConfig()):
        super().__init__()
        self.cfg = cfg
        self.lstm = nn.LSTM(
            input_size=cfg.input_dim,
            hidden_size=cfg.hidden_dim,
            num_layers=cfg.num_layers,
            batch_first=True,
            dropout=cfg.dropout if cfg.num_layers > 1 else 0.0,
        )
        self.head = nn.Sequential(
            nn.Linear(cfg.hidden_dim, cfg.hidden_dim),
            nn.ReLU(),
            nn.Linear(cfg.hidden_dim, 2 * cfg.num_taps),  # real + imag taps
        )

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        """x: [B, T, input_dim] → [B, 2*num_taps]"""
        out, _ = self.lstm(x)
        return self.head(out[:, -1, :])   # use last timestep

    def predict_cir(self, x: torch.Tensor) -> tuple[torch.Tensor, torch.Tensor]:
        """Returns (real_taps, imag_taps) each [B, num_taps]."""
        pred = self.forward(x)
        return pred[:, :self.cfg.num_taps], pred[:, self.cfg.num_taps:]


# ── Dataset ───────────────────────────────────────────────────────────────────

class CirDataset(torch.utils.data.Dataset):
    """
    Sliding-window dataset of (metrics_window, cir_target) pairs.

    metrics: np.ndarray [T_total, 4]  — CQI, SNR, UL_TPT, UL_MCS
    cir_real: np.ndarray [T_total, N] — real CIR taps per TTI
    cir_imag: np.ndarray [T_total, N] — imag CIR taps per TTI
    """

    def __init__(self, metrics, cir_real, cir_imag, window: int = 16):
        import numpy as np
        assert len(metrics) == len(cir_real) == len(cir_imag)
        self.metrics  = torch.tensor(metrics,  dtype=torch.float32)
        self.cir_real = torch.tensor(cir_real, dtype=torch.float32)
        self.cir_imag = torch.tensor(cir_imag, dtype=torch.float32)
        self.window   = window
        self.length   = len(metrics) - window

    def __len__(self):
        return max(0, self.length)

    def __getitem__(self, idx):
        x = self.metrics[idx : idx + self.window]           # [W, 4]
        yr = self.cir_real[idx + self.window]               # [N]
        yi = self.cir_imag[idx + self.window]               # [N]
        return x, torch.cat([yr, yi])                       # [W, 4], [2N]


def normalize_metrics(metrics):
    """Z-score normalize per feature. Returns (normalized, mean, std)."""
    import numpy as np
    m = metrics.mean(axis=0, keepdims=True)
    s = metrics.std(axis=0, keepdims=True) + 1e-8
    return (metrics - m) / s, m.squeeze(), s.squeeze()
