"""
M3 — Selective-Fidelity Twin

Three-tier fidelity mode selector for the Tiny_Twin PHY layer.
Python side is used by state_sync.py to control FIFO writes and by
OracleReplicaPool to configure per-replica TT_FIDELITY_MODE env vars.
C side (apply_channelmod.c) reads TT_FIDELITY_MODE once at container
startup; mode changes trigger a graceful docker restart.

Modes
─────
  full_iq    – 100-tap CIR full convolution (default)
  sparse_tap – top-K taps by power (K=4 default); others zeroed
  mac_only   – skip CIR writes and convolution; twin uses analytic CQI→MCS

Selector signals are built from live M1 + M2 orchestrator state and
evaluated every K_MODE_CHECK CQI observations (default 50).
"""

from __future__ import annotations

import time
from contextlib import contextmanager
from dataclasses import dataclass
from enum import Enum
from typing import Optional

import numpy as np


class Mode(str, Enum):
    FULL_IQ    = "full_iq"
    SPARSE_TAP = "sparse_tap"
    MAC_ONLY   = "mac_only"


@dataclass
class SelectorSignals:
    D_t: Optional[float]       # M1 KL drift (None if < DRIFT_MIN_SAMPLES)
    cqi_var: float             # rolling CQI variance (last 50 samples)
    regret_width: float        # last oracle mean CI width (ci_upper - ci_lower)
    regret_mean: float         # last oracle mean improvement magnitude
    policies_disagree: float   # (max - min) improvement / abs(mean) across replicas
    recent_recalibration: bool # M1 trigger fired within last 30 s
    rnti_count: int            # number of active UEs


# Signals used when no oracle data available yet (conservative → full_iq)
COLD_START_SIGNALS = SelectorSignals(
    D_t=None,
    cqi_var=999.0,
    regret_width=999.0,
    regret_mean=0.0,
    policies_disagree=999.0,
    recent_recalibration=False,
    rnti_count=1,
)


class ModeSelector:
    def choose_mode(self, sig: SelectorSignals) -> Mode:
        raise NotImplementedError


class RuleBasedSelector(ModeSelector):
    """Deterministic priority-rule selector for cold start (W2–W3).

    Priority 1: hard escalation conditions → full_iq
    Priority 2: stable low-drift conditions → mac_only
    Default: sparse_tap
    """

    def __init__(
        self,
        tau_drift_high: float = 0.20,
        tau_drift_low:  float = 0.02,
        cqi_var_low:    float = 1.0,
        disagree_high:  float = 0.5,
    ):
        self.tau_drift_high = tau_drift_high
        self.tau_drift_low  = tau_drift_low
        self.cqi_var_low    = cqi_var_low
        self.disagree_high  = disagree_high

    def choose_mode(self, sig: SelectorSignals) -> Mode:
        # Priority 1: any uncertainty / instability → full_iq
        if sig.recent_recalibration:
            return Mode.FULL_IQ
        if sig.D_t is not None and sig.D_t > self.tau_drift_high:
            return Mode.FULL_IQ
        if sig.policies_disagree > self.disagree_high:
            return Mode.FULL_IQ
        # High CI width relative to mean improvement → uncertain, escalate
        if sig.regret_mean != 0 and sig.regret_width > 2 * abs(sig.regret_mean):
            return Mode.FULL_IQ

        # Priority 2: stable + low drift → mac_only
        if (sig.D_t is not None
                and sig.D_t < self.tau_drift_low
                and sig.cqi_var < self.cqi_var_low):
            return Mode.MAC_ONLY

        return Mode.SPARSE_TAP


class LearnedSelector(ModeSelector):
    """Small MLP trained offline on (sig_vector, ground_truth_mode) pairs.
    Falls back to RuleBasedSelector if checkpoint not found (W9+ only).
    """

    def __init__(self, ckpt_path: str, fallback: ModeSelector = None):
        self._fallback = fallback or RuleBasedSelector()
        self._ready = False
        try:
            import torch
            self._model = torch.load(ckpt_path, map_location="cpu")
            self._model.eval()
            self._ready = True
        except Exception:
            pass

    def choose_mode(self, sig: SelectorSignals) -> Mode:
        if not self._ready:
            return self._fallback.choose_mode(sig)
        import torch
        vec = torch.tensor([
            sig.D_t if sig.D_t is not None else -1.0,
            sig.cqi_var,
            sig.regret_width,
            sig.regret_mean,
            sig.policies_disagree,
            float(sig.recent_recalibration),
            float(sig.rnti_count),
        ], dtype=torch.float32)
        with torch.no_grad():
            logits = self._model(vec.unsqueeze(0))
        idx = int(logits.argmax(dim=-1).item())
        return [Mode.MAC_ONLY, Mode.SPARSE_TAP, Mode.FULL_IQ][idx]


# ── Signal builder ────────────────────────────────────────────────────────────

def build_signals(
    calibrator,             # channel_converter.ChannelCalibrator
    cqi_window: list,       # recent CQI values (rolling last-50)
    last_verdicts: list,    # list of PolicyVerdict from last oracle call
    last_recal_time: float, # time.time() of last M1 trigger (0 if never)
    recal_window_s: float = 30.0,
    rnti_count: int = 1,
) -> SelectorSignals:
    """Build SelectorSignals from live orchestrator state."""
    D_t     = calibrator.kl_drift_metric() if calibrator is not None else None
    cqi_var = float(np.var(cqi_window)) if len(cqi_window) > 1 else 999.0
    recent_recal = (
        (time.time() - last_recal_time) < recal_window_s
        if last_recal_time > 0 else False
    )

    if not last_verdicts:
        return SelectorSignals(
            D_t=D_t, cqi_var=cqi_var,
            regret_width=999.0, regret_mean=0.0,
            policies_disagree=999.0,
            recent_recalibration=recent_recal,
            rnti_count=rnti_count,
        )

    widths        = [v.ci_upper - v.ci_lower for v in last_verdicts]
    improvements  = [v.mean_improvement for v in last_verdicts]
    spread        = max(improvements) - min(improvements)
    mean_abs      = abs(float(np.mean(improvements))) or 1e-9
    return SelectorSignals(
        D_t=D_t,
        cqi_var=cqi_var,
        regret_width=float(np.mean(widths)),
        regret_mean=float(np.mean(improvements)),
        policies_disagree=float(spread / mean_abs),
        recent_recalibration=recent_recal,
        rnti_count=rnti_count,
    )


# ── CIR helpers ───────────────────────────────────────────────────────────────

def sparsify_topk(r_taps, i_taps, K: int = 4) -> tuple:
    """Zero out all but top-K taps by power.

    Mirrors the C-side logic in apply_channelmod.c (TT_FIDELITY_MODE=sparse_tap)
    so the Python FIFO writer sends pre-sparsified data when mode=sparse_tap.
    The C side re-sparsifies, but since we pre-zero the weak taps the operation
    is idempotent and the result is identical to pure-C sparse path.
    """
    r_taps = np.asarray(r_taps, dtype=float)
    i_taps = np.asarray(i_taps, dtype=float)
    power = r_taps ** 2 + i_taps ** 2
    if K >= len(power):
        return r_taps.copy(), i_taps.copy()
    threshold = np.partition(power, -K)[-K]
    mask = power >= threshold
    return r_taps * mask, i_taps * mask


@contextmanager
def cir_writer_gate(mode: Mode):
    """Yield True if the FIFO CIR write should proceed; False for mac_only."""
    yield mode != Mode.MAC_ONLY


def apply_mode_to_replica(replica_id: int, mode: Mode, sparse_k: int = 4) -> dict:
    """Return env-var dict for docker run / docker-compose."""
    return {
        "TT_FIDELITY_MODE": mode.value,
        "TT_SPARSE_K":      str(sparse_k),
    }


# ── Smoke test ────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    sel = RuleBasedSelector()

    # Cold start → full_iq (high cqi_var and disagree)
    m = sel.choose_mode(COLD_START_SIGNALS)
    assert m == Mode.FULL_IQ, f"Expected FULL_IQ, got {m}"

    # Stable low-drift → mac_only
    stable = SelectorSignals(
        D_t=0.01, cqi_var=0.5, regret_width=0.1, regret_mean=1.0,
        policies_disagree=0.1, recent_recalibration=False, rnti_count=2,
    )
    m = sel.choose_mode(stable)
    assert m == Mode.MAC_ONLY, f"Expected MAC_ONLY, got {m}"

    # Moderate drift → sparse_tap
    mid = SelectorSignals(
        D_t=0.08, cqi_var=2.0, regret_width=0.5, regret_mean=1.0,
        policies_disagree=0.2, recent_recalibration=False, rnti_count=2,
    )
    m = sel.choose_mode(mid)
    assert m == Mode.SPARSE_TAP, f"Expected SPARSE_TAP, got {m}"

    # Recent recalibration → full_iq regardless
    recal = SelectorSignals(
        D_t=0.01, cqi_var=0.3, regret_width=0.1, regret_mean=1.0,
        policies_disagree=0.05, recent_recalibration=True, rnti_count=1,
    )
    m = sel.choose_mode(recal)
    assert m == Mode.FULL_IQ, f"Expected FULL_IQ on recent_recalibration, got {m}"

    # sparsify_topk: only top-2 taps survive
    r = np.array([0.1, 0.9, 0.0, 0.5])
    i = np.array([0.0, 0.0, 0.0, 0.0])
    rs, is_ = sparsify_topk(r, i, K=2)
    assert rs[0] == 0.0 and rs[1] == 0.9 and rs[3] == 0.5, f"sparsify_topk failed: {rs}"

    # cir_writer_gate
    with cir_writer_gate(Mode.MAC_ONLY) as should_write:
        assert not should_write
    with cir_writer_gate(Mode.SPARSE_TAP) as should_write:
        assert should_write

    print("selective_fidelity smoke test passed.")
