"""
CIR Converter — Contribution 1.

Converts per-UE EdgeRIC measurements (UL CQI) to multi-tap CIR coefficients
for Tiny_Twin's channel file format (one line per TTI, space-separated taps).

snr.txt log format (from gNB_scheduler_ulsch.c):
    TTI Index: <tti>
    UL CQI: <cqi_int> RNTI: <rnti_hex>
    TTI Index: <tti>
    UL ACK: <ack> RNTI: <rnti_hex>
    TTI Index: <tti>
    UL TPT: <tpt_float> RNTI: <rnti_hex>
    TTI Index: <tti>
    UL MCS: <mcs> RNTI: <rnti_hex>
    ...

drops.txt format (from gNB_scheduler_dlsch.c):
    <num_retx_int> <rnti_dec>
"""
import math
import re
import random
import numpy as np
from collections import defaultdict, deque
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterator


NUM_TAPS = 100     # must match OAI channel file format (100 values per line)
TAP_DECAY = 0.3   # power ratio decay per tap


# ── Data structures ──────────────────────────────────────────────────────────

@dataclass
class TtiRecord:
    tti: int
    rnti: int
    cqi: float = 0.0
    ul_tpt: float = 0.0
    ul_mcs: int = 0
    ul_ack: int = 0


# ── Physics-based CIR estimation ─────────────────────────────────────────────

# 3GPP CQI → approximate wideband SNR mapping (Table 7.2.3-1 in TS 38.214)
_CQI_TO_SNR_DB = {
    0: -6.0,   # out of range
    1: -5.0, 2: -3.0, 3: -1.0, 4: 1.0, 5: 3.0,
    6: 5.0,  7: 7.0,  8: 9.5, 9: 12.0, 10: 14.5,
    11: 17.0, 12: 19.5, 13: 22.0, 14: 24.5, 15: 26.0,
}


def cqi_to_snr_db(cqi: int) -> float:
    # OAI logs SNR*8 (Q8 fixed-point) when the raw value exceeds 3GPP CQI range 0-15
    if cqi > 15:
        return cqi / 8.0
    return _CQI_TO_SNR_DB.get(max(0, min(15, cqi)), 0.0)


def snr_db_to_cqi(snr_db: float) -> int:
    """Inverse of cqi_to_snr_db on the 0-15 range.  Used by M1 to derive a
    twin-side CQI from twin-side SNR for joint (CQI, BLER) drift histograms."""
    best_cqi, best_err = 0, float("inf")
    for cqi, snr in _CQI_TO_SNR_DB.items():
        err = abs(snr - snr_db)
        if err < best_err:
            best_err, best_cqi = err, cqi
    return best_cqi


def bler_from_snr(snr_db: float, snr_at_10pct: float = 10.0, slope: float = 4.0) -> float:
    """
    Simple parametric BLER model used by M1 to approximate twin-side BLER
    from twin-side SNR (we don't always have a twin ACK stream available).
    Logistic curve centered at the 10%-BLER operating point.

      BLER(snr) = 1 / (1 + exp((snr - snr_at_10pct) / slope))

    Tuned so BLER(10 dB)≈0.5, BLER(20 dB)≈0.08, BLER(0 dB)≈0.92.
    """
    return 1.0 / (1.0 + math.exp((snr_db - snr_at_10pct) / slope))


def snr_db_to_cir(
    snr_db: float,
    num_taps: int = NUM_TAPS,
    decay: float = TAP_DECAY,
    rng=None,
    amplitude_scale: float = 1.0,
) -> tuple:
    """Return (real_taps, imag_taps) for one TTI given SNR in dB.

    amplitude_scale: multiplicative correction applied to every tap amplitude,
    used by ChannelCalibrator to close the real-vs-twin SNR gap.
    """
    if rng is None:
        rng = np.random.default_rng()
    linear_snr = max(10 ** (snr_db / 10), 1e-6)
    decay_sum = sum(decay ** k for k in range(num_taps))
    h0_power = linear_snr / decay_sum

    real_taps, imag_taps = [], []
    for k in range(num_taps):
        amplitude = math.sqrt(h0_power * (decay ** k)) * amplitude_scale
        phase = rng.uniform(0, 2 * math.pi)
        real_taps.append(amplitude * math.cos(phase))
        imag_taps.append(amplitude * math.sin(phase))
    return real_taps, imag_taps


def cqi_to_cir(cqi: int, calibrator=None, **kwargs) -> tuple:
    """Convert raw OAI CQI to CIR taps, with optional data-driven calibration."""
    scale = calibrator.amplitude_scale(cqi) if calibrator is not None else 1.0
    return snr_db_to_cir(cqi_to_snr_db(cqi), amplitude_scale=scale, **kwargs)


# ── Data-driven beamspace calibration ────────────────────────────────────────

class ChannelCalibrator:
    """
    Lightweight beamspace calibration for DT channel synchronization.

    Learns a per-CQI-bin amplitude correction scale so that the twin UE's
    measured UL SNR tracks the real-side SNR derived from CQI measurements.
    This is the scalar (wideband) instance of the diagonal beamspace correction
    matrix Λ from the paper: H_corrected = Λ * H_DT in the frequency domain.

    Usage:
        cal = ChannelCalibrator(cal_path="logs/calibration.npy")
        # during sync loop: feed paired observations
        cal.update(real_cqi=184, twin_snr_db=20.5)
        # when generating CIR:
        r, i = cqi_to_cir(cqi, calibrator=cal, rng=rng)
    """

    EMA_ALPHA = 0.15   # EMA smoothing; higher = faster adaptation
    MIN_SAMPLES = 5    # samples per bin before correction is applied
    NUM_BINS = 32      # CQI bins (covers OAI Q8 range 0-255 in steps of 8)

    # ── M1: drift-aware live calibration ─────────────────────────────────────
    # Sliding-window joint distribution of (CQI, BLER) for both real and twin.
    # KL[real || twin] = drift metric D(t); when D(t) > tau, EMA is advanced.
    DRIFT_WINDOW       = 200              # paired samples kept in window
    DRIFT_MIN_SAMPLES  = 50               # minimum before D(t) is reported
    DRIFT_NUM_CQI_BINS = 4                # coarse CQI binning for joint hist
    DRIFT_BLER_EDGES   = (0.0, 0.05, 0.10, 0.20, 1.01)   # 4 BLER bins
    DEFAULT_TAU_DRIFT  = 0.10             # KL trigger threshold

    def __init__(self, cal_path=None):
        # Per-bin EMA of (real_snr_db - twin_snr_db); positive = twin underestimates
        self._offset_db = np.zeros(self.NUM_BINS)
        self._count = np.zeros(self.NUM_BINS, dtype=int)
        self._cal_path = Path(cal_path) if cal_path else None
        # M1: drift tracking
        self._drift_buffer: deque = deque(maxlen=self.DRIFT_WINDOW)
        self._trigger_count = 0
        self._samples_since_last_trigger = 0
        if self._cal_path and self._cal_path.exists():
            self._load()

    # ── public API ────────────────────────────────────────────────────────────

    def update(self, real_cqi: int, twin_snr_db: float) -> None:
        """Feed one calibration observation: real-side CQI and twin UL SNR."""
        real_snr = cqi_to_snr_db(real_cqi)
        offset = real_snr - twin_snr_db
        b = self._bin(real_cqi)
        if self._count[b] == 0:
            self._offset_db[b] = offset
        else:
            self._offset_db[b] = (
                (1 - self.EMA_ALPHA) * self._offset_db[b]
                + self.EMA_ALPHA * offset
            )
        self._count[b] += 1
        if self._cal_path and self._count[b] % 100 == 0:
            self._save()

    def amplitude_scale(self, cqi: int) -> float:
        """Return tap amplitude multiplier for a given raw CQI.

        SNR scales as amplitude^2, so a +X dB SNR correction needs
        amplitude *= 10^(X/20).
        """
        b = self._bin(cqi)
        if self._count[b] < self.MIN_SAMPLES:
            return 1.0
        return float(10 ** (self._offset_db[b] / 20.0))

    # ── M1: drift-aware live calibration API ──────────────────────────────────

    def observe_drift_sample(
        self,
        real_cqi: int,
        real_bler: float,
        twin_cqi: int,
        twin_bler: float,
    ) -> None:
        """
        Record one paired (real, twin) sample for drift estimation.
        Cheap O(1); always safe to call. Window auto-evicts oldest.
        """
        self._drift_buffer.append(
            (int(real_cqi), float(real_bler), int(twin_cqi), float(twin_bler))
        )
        self._samples_since_last_trigger += 1

    def kl_drift_metric(self) -> "float | None":
        """
        Sliding-window D(t) = KL[(real_CQI, real_BLER) ‖ (twin_CQI, twin_BLER)].
        Returns None if too few paired samples are buffered.

        Joint histogram: DRIFT_NUM_CQI_BINS × len(DRIFT_BLER_EDGES)-1 cells.
        Laplace smoothing (eps=1e-6) avoids log(0) on sparse bins.
        """
        if len(self._drift_buffer) < self.DRIFT_MIN_SAMPLES:
            return None
        nC = self.DRIFT_NUM_CQI_BINS
        nB = len(self.DRIFT_BLER_EDGES) - 1
        real_h = np.zeros((nC, nB))
        twin_h = np.zeros((nC, nB))
        for r_cqi, r_bler, t_cqi, t_bler in self._drift_buffer:
            real_h[self._drift_cqi_bin(r_cqi), self._drift_bler_bin(r_bler)] += 1
            twin_h[self._drift_cqi_bin(t_cqi), self._drift_bler_bin(t_bler)] += 1
        eps = 1e-6
        real_p = (real_h + eps) / (real_h.sum() + eps * real_h.size)
        twin_p = (twin_h + eps) / (twin_h.sum() + eps * twin_h.size)
        return float(np.sum(real_p * np.log(real_p / twin_p)))

    def should_recalibrate(self, tau_drift: "float | None" = None) -> bool:
        """True iff D(t) exceeds tau_drift. Used to gate EMA recalibration."""
        if tau_drift is None:
            tau_drift = self.DEFAULT_TAU_DRIFT
        d = self.kl_drift_metric()
        return d is not None and d > tau_drift

    def mark_recalibration(self) -> None:
        """
        Caller invokes this after applying an EMA recalibration step
        (i.e. one or more update() calls triggered by should_recalibrate()).
        Resets the inter-trigger sample counter used for tau_coherence.
        """
        self._trigger_count += 1
        self._samples_since_last_trigger = 0

    def coherence_samples(self) -> int:
        """Samples observed since last recalibration trigger (proxy for tau_coherence)."""
        return self._samples_since_last_trigger

    def trigger_count(self) -> int:
        """Total recalibration triggers since startup (for E1 figure)."""
        return self._trigger_count

    def _drift_cqi_bin(self, cqi: int) -> int:
        """Coarse CQI bin for drift histogram. Handles raw CQI and OAI Q8 SNR."""
        if cqi > 15:           # OAI Q8 fixed-point SNR encoding
            snr = cqi / 8.0
            if snr < 0:   return 0
            if snr < 10:  return 1
            if snr < 20:  return 2
            return 3
        return min(int(cqi) // 4, self.DRIFT_NUM_CQI_BINS - 1)

    def _drift_bler_bin(self, bler: float) -> int:
        for k, edge in enumerate(self.DRIFT_BLER_EDGES[1:]):
            if bler < edge:
                return k
        return len(self.DRIFT_BLER_EDGES) - 2

    def summary(self) -> str:
        active = np.where(self._count >= self.MIN_SAMPLES)[0]
        d = self.kl_drift_metric()
        d_str = f"D(t)={d:.3f}" if d is not None else "D(t)=n/a"
        trig_str = (
            f"triggers={self._trigger_count} | "
            f"coh_samples={self._samples_since_last_trigger}"
        )
        if len(active) == 0:
            return (
                f"ChannelCalibrator: {self._count.sum()} samples, calibration pending | "
                f"{d_str} | {trig_str}"
            )
        offsets = self._offset_db[active]
        return (
            f"ChannelCalibrator: {int(self._count.sum())} samples | "
            f"SNR offset {offsets.mean():.1f}+/-{offsets.std():.1f} dB | "
            f"{len(active)} active CQI bins | {d_str} | {trig_str}"
        )

    def save(self) -> None:
        if self._cal_path:
            self._save()

    # ── internal ──────────────────────────────────────────────────────────────

    def _bin(self, cqi: int) -> int:
        return min(int(cqi) // 8, self.NUM_BINS - 1)

    def _save(self) -> None:
        self._cal_path.parent.mkdir(parents=True, exist_ok=True)
        np.save(str(self._cal_path), {
            "offset_db": self._offset_db,
            "count": self._count,
        })

    def _load(self) -> None:
        data = np.load(str(self._cal_path), allow_pickle=True).item()
        self._offset_db = data["offset_db"]
        self._count = data["count"]
        print(f"[Calibrator] Loaded from {self._cal_path}: {self.summary()}")


# ── Log parsing ───────────────────────────────────────────────────────────────

def parse_snr_log(path) -> dict:
    """
    Parse logs/snr.txt -> {rnti: [TtiRecord, ...]} sorted by TTI.
    Handles interleaved multi-UE output.
    """
    records = defaultdict(dict)  # rnti -> tti -> record
    current_tti = 0

    tti_re  = re.compile(r"TTI (?:Index|Count): (\d+)")
    cqi_re  = re.compile(r"UL CQI: (\d+) RNTI: ([0-9a-fA-F]+)")
    tpt_re  = re.compile(r"UL TPT: ([\d.]+) RNTI: ([0-9a-fA-F]+)")
    mcs_re  = re.compile(r"UL MCS: (\d+) RNTI: ([0-9a-fA-F]+)")
    ack_re  = re.compile(r"UL ACK: (\d+) RNTI: ([0-9a-fA-F]+)")

    with open(path) as f:
        for line in f:
            line = line.strip()
            if m := tti_re.match(line):
                current_tti = int(m.group(1))
            elif m := cqi_re.match(line):
                cqi, rnti = int(m.group(1)), int(m.group(2), 16)
                rec = records[rnti].setdefault(current_tti, TtiRecord(tti=current_tti, rnti=rnti))
                rec.cqi = cqi
            elif m := tpt_re.match(line):
                tpt, rnti = float(m.group(1)), int(m.group(2), 16)
                rec = records[rnti].setdefault(current_tti, TtiRecord(tti=current_tti, rnti=rnti))
                rec.ul_tpt = tpt
            elif m := mcs_re.match(line):
                mcs, rnti = int(m.group(1)), int(m.group(2), 16)
                rec = records[rnti].setdefault(current_tti, TtiRecord(tti=current_tti, rnti=rnti))
                rec.ul_mcs = mcs
            elif m := ack_re.match(line):
                ack, rnti = int(m.group(1)), int(m.group(2), 16)
                rec = records[rnti].setdefault(current_tti, TtiRecord(tti=current_tti, rnti=rnti))
                rec.ul_ack = ack

    return {rnti: sorted(recs.values(), key=lambda r: r.tti)
            for rnti, recs in records.items()}


def parse_drops_log(path) -> list:
    """Parse logs/drops.txt -> [(num_retx, rnti), ...]"""
    result = []
    with open(path) as f:
        for line in f:
            parts = line.strip().split()
            if len(parts) == 2:
                result.append((int(parts[0]), int(parts[1])))
    return result


# ── File / pipe generation ────────────────────────────────────────────────────

def generate_cir_files(
    tti_records,
    out_real,
    out_imag,
    num_taps: int = NUM_TAPS,
    seed: int = 42,
    calibrator=None,
) -> None:
    """Write CIR file pair from a list of TtiRecords (physics-based model)."""
    rng = np.random.default_rng(seed)
    with open(out_real, "w") as fr, open(out_imag, "w") as fi:
        for rec in tti_records:
            r, i = cqi_to_cir(int(rec.cqi), calibrator=calibrator, num_taps=num_taps, rng=rng)
            fr.write(" ".join(f"{v:.6f}" for v in r) + "\n")
            fi.write(" ".join(f"{v:.6f}" for v in i) + "\n")


def stream_cir_to_pipe(
    cqi_iter: Iterator[float],
    pipe_real,
    pipe_imag,
    num_taps: int = NUM_TAPS,
    calibrator=None,
) -> None:
    """
    Stream CIR taps to named pipes (line-buffered), one line per TTI.
    Blocks until pipes have readers on the other end.
    """
    rng = np.random.default_rng()
    with open(pipe_real, "w", buffering=1) as fr, \
         open(pipe_imag, "w", buffering=1) as fi:
        for cqi in cqi_iter:
            r, i = cqi_to_cir(int(cqi), calibrator=calibrator, num_taps=num_taps, rng=rng)
            fr.write(" ".join(f"{v:.6f}" for v in r) + "\n")
            fi.write(" ".join(f"{v:.6f}" for v in i) + "\n")


if __name__ == "__main__":
    import sys

    if len(sys.argv) >= 2:
        log_path = sys.argv[1]
        print(f"Parsing {log_path}...")
        data = parse_snr_log(log_path)
        for rnti, recs in data.items():
            print(f"  RNTI 0x{rnti:04x}: {len(recs)} TTIs, "
                  f"CQI range [{min(r.cqi for r in recs):.0f}, {max(r.cqi for r in recs):.0f}]")
    else:
        # Smoke test: verify calibrator drives SNR toward target
        print("=== ChannelCalibrator smoke test ===")
        cal = ChannelCalibrator()
        rng = np.random.default_rng(42)

        # Simulate: real CQI 184 -> 23 dB, but twin measures 20 dB -> calibrator should
        # learn to upscale by +3 dB (amplitude *= 10^(3/20) = 1.413)
        for _ in range(20):
            cal.update(real_cqi=184, twin_snr_db=20.0)
        print(cal.summary())
        scale = cal.amplitude_scale(184)
        print(f"  amplitude_scale(184) = {scale:.4f}  (expect ~{10**(3/20):.4f})")

        # Generate uncalibrated vs calibrated CIR and check power
        r_raw, i_raw = snr_db_to_cir(23.0, rng=rng, amplitude_scale=1.0)
        r_cal, i_cal = snr_db_to_cir(23.0, rng=rng, amplitude_scale=scale)
        power_raw = sum(x**2 + y**2 for x, y in zip(r_raw, i_raw))
        power_cal = sum(x**2 + y**2 for x, y in zip(r_cal, i_cal))
        print(f"  raw total power = {power_raw:.4f}")
        print(f"  cal total power = {power_cal:.4f}  (ratio {power_cal/power_raw:.4f})")

        # End-to-end file generation smoke test
        fake_records = [TtiRecord(tti=i, rnti=0x4401, cqi=184) for i in range(10)]
        generate_cir_files(fake_records, "/tmp/test_cir_real.txt", "/tmp/test_cir_imag.txt",
                           calibrator=cal)
        print("\nGenerated /tmp/test_cir_{real,imag}.txt with calibration")
        with open("/tmp/test_cir_real.txt") as f:
            first = f.readline().split()
            print(f"  First line: {len(first)} taps, tap[0]={first[0]}")

        # ── M1: drift metric smoke test ──────────────────────────────────────
        print("\n=== M1 drift metric smoke test ===")
        cal2 = ChannelCalibrator()
        # Phase 1: real and twin agree → low drift
        for _ in range(150):
            cal2.observe_drift_sample(real_cqi=12, real_bler=0.05,
                                      twin_cqi=12, twin_bler=0.05)
        d_low = cal2.kl_drift_metric()
        print(f"  D(t) when real==twin     : {d_low:.4f}  (expect ~0)")
        assert d_low is not None and d_low < 0.05, "low-drift case should be near zero"
        assert not cal2.should_recalibrate(), "should not trigger when aligned"

        # Phase 2: twin BLER stays low while real BLER spikes → high drift
        cal2._drift_buffer.clear()  # reset for clarity
        for _ in range(150):
            cal2.observe_drift_sample(real_cqi=4,  real_bler=0.30,
                                      twin_cqi=12, twin_bler=0.05)
        d_high = cal2.kl_drift_metric()
        print(f"  D(t) when distrib differ : {d_high:.4f}  (expect > tau)")
        assert d_high is not None and d_high > cal2.DEFAULT_TAU_DRIFT, \
            "high-drift case should exceed tau"
        assert cal2.should_recalibrate(), "should trigger on distribution shift"

        # Phase 3: trigger gating coherence counter
        cal2.mark_recalibration()
        assert cal2.coherence_samples() == 0
        cal2.observe_drift_sample(4, 0.30, 12, 0.05)
        assert cal2.coherence_samples() == 1
        print(f"  trigger_count={cal2.trigger_count()}, "
              f"coh_samples after 1 obs={cal2.coherence_samples()}")
        print("  M1 smoke test passed.")
