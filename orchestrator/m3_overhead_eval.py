"""
M3 / E3 — Selective-Fidelity Overhead Evaluation

Sweeps TT_FIDELITY_MODE across {full_iq, sparse_tap, mac_only} and measures:
  - Wall-clock time per CIR write (FIFO write throughput)
  - Oracle ranking accuracy relative to full_iq (regret-bound accuracy)
  - CPU savings factor vs. always-full_iq baseline

Experiment design (E3 in paper):
  - N_SAMPLES CIR writes per mode; time each write
  - For each mode, run oracle.evaluate(candidates) M_TRIALS times;
    compare rankings to full_iq oracle reference
  - Plot: CPU-savings vs. ranking-accuracy scatter; goal ≥3× savings at <5% loss

Usage:
    # Synthetic timing benchmark only (no real twin required):
    python orchestrator/m3_overhead_eval.py --mock --out logs/e3_overhead.csv

    # Full sweep against live twin:
    python orchestrator/m3_overhead_eval.py --twin-host powder-twin \\
        --n-samples 500 --m-trials 10 --out logs/e3_overhead.csv
"""

import argparse
import csv
import os
import subprocess
import sys
import threading
import time
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path(__file__).parent.parent))

from orchestrator.selective_fidelity import (
    Mode, RuleBasedSelector, SelectorSignals,
    sparsify_topk, COLD_START_SIGNALS,
)
from orchestrator.channel_converter import cqi_to_cir, ChannelCalibrator
from orchestrator.counterfactual_oracle import (
    CounterfactualOracle, EqualWeightPolicy, MaxCQIPolicy, PerturbedPolicy,
)


# ── Container-mode FIFO paths (must match Tiny_Twin's apply_channelmod.c) ────

_PIPE_REAL = "/tmp/tt_cir_real.fifo"
_PIPE_IMAG = "/tmp/tt_cir_imag.fifo"


# ── CIR write timing ──────────────────────────────────────────────────────────

def _generate_cir_sample(cqi: int, mode: Mode, calibrator, rng) -> tuple:
    """Generate CIR taps for a given mode (pure Python side, no FIFO)."""
    r_list, i_list = cqi_to_cir(cqi, calibrator=calibrator, rng=rng)
    r_taps, i_taps = np.asarray(r_list, float), np.asarray(i_list, float)
    if mode == Mode.SPARSE_TAP:
        r_taps, i_taps = sparsify_topk(r_taps, i_taps, K=4)
    elif mode == Mode.MAC_ONLY:
        return np.array([]), np.array([])   # nothing written
    return r_taps, i_taps


def _format_line(taps: np.ndarray) -> str:
    return " ".join(f"{v:.6f}" for v in taps) + "\n"


def benchmark_cir_write(mode: Mode, n_samples: int, rng) -> dict:
    """Time CIR generation + formatting for n_samples at a given mode.

    This exercises the Python-side cost per TTI (cqi_to_cir + sparsify).
    The FIFO write itself is I/O bound and excluded here; actual savings
    on the C side (convolution skip/sparsify) are larger.
    """
    calibrator = ChannelCalibrator()
    cqis = rng.integers(1, 15, size=n_samples)
    latencies = []
    for cqi in cqis:
        t0 = time.perf_counter()
        r, i = _generate_cir_sample(int(cqi), mode, calibrator, rng)
        if len(r):
            _ = _format_line(r)
            _ = _format_line(i)
        latencies.append((time.perf_counter() - t0) * 1e6)  # µs
    arr = np.array(latencies)
    return {
        "mode":   mode.value,
        "n":      n_samples,
        "mean_us": float(arr.mean()),
        "p50_us":  float(np.percentile(arr, 50)),
        "p95_us":  float(np.percentile(arr, 95)),
        "total_ms": float(arr.sum() / 1e3),
    }


# ── Oracle ranking accuracy vs. full_iq ──────────────────────────────────────

_MODE_NOISE_FLOOR = {
    # Each mode has a different physical fidelity. In mock-mode we model
    # this as a per-mode reward-noise floor so the three modes don't
    # collapse onto the same RNG seed (which would give Spearman=1 by
    # construction and silently pass the "≤5% accuracy loss" claim).
    "full_iq":    0.5,
    "sparse_tap": 1.5,
    "mac_only":   3.5,
}


def _run_oracle_trial(mode: Mode, twin_host: str, n_replicas: int,
                      horizon: int, mock: bool, trial_idx: int = 0) -> list:
    """Run one oracle evaluation in the given fidelity mode. Returns verdicts."""
    oracle = CounterfactualOracle(
        twin_host=twin_host,
        n_replicas=n_replicas,
        horizon_ttis=horizon,
        mock=mock,
    )
    # Mock-mode: vary RNG by (mode, trial) so different fidelity modes
    # produce different reward distributions. Real-mode oracle ignores
    # this seed; the live PHY/MAC pipeline drives differences.
    mode_noise = _MODE_NOISE_FLOOR.get(mode.value, 1.0)
    seed = abs(hash((mode.value, trial_idx))) & 0xFFFF_FFFF
    rng = np.random.default_rng(seed)
    for r in rng.normal(18.0, 3.0 + mode_noise, horizon):
        oracle.update_baseline(float(r))

    candidates = [
        EqualWeightPolicy(),
        MaxCQIPolicy(),
        PerturbedPolicy(EqualWeightPolicy(), sigma=0.3, seed=7),
    ]
    return oracle.evaluate(candidates)


def spearman_rho(rank_a: list, rank_b: list) -> float:
    a, b = np.asarray(rank_a, float), np.asarray(rank_b, float)
    n = len(a)
    if n < 2:
        return float("nan")
    ra = np.argsort(np.argsort(a)).astype(float)
    rb = np.argsort(np.argsort(b)).astype(float)
    d2 = ((ra - rb) ** 2).sum()
    return float(1 - 6 * d2 / (n * (n ** 2 - 1)))


def benchmark_oracle_accuracy(modes: list, twin_host: str, n_replicas: int,
                               horizon: int, m_trials: int, mock: bool) -> list:
    """Compare oracle rankings across modes vs. full_iq reference."""
    rows = []
    ref_rankings_by_trial = []

    for trial in range(m_trials):
        ref = _run_oracle_trial(Mode.FULL_IQ, twin_host, n_replicas, horizon, mock, trial)
        ref_order = {v.policy_id: rank
                     for rank, v in enumerate(
                         sorted(ref, key=lambda v: v.mean_improvement, reverse=True)
                     )}
        ref_rankings_by_trial.append(ref_order)

    for mode in modes:
        rhos = []
        for trial in range(m_trials):
            verdicts = _run_oracle_trial(mode, twin_host, n_replicas, horizon, mock, trial)
            mode_order = {v.policy_id: rank
                          for rank, v in enumerate(
                              sorted(verdicts, key=lambda v: v.mean_improvement, reverse=True)
                          )}
            ref_order = ref_rankings_by_trial[trial]
            common = [p for p in ref_order if p in mode_order]
            rho = spearman_rho(
                [ref_order[p] for p in common],
                [mode_order[p] for p in common],
            ) if len(common) >= 2 else float("nan")
            rhos.append(rho)
        rows.append({
            "mode":          mode.value,
            "mean_rho":      float(np.nanmean(rhos)),
            "min_rho":       float(np.nanmin(rhos)),
            "accuracy_loss": float(1.0 - np.nanmean(rhos)),  # vs. perfect 1.0
            "m_trials":      m_trials,
        })
    return rows


# ── Container-mode C-side CPU benchmark ──────────────────────────────────────

def _ensure_fifos() -> None:
    for p in (_PIPE_REAL, _PIPE_IMAG):
        if not os.path.exists(p):
            os.mkfifo(p)


def _spawn_cir_writer(mode: Mode, duration_s: float, stop_event: threading.Event) -> threading.Thread:
    """Background writer: feed CIR samples to the FIFO at ~1ms cadence so the
    C side is never starved when running in full_iq / sparse_tap. For mac_only
    no writes happen — the container's apply_channelmod.c short-circuits before
    any FIFO read."""

    def _writer():
        if mode == Mode.MAC_ONLY:
            stop_event.wait(timeout=duration_s)
            return
        rng = np.random.default_rng(0)
        cal = ChannelCalibrator()
        try:
            fr = open(_PIPE_REAL, "w", buffering=1)
            fi = open(_PIPE_IMAG, "w", buffering=1)
        except Exception as e:
            print(f"[E3/container] FIFO open failed: {e}", flush=True)
            return
        try:
            t_end = time.time() + duration_s
            while not stop_event.is_set() and time.time() < t_end:
                cqi = int(rng.integers(1, 15))
                r, i = cqi_to_cir(cqi, calibrator=cal, rng=rng)
                r = np.asarray(r, float)
                i = np.asarray(i, float)
                if mode == Mode.SPARSE_TAP:
                    r, i = sparsify_topk(r, i, K=4)
                try:
                    fr.write(_format_line(r))
                    fi.write(_format_line(i))
                except BrokenPipeError:
                    break
                time.sleep(0.001)
        finally:
            try:
                fr.close(); fi.close()
            except Exception:
                pass

    t = threading.Thread(target=_writer, daemon=True)
    t.start()
    return t


def _docker_cpu_sample(name: str) -> "float | None":
    try:
        r = subprocess.run(
            ["docker", "stats", "--no-stream", "--format", "{{.CPUPerc}}", name],
            capture_output=True, timeout=5,
        )
        s = r.stdout.decode().strip().rstrip("%")
        return float(s) if s else None
    except Exception:
        return None


def benchmark_cir_container(
    mode:           Mode,
    duration_s:     float,
    image:          str = "tt-gnb",
    container_name: str = "tt-gnb-bench",
) -> dict:
    """Launch tt-gnb container with TT_FIDELITY_MODE=mode, drive its FIFO with
    a synthetic CIR stream, sample `docker stats` CPU% over duration_s, then
    tear down. Returns a row dict suitable for merge with the timing rows.

    On Macs without the tt-gnb image / Linux container support this returns
    NaN with a `note` field — POWDER-side OTA runs will fill the real numbers.
    """
    _ensure_fifos()

    # Best-effort: stop any leftover container with the same name
    subprocess.run(
        ["docker", "rm", "-f", container_name],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )

    try:
        proc = subprocess.run(
            [
                "docker", "run", "-d", "--rm",
                "--name", container_name,
                "-e", f"TT_FIDELITY_MODE={mode.value}",
                "-e", "TT_SPARSE_K=4",
                "-v", f"{_PIPE_REAL}:{_PIPE_REAL}",
                "-v", f"{_PIPE_IMAG}:{_PIPE_IMAG}",
                image,
            ],
            capture_output=True, timeout=30,
        )
    except FileNotFoundError:
        return {"mode": mode.value, "c_side_cpu_pct": float("nan"),
                "c_side_n": 0, "note": "docker CLI not found"}
    except Exception as e:
        return {"mode": mode.value, "c_side_cpu_pct": float("nan"),
                "c_side_n": 0, "note": f"docker run errored: {e}"}

    if proc.returncode != 0:
        err = proc.stderr.decode().strip().splitlines()[-1] if proc.stderr else ""
        return {"mode": mode.value, "c_side_cpu_pct": float("nan"),
                "c_side_n": 0, "note": f"container start failed: {err}"}

    # Drive the FIFO and sample CPU concurrently
    stop_event = threading.Event()
    writer_thr = _spawn_cir_writer(mode, duration_s + 5.0, stop_event)

    samples = []
    t_end   = time.time() + duration_s
    # Allow OAI a moment to settle before starting samples
    time.sleep(2.0)
    while time.time() < t_end:
        s = _docker_cpu_sample(container_name)
        if s is not None:
            samples.append(s)
        time.sleep(1.0)

    stop_event.set()
    writer_thr.join(timeout=2.0)
    subprocess.run(
        ["docker", "stop", container_name],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )

    if not samples:
        return {"mode": mode.value, "c_side_cpu_pct": float("nan"),
                "c_side_n": 0, "note": "no docker stats samples"}
    return {
        "mode":            mode.value,
        "c_side_cpu_pct":  float(np.mean(samples)),
        "c_side_p95":      float(np.percentile(samples, 95)),
        "c_side_n":        len(samples),
    }


# ── Summary: CPU savings factor ───────────────────────────────────────────────

def compute_savings_factor(timing_rows: list) -> list:
    """Normalize Python-side generation/formatting time to full_iq baseline.

    This is a local mock sanity metric only. The paper's >=3x compute claim
    must come from `c_side_savings` gathered with --container or a twin run.
    """
    ref = next((r["mean_us"] for r in timing_rows if r["mode"] == "full_iq"), None)
    for row in timing_rows:
        row["savings_factor"] = ref / max(row["mean_us"], 1e-9) if ref else float("nan")
        row["savings_metric"] = "python_side_sanity"
    return timing_rows


def compute_c_side_savings(container_rows: list) -> list:
    """Normalize C-side CPU% to full_iq baseline (the actual paper metric)."""
    ref = next(
        (r["c_side_cpu_pct"] for r in container_rows
         if r["mode"] == "full_iq" and r["c_side_cpu_pct"] == r["c_side_cpu_pct"]),
        None,
    )
    for row in container_rows:
        v = row.get("c_side_cpu_pct", float("nan"))
        if ref and v == v and v > 0:
            row["c_side_savings"] = ref / v
        else:
            row["c_side_savings"] = float("nan")
    return container_rows


# ── Main E3 experiment ────────────────────────────────────────────────────────

def run_e3(
    twin_host: str,
    n_samples: int,
    n_replicas: int,
    horizon: int,
    m_trials: int,
    out_path: Path,
    mock: bool,
    container:           bool   = False,
    container_duration:  float  = 60.0,
    container_image:     str    = "tt-gnb",
) -> None:
    modes = [Mode.FULL_IQ, Mode.SPARSE_TAP, Mode.MAC_ONLY]
    rng   = np.random.default_rng(0)

    print(f"[E3] Timing sweep: {n_samples} samples × 3 modes ...", flush=True)
    timing_rows = [benchmark_cir_write(m, n_samples, rng) for m in modes]
    timing_rows = compute_savings_factor(timing_rows)

    print(f"[E3] Oracle accuracy sweep: {m_trials} trials × 3 modes ...", flush=True)
    accuracy_rows = benchmark_oracle_accuracy(
        modes, twin_host, n_replicas, horizon, m_trials, mock
    )

    container_rows = []
    if container:
        print(
            f"[E3] Container CPU sweep: {container_duration:.0f}s × 3 modes "
            f"(image={container_image}) ...",
            flush=True,
        )
        for m in modes:
            row = benchmark_cir_container(
                m, duration_s=container_duration, image=container_image,
            )
            print(
                f"[E3/container] {m.value:<11s} cpu%={row.get('c_side_cpu_pct', float('nan')):.2f} "
                f"n={row.get('c_side_n', 0)} {row.get('note', '')}",
                flush=True,
            )
            container_rows.append(row)
        container_rows = compute_c_side_savings(container_rows)

    # Merge by mode
    acc_by_mode = {r["mode"]: r for r in accuracy_rows}
    cont_by_mode = {r["mode"]: r for r in container_rows}
    merged = []
    for tr in timing_rows:
        ar = acc_by_mode.get(tr["mode"], {})
        cr = cont_by_mode.get(tr["mode"], {})
        merged.append({**tr, **ar, **cr})

    # Print summary
    print("\n─── E3 Results ──────────────────────────────────────────────────────")
    print("Note: savings× below is Python-side sanity only; use c_side_savings for the paper claim.")
    header = f"{'Mode':<12}  {'mean µs':>9}  {'savings×':>9}  {'ρ vs full_iq':>13}  {'acc_loss':>9}"
    print(header)
    for row in merged:
        print(
            f"{row['mode']:<12}  {row['mean_us']:>9.2f}  "
            f"{row.get('savings_factor', float('nan')):>9.2f}×  "
            f"{row.get('mean_rho', float('nan')):>13.3f}  "
            f"{row.get('accuracy_loss', float('nan')):>9.1%}"
        )
    print(f"\nTarget: ≥3× savings at <5% accuracy loss")

    # Write CSV
    out_path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = list(merged[0].keys())
    with open(out_path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        w.writeheader()
        w.writerows(merged)
    print(f"\n[E3] Results → {out_path}")

    # Plot
    try:
        if int(np.__version__.split(".", 1)[0]) >= 2:
            raise RuntimeError(
                f"NumPy {np.__version__} detected; install requirements.txt "
                "(numpy<2) before generating matplotlib plots"
            )
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt

        fig, axes = plt.subplots(1, 2, figsize=(10, 4))

        # Left: savings factor
        labels  = [r["mode"] for r in merged]
        savings = [r.get("savings_factor", float("nan")) for r in merged]
        colors  = ["#d62728", "#ff7f0e", "#2ca02c"]
        axes[0].bar(labels, savings, color=colors)
        axes[0].axhline(3.0, color="black", linestyle="--", linewidth=0.8, label="3× target")
        axes[0].set_ylabel("CPU savings factor (×)")
        axes[0].set_title("E3: Compute savings by mode")
        axes[0].legend()

        # Right: accuracy (Spearman ρ)
        rhos   = [r.get("mean_rho", float("nan")) for r in merged]
        axes[1].bar(labels, rhos, color=colors)
        axes[1].axhline(0.95, color="black", linestyle="--", linewidth=0.8,
                        label="ρ=0.95 (5% loss)")
        axes[1].set_ylim(0, 1.05)
        axes[1].set_ylabel("Oracle ranking ρ vs full_iq")
        axes[1].set_title("E3: Ranking accuracy by mode")
        axes[1].legend()

        plt.tight_layout()
        fig.savefig(out_path.with_suffix(".pdf"), dpi=150)
        print(f"[E3] Plot → {out_path.with_suffix('.pdf')}")
    except Exception as e:
        print(f"[E3] Plot skipped: {type(e).__name__}: {e}", flush=True)


def main():
    parser = argparse.ArgumentParser(description="E3 M3 overhead sweep")
    parser.add_argument("--twin-host",   default="localhost")
    parser.add_argument("--n-samples",   type=int, default=500,
                        help="CIR write samples per mode for timing")
    parser.add_argument("--n-replicas",  type=int, default=4)
    parser.add_argument("--horizon",     type=int, default=200)
    parser.add_argument("--m-trials",    type=int, default=5,
                        help="Oracle eval trials per mode for accuracy")
    parser.add_argument("--out",         default="logs/e3_overhead.csv")
    parser.add_argument("--mock",        action="store_true",
                        help="Synthetic metrics (no live twin required)")
    parser.add_argument("--container",   action="store_true",
                        help="Also run a per-mode tt-gnb docker container "
                             "and sample CPU% from `docker stats` (the C-side "
                             "convolution cost — what M3's ≥3× claim hinges on)")
    parser.add_argument("--container-duration", type=float, default=60.0,
                        help="Per-mode container sampling window in seconds")
    parser.add_argument("--container-image",    default="tt-gnb",
                        help="Docker image name for the tt-gnb container")
    args = parser.parse_args()

    run_e3(
        twin_host          = args.twin_host,
        n_samples          = args.n_samples,
        n_replicas         = args.n_replicas,
        horizon            = args.horizon,
        m_trials           = args.m_trials,
        out_path           = Path(args.out),
        mock               = args.mock,
        container          = args.container,
        container_duration = args.container_duration,
        container_image    = args.container_image,
    )


if __name__ == "__main__":
    main()
