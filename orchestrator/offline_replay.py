"""
W4 — Offline CIR Replay Pipeline.

Takes real-side logs (collected in Phase A) and produces CIR files
that can be fed to Tiny_Twin for E1 fidelity experiments.

Usage:
    # After Phase A, logs are in logs/real_snr.txt
    python orchestrator/offline_replay.py \
        --snr-log logs/real_snr.txt \
        --out-dir logs/cir_offline \
        --rnti 0x4401          # optional, defaults to first RNTI found
"""
import argparse
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
from orchestrator.channel_converter import (
    parse_snr_log, generate_cir_files, TtiRecord
)


def run(snr_log: Path, out_dir: Path, rnti: int | None = None) -> dict[int, Path]:
    """
    Parse SNR log, generate per-UE CIR file pairs.
    Returns {rnti: (real_path, imag_path)}.
    """
    out_dir.mkdir(parents=True, exist_ok=True)
    print(f"Parsing {snr_log} ...")
    data = parse_snr_log(snr_log)

    if not data:
        print("ERROR: no records found in log — check log format")
        sys.exit(1)

    if rnti is not None:
        if rnti not in data:
            print(f"ERROR: RNTI 0x{rnti:04x} not found. Available: "
                  f"{[hex(r) for r in data]}")
            sys.exit(1)
        ue_subset = {rnti: data[rnti]}
    else:
        ue_subset = data

    output_paths = {}
    for ue_rnti, records in ue_subset.items():
        real_path = out_dir / f"cir_ue{ue_rnti:04x}_real.txt"
        imag_path = out_dir / f"cir_ue{ue_rnti:04x}_imag.txt"
        generate_cir_files(records, real_path, imag_path)
        output_paths[ue_rnti] = (real_path, imag_path)
        print(f"  RNTI 0x{ue_rnti:04x}: {len(records)} TTIs → "
              f"{real_path.name} / {imag_path.name}")

    # Print launch commands for twin
    print("\n── Twin Launch Commands (Phase C) ───────────────────────────────")
    for ue_rnti, (real_p, imag_p) in output_paths.items():
        print(f"\n# RNTI 0x{ue_rnti:04x}")
        print(f"TT_CHANNEL_FILE_REAL={real_p.resolve()} \\")
        print(f"TT_CHANNEL_FILE_IMAG={imag_p.resolve()} \\")
        print("docker compose -f sims/docker-compose.twin.yaml up -d tt-gnb tt-nrue1")

    return output_paths


def main():
    parser = argparse.ArgumentParser(description="Offline CIR replay for Phase C")
    parser.add_argument("--snr-log", default="logs/real_snr.txt",
                        help="Path to real-side snr.txt log")
    parser.add_argument("--out-dir", default="logs/cir_offline",
                        help="Output directory for CIR files")
    parser.add_argument("--rnti", default=None,
                        help="Specific RNTI in hex (e.g. 0x4401), default=all")
    args = parser.parse_args()

    rnti = int(args.rnti, 16) if args.rnti else None
    run(Path(args.snr_log), Path(args.out_dir), rnti)


if __name__ == "__main__":
    main()
