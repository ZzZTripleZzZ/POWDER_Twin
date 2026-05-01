"""
W7 — Policy Arbiter + Twin Ensemble Safety Filter (Contribution 2).

Flow:
  1. Receive candidate policy π = {rnti: weight} from RL agent
  2. Generate N CIR perturbations around the current physics estimate
  3. Spin up (or reuse) N twin replicas, each with a different CIR perturbation
  4. Send π to all N replicas, collect K TTIs of metrics
  5. Safety criterion: ALL replicas must satisfy BLER < bler_threshold
                       AND mean_throughput > tpt_threshold
  6. Pass → forward π to real EdgeRIC; Fail → reject / log

Usage:
    from orchestrator.policy_arbiter import PolicyArbiter, Policy, SafetyCriterion
    arbiter = PolicyArbiter(twin_host="powder-twin", real_host="powder-gnb", n_replicas=4)
    policy  = Policy(weights={0x4401: 0.6, 0x4402: 0.4})
    result  = arbiter.evaluate(policy)
    if result.safe:
        arbiter.deploy(policy)
"""
import sys
import time
import threading
import subprocess
import tempfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import NamedTuple

import numpy as np
import zmq

sys.path.insert(0, str(Path(__file__).parent.parent))
sys.path.insert(0, str(Path(__file__).parent.parent / "proto"))

import metrics_pb2
import control_weights_pb2
from orchestrator.channel_converter import cqi_to_cir, NUM_TAPS

# ── Port layout: replica i uses base + i*100 ─────────────────────────────────
PORT_BASE       = 5555   # metrics PUB of replica 0
PORT_STRIDE     = 100
REAL_METRICS_PORT  = 5555
REAL_WEIGHTS_PORT  = 5556

EVAL_TTIS       = 200    # TTIs to collect per replica during evaluation
TTI_MS          = 1      # 1 ms per TTI


# ── Data structures ───────────────────────────────────────────────────────────

@dataclass
class Policy:
    """Scheduling weights per UE (RNTI → weight, will be normalized)."""
    weights: dict[int, float]
    mcs_override: dict[int, int] = field(default_factory=dict)   # optional

    def to_proto(self, ran_index: int = 0) -> bytes:
        msg = control_weights_pb2.SchedulingWeights()
        msg.ran_index = ran_index
        for rnti, w in self.weights.items():
            msg.weights.append(float(rnti))
            msg.weights.append(float(w))
        return msg.SerializeToString()


@dataclass
class SafetyCriterion:
    bler_threshold: float = 0.10    # max allowed mean BLER
    tpt_threshold:  float = 10.0    # min required mean DL throughput (Mbps)
    min_pass_ratio: float = 1.0     # fraction of replicas that must pass (1.0 = all)


class ReplicaResult(NamedTuple):
    replica_id:   int
    mean_bler:    float
    mean_tpt:     float
    n_samples:    int
    passed:       bool


@dataclass
class EvalResult:
    policy:       Policy
    replica_results: list[ReplicaResult]
    safe:         bool
    pass_ratio:   float

    def summary(self) -> str:
        lines = [f"Policy evaluation: safe={self.safe} "
                 f"({self.pass_ratio*100:.0f}% replicas passed)"]
        for r in self.replica_results:
            lines.append(f"  replica {r.replica_id}: BLER={r.mean_bler:.3f} "
                         f"TPT={r.mean_tpt:.1f} Mbps  {'✓' if r.passed else '✗'}")
        return "\n".join(lines)


# ── CIR Perturbation ──────────────────────────────────────────────────────────

def perturb_cir_file(base_cir_real: Path, base_cir_imag: Path,
                     out_real: Path, out_imag: Path,
                     sigma: float = 0.15, seed: int = 0) -> None:
    """
    Add Gaussian noise to CIR tap amplitudes (±sigma fraction of mean amplitude).
    This models uncertainty in the CIR estimation.
    """
    rng = np.random.default_rng(seed)
    real_data = np.loadtxt(base_cir_real)
    imag_data = np.loadtxt(base_cir_imag)

    amp = np.sqrt(real_data**2 + imag_data**2)
    mean_amp = amp.mean()
    noise_r = rng.normal(0, sigma * mean_amp, real_data.shape)
    noise_i = rng.normal(0, sigma * mean_amp, imag_data.shape)

    np.savetxt(out_real, real_data + noise_r, fmt="%.6f")
    np.savetxt(out_imag, imag_data + noise_i, fmt="%.6f")


# ── Replica Manager ───────────────────────────────────────────────────────────

class ReplicaManager:
    """
    Manages N twin replicas on a remote POWDER node via SSH + Docker compose.
    Each replica gets its own Docker network and port mapping.
    """

    def __init__(self, twin_host: str, n_replicas: int,
                 base_cir_real: Path, base_cir_imag: Path,
                 tmp_dir: Path = Path("/tmp/dt_sync_replicas")):
        self.twin_host     = twin_host
        self.n_replicas    = n_replicas
        self.base_cir_real = base_cir_real
        self.base_cir_imag = base_cir_imag
        self.tmp_dir       = tmp_dir
        self._compose_files: list[Path] = []

    def _render_compose(self, replica_id: int,
                        cir_real: str, cir_imag: str) -> str:
        from jinja2 import Template
        template_path = Path(__file__).parent.parent / "sims" / \
                        "docker-compose.replica.yaml.j2"
        tmpl = Template(template_path.read_text())
        return tmpl.render(
            replica_id=replica_id,
            cir_real_path=cir_real,
            cir_imag_path=cir_imag,
            port_base=PORT_BASE + replica_id * PORT_STRIDE,
        )

    def start_all(self, perturbation_sigma: float = 0.15) -> None:
        """Generate perturbed CIR files and start all N replicas."""
        import asyncssh, asyncio

        async def _deploy():
            async with asyncssh.connect(
                self.twin_host, username="zifanzhang", known_hosts=None
            ) as conn:
                await conn.run(f"mkdir -p {self.tmp_dir}")

                for i in range(self.n_replicas):
                    # Upload perturbed CIR files
                    rp = f"{self.tmp_dir}/cir_r{i}_real.txt"
                    ip = f"{self.tmp_dir}/cir_r{i}_imag.txt"
                    local_r = Path(f"/tmp/dt_cir_perturb_r{i}_real.txt")
                    local_i = Path(f"/tmp/dt_cir_perturb_r{i}_imag.txt")
                    perturb_cir_file(
                        self.base_cir_real, self.base_cir_imag,
                        local_r, local_i,
                        sigma=perturbation_sigma, seed=i,
                    )
                    await asyncssh.scp(str(local_r), f"{self.twin_host}:{rp}")
                    await asyncssh.scp(str(local_i), f"{self.twin_host}:{ip}")

                    # Render and upload compose file
                    compose_str = self._render_compose(i, rp, ip)
                    compose_path = f"{self.tmp_dir}/compose_r{i}.yaml"
                    async with conn.start_client_session() as sess:
                        await conn.run(
                            f"cat > {compose_path}",
                            input=compose_str,
                        )
                    await conn.run(
                        f"sudo docker compose -f {compose_path} up -d",
                        check=True,
                    )
                    print(f"  Replica {i} started (ports "
                          f"{PORT_BASE + i*PORT_STRIDE}-{PORT_BASE + i*PORT_STRIDE+2})")

        asyncio.run(_deploy())

    def stop_all(self) -> None:
        import asyncssh, asyncio

        async def _stop():
            async with asyncssh.connect(
                self.twin_host, username="zifanzhang", known_hosts=None
            ) as conn:
                for i in range(self.n_replicas):
                    compose_path = f"{self.tmp_dir}/compose_r{i}.yaml"
                    await conn.run(f"sudo docker compose -f {compose_path} down 2>/dev/null; true")
        asyncio.run(_stop())


# ── Policy Arbiter ────────────────────────────────────────────────────────────

class PolicyArbiter:

    def __init__(
        self,
        twin_host: str,
        real_host: str,
        n_replicas: int = 4,
        base_cir_real: Path = Path("logs/cir_offline/cir_ue4401_real.txt"),
        base_cir_imag: Path = Path("logs/cir_offline/cir_ue4401_imag.txt"),
        criterion: SafetyCriterion | None = None,
        perturbation_sigma: float = 0.15,
    ):
        self.twin_host  = twin_host
        self.real_host  = real_host
        self.n_replicas = n_replicas
        self.criterion  = criterion or SafetyCriterion()
        self.replica_mgr = ReplicaManager(
            twin_host, n_replicas, base_cir_real, base_cir_imag
        )
        self._zmq_ctx = zmq.Context()
        self._ran_index = 0

    def _send_policy(self, policy: Policy, host: str, port: int) -> None:
        sock = self._zmq_ctx.socket(zmq.PUB)
        sock.connect(f"tcp://{host}:{port}")
        time.sleep(0.05)   # let subscriber connect
        sock.send(policy.to_proto(self._ran_index))
        sock.close()

    def _collect_metrics(
        self, host: str, metrics_port: int, n_ttis: int
    ) -> list[dict]:
        """Subscribe to EdgeRIC metrics PUB for n_ttis samples."""
        sock = self._zmq_ctx.socket(zmq.SUB)
        sock.connect(f"tcp://{host}:{metrics_port}")
        sock.setsockopt(zmq.SUBSCRIBE, b"")
        sock.setsockopt(zmq.RCVTIMEO, 5000)

        samples = []
        for _ in range(n_ttis):
            try:
                raw = sock.recv()
                m = metrics_pb2.Metrics()
                m.ParseFromString(raw)
                for ue in m.ue_metrics:
                    samples.append({
                        "rnti":      ue.rnti,
                        "cqi":       ue.cqi,
                        "snr":       ue.snr,
                        "tx_bytes":  ue.tx_bytes,
                        "dl_buffer": ue.dl_buffer,
                    })
            except zmq.Again:
                break
        sock.close()
        return samples

    def _eval_replica(
        self, replica_id: int, policy: Policy
    ) -> ReplicaResult:
        metrics_port = PORT_BASE + replica_id * PORT_STRIDE
        weights_port = metrics_port + 1

        # Send policy to this replica
        self._send_policy(policy, self.twin_host, weights_port)

        # Collect metrics
        samples = self._collect_metrics(self.twin_host, metrics_port, EVAL_TTIS)
        if not samples:
            return ReplicaResult(replica_id, 1.0, 0.0, 0, False)

        # BLER proxy: fraction of TTIs with tx_bytes == 0 (no successful transmission)
        n_zero = sum(1 for s in samples if s["tx_bytes"] == 0)
        mean_bler = n_zero / len(samples)
        mean_tpt  = np.mean([s["tx_bytes"] * 8 / 1e6 for s in samples])   # Mbps

        passed = (mean_bler < self.criterion.bler_threshold and
                  mean_tpt  > self.criterion.tpt_threshold)
        return ReplicaResult(replica_id, mean_bler, mean_tpt, len(samples), passed)

    def evaluate(self, policy: Policy) -> EvalResult:
        """Evaluate policy across all N replicas in parallel."""
        self._ran_index += 1
        results: list[ReplicaResult] = [None] * self.n_replicas  # type: ignore

        def _worker(i):
            results[i] = self._eval_replica(i, policy)

        threads = [threading.Thread(target=_worker, args=(i,))
                   for i in range(self.n_replicas)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        pass_ratio = sum(r.passed for r in results) / self.n_replicas
        safe = pass_ratio >= self.criterion.min_pass_ratio
        return EvalResult(policy, results, safe, pass_ratio)

    def deploy(self, policy: Policy) -> None:
        """Send approved policy to the real EdgeRIC."""
        self._send_policy(policy, self.real_host, REAL_WEIGHTS_PORT)
        print(f"[Arbiter] Policy deployed to real network (ran_index={self._ran_index})")

    def start_replicas(self, sigma: float = 0.15) -> None:
        print(f"[Arbiter] Starting {self.n_replicas} twin replicas ...")
        self.replica_mgr.start_all(perturbation_sigma=sigma)

    def stop_replicas(self) -> None:
        self.replica_mgr.stop_all()


# ── CLI smoke test ─────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--twin-host",  required=True)
    parser.add_argument("--real-host",  required=True)
    parser.add_argument("--n-replicas", type=int, default=4)
    parser.add_argument("--cir-real",   default="logs/cir_offline/cir_ue4401_real.txt")
    parser.add_argument("--cir-imag",   default="logs/cir_offline/cir_ue4401_imag.txt")
    parser.add_argument("--sigma",      type=float, default=0.15,
                        help="CIR perturbation magnitude")
    args = parser.parse_args()

    arbiter = PolicyArbiter(
        twin_host=args.twin_host,
        real_host=args.real_host,
        n_replicas=args.n_replicas,
        base_cir_real=Path(args.cir_real),
        base_cir_imag=Path(args.cir_imag),
    )

    # Example: equal-weight policy for 2 UEs
    policy = Policy(weights={0x4401: 0.5, 0x4402: 0.5})

    arbiter.start_replicas(sigma=args.sigma)
    time.sleep(10)   # wait for replicas to come up

    result = arbiter.evaluate(policy)
    print(result.summary())

    if result.safe:
        arbiter.deploy(policy)
    else:
        print("[Arbiter] Policy REJECTED by safety filter.")

    arbiter.stop_replicas()
