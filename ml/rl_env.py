"""
W8 — Gym Environment wrapping Twin EdgeRIC (Contribution 3).

Observation: per-UE [CQI, SNR, dl_buffer, tx_bytes_norm, bler_proxy]
             flattened → shape (N_UE * 5,)
Action:      per-UE scheduling weight, shape (N_UE,), continuous [0, 1]
             (normalized internally before sending to EdgeRIC)
Reward:      proportional-fairness: Σ log(1 + throughput_i) - λ * Σ BLER_i

The environment talks to a single twin replica's EdgeRIC via ZMQ.
For training, the twin IS the environment (no real network involved).
"""
import sys
import time
from pathlib import Path
from typing import Any

import numpy as np
import zmq

try:
    import gymnasium as gym
    from gymnasium import spaces
except ImportError:
    import gym
    from gym import spaces

sys.path.insert(0, str(Path(__file__).parent.parent / "proto"))
import metrics_pb2
import control_weights_pb2


class TwinRanEnv(gym.Env):

    metadata = {"render_modes": []}

    def __init__(
        self,
        twin_host: str = "powder-twin",
        metrics_port: int = 5555,
        weights_port: int = 5556,
        n_ue: int = 1,
        step_ttis: int = 10,          # TTIs between actions
        bler_penalty: float = 0.5,    # λ
        max_steps: int = 1000,
    ):
        super().__init__()
        self.twin_host    = twin_host
        self.metrics_port = metrics_port
        self.weights_port = weights_port
        self.n_ue         = n_ue
        self.step_ttis    = step_ttis
        self.bler_penalty = bler_penalty
        self.max_steps    = max_steps

        self._ctx  = zmq.Context()
        self._msub = None   # metrics subscriber
        self._wpub = None   # weights publisher
        self._step = 0
        self._rntis: list[int] = []

        # obs: N_UE × [cqi_norm, snr_norm, dl_buf_norm, tpt_norm, bler]
        self.observation_space = spaces.Box(
            low=0.0, high=1.0, shape=(n_ue * 5,), dtype=np.float32
        )
        # action: N_UE weights in [0, 1]
        self.action_space = spaces.Box(
            low=0.0, high=1.0, shape=(n_ue,), dtype=np.float32
        )

    def _connect(self):
        if self._msub is None:
            self._msub = self._ctx.socket(zmq.SUB)
            self._msub.connect(f"tcp://{self.twin_host}:{self.metrics_port}")
            self._msub.setsockopt(zmq.SUBSCRIBE, b"")
            self._msub.setsockopt(zmq.CONFLATE, 1)
            self._msub.setsockopt(zmq.RCVTIMEO, 3000)

        if self._wpub is None:
            self._wpub = self._ctx.socket(zmq.PUB)
            self._wpub.connect(f"tcp://{self.twin_host}:{self.weights_port}")
            time.sleep(0.1)

    def _recv_metrics(self) -> dict[int, dict]:
        """Receive one metrics protobuf message, return {rnti: {...}}."""
        try:
            raw = self._msub.recv()
            m = metrics_pb2.Metrics()
            m.ParseFromString(raw)
            return {
                ue.rnti: {
                    "cqi":      ue.cqi,
                    "snr":      ue.snr,
                    "tx_bytes": ue.tx_bytes,
                    "dl_buf":   ue.dl_buffer,
                }
                for ue in m.ue_metrics
            }
        except zmq.Again:
            return {}

    def _send_weights(self, weights: np.ndarray, rntis: list[int], ran_index: int):
        msg = control_weights_pb2.SchedulingWeights()
        msg.ran_index = ran_index
        for rnti, w in zip(rntis, weights):
            msg.weights.append(float(rnti))
            msg.weights.append(float(w))
        self._wpub.send(msg.SerializeToString())

    def _obs_and_info(self, metrics: dict[int, dict]) -> tuple[np.ndarray, dict]:
        obs = np.zeros(self.n_ue * 5, dtype=np.float32)
        if not self._rntis and metrics:
            self._rntis = sorted(metrics.keys())[:self.n_ue]

        for i, rnti in enumerate(self._rntis[:self.n_ue]):
            m = metrics.get(rnti, {})
            base = i * 5
            obs[base + 0] = m.get("cqi", 0) / 15.0              # CQI [0,15]
            obs[base + 1] = np.clip(m.get("snr", 0) / 30.0, 0, 1)  # SNR ~[0,30]dB
            obs[base + 2] = np.clip(m.get("dl_buf", 0) / 1e6, 0, 1) # buffer [0,1MB]
            obs[base + 3] = np.clip(m.get("tx_bytes", 0) / 1e5, 0, 1)
            # BLER proxy: 1 if no bytes transmitted
            obs[base + 4] = 1.0 if m.get("tx_bytes", 0) == 0 else 0.0

        return obs, {"rntis": self._rntis, "raw_metrics": metrics}

    def _compute_reward(self, metrics: dict[int, dict]) -> float:
        reward = 0.0
        for rnti in self._rntis[:self.n_ue]:
            m = metrics.get(rnti, {})
            tpt  = m.get("tx_bytes", 0) * 8 / 1e6   # Mbps
            bler = 1.0 if m.get("tx_bytes", 0) == 0 else 0.0
            reward += np.log1p(tpt) - self.bler_penalty * bler
        return float(reward)

    def reset(self, *, seed=None, options=None):
        super().reset(seed=seed)
        self._connect()
        self._step = 0
        self._rntis = []
        metrics = self._recv_metrics()
        obs, info = self._obs_and_info(metrics)
        return obs, info

    def step(self, action: np.ndarray):
        # Normalize action weights
        action = np.clip(action, 0, 1)
        if action.sum() < 1e-6:
            action = np.ones_like(action)
        action = action / action.sum()

        self._send_weights(action, self._rntis[:self.n_ue], ran_index=self._step)

        # Collect metrics over step_ttis TTIs
        all_metrics: list[dict] = []
        for _ in range(self.step_ttis):
            m = self._recv_metrics()
            if m:
                all_metrics.append(m)

        # Aggregate: mean per UE over collected TTIs
        agg: dict[int, dict] = {}
        for rnti in self._rntis[:self.n_ue]:
            vals = [m[rnti] for m in all_metrics if rnti in m]
            if vals:
                agg[rnti] = {
                    k: np.mean([v[k] for v in vals])
                    for k in ("cqi", "snr", "tx_bytes", "dl_buf")
                }

        obs, info = self._obs_and_info(agg)
        reward     = self._compute_reward(agg)
        self._step += 1
        terminated = False
        truncated  = self._step >= self.max_steps
        return obs, reward, terminated, truncated, info

    def close(self):
        if self._msub:
            self._msub.close()
        if self._wpub:
            self._wpub.close()
