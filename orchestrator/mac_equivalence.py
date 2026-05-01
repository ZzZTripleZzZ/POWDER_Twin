"""
M4 — Provable Per-UE MAC-State Equivalence

Theorem (M4): For any wall-clock time τ and UE u, the per-UE skew between
the real gNB's MAC state and the twin orchestrator's metrics-layer view
satisfies:

    Δ ≤ K + ⌈(R + S) / T_TTI⌉

where:
  K  = reconciliation interval (TTIs between REQ/REP exchanges)
  R  = reconciliation REQ/REP RTT upper bound (ms)
  S  = real-side protobuf serialize + ZMQ one-way latency upper bound (ms)
  T_TTI = 1 ms (5G NR numerology 0)

The claim scopes to the orchestrator's metrics-layer view (protobuf data
forwarded by layer2_mac_sync), not OAI's internal C structures.

Protocol:
  Real side — MacReconciler: bind REP on port 5560; on each recv:
    - compare twin's global_state_hash against latest forwarded metrics
    - if matched: respond hash_matched=True (no data transfer)
    - if mismatch: respond with full UE state + updated epoch

  Twin side — MacReconcilerClient: connect REQ to real:5560 every K TTIs:
    - compute twin's current hash; send request
    - on mismatch response: apply real-side diff, record empirical Δ

E5 evaluation: inject artificial ZMQ delays, verify p99(Δ) ≤ bound.
"""

from __future__ import annotations

import hashlib
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import numpy as np
import zmq

sys.path.insert(0, str(Path(__file__).parent.parent / "proto"))
import metrics_pb2
import reconciliation_pb2


# ── State hashing ─────────────────────────────────────────────────────────────

def hash_ue_state(ue: metrics_pb2.UeMetrics) -> bytes:
    """SHA-256[:16] of canonical per-UE MAC state representation.

    Uses only fields that matter for scheduler decisions and HARQ correctness:
    rnti, cqi, harq_state vector, bsr_kbytes, scheduler_queue_age.
    """
    h = hashlib.sha256()
    h.update(ue.rnti.to_bytes(4, "little"))
    h.update(ue.cqi.to_bytes(4, "little"))
    h.update(ue.bsr_kbytes.to_bytes(4, "little"))
    h.update(ue.scheduler_queue_age.to_bytes(4, "little"))
    for s in ue.harq_state:
        h.update(s.to_bytes(4, "little"))
    return h.digest()[:16]


def hash_global_state(metrics: metrics_pb2.Metrics) -> bytes:
    """SHA-256[:16] of all per-UE hashes sorted by RNTI (deterministic ordering)."""
    h = hashlib.sha256()
    for ue in sorted(metrics.ue_metrics, key=lambda u: u.rnti):
        ue_hash = ue.ue_state_hash if ue.ue_state_hash else hash_ue_state(ue)
        h.update(ue_hash)
    return h.digest()[:16]


def stamp_metrics(metrics: metrics_pb2.Metrics, tti_seq: int, epoch: int) -> None:
    """Mutate metrics in-place: fill tti_seq, wall_clock_us, per-UE and global hashes."""
    metrics.tti_seq          = tti_seq
    metrics.wall_clock_us    = int(time.time() * 1e6)
    metrics.reconcile_epoch  = epoch
    for ue in metrics.ue_metrics:
        ue.ue_state_hash = hash_ue_state(ue)
    metrics.global_state_hash = hash_global_state(metrics)


# ── Stats ─────────────────────────────────────────────────────────────────────

@dataclass
class ReconcileStats:
    epochs:           int  = 0
    matches:          int  = 0
    mismatches:       int  = 0
    delta_history:    list = field(default_factory=list)  # observed Δ in TTIs
    rtt_history_ms:   list = field(default_factory=list)  # empirical round-trip times
    last_wall_clock_us: int = 0

    def empirical_delta_p99(self) -> float:
        if not self.delta_history:
            return 0.0
        return float(np.percentile(self.delta_history, 99))

    def mean_rtt_ms(self) -> float:
        if not self.rtt_history_ms:
            return float("nan")
        return float(np.mean(self.rtt_history_ms))

    def summary(self) -> str:
        return (
            f"epochs={self.epochs} matched={self.matches} "
            f"mismatched={self.mismatches} "
            f"p99Δ={self.empirical_delta_p99():.0f}TTI "
            f"mean_rtt={self.mean_rtt_ms():.1f}ms"
        )


# ── Real-side reconciliation server ──────────────────────────────────────────

class MacReconciler:
    """Real-side REP socket server (runs in layer2_mac_sync loop).

    Maintains the most-recently-stamped Metrics protobuf as the canonical
    source of truth. Each `serve_one()` call is non-blocking; it should be
    called once per layer2_mac_sync iteration.
    """

    def __init__(self, bind_endpoint: str = "tcp://*:5560"):
        ctx = zmq.Context.instance()
        self._rep = ctx.socket(zmq.REP)
        self._rep.bind(bind_endpoint)
        self._rep.setsockopt(zmq.RCVTIMEO, 0)   # non-blocking
        self._latest: Optional[metrics_pb2.Metrics] = None
        self._epoch  = 0
        self.stats   = ReconcileStats()

    @property
    def epoch(self) -> int:
        return self._epoch

    def update(self, metrics: metrics_pb2.Metrics) -> None:
        """Called from layer2_mac_sync after stamp_metrics(); stores reference."""
        self._latest = metrics

    def serve_one(self) -> bool:
        """Serve at most one reconciliation request. Non-blocking; returns True if served."""
        try:
            raw = self._rep.recv(zmq.NOBLOCK)
        except zmq.Again:
            return False
        except zmq.ZMQError:
            return False

        req = reconciliation_pb2.ReconciliationRequest()
        req.ParseFromString(raw)

        resp = reconciliation_pb2.ReconciliationResponse()
        resp.reconcile_epoch = self._epoch
        resp.wall_clock_us   = int(time.time() * 1e6)

        if self._latest is None:
            resp.hash_matched = False
        elif req.twin_global_hash == self._latest.global_state_hash:
            resp.hash_matched     = True
            resp.real_tti_seq     = self._latest.tti_seq
            resp.real_global_hash = self._latest.global_state_hash
        else:
            resp.hash_matched     = False
            resp.real_tti_seq     = self._latest.tti_seq
            resp.real_global_hash = self._latest.global_state_hash
            # Encode full UE state into UeState submessages
            for ue in self._latest.ue_metrics:
                us = resp.full_state.add()
                us.rnti                = ue.rnti
                us.cqi                 = ue.cqi
                us.harq_state[:]       = list(ue.harq_state)
                us.scheduler_queue_age = ue.scheduler_queue_age
                us.bsr_kbytes          = ue.bsr_kbytes
                us.ue_state_hash       = ue.ue_state_hash
            self._epoch += 1
            resp.reconcile_epoch = self._epoch

        self._rep.send(resp.SerializeToString())

        self.stats.epochs += 1
        if resp.hash_matched:
            self.stats.matches += 1
        else:
            self.stats.mismatches += 1
        return True


# ── Twin-side reconciliation client ──────────────────────────────────────────

class MacReconcilerClient:
    """Twin-side REQ client (runs on twin host or via SSH tunnel from Mac).

    Connects to real-side MacReconciler on port 5560.
    `reconcile(twin_state)` should be called every K TTIs; it blocks for at
    most `timeout_ms` waiting for the response.

    State claim: `twin_state` is the orchestrator's metrics-layer view (the
    Metrics protobuf most recently forwarded by layer2_mac_sync), NOT OAI's
    internal C state.  The skew bound Δ ≤ K + ⌈(R+S)/T_TTI⌉ applies to this
    metrics-layer view.
    """

    def __init__(
        self,
        real_endpoint:  str   = "tcp://powder-gnb:5560",
        timeout_ms:     int   = 500,
    ):
        ctx = zmq.Context.instance()
        self._req = ctx.socket(zmq.REQ)
        self._req.connect(real_endpoint)
        self._req.setsockopt(zmq.RCVTIMEO, timeout_ms)
        self.stats = ReconcileStats()

    def reconcile(
        self,
        twin_state: metrics_pb2.Metrics,
    ) -> Optional[int]:
        """Send reconciliation request; apply diff on mismatch.

        Returns:
          0    — hash matched, twin view in sync
          > 0  — Δ (TTIs) on mismatch; twin_state mutated with real full_state
          None — timeout or ZMQ error
        """
        sent_us = int(time.time() * 1e6)
        req = reconciliation_pb2.ReconciliationRequest(
            twin_tti_seq     = twin_state.tti_seq,
            twin_global_hash = twin_state.global_state_hash,
            reconcile_epoch  = twin_state.reconcile_epoch,
            wall_clock_us    = sent_us,
        )
        self._req.send(req.SerializeToString())

        try:
            raw = self._req.recv()
        except zmq.Again:
            return None
        except zmq.ZMQError:
            return None

        recv_us = int(time.time() * 1e6)
        resp = reconciliation_pb2.ReconciliationResponse()
        resp.ParseFromString(raw)

        rtt_ms = (recv_us - sent_us) / 1e3
        self.stats.rtt_history_ms.append(rtt_ms)
        self.stats.epochs += 1

        if resp.hash_matched:
            self.stats.matches += 1
            return 0

        # Apply real-side diff to twin orchestrator's local view
        delta = int(resp.real_tti_seq) - int(twin_state.tti_seq)
        delta = max(delta, 0)

        # Update twin metrics-layer view from real full_state
        del twin_state.ue_metrics[:]
        for us in resp.full_state:
            ue = twin_state.ue_metrics.add()
            ue.rnti                = us.rnti
            ue.cqi                 = us.cqi
            ue.harq_state[:]       = list(us.harq_state)
            ue.scheduler_queue_age = us.scheduler_queue_age
            ue.bsr_kbytes          = us.bsr_kbytes
            ue.ue_state_hash       = us.ue_state_hash

        twin_state.tti_seq           = resp.real_tti_seq
        twin_state.global_state_hash = resp.real_global_hash
        twin_state.reconcile_epoch   = resp.reconcile_epoch

        self.stats.mismatches += 1
        self.stats.delta_history.append(delta)
        return delta


# ── Theorem instantiation helper ──────────────────────────────────────────────

def derive_delta_bound(
    K:            int,
    rtt_ms:       float,
    serialize_ms: float,
    tti_ms:       float = 1.0,
) -> float:
    """Theorem M4 closed-form bound. Returns Δ upper bound in TTIs.

    Args:
        K:            reconciliation interval (TTIs)
        rtt_ms:       REQ/REP round-trip latency upper bound (ms)
        serialize_ms: real-side serialize + ZMQ one-way latency upper bound (ms)
        tti_ms:       TTI duration in ms (1.0 for 5G NR numerology 0)
    """
    import math
    return K + math.ceil((rtt_ms + serialize_ms) / tti_ms)


# ── Smoke test ────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    # Verify hash functions
    m = metrics_pb2.Metrics()
    m.tti_seq = 100
    ue = m.ue_metrics.add()
    ue.rnti = 0x4401; ue.cqi = 10
    ue.harq_state.extend([0, 1, 0, 0, 0, 0, 0, 0])
    ue.bsr_kbytes = 5; ue.scheduler_queue_age = 12

    h1 = hash_ue_state(ue)
    assert len(h1) == 16, "hash should be 16 bytes"

    stamp_metrics(m, tti_seq=42, epoch=1)
    assert m.tti_seq == 42
    assert m.reconcile_epoch == 1
    assert len(m.global_state_hash) == 16

    # Theorem bound: POWDER K=100, RTT=2ms, S=0.5ms → 103
    bound = derive_delta_bound(K=100, rtt_ms=2.0, serialize_ms=0.5)
    assert bound == 103, f"Expected 103, got {bound}"

    # Changing CQI should change hash
    h_before = hash_ue_state(ue)
    ue.cqi = 15
    h_after  = hash_ue_state(ue)
    assert h_before != h_after, "Hash should change on CQI change"

    print(f"Theorem M4 bound (K=100, R=2ms, S=0.5ms): Δ ≤ {bound:.0f} TTI")
    print("mac_equivalence smoke test passed.")
