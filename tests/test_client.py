"""ClientConnection without a transport: status sequencing and the clock."""

from __future__ import annotations

from awp import jsonrpc
from awp.client import ClientConnection, ProtocolViolation

from .world import AGENT, MODALITIES, opened


def test_pre_session_pings_do_not_feed_the_clock():
    conn = ClientConnection(AGENT, MODALITIES, clock_ns=lambda: 1_000)
    rid = conn.ping()
    conn.receive(jsonrpc.result(rid, {"origin_ns": 1_000, "receive_ns": 5, "transmit_ns": 5}))
    assert conn.clock.samples == 0


def test_a_status_gap_is_not_acknowledged():
    conn = opened("streaming", ["proprio"])
    base = conn.last_status_seq

    def state(seq):
        params = {"state": "active", "status_seq": seq, "ts_mono_ns": 1, "reason": "resumed"}
        return jsonrpc.notification("session.state", params)

    events = conn.receive(state(base + 2))
    assert any(isinstance(e, ProtocolViolation) for e in events)
    assert conn.last_status_seq == base
    conn.receive(state(base + 1))
    assert conn.last_status_seq == base + 2
    assert conn.receive(state(base + 2)) == []  # redelivery (AWP-LIF-009)
