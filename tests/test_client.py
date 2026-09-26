"""ClientConnection without a transport: status sequencing and the clock."""

from __future__ import annotations

import pytest

from awp import frames, jsonrpc
from awp.client import ClientConnection, ProtocolViolation
from awp.clock import ClockEstimator, sample
from awp.errors import AwpError, ErrorCode, ProtocolError

from .world import AGENT, MODALITIES, manifest, opened, ready


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


def frame(conn: ClientConnection, channel: str, seq: int) -> jsonrpc.Message:
    f = frames.Frame(
        conn.channels[channel], seq, seq * 1_000_000, b'{"p_m":[0,0,0],"v_mps":[0,0,0]}'
    )
    return jsonrpc.notification("obs.frame", frames.to_inline(f))


def test_a_new_session_starts_from_nothing_of_the_last():
    conn = opened("streaming", ["proprio"])
    for seq in (1, 2, 3):
        conn.receive(frame(conn, "proprio", seq))
    conn.receive(jsonrpc.result(conn.close(), {}))
    assert conn.ready is None  # nothing to acknowledge, resume, or stream for
    rid = conn.open_session("streaming", embodiment="arm_01", subscribe=["proprio"])
    conn.receive(jsonrpc.result(rid, ready("streaming", conn.outgoing()[-1]["params"])))
    events = conn.receive(frame(conn, "proprio", 1))
    assert not [e for e in events if isinstance(e, ProtocolViolation)]
    assert conn.delivery() == {"proprio": (1, 0)}


def test_a_multi_bind_session_names_the_embodiment_of_each_submission():
    conn = ClientConnection(AGENT, MODALITIES, clock_ns=lambda: 0)
    conn.receive(jsonrpc.result(conn.initialize(), manifest("streaming")))
    with pytest.raises(ValueError, match="not both"):
        conn.open_session("streaming", embodiment="arm_01", embodiments=["arm_01", "gripper_01"])
    rid = conn.open_session("streaming", embodiments=["arm_01", "gripper_01"])
    params = conn.outgoing()[-1]["params"]
    assert params["embodiments"] == ["arm_01", "gripper_01"]
    conn.receive(jsonrpc.result(rid, ready("streaming", params)))
    assert conn.embodiments == ["arm_01", "gripper_01"]
    with pytest.raises(AwpError) as exc:  # AWP-EMB-005
        conn.submit("stop", {})
    assert exc.value.code == ErrorCode.INVALID_PARAMS
    conn.submit("stop", {}, embodiment_id="arm_01")
    assert conn.outgoing()[-1]["params"]["embodiment_id"] == "arm_01"


def test_one_world_tick_at_a_time():
    conn = opened("lockstep", ["proprio"])
    conn.advance()
    with pytest.raises(ProtocolError, match="pending"):
        conn.advance()  # at a barrier the first may wait on other sessions (AWP-TIM-012)


def test_the_clock_maps_both_ways_through_the_best_sample():
    clock = ClockEstimator()
    clock.add(sample(100, 1_100, 1_100, 300))  # rtt 200
    clock.add(sample(100, 1_050, 1_050, 150))  # rtt 50: the one to trust
    assert (clock.offset_ns, clock.error_bound_ns) == (925, 25)
    assert clock.to_agent(clock.to_session(7_000)) == 7_000


def test_a_transferred_embodiment_is_no_longer_bound():
    conn = ClientConnection(AGENT, MODALITIES, clock_ns=lambda: 0)
    conn.receive(jsonrpc.result(conn.initialize(), manifest("streaming")))
    rid = conn.open_session("streaming", embodiments=["arm_01", "gripper_01"])
    conn.receive(jsonrpc.result(rid, ready("streaming", conn.outgoing()[-1]["params"])))
    seq = conn.last_status_seq + 1
    event = {"event": "embodiment_transferred", "status_seq": seq, "ts_mono_ns": 1}
    event["detail"] = {"embodiment": "arm_01"}
    conn.receive(jsonrpc.notification("world.event", event))
    assert conn.embodiments == ["gripper_01"]  # AWP-EMB-003
    conn.submit("stop", {})  # one embodiment left: embodiment_id may be left out
