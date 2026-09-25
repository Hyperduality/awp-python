"""Evidence for the requirement-matrix rows the conformance suite marks `manual` for the agent.

conformance/README.md cites each test here. AWP-DAT-008 is tests/test_frames.py.
"""

from __future__ import annotations

import pytest

from awp import jsonrpc
from awp.client import FrameReceived

from .world import opened

# ------------------------------------------------------------ AWP-DAT-001, AWP-DAT-009


@pytest.mark.parametrize("mode", ["lockstep", "streaming"])
def test_the_receiver_counts_seq_gaps_as_loss_but_not_the_gap_before_a_resync(mode):
    conn = opened(mode, ["proprio", "arm_state"])
    ids = conn.channels

    def deliver(channel: str, seq: int, *, resync: bool = False) -> None:
        params = {
            "channel_id": ids[channel],
            "seq": seq,
            "ts_mono_ns": 0,
            "flags": 0x09 if resync else 0x01,
            "payload_b64": "e30=",
            **({"tick": 0} if mode == "lockstep" else {"ts_send_ns": 0}),
        }
        events = conn.receive(jsonrpc.notification("obs.frame", params))
        assert [type(e) for e in events] == [FrameReceived]

    for seq in (1, 2, 5):  # seqs 3 and 4 are lost
        deliver("proprio", seq)
    deliver("arm_state", 1)
    deliver("arm_state", 7, resync=True)  # a discontinuity the sender knew of
    assert conn.delivery() == {"proprio": (3, 2), "arm_state": (2, 0)}
    if mode == "streaming":  # and so the receiver report states (AWP-OBS-007)
        report = conn.report()["params"]["channels"]
        assert report[str(ids["proprio"])]["gaps"] == 2
        assert report[str(ids["arm_state"])]["gaps"] == 0
