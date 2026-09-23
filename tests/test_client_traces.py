"""Drive ClientConnection through every spec trace: it must emit the agent's messages and accept
the world's without flagging a violation."""

from __future__ import annotations

from typing import Any

import pytest

from awp import schema
from awp.client import ClientConnection, ProtocolViolation

from .conftest import load_trace

PREFIX_TO_EXECUTING = 14  # core-streaming lines 1..14 end with a-7 executing at status_seq 4
PREFIX_TO_ACTIVE = 11  # ... and lines 1..11 with the session active at status_seq 2


class FakeClock:
    def __init__(self) -> None:
        self.now = 0

    def __call__(self) -> int:
        return self.now


def strip(msg: dict[str, Any]) -> dict[str, Any]:
    return {k: v for k, v in msg.items() if k != "id"}


class Driver:
    def __init__(self) -> None:
        self.clock = FakeClock()
        self.client: ClientConnection | None = None
        self.ids: dict[Any, int] = {}
        self.violations: list[str] = []

    def agent(self, msg: dict[str, Any]) -> None:
        method, params = msg.get("method"), msg.get("params") or {}
        if method is None:  # the agent's answer to a world ping
            (pong,) = self.c.outgoing()
            assert pong["result"]["origin_ns"] == msg["result"]["origin_ns"]
            return
        if method == "initialize":
            if self.client is None:
                self.client = ClientConnection(
                    params["agent"], params["consumes_modalities"], clock_ns=self.clock
                )
            else:
                self.c.connection_lost()
            rid = self.c.initialize()
        elif method == "session.open":
            rid = self.c.open_session(**params)
        elif method == "session.resume":
            rid = self.c.resume()
        elif method == "session.close":
            rid = self.c.close()
        elif method == "ping":
            self.clock.now = params["origin_ns"]
            rid = self.c.ping(ack="last_status_seq" in params)
        elif method == "world.tick":
            rid = self.c.advance(params.get("count"))
        elif method == "action.cancel":
            rid = self.c.cancel(params["action_id"])
        elif method == "action.submit":
            known = self.c.actions.get(params["action_id"])
            if known is not None and known.content == params:
                rid = self.c.resubmit(params["action_id"])
            elif known is not None:
                rid = self.c.request("action.submit", params)
            else:
                extra = {
                    k: params[k]
                    for k in ("preempt", "deadline_ms", "valid_until_ns")
                    if k in params
                }
                self.c.submit(
                    params["type"],
                    params["params"],
                    action_id=params["action_id"],
                    basis=params.get("basis_ts_mono_ns"),
                    **extra,
                )
                rid = self.c.last_id
        elif method == "obs.report":
            return
        else:
            raise AssertionError(f"driver does not handle {method}")
        (out,) = self.c.outgoing()
        assert strip(out) == strip(msg)
        name = {
            "initialize": "agent-manifest",
            "session.open": "session-open",
            "ping": "ping",
            "action.submit": "action-submit",
            "world.tick": "tick",
        }.get(method)
        if name:
            assert schema.errors(name, out.get("params", {}), sender=True) == []
        self.ids[msg["id"]] = rid

    def world(self, entry: dict[str, Any]) -> list[Any]:
        if entry.get("delivered") is False:
            return []
        msg = dict(entry["msg"])
        if "method" not in msg:
            msg["id"] = self.ids[msg["id"]]
        events = self.c.receive(msg)
        self.violations += [e.detail for e in events if isinstance(e, ProtocolViolation)]
        return events

    @property
    def c(self) -> ClientConnection:
        assert self.client is not None
        return self.client

    def run(self, entries: list[dict[str, Any]]) -> list[Any]:
        events: list[Any] = []
        for e in entries:
            if "marker" in e:
                continue
            if e["from"] == "agent":
                self.agent(e["msg"])
            else:
                events += self.world(e)
        return events


@pytest.mark.parametrize("name", ["core-lockstep.jsonl", "core-streaming.jsonl"])
def test_core_traces(spec_dir, name):
    d = Driver()
    d.run(load_trace(name))
    assert d.violations == []
    assert d.c.session_state == "closed"


@pytest.mark.parametrize(
    ("name", "prefix"),
    [
        ("cancel.jsonl", PREFIX_TO_EXECUTING),
        ("disconnect.jsonl", PREFIX_TO_EXECUTING),
        ("retry.jsonl", PREFIX_TO_EXECUTING),
        ("estop.jsonl", PREFIX_TO_EXECUTING),
        ("quiet-agent.jsonl", PREFIX_TO_EXECUTING),
        ("approval.jsonl", PREFIX_TO_ACTIVE),
    ],
)
def test_continuation_traces(spec_dir, name, prefix):
    d = Driver()
    d.run(load_trace("core-streaming.jsonl")[:prefix])
    d.run(load_trace(name))
    assert d.violations == []


def test_lockstep_state(spec_dir):
    d = Driver()
    d.run(load_trace("core-lockstep.jsonl"))
    a1 = d.c.actions["a-1"]
    assert (a1.state, a1.progress, d.c.tick, d.c.last_status_seq) == ("completed", 1, 2, 6)


def test_retry_replays_lost_admission_and_resubmits_idempotently(spec_dir):
    d = Driver()
    d.run(load_trace("core-streaming.jsonl")[:PREFIX_TO_EXECUTING])
    events = d.run(load_trace("retry.jsonl"))
    replayed = [e for e in events if getattr(e, "replayed", False)]
    assert [e.status["status_seq"] for e in replayed if hasattr(e, "status")] == [5]
    assert d.c.actions["a-8"].state == "executing"
    assert d.c.last_status_seq == 10


def test_disconnect_marks_replay_complete(spec_dir):
    from awp.client import ReplayCompleted

    d = Driver()
    d.run(load_trace("core-streaming.jsonl")[:PREFIX_TO_EXECUTING])
    events = d.run(load_trace("disconnect.jsonl"))
    assert [e.status_seq for e in events if isinstance(e, ReplayCompleted)] == [7]
    assert d.c.actions["a-7"].reason == "connection_lost"
