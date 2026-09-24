"""The agent side of AWP as a sans-IO state machine.

`ClientConnection` never touches a socket or a timer. Callers feed it decoded messages with
`receive()`, send whatever `outgoing()` returns, and call the request methods to act. Every
request method returns the JSON-RPC id it used.
"""

from __future__ import annotations

import time
import uuid
from collections import deque
from collections.abc import Callable, Iterable
from dataclasses import dataclass, field
from typing import Any

from . import frames, jsonrpc, schema
from .clock import ClockEstimator, sample
from .errors import AwpError, ErrorCode, ProtocolError
from .jsonrpc import Message
from .lifecycle import ActionState, permitted

PROTOCOL_VERSIONS = ("0.1",)

# ---------------------------------------------------------------------------- events


@dataclass(frozen=True, slots=True)
class Initialized:
    manifest: dict[str, Any]


@dataclass(frozen=True, slots=True)
class SessionOpened:
    ready: dict[str, Any]
    resumed: bool


@dataclass(frozen=True, slots=True)
class SessionStateChanged:
    state: str
    reason: str | None
    replayed: bool


@dataclass(frozen=True, slots=True)
class ActionUpdated:
    action: ActionRecord
    status: dict[str, Any]
    replayed: bool


@dataclass(frozen=True, slots=True)
class WorldEvent:
    event: str
    params: dict[str, Any]
    replayed: bool


@dataclass(frozen=True, slots=True)
class FrameReceived:
    channel: str
    frame: frames.Frame
    received_ns: int


@dataclass(frozen=True, slots=True)
class Telemetry:
    params: dict[str, Any]


@dataclass(frozen=True, slots=True)
class TickCompleted:
    tick: int


@dataclass(frozen=True, slots=True)
class ReplayCompleted:
    status_seq: int


@dataclass(frozen=True, slots=True)
class Response:
    id: int
    method: str
    result: dict[str, Any]


@dataclass(frozen=True, slots=True)
class ErrorResponse:
    id: int
    method: str
    error: AwpError
    action_id: str | None = None


@dataclass(frozen=True, slots=True)
class ProtocolViolation:
    """The world broke the specification. Informational; the connection stays usable."""

    detail: str


Event = (
    Initialized
    | SessionOpened
    | SessionStateChanged
    | ActionUpdated
    | WorldEvent
    | FrameReceived
    | Telemetry
    | TickCompleted
    | ReplayCompleted
    | Response
    | ErrorResponse
    | ProtocolViolation
)


# ---------------------------------------------------------------------------- state


@dataclass(slots=True)
class ActionRecord:
    action_id: str
    content: dict[str, Any]
    state: ActionState = ActionState.SUBMITTED
    status_seq: int = 0
    status: dict[str, Any] = field(default_factory=dict)

    @property
    def terminal(self) -> bool:
        return self.state.terminal

    @property
    def reason(self) -> str | None:
        return self.status.get("reason")

    @property
    def progress(self) -> float | None:
        return self.status.get("progress")


@dataclass(slots=True)
class _ChannelStats:
    frames: int = 0
    gaps: int = 0
    last_seq: int | None = None
    last_transit: int | None = None
    jitter_sum: int = 0
    jitter_n: int = 0
    staleness: deque[int] = field(default_factory=lambda: deque(maxlen=_SAMPLES))


_SAMPLES = 4096  # latency samples kept between reports


def _stats(values: Iterable[int]) -> dict[str, int] | None:
    if not values:
        return None
    ordered = sorted(values)

    def pct(p: float) -> int:
        return ordered[min(len(ordered) - 1, round(p * (len(ordered) - 1)))]

    return {"count": len(ordered), "p50": pct(0.5), "p95": pct(0.95), "max": ordered[-1]}


# ---------------------------------------------------------------------------- connection


class ClientConnection:
    def __init__(
        self,
        agent: dict[str, str],
        consumes_modalities: Iterable[str],
        *,
        time_models: Iterable[str] | None = None,
        clock_ns: Callable[[], int] = time.monotonic_ns,
        validate: bool = True,
    ) -> None:
        self.agent = agent
        self.consumes_modalities = list(consumes_modalities)
        self.time_models = list(time_models) if time_models else None
        self.clock_ns = clock_ns
        self.validate = validate

        self.manifest: dict[str, Any] | None = None
        self.ready: dict[str, Any] | None = None
        self.session_state: str | None = None
        self.tick: int | None = None
        self.last_status_seq = 0  # every status_seq up to here has been processed (the ack point)
        self.actions: dict[str, ActionRecord] = {}
        self.clock = ClockEstimator()

        self._out: list[Message] = []
        self._next_id = 1
        self._pending: dict[int, tuple[str, dict[str, Any]]] = {}
        self._replay_to: int | None = None
        self._seen_above: set[int] = set()  # processed status_seqs beyond a gap
        self._session_pings: set[int] = set()
        self._channel_names: dict[int, str] = {}
        self._channel_stats: dict[int, _ChannelStats] = {}
        self._receipts: deque[tuple[int, int]] = deque(maxlen=512)  # (ts_mono_ns, agent receipt)
        self._decision_latency: deque[int] = deque(maxlen=_SAMPLES)
        self._last_report_ns: int | None = None

    # ------------------------------------------------------------ plumbing

    def outgoing(self) -> list[Message]:
        """Drain the messages to send, in order."""
        out, self._out = self._out, []
        return out

    @property
    def last_id(self) -> int:
        """The id of the most recent request."""
        return self._next_id - 1

    def request(self, method: str, params: dict[str, Any] | None = None) -> int:
        rid = self._next_id
        self._next_id += 1
        self._pending[rid] = (method, params or {})
        self._out.append(jsonrpc.request(rid, method, params))
        return rid

    def forget(self, rid: int) -> None:
        """Stop waiting for the response to `rid` (e.g. after a timeout)."""
        self._pending.pop(rid, None)
        self._session_pings.discard(rid)

    def connection_lost(self) -> None:
        """The transport closed. Unanswered requests will never be answered on this connection."""
        self._pending.clear()
        self._session_pings.clear()
        if self.session_state not in (None, "closed"):
            self.session_state = "suspended"

    @property
    def replaying(self) -> bool:
        """True between a session.resume result and the end of its replay."""
        return self._replay_to is not None

    @property
    def session_token(self) -> str | None:
        return self.ready["session_token"] if self.ready else None

    @property
    def channels(self) -> dict[str, int]:
        """Granted channel name → channel_id."""
        return {name: cid for cid, name in self._channel_names.items()}

    # ------------------------------------------------------------ requests

    def initialize(self) -> int:
        params: dict[str, Any] = {
            "protocol_versions": list(PROTOCOL_VERSIONS),
            "agent": self.agent,
            "consumes_modalities": self.consumes_modalities,
        }
        if self.time_models:
            params["time_models"] = self.time_models
        return self.request("initialize", params)

    def open_session(
        self,
        mode: str,
        *,
        embodiment: str | None = None,
        subscribe: Iterable[dict[str, Any] | str] = (),
        action_types: Iterable[str] | None = None,
        admin: Iterable[str] | None = None,
        **extra: Any,
    ) -> int:
        if self.manifest is None:
            raise ProtocolError("initialize first")
        params: dict[str, Any] = {"mode": mode, **extra}
        if embodiment is not None:
            params["embodiment"] = embodiment
        subs = [s if isinstance(s, dict) else {"channel": s} for s in subscribe]
        if subs:
            params["subscribe"] = subs
        if action_types is not None:
            params["action_types"] = list(action_types)
        if admin is not None:
            params["admin"] = list(admin)
        return self.request("session.open", params)

    def resume(self) -> int:
        if self.ready is None:
            raise ProtocolError("no session to resume")
        return self.request(
            "session.resume",
            {"session_token": self.ready["session_token"], "last_status_seq": self.last_status_seq},
        )

    def close(self) -> int:
        return self.request("session.close")

    def submit(
        self,
        type: str,
        params: dict[str, Any],
        *,
        action_id: str | None = None,
        preempt: str | None = None,
        deadline_ms: int | None = None,
        basis: frames.Frame | int | None = None,
        valid_until_ns: int | None = None,
        valid_for_ms: float | None = None,
        embodiment_id: str | None = None,
    ) -> str:
        """Submit an action; returns its action_id. The admission arrives as a Response.

        `basis` is the observation the intent rests on (a frame or its `ts_mono_ns`). The validity
        window is either an absolute session-clock `valid_until_ns` or `valid_for_ms` from now,
        mapped through the clock offset (AWP-CLK-009).
        """
        action_id = action_id or f"a-{uuid.uuid4().hex[:16]}"
        if action_id in self.actions:
            raise ValueError(f"action_id {action_id} already used; use resubmit() to retry")
        now = self.clock_ns()
        content: dict[str, Any] = {"action_id": action_id, "type": type, "params": params}
        if embodiment_id is not None:
            content["embodiment_id"] = embodiment_id
        if preempt is not None:
            content["preempt"] = preempt
        if deadline_ms is not None:
            content["deadline_ms"] = deadline_ms
        if basis is not None:
            basis_ts = basis.ts_mono_ns if isinstance(basis, frames.Frame) else basis
            content["basis_ts_mono_ns"] = basis_ts
            received = self._receipt_of(basis_ts)
            if received is not None:
                self._decision_latency.append(now - received)
        if valid_until_ns is not None:
            content["valid_until_ns"] = valid_until_ns
        elif valid_for_ms is not None:
            content["valid_until_ns"] = self.clock.to_session(now) + int(valid_for_ms * 1e6)
        self.actions[action_id] = ActionRecord(action_id, content)
        self.request("action.submit", content)
        return action_id

    def resubmit(self, action_id: str) -> int:
        """Retry a submission with identical content. While the world retains the action it is
        not executed again (AWP-ACT-001, AWP-ACT-006)."""
        return self.request("action.submit", self.actions[action_id].content)

    def cancel(self, action_id: str) -> int:
        return self.request("action.cancel", {"action_id": action_id})

    def pull_status(self, action_id: str) -> int:
        return self.request("action.status", {"action_id": action_id})

    def advance(self, count: int | None = None) -> int:
        """world.tick from the tick the agent holds (AWP-TIM-011). Never implicit (AWP-AGT-009)."""
        if self.tick is None:
            raise ProtocolError("not a lockstep session")
        params: dict[str, Any] = {"expected_tick": self.tick}
        if count is not None:
            params["count"] = count
        return self.request("world.tick", params)

    def subscribe(self, channels: Iterable[dict[str, Any] | str]) -> int:
        subs = [c if isinstance(c, dict) else {"channel": c} for c in channels]
        return self.request("obs.subscribe", {"channels": subs})

    def unsubscribe(self, channels: Iterable[str]) -> int:
        return self.request("obs.unsubscribe", {"channels": list(channels)})

    def ping(self, *, ack: bool = True) -> int:
        """A heartbeat and clock-sync sample; with `ack`, acknowledges delivery (AWP-CTL-010)."""
        now = self.clock_ns()
        params: dict[str, Any] = {"origin_ns": now}
        if ack and self.ready is not None:
            params["last_status_seq"] = self.last_status_seq
        rid = self.request("ping", params)
        if self.ready is not None and "tick" not in self.ready:
            # Only streaming pongs are stamped on a clock the offset can track (AWP-CLK-008).
            self._session_pings.add(rid)
        return rid

    def report(self) -> Message:
        """Queue an obs.report covering the time since the previous one (AWP-OBS-007)."""
        now = self.clock_ns()
        best = self.clock.best
        if best is None:
            raise ProtocolError("no clock sample yet")
        since = self._last_report_ns if self._last_report_ns is not None else now - 1_000_000_000
        channels: dict[str, Any] = {}
        for cid, st in self._channel_stats.items():
            entry: dict[str, Any] = {"frames": st.frames, "gaps": st.gaps}
            if st.jitter_n:
                entry["jitter_ns"] = st.jitter_sum // st.jitter_n
            staleness = _stats(st.staleness)
            if staleness:
                entry["staleness_ns"] = staleness
            channels[str(cid)] = entry
            self._channel_stats[cid] = _ChannelStats(
                last_seq=st.last_seq, last_transit=st.last_transit
            )
        params: dict[str, Any] = {
            "window_ms": max(1, (now - since) // 1_000_000),
            "sync": {
                "offset_ns": best.offset_ns,
                "rtt_ns": best.rtt_ns,
                "samples": self.clock.samples,
            },
            "channels": channels,
        }
        decision = _stats(self._decision_latency)
        if decision:
            params["decision_latency_ns"] = decision
        self._decision_latency.clear()
        self._last_report_ns = now
        msg = jsonrpc.notification("obs.report", params)
        self._out.append(msg)
        return msg

    # ------------------------------------------------------------ input

    def receive(self, msg: Message) -> list[Event]:
        if jsonrpc.is_response(msg):
            return self._on_response(msg)
        if jsonrpc.is_request(msg):
            return self._on_world_request(msg)
        return self._on_notification(msg)

    def _check(self, name: str, instance: Any, events: list[Event]) -> bool:
        if not self.validate:
            return True
        problems = schema.errors(name, instance)
        if problems:
            events.append(ProtocolViolation(f"{name}: {'; '.join(problems[:3])}"))
        return not problems

    def _on_world_request(self, msg: Message) -> list[Event]:
        if msg["method"] == "ping":
            now = self.clock_ns()
            origin = (msg.get("params") or {}).get("origin_ns", 0)
            self._out.append(
                jsonrpc.result(
                    msg["id"],
                    {"origin_ns": origin, "receive_ns": now, "transmit_ns": self.clock_ns()},
                )
            )
        else:
            err = AwpError(ErrorCode.METHOD_NOT_FOUND, msg["method"])
            self._out.append(jsonrpc.error(msg["id"], err))
        return []

    def _on_response(self, msg: Message) -> list[Event]:
        events: list[Event] = []
        pending = self._pending.pop(msg["id"], None) if isinstance(msg["id"], int) else None
        if pending is None:
            return [ProtocolViolation(f"response to unknown request id {msg['id']!r}")]
        method, params = pending
        if "error" in msg:
            err = AwpError.from_dict(msg["error"])
            action_id = params.get("action_id") if method.startswith("action.") else None
            if method == "action.submit":
                rec = self.actions.get(params["action_id"])
                if (
                    rec
                    and rec.state is ActionState.SUBMITTED
                    and err.code != ErrorCode.ACTION_ID_CONFLICT
                ):
                    del self.actions[params["action_id"]]  # no action was created (AWP-ACT-010)
            elif (
                method == "world.tick"
                and err.code == ErrorCode.TICK_MISMATCH
                and "tick" in err.data
            ):
                self.tick = err.data["tick"]
            events.append(ErrorResponse(msg["id"], method, err, action_id))
            return events
        res = msg["result"]
        result_schema = schema.schema_for(method, "result")
        valid = result_schema is None or self._check(result_schema, res, events)
        if not valid and method in ("initialize", "world.manifest"):
            raise ProtocolError("world manifest does not validate (AWP-AGT-002)")
        if method == "ping":
            if msg["id"] in self._session_pings:
                self._session_pings.discard(msg["id"])
                if valid:
                    received = self.clock_ns()
                    s = sample(params["origin_ns"], res["receive_ns"], res["transmit_ns"], received)
                    self.clock.add(s)
        elif valid:
            handler = getattr(self, "_result_" + method.replace(".", "_"), None)
            if handler is not None:
                handler(params, res, events)
        events.append(Response(msg["id"], method, res))
        return events

    def _result_initialize(
        self, params: dict[str, Any], res: dict[str, Any], events: list[Event]
    ) -> None:
        if res.get("protocol_version") not in PROTOCOL_VERSIONS:
            raise ProtocolError(
                f"world selected unsupported version {res.get('protocol_version')!r}"
            )
        self.manifest = res
        events.append(Initialized(res))

    _result_world_manifest = _result_initialize

    def _result_session_open(
        self, params: dict[str, Any], res: dict[str, Any], events: list[Event]
    ) -> None:
        self.ready = res
        self.clock = ClockEstimator()  # a new session clock
        self._seen_above.clear()
        self.last_status_seq = 0
        self.tick = res.get("tick")
        self._set_channels(res["granted"]["channels"])
        self.session_state = "ready"
        events.append(SessionOpened(res, resumed=False))

    def _result_session_resume(
        self, params: dict[str, Any], res: dict[str, Any], events: list[Event]
    ) -> None:
        self.ready = {**res, "session_token": res.get("session_token", self.session_token)}
        if "tick" in res:
            self.tick = res["tick"]
        self._set_channels(res["granted"]["channels"])
        for st in self._channel_stats.values():
            st.last_seq = None  # the seq gap across a resumption is not loss
        self._replay_to = res.get("replay_to_status_seq")
        events.append(SessionOpened(res, resumed=True))
        if self._replay_to is not None and self._replay_to <= self.last_status_seq:
            events.append(ReplayCompleted(self._replay_to))
            self._replay_to = None

    def _result_session_close(
        self, params: dict[str, Any], res: dict[str, Any], events: list[Event]
    ) -> None:
        self.session_state = "closed"

    def _result_action_submit(
        self, params: dict[str, Any], res: dict[str, Any], events: list[Event]
    ) -> None:
        rec = self.actions.get(res["action_id"])
        if rec is None:
            rec = self.actions[res["action_id"]] = ActionRecord(res["action_id"], params)
        if rec.state is ActionState.SUBMITTED:
            self._apply_status(res, events)
        elif res["status_seq"] > rec.status_seq:
            self._apply_status(res, events)  # idempotent result newer than anything we processed

    def _result_action_cancel(
        self, params: dict[str, Any], res: dict[str, Any], events: list[Event]
    ) -> None:
        self._apply_status(res, events)

    def _result_action_status(
        self, params: dict[str, Any], res: dict[str, Any], events: list[Event]
    ) -> None:
        rec = self.actions.get(res["action_id"])
        if rec is not None and res["status_seq"] > rec.status_seq:
            self._apply_status(res, events)

    def _result_world_tick(
        self, params: dict[str, Any], res: dict[str, Any], events: list[Event]
    ) -> None:
        self.tick = res["tick"]
        events.append(TickCompleted(res["tick"]))

    def _result_world_reset(
        self, params: dict[str, Any], res: dict[str, Any], events: list[Event]
    ) -> None:
        if "tick" in res:
            self.tick = res["tick"]

    def _result_obs_subscribe(
        self, params: dict[str, Any], res: dict[str, Any], events: list[Event]
    ) -> None:
        self._set_channels(res["granted"])

    _result_obs_unsubscribe = _result_obs_subscribe

    def _on_notification(self, msg: Message) -> list[Event]:
        method, params = msg["method"], msg.get("params") or {}
        events: list[Event] = []
        note_schema = schema.schema_for(method, "notification")
        if note_schema is not None and not self._check(note_schema, params, events):
            return events
        if method == "obs.frame":
            self._on_frame(params, events)
        elif method == "session.telemetry":
            events.append(Telemetry(params))
        elif method == "action.status":
            self._apply_status(params, events)
        elif method == "world.event":
            if self._sequence(params["status_seq"], events):
                events.append(WorldEvent(params["event"], params, self._replaying(params)))
        elif method == "session.state" and self._sequence(params["status_seq"], events):
            self.session_state = params["state"]
            events.append(
                SessionStateChanged(params["state"], params.get("reason"), self._replaying(params))
            )
        self._finish_replay(events)
        return events

    # ------------------------------------------------------------ status sequencing

    def _replaying(self, params: dict[str, Any]) -> bool:
        return self._replay_to is not None and params["status_seq"] <= self._replay_to

    def _sequence(self, seq: int, events: list[Event]) -> bool:
        """True for a status_seq not yet processed; False for a redelivery (AWP-LIF-009).

        A gap is reported but not acknowledged: `last_status_seq` stays below it, so a resume
        replays what is missing instead of the world discarding it.
        """
        if seq <= self.last_status_seq or seq in self._seen_above:
            return False
        if seq != self.last_status_seq + 1:
            events.append(ProtocolViolation(f"status_seq {seq} after {self.last_status_seq}"))
            self._seen_above.add(seq)
            return True
        self.last_status_seq = seq
        while self.last_status_seq + 1 in self._seen_above:
            self._seen_above.remove(self.last_status_seq + 1)
            self.last_status_seq += 1
        return True

    def _finish_replay(self, events: list[Event]) -> None:
        if self._replay_to is not None and self.last_status_seq >= self._replay_to:
            events.append(ReplayCompleted(self._replay_to))
            self._replay_to = None

    def _apply_status(self, status: dict[str, Any], events: list[Event]) -> None:
        if not self._sequence(status["status_seq"], events):
            return
        replayed = self._replaying(status)
        action_id = status["action_id"]
        rec = self.actions.get(action_id)
        if rec is None:
            rec = self.actions[action_id] = ActionRecord(action_id, {"action_id": action_id})
        target = ActionState(status["state"])
        if rec.state.terminal:
            events.append(ProtocolViolation(f"{action_id}: {target} after terminal {rec.state}"))
        elif target is not rec.state and not permitted(rec.state, target, status.get("reason")):
            events.append(
                ProtocolViolation(f"{action_id}: {rec.state} → {target} is not permitted")
            )
        rec.state = target
        rec.status_seq = status["status_seq"]
        rec.status = status
        events.append(ActionUpdated(rec, status, replayed))
        self._finish_replay(events)

    # ------------------------------------------------------------ frames

    def _set_channels(self, grants: list[dict[str, Any]]) -> None:
        self._channel_names = {g["channel_id"]: g["channel"] for g in grants}

    def _receipt_of(self, ts_mono_ns: int) -> int | None:
        return next((r for ts, r in reversed(self._receipts) if ts == ts_mono_ns), None)

    def _on_frame(self, params: dict[str, Any], events: list[Event]) -> None:
        try:
            frame = frames.from_inline(params)
        except AwpError as exc:
            events.append(ProtocolViolation(str(exc)))
            return
        now = self.clock_ns()
        name = self._channel_names.get(frame.channel_id)
        if name is None:
            events.append(ProtocolViolation(f"frame on ungranted channel {frame.channel_id}"))
            return
        st = self._channel_stats.setdefault(frame.channel_id, _ChannelStats())
        if st.last_seq is not None:
            if frame.seq <= st.last_seq:
                events.append(ProtocolViolation(f"channel {name}: seq {frame.seq} not increasing"))
            elif frame.seq != st.last_seq + 1 and not frame.resync:
                st.gaps += frame.seq - st.last_seq - 1
        st.last_seq = frame.seq
        st.frames += 1
        offset = self.clock.offset_ns
        if offset is not None and frame.ts_send_ns is not None:
            received = now + offset
            st.staleness.append(max(0, received - frame.ts_mono_ns))
            transit = received - frame.ts_send_ns
            if st.last_transit is not None:
                st.jitter_sum += abs(transit - st.last_transit)
                st.jitter_n += 1
            st.last_transit = transit
        self._receipts.append((frame.ts_mono_ns, now))
        if self.session_state == "ready":
            self.session_state = "active"
        events.append(FrameReceived(name, frame, now))
