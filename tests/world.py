"""A world each test scripts by hand, in place of a world implementation.

`ScriptedWorld` serves over real WebSockets. By default it answers `initialize`, `session.open`,
`session.resume`, `ping`, `world.tick`, and `session.close` from the awp-sim manifests in
`conformance/`; everything else, and any of those, a test answers itself. What it sends validates
against the sender form of its schema.
"""

from __future__ import annotations

import asyncio
import json
import time
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager
from http import HTTPStatus
from pathlib import Path
from typing import Any

from websockets.asyncio.server import ServerConnection, serve
from websockets.exceptions import ConnectionClosed
from websockets.http11 import Request, Response
from websockets.typing import Subprotocol

from awp import frames, jsonrpc, schema
from awp.client import ClientConnection
from awp.errors import AwpError

CONFORMANCE = Path(__file__).resolve().parent.parent / "conformance"
TOKEN = "st_scripted_session_token"
TICK_NS = 20_000_000
AGENT = {"name": "tests", "version": "0.1.0", "vendor": "tests"}
MODALITIES = ["proprio/json", "text/event+json"]

Message = dict[str, Any]
Handler = Callable[[Message], Awaitable[None]]


def manifest(mode: str) -> dict[str, Any]:
    m: dict[str, Any] = json.loads((CONFORMANCE / f"manifest-{mode}.json").read_text())
    return m


def ready(
    mode: str,
    params: dict[str, Any],
    *,
    heartbeat_ms: int = 5000,
    stream_url: str | None = None,
    tick: int = 0,
    command: tuple[str, ...] = (),
) -> dict[str, Any]:
    """The `session.open` result for `params`: every requested channel, every action type, and
    the command channels named in `command`."""
    m = manifest(mode)
    rates = {c["id"]: c["rate_hz"] for c in m["observation_channels"]}
    names = [(sub["channel"], rates[sub["channel"]]) for sub in params.get("subscribe", [])]
    names += [(name, 50.0) for name in command]
    result: dict[str, Any] = {
        "session_id": "sess_scripted",
        "session_token": TOKEN,
        "reconnect_window_ms": 30000,
        "heartbeat_interval_ms": heartbeat_ms,
        "granted": {
            "channels": [
                {"channel": name, "rate_hz": rate, "channel_id": i}
                for i, (name, rate) in enumerate(names, start=1)
            ],
            "action_types": m["embodiments"][0]["action_types"],
            "admin": ["tick"] if mode == "lockstep" else [],
            "envelopes": m["safety_policy"]["envelopes"],
        },
        "stream_endpoints": [
            *([{"binding": "ws", "url": stream_url}] if stream_url else []),
            {"binding": "inline"},
        ],
        "clock_anchor": "2026-09-25T00:00:00Z",
    }
    if mode == "lockstep":
        result["tick"] = tick
    return result


def opened(mode: str, subscribe: list[str]) -> ClientConnection:
    """A ClientConnection with a session open on `mode`, without a transport."""
    conn = ClientConnection(AGENT, MODALITIES, clock_ns=lambda: 0)
    rid = conn.initialize()
    conn.receive(jsonrpc.result(rid, manifest(mode)))
    rid = conn.open_session(mode, embodiment="arm_01", subscribe=subscribe)
    params = conn.outgoing()[-1]["params"]
    conn.receive(jsonrpc.result(rid, ready(mode, params)))
    rid = conn.ping()  # a clock sample, as AsyncClient takes on opening (AWP-CLK-008)
    conn.receive(jsonrpc.result(rid, {"origin_ns": 0, "receive_ns": 0, "transmit_ns": 0}))
    conn.outgoing()
    return conn


class ScriptedWorld:
    def __init__(
        self,
        mode: str = "streaming",
        *,
        stream: bool = False,
        heartbeat_ms: int = 5000,
        command: tuple[str, ...] = (),
    ) -> None:
        self.mode = mode
        self.manifest = manifest(mode)
        self.stream = stream
        self.heartbeat_ms = heartbeat_ms
        self.command = command
        self.refuse_streams = False  # answer stream handshakes with 403
        self.url = ""
        self.received: list[Message] = []  # from the agent on control connections, in order
        self.stream_received: list[bytes] = []
        self.headers: list[dict[str, str]] = []  # of every connection, in order
        self.closes: list[tuple[int | None, str | None]] = []  # control connections
        self.stream_closes: list[tuple[int | None, str | None]] = []
        self.handlers: dict[str, Handler] = {
            "initialize": self._initialize,
            "session.open": self._open,
            "session.resume": self._resume,
            "ping": self._ping,
            "world.tick": self._tick,
            "session.close": self._close,
        }
        self.control: ServerConnection | None = None
        self.stream_conn: ServerConnection | None = None
        self.granted: list[dict[str, Any]] = []
        self.backlog: list[tuple[str, dict[str, Any]]] = []  # sent while no connection was up
        self.status_seq = 0
        self.tick = 0
        self._seqs: dict[int, int] = {}
        self._taken: set[int] = set()
        self._started = time.monotonic_ns()

    # ------------------------------------------------------------ serving

    @asynccontextmanager
    async def serving(self) -> AsyncIterator[ScriptedWorld]:
        async with serve(
            self._serve,
            "127.0.0.1",
            0,
            subprotocols=[Subprotocol("awp")],
            process_request=self._handshake,
        ) as server:
            port = next(iter(server.sockets)).getsockname()[1]
            self.url = f"ws://127.0.0.1:{port}"
            yield self

    def _handshake(self, ws: ServerConnection, request: Request) -> Response | None:
        if self.refuse_streams and request.path == "/stream":
            return ws.respond(HTTPStatus.FORBIDDEN, "no stream connection\n")
        return None

    async def _serve(self, ws: ServerConnection) -> None:
        assert ws.request is not None
        self.headers.append(dict(ws.request.headers))
        if ws.request.path == "/stream":
            await self._serve_stream(ws)
            return
        self.control = ws
        try:
            async for raw in ws:
                msg = json.loads(raw)
                self.received.append(msg)
                handler = self.handlers.get(msg.get("method", ""))
                if handler is not None and "id" in msg:
                    await handler(msg)
        except ConnectionClosed:
            pass
        self.closes.append((ws.close_code, ws.close_reason))

    async def _serve_stream(self, ws: ServerConnection) -> None:
        self.stream_conn = ws
        try:
            async for raw in ws:
                self.stream_received.append(raw if isinstance(raw, bytes) else raw.encode())
        except ConnectionClosed:
            pass
        self.stream_closes.append((ws.close_code, ws.close_reason))
        if self.stream_conn is ws:
            self.stream_conn = None

    # ------------------------------------------------------------ what the agent sent

    async def expect(self, method: str, timeout: float = 3.0) -> Message:
        """The agent's next `method` message not yet expected."""
        deadline = time.monotonic() + timeout
        while True:
            for i, msg in enumerate(self.received):
                if i not in self._taken and msg.get("method") == method:
                    self._taken.add(i)
                    return msg
            if time.monotonic() > deadline:
                raise TimeoutError(f"the agent sent no {method}")
            await asyncio.sleep(0.01)

    def sent(self, method: str) -> list[Message]:
        return [m for m in self.received if m.get("method") == method]

    async def until(self, predicate: Callable[[], object], timeout: float = 3.0) -> None:
        deadline = time.monotonic() + timeout
        while not predicate():
            if time.monotonic() > deadline:
                raise TimeoutError("condition not reached")
            await asyncio.sleep(0.01)

    # ------------------------------------------------------------ sending

    @property
    def channels(self) -> dict[str, int]:
        return {g["channel"]: g["channel_id"] for g in self.granted}

    def now(self) -> int:
        """The session clock: advances in lockstep, elapsed time in streaming."""
        if self.mode == "lockstep":
            return self.tick * TICK_NS
        return time.monotonic_ns() - self._started

    async def send(self, msg: Message) -> None:
        assert self.control is not None
        await self.control.send(json.dumps(msg))

    async def reply(self, req: Message, result: dict[str, Any]) -> None:
        name = schema.schema_for(req["method"], "result")
        if name is not None:
            assert schema.errors(name, result, sender=True) == [], (name, result)
        await self.send(jsonrpc.result(req["id"], result))

    async def fail(self, req: Message, err: AwpError) -> None:
        await self.send(jsonrpc.error(req["id"], err))

    async def notify(self, method: str, params: dict[str, Any]) -> None:
        name = schema.schema_for(method, "notification")
        if name is not None:
            assert schema.errors(name, params, sender=True) == [], (name, params)
        await self.send(jsonrpc.notification(method, params))

    def next_status_seq(self) -> int:
        self.status_seq += 1
        return self.status_seq

    async def state(self, state: str, reason: str) -> None:
        params = {"state": state, "status_seq": self.next_status_seq(), "ts_mono_ns": self.now()}
        await self.notify("session.state", {**params, "reason": reason})

    async def accept(self, req: Message) -> str:
        """Admit an `action.submit`; returns its action_id."""
        action_id: str = req["params"]["action_id"]
        now = self.now()
        await self.reply(
            req,
            {
                "action_id": action_id,
                "state": "accepted",
                "status_seq": self.next_status_seq(),
                "received_ts_mono_ns": now,
                "ts_mono_ns": now,
            },
        )
        return action_id

    def status(self, action_id: str, state: str, **extra: Any) -> dict[str, Any]:
        """The params of the next `action.status`, to `notify` now or to replay later."""
        params = {
            "action_id": action_id,
            "state": state,
            "status_seq": self.next_status_seq(),
            "ts_mono_ns": self.now(),
            **extra,
        }
        if self.mode == "lockstep":
            params["tick"] = self.tick
        return params

    def frame(self, channel: str, *, seq: int | None = None, resync: bool = False) -> frames.Frame:
        cid = self.channels[channel]
        seq = self._seqs[cid] = seq if seq is not None else self._seqs.get(cid, 0) + 1
        lockstep = self.mode == "lockstep"
        return frames.Frame(
            cid,
            seq,
            self.now(),
            json.dumps({"p_m": [0, 0, 0.4], "v_mps": [0, 0, 0]}).encode(),
            keyframe=True,
            resync=resync,
            tick=self.tick if lockstep else None,
            ts_send_ns=None if lockstep else self.now(),
        )

    async def send_frame(self, channel: str, **kw: Any) -> None:
        """A frame on the stream connection when one is open, otherwise inline."""
        frame = self.frame(channel, **kw)
        if self.stream_conn is not None:
            await self.stream_conn.send(frames.encode(frame))
        else:
            await self.notify("obs.frame", frames.to_inline(frame))

    async def send_frames(self) -> None:
        for channel in self.channels:
            if channel not in self.command:
                await self.send_frame(channel)

    # ------------------------------------------------------------ default handlers

    async def _initialize(self, req: Message) -> None:
        await self.reply(req, self.manifest)

    async def _open(self, req: Message) -> None:
        result = ready(
            self.mode,
            req.get("params", {}),
            heartbeat_ms=self.heartbeat_ms,
            stream_url=f"{self.url}/stream" if self.stream else None,
            tick=self.tick,
            command=self.command,
        )
        self.granted = result["granted"]["channels"]
        await self.reply(req, result)
        await self.state("ready", "opened")
        await self.send_frames()

    async def _resume(self, req: Message) -> None:
        """Resume, replaying the backlog after the agent's `last_status_seq`."""
        result = ready(self.mode, {}, heartbeat_ms=self.heartbeat_ms, tick=self.tick)
        result["granted"]["channels"] = self.granted
        await self.reply(req, {**result, "replay_to_status_seq": self.status_seq})
        for method, params in self.backlog:
            if params["status_seq"] > req["params"]["last_status_seq"]:
                await self.notify(method, params)
        self.backlog.clear()
        await self.state("active", "resumed")

    async def _ping(self, req: Message) -> None:
        now = self.now()
        origin = req["params"]["origin_ns"]
        await self.reply(req, {"origin_ns": origin, "receive_ns": now, "transmit_ns": now})

    async def _tick(self, req: Message) -> None:
        self.tick += req["params"].get("count", 1)
        await self.send_frames()
        await self.reply(req, {"tick": self.tick})

    async def _close(self, req: Message) -> None:
        await self.state("closed", "session_closed")
        await self.reply(req, {})
