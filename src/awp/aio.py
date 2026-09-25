"""Drive a ClientConnection over a WebSocket with asyncio.

This adapter adds only transport and waiting: it sends what the connection queues, feeds it what
arrives, runs the heartbeat, and lets callers await responses and events. Protocol behavior stays
in `ClientConnection`.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
from collections.abc import AsyncIterator, Callable
from types import TracebackType
from typing import Any

from websockets.asyncio.client import ClientConnection as WebSocket
from websockets.asyncio.client import connect
from websockets.exceptions import ConnectionClosed, InvalidHandshake
from websockets.typing import Subprotocol

from . import jsonrpc
from .client import (
    ActionRecord,
    ActionUpdated,
    ClientConnection,
    ErrorResponse,
    Event,
    FrameReceived,
    Response,
    SessionStateChanged,
)
from .errors import AwpError, ErrorCode

log = logging.getLogger("awp")

SUBPROTOCOL = Subprotocol("awp")


class AsyncClient:
    """An agent's connection to a world.

    With `heartbeat=True` a timer sends pings. An agent that actuates something should pass
    `heartbeat=False` and call `ping()` from its decision loop instead, so that a stalled policy
    also stops the heartbeat that holds off the world's watchdog (AWP-SAF-005).
    """

    def __init__(
        self,
        conn: ClientConnection,
        url: str,
        *,
        token: str | None = None,
        heartbeat: bool = True,
        report_interval_s: float | None = 2.0,
        open_timeout_s: float = 10.0,
        streams: bool = True,
    ) -> None:
        self.conn = conn
        self.streams = streams
        self.url = url
        self.token = token
        self.heartbeat = heartbeat
        self.report_interval_s = report_interval_s
        self.open_timeout_s = open_timeout_s
        self.latest: dict[str, FrameReceived] = {}
        self._ws: WebSocket | None = None
        self._tasks: list[asyncio.Task[None]] = []
        self._send_lock = asyncio.Lock()
        self._waiters: list[tuple[Callable[[Event], bool], asyncio.Future[Event]]] = []
        self._subscribers: list[asyncio.Queue[Event]] = []
        self._closed = asyncio.Event()
        self._last_rx = 0.0

    # ------------------------------------------------------------ transport

    async def connect(self) -> None:
        """Open the WebSocket. The token goes in the Authorization header (AWP-SEC-005)."""
        if self._ws is not None:
            await self._drop()
        headers = {"Authorization": f"Bearer {self.token}"} if self.token else None
        self._ws = await connect(
            self.url,
            subprotocols=[SUBPROTOCOL],
            additional_headers=headers,
            ping_interval=None,  # AWP heartbeats govern liveness (AWP-SAF-001)
            open_timeout=self.open_timeout_s,
            max_size=None,
        )
        self._closed.clear()
        self._last_rx = asyncio.get_running_loop().time()
        self._tasks = [
            asyncio.create_task(self._read(self._ws)),
            asyncio.create_task(self._watch(self._ws)),
        ]
        if self.heartbeat:
            self._tasks.append(asyncio.create_task(self._beat()))
        if self.report_interval_s:
            self._tasks.append(asyncio.create_task(self._report()))

    async def reconnect(self) -> dict[str, Any]:
        """Replace the transport and resume the session; returns the resume result."""
        await self._drop()
        await self.connect()
        await self.call(self.conn.initialize())
        live = self._waiter(lambda e: isinstance(e, SessionStateChanged) and not e.replayed)
        try:
            ready = await self.call(self.conn.resume())
            if self.conn.session_state != "active":
                await self._await(live, 10.0)  # replay done; the world reports the resumption live
        finally:
            live.cancel()
            self._waiters = [(p, f) for p, f in self._waiters if f is not live]
        await self.ping()
        await self._attach_streams()
        return ready

    async def aclose(self) -> None:
        await self._drop()

    async def __aenter__(self) -> AsyncClient:
        await self.connect()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        await self.aclose()

    @property
    def connected(self) -> bool:
        return self._ws is not None and not self._closed.is_set()

    async def flush(self) -> None:
        """Send everything the connection has queued, in order."""
        async with self._send_lock:
            for msg in self.conn.outgoing():
                if self._ws is None:
                    raise ConnectionError("not connected")
                await self._ws.send(jsonrpc.encode(msg))

    async def _drop(self) -> None:
        for task in self._tasks:
            task.cancel()
        for task in self._tasks:
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await task
        self._tasks = []
        if self._ws is not None:
            await self._ws.close()
            self._ws = None
        self._on_closed()

    async def _attach_streams(self) -> None:
        """Carry frames on the world's first `ws` stream endpoint, if it offers one."""
        if not self.streams:
            return
        endpoint = next(
            (e for e in self.conn.stream_endpoints if e.get("binding") == "ws" and e.get("url")),
            None,
        )
        if endpoint is not None:
            self._tasks.append(asyncio.create_task(self._stream(endpoint["url"])))

    async def _stream(self, url: str) -> None:
        """A stream connection, re-established while the control connection lives (AWP-TRN-010)."""
        while not self._closed.is_set() and self.conn.session_token is not None:
            try:
                async with connect(
                    url,
                    subprotocols=[SUBPROTOCOL],
                    additional_headers={"Authorization": f"Bearer {self.conn.session_token}"},
                    ping_interval=None,
                    open_timeout=self.open_timeout_s,
                    max_size=None,
                ) as ws:
                    async for raw in ws:
                        if isinstance(raw, bytes):
                            for event in self.conn.receive_frame(raw):
                                self._dispatch(event)
            except (OSError, ConnectionClosed, InvalidHandshake) as exc:
                log.info("stream connection lost: %s", exc)
            await asyncio.sleep(0.5)

    def _on_closed(self) -> None:
        if not self._closed.is_set():
            self._closed.set()
            self.conn.connection_lost()
            for _, fut in self._waiters:
                if not fut.done():
                    fut.set_exception(ConnectionError("connection closed"))
            self._waiters.clear()

    async def _read(self, ws: WebSocket) -> None:
        try:
            async for raw in ws:
                self._last_rx = asyncio.get_running_loop().time()
                try:
                    msg = jsonrpc.decode(raw)
                except AwpError as err:
                    if err.code == ErrorCode.INTEGER_RANGE:  # AWP-CTL-009
                        raise
                    log.warning("dropping undecodable message: %s", err)
                    continue
                events = self.conn.receive(msg)
                await self.flush()
                for event in events:
                    self._dispatch(event)
        except ConnectionClosed:
            pass
        except Exception:
            # Never keep a connection whose reader has died: its heartbeat would hold off the
            # world's watchdog while nothing processes the world's messages.
            log.exception("closing the connection after a reader failure")
            await ws.close(code=1011)
        finally:
            self._on_closed()

    def _dispatch(self, event: Event) -> None:
        if isinstance(event, FrameReceived):
            self.latest[event.channel] = event
        for q in self._subscribers:
            q.put_nowait(event)
        for item in list(self._waiters):
            predicate, fut = item
            if not fut.done() and predicate(event):
                fut.set_result(event)
                self._waiters.remove(item)

    async def _beat(self) -> None:
        loop = asyncio.get_running_loop()
        last = loop.time()
        while not self._closed.is_set():
            await asyncio.sleep(0.05)  # re-read the interval: it is only known once a session opens
            if self.conn.ready is not None and loop.time() - last >= self._heartbeat_s():
                last = loop.time()
                self.conn.ping()
                await self.flush()

    async def _watch(self, ws: WebSocket) -> None:
        """Three heartbeat intervals without any message from the world is loss (AWP-SAF-002)."""
        loop = asyncio.get_running_loop()
        while not self._closed.is_set():
            await asyncio.sleep(0.1)
            if self.conn.ready is None:
                continue
            interval = (self.conn.ready.get("heartbeat_interval_ms", 5000)) / 1000
            if loop.time() - self._last_rx > 3 * interval:
                log.warning("no message from the world for %.1f s; closing", 3 * interval)
                await ws.close(code=1001, reason="heartbeat lost")
                return

    def _heartbeat_s(self) -> float:
        """Every heartbeat interval, and at least twice per watchdog period (AWP-SAF-005)."""
        interval = (self.conn.ready or {}).get("heartbeat_interval_ms", 5000)
        safe_state = ((self.conn.manifest or {}).get("safety_policy") or {}).get("safe_state")
        if safe_state:
            interval = min(interval, safe_state["watchdog_ms"] / 2)
        return float(interval) / 1000

    async def _report(self) -> None:
        assert self.report_interval_s is not None
        while not self._closed.is_set():
            await asyncio.sleep(self.report_interval_s)
            streaming = self.conn.tick is None and self.conn.ready is not None
            if streaming and self.conn.clock.samples:
                self.conn.report()
                await self.flush()

    # ------------------------------------------------------------ waiting

    def _waiter(self, predicate: Callable[[Event], bool]) -> asyncio.Future[Event]:
        fut: asyncio.Future[Event] = asyncio.get_running_loop().create_future()
        if self._closed.is_set():
            fut.set_exception(ConnectionError("connection closed"))
        else:
            self._waiters.append((predicate, fut))
        return fut

    async def _await(self, fut: asyncio.Future[Event], timeout: float) -> Event:
        try:
            return await asyncio.wait_for(fut, timeout)
        finally:
            self._waiters = [(p, f) for p, f in self._waiters if f is not fut]

    async def wait_for(self, predicate: Callable[[Event], bool], timeout: float = 10.0) -> Event:
        return await self._await(self._waiter(predicate), timeout)

    async def call(self, rid: int, timeout: float = 10.0) -> dict[str, Any]:
        """Send what is queued and await the response to request `rid`."""
        fut = self._waiter(lambda e: isinstance(e, (Response, ErrorResponse)) and e.id == rid)
        await self.flush()
        try:
            event = await self._await(fut, timeout)
        except TimeoutError:
            self.conn.forget(rid)
            raise
        if isinstance(event, ErrorResponse):
            raise event.error
        assert isinstance(event, Response)
        return event.result

    async def events(self) -> AsyncIterator[Event]:
        """Every event from now on, until the connection closes."""
        q: asyncio.Queue[Event] = asyncio.Queue()
        self._subscribers.append(q)
        try:
            while not self._closed.is_set() or not q.empty():
                get = asyncio.ensure_future(q.get())
                closed = asyncio.ensure_future(self._closed.wait())
                done, _ = await asyncio.wait({get, closed}, return_when=asyncio.FIRST_COMPLETED)
                closed.cancel()
                if get in done:
                    yield get.result()
                else:
                    get.cancel()
        finally:
            self._subscribers.remove(q)

    # ------------------------------------------------------------ conveniences

    async def ping(self) -> None:
        """A heartbeat and clock sample, for agents that run their own heartbeat."""
        await self.call(self.conn.ping())

    async def initialize(self) -> dict[str, Any]:
        return await self.call(self.conn.initialize())

    async def open_session(self, mode: str, **kw: Any) -> dict[str, Any]:
        """Open a session, then take the clock samples AWP-CLK-008 asks for."""
        ready = await self.call(self.conn.open_session(mode, **kw))
        for _ in range(4):
            await self.ping()
        await self._attach_streams()
        return ready

    async def submit(self, type: str, params: dict[str, Any], **kw: Any) -> ActionRecord:
        """Submit and await admission. Raises AwpError if the world refuses it."""
        action_id = self.conn.submit(type, params, **kw)
        await self.call(self.conn.last_id)
        return self.conn.actions[action_id]

    async def wait_terminal(self, action_id: str, timeout: float = 30.0) -> ActionRecord:
        record = self.conn.actions[action_id]
        if not record.terminal:
            await self.wait_for(
                lambda e: (
                    isinstance(e, ActionUpdated)
                    and e.action.action_id == action_id
                    and e.action.terminal
                ),
                timeout,
            )
        return self.conn.actions[action_id]

    async def cancel(self, action_id: str) -> dict[str, Any]:
        return await self.call(self.conn.cancel(action_id))

    async def advance(self, count: int | None = None) -> int:
        result = await self.call(self.conn.advance(count))
        return int(result["tick"])

    async def close_session(self, timeout: float = 10.0) -> None:
        await self.call(self.conn.close(), timeout)
