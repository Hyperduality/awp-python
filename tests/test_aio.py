"""AsyncClient against a scripted world over real WebSockets."""

from __future__ import annotations

import asyncio

import pytest

from awp.aio import AsyncClient
from awp.client import ClientConnection, FrameReceived
from awp.errors import AwpError, ErrorCode

from .world import AGENT, MODALITIES, TOKEN, Message, ScriptedWorld

POSE = {"pose": {"frame": "base", "p_m": [0.3, 0.2, 0.5], "q": [0, 0, 0, 1]}}


def client(world: ScriptedWorld, **kw) -> AsyncClient:
    return AsyncClient(ClientConnection(AGENT, MODALITIES), world.url, **kw)


async def opened(c: AsyncClient, world: ScriptedWorld, subscribe=("proprio",)) -> None:
    await c.initialize()
    await c.open_session(world.mode, embodiment="arm_01", subscribe=list(subscribe))


async def submitted(c: AsyncClient, world: ScriptedWorld) -> str:
    submitting = asyncio.create_task(c.submit("move_to_pose", POSE))
    action_id = await world.accept(await world.expect("action.submit"))
    await submitting
    return action_id


async def test_a_session_runs_over_a_websocket():
    world = ScriptedWorld()
    async with world.serving(), client(world, token="t0ken-abcdefghijk") as c:
        await opened(c, world)
        assert len(world.sent("ping")) >= 4  # clock samples before acting (AWP-CLK-008)
        await c.wait_for(lambda e: "proprio" in c.latest)
        action_id = await submitted(c, world)
        await world.notify("action.status", world.status(action_id, "executing", progress=0.0))
        await world.notify("action.status", world.status(action_id, "completed", progress=1.0))
        assert (await c.wait_terminal(action_id, 3)).state == "completed"
        await c.close_session()
        assert c.conn.session_state == "closed"
    (headers,) = world.headers
    assert headers["authorization"] == "Bearer t0ken-abcdefghijk"  # AWP-SEC-005
    assert headers["sec-websocket-protocol"] == "awp"


async def test_reconnect_resumes_the_session_and_replays_what_was_missed():
    world = ScriptedWorld()
    async with world.serving(), client(world) as c:
        await opened(c, world)
        action_id = await submitted(c, world)
        acknowledged = c.conn.last_status_seq
        world.backlog.append(("action.status", world.status(action_id, "executing", progress=0.0)))
        result = await c.reconnect()
        resume = await world.expect("session.resume")
        assert resume["params"] == {"session_token": TOKEN, "last_status_seq": acknowledged}
        assert result["replay_to_status_seq"] == acknowledged + 1
        assert c.conn.actions[action_id].state == "executing"
        assert (c.conn.session_state, c.conn.last_status_seq) == ("active", acknowledged + 2)


async def test_a_session_the_world_no_longer_holds_is_closed():
    world = ScriptedWorld()

    async def refuse(req: Message) -> None:
        await world.fail(req, AwpError(ErrorCode.SESSION_UNKNOWN, "no such session"))

    world.handlers["session.resume"] = refuse
    async with world.serving(), client(world) as c:
        await opened(c, world)
        await submitted(c, world)
        with pytest.raises(AwpError) as exc:
            await c.reconnect()
        assert exc.value.code == ErrorCode.SESSION_UNKNOWN
        assert (c.conn.session_state, c.conn.actions) == ("closed", {})  # AWP-SES-008


async def test_an_integer_beyond_2_53_ends_the_session():
    world = ScriptedWorld()
    async with world.serving(), client(world) as c:
        await opened(c, world)
        params = {"state": "active", "status_seq": 2**53, "ts_mono_ns": 0, "reason": "x"}
        await world.send({"jsonrpc": "2.0", "method": "session.state", "params": params})
        await world.expect("session.close")
        await world.until(lambda: world.closes)
        assert world.closes == [(1002, "AWP_INTEGER_RANGE")]  # AWP-CTL-009
        assert c.conn.session_state == "closed"


async def test_frames_move_to_the_stream_connection():
    world = ScriptedWorld(stream=True)
    async with world.serving(), client(world) as c:
        await opened(c, world)
        await world.until(lambda: world.stream_conn)
        assert world.headers[1]["authorization"] == f"Bearer {TOKEN}"  # AWP-TRN-013
        await world.send_frame("proprio")
        await c.wait_for(lambda e: isinstance(e, FrameReceived) and e.frame.seq == 2)
        assert c.conn.delivery()["proprio"] == (2, 0)


async def test_a_malformed_stream_frame_closes_the_stream_connection_and_reopens_it():
    world = ScriptedWorld(stream=True)
    async with world.serving(), client(world) as c:
        await opened(c, world)
        await world.until(lambda: world.stream_conn)
        stream = world.stream_conn
        assert stream is not None
        await stream.send(b"AWPF not a frame")
        await world.until(lambda: world.stream_closes)
        assert world.stream_closes == [(1002, "AWP_MALFORMED")]  # AWP-DAT-010
        await world.until(lambda: world.stream_conn not in (None, stream))
        assert c.connected
        assert world.closes == []


async def test_setpoints_are_not_sent_inline_while_their_stream_is_down():
    world = ScriptedWorld(stream=True, command=("servo_arm",))
    async with world.serving(), client(world) as c:
        await opened(c, world)
        await world.until(lambda: world.stream_conn)
        await c.command("servo_arm", {"v_mps": [0.01, 0.0, 0.0]})
        await world.until(lambda: world.stream_received)
        world.refuse_streams = True
        stream = world.stream_conn
        assert stream is not None
        await stream.close()
        await world.until(lambda: c._stream_ws is None)
        with pytest.raises(ConnectionError):  # AWP-TRN-010
            await c.command("servo_arm", {"v_mps": [0.01, 0.0, 0.0]})
        assert world.sent("cmd.frame") == []


async def test_an_advance_waits_for_frames_that_follow_its_result_on_the_stream():
    world = ScriptedWorld("lockstep", stream=True)

    async def tick_then_frames(req: Message) -> None:
        world.tick += 1
        await world.reply(req, {"tick": world.tick})
        await asyncio.sleep(0.2)
        await world.send_frames()

    world.handlers["world.tick"] = tick_then_frames
    async with world.serving(), client(world) as c:
        await opened(c, world, subscribe=("proprio", "arm_state"))
        await world.until(lambda: world.stream_conn)
        assert await c.advance() == 1
        assert c.conn.holds_tick(1)  # AWP-TIM-003


async def test_a_silent_world_is_a_lost_connection():
    world = ScriptedWorld(heartbeat_ms=100)
    async with world.serving(), client(world) as c:
        await opened(c, world)

        async def ignore(req: Message) -> None:
            pass

        world.handlers["ping"] = ignore
        await world.until(lambda: world.closes, 2.0)
        assert world.closes[0][0] == 1001  # three intervals without a message (AWP-SAF-002)
        assert not c.connected


async def test_a_reader_failure_closes_the_connection_and_stops_the_heartbeat():
    world = ScriptedWorld(heartbeat_ms=100)
    async with world.serving(), client(world) as c:
        await opened(c, world)
        c.conn.receive = lambda msg: (_ for _ in ()).throw(KeyError("boom"))  # type: ignore[method-assign]
        await world.send_frame("proprio")
        await world.until(lambda: world.closes)
        assert world.closes[0][0] == 1011
        pings = len(world.sent("ping"))
        await asyncio.sleep(0.4)
        assert len(world.sent("ping")) == pings  # no stray heartbeat holds off a watchdog


async def test_the_agent_answers_a_world_ping():
    world = ScriptedWorld()
    async with world.serving(), client(world) as c:
        await opened(c, world)
        await world.send(
            {"jsonrpc": "2.0", "id": "w1", "method": "ping", "params": {"origin_ns": 7}}
        )
        await world.until(lambda: any(m.get("id") == "w1" for m in world.received))
        (pong,) = [m for m in world.received if m.get("id") == "w1"]
        assert pong["result"]["origin_ns"] == 7
