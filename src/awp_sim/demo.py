"""A scripted agent that exercises a running world: moves, a cancel, and an envelope refusal."""

from __future__ import annotations

import time
from typing import Any

from awp.aio import AsyncClient
from awp.client import ClientConnection
from awp.errors import AwpError

from . import __version__

TARGETS = [(0.3, 0.2, 0.5), (-0.2, 0.1, 0.3), (0.0, -0.3, 0.45), (0.0, 0.0, 0.4)]


def _pose(p: tuple[float, float, float]) -> dict[str, Any]:
    return {"pose": {"frame": "base", "p_m": list(p), "q": [0, 0, 0, 1]}}


def _pct(values: list[float], p: float) -> float:
    ordered = sorted(values)
    return ordered[min(len(ordered) - 1, round(p * (len(ordered) - 1)))]


async def run_demo(url: str, *, token: str | None = None, echo: bool = True) -> dict[str, float]:
    say = print if echo else (lambda *_: None)
    conn = ClientConnection(
        {"name": "awp-sim-demo", "version": __version__, "vendor": "hyperduality"},
        ["proprio/json", "text/event+json"],
    )
    admission_ms: list[float] = []
    async with AsyncClient(conn, url, token=token) as client:
        manifest = await client.initialize()
        mode = manifest["time_models"][0]
        ready = await client.open_session(
            mode, embodiment="arm_01", subscribe=["proprio", "arm_state"]
        )
        say(f"session {ready['session_id']} ({mode}) on {manifest['world']['name']}")
        lockstep = mode == "lockstep"
        if "move_to_pose" not in conn.granted_action_types:
            say("  move_to_pose is not granted; nothing to do")
            await client.close_session()
            return {}

        async def move(target: tuple[float, float, float], **kw: Any) -> str:
            basis = None if lockstep else client.latest["proprio"].frame
            validity = None if lockstep else 200
            started = time.perf_counter()
            record = await client.submit(
                "move_to_pose", _pose(target), basis=basis, valid_for_ms=validity, **kw
            )
            admission_ms.append((time.perf_counter() - started) * 1000)
            return record.action_id

        for target in TARGETS:
            action = await move(target)
            while lockstep and not conn.actions[action].terminal:
                await client.advance(10)
            record = await client.wait_terminal(action)
            say(f"  move to {target}: {record.state}")

        if not lockstep:
            action = await move(TARGETS[0])
            await client.wait_for(lambda e: (conn.actions[action].progress or 0) > 0.2, 5)
            await client.cancel(action)
            record = await client.wait_terminal(action)
            say(f"  cancelled move: {record.state} at {record.status.get('aborted_at_progress')}")

        try:
            await move((0.9, 0.0, 0.4))
        except AwpError as err:
            say(f"  outside the envelope: {err.message}")

        await client.close_session()

    metrics = {
        "admission_p50_ms": _pct(admission_ms, 0.5),
        "admission_p95_ms": _pct(admission_ms, 0.95),
    }
    say(
        f"admission latency p50 {metrics['admission_p50_ms']:.2f} ms, "
        f"p95 {metrics['admission_p95_ms']:.2f} ms"
    )
    return metrics
