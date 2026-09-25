"""`awp-demo`: a scripted agent for the awp-sim reference world's arm: moves, a cancel, and an
envelope refusal.

It rides out a lost connection: it resumes the session, or opens a new one if the world no longer
holds it.
"""

from __future__ import annotations

import argparse
import asyncio
import os
import sys
import time
from typing import Any

from websockets.exceptions import ConnectionClosed, InvalidHandshake

from . import __version__
from .aio import AsyncClient
from .client import ActionRecord, ClientConnection
from .errors import AwpError, ErrorCode

TARGETS = [(0.3, 0.2, 0.5), (-0.2, 0.1, 0.3), (0.0, -0.3, 0.45), (0.0, 0.0, 0.4)]


def _pose(p: tuple[float, float, float]) -> dict[str, Any]:
    return {"pose": {"frame": "base", "p_m": list(p), "q": [0, 0, 0, 1]}}


class _SessionEnded(Exception):
    """The session ended on the agent's side after a protocol error (AWP-CTL-009)."""


def _pct(values: list[float], p: float) -> float:
    ordered = sorted(values)
    return ordered[min(len(ordered) - 1, round(p * (len(ordered) - 1)))]


async def run_demo(url: str, *, token: str | None = None, echo: bool = True) -> dict[str, float]:
    say = print if echo else (lambda *_: None)
    conn = ClientConnection(
        {"name": "awp-python-demo", "version": __version__, "vendor": "hyperduality"},
        ["proprio/json", "text/event+json"],
    )
    admission_ms: list[float] = []
    async with AsyncClient(conn, url, token=token) as client:
        manifest = await client.initialize()
        mode = manifest["time_models"][0]
        lockstep = mode == "lockstep"

        async def open_session() -> None:
            ready = await client.open_session(
                mode, embodiment="arm_01", subscribe=["proprio", "arm_state"]
            )
            say(f"session {ready['session_id']} ({mode}) on {manifest['world']['name']}")

        async def recover() -> bool:
            """Resume after a lost connection (AWP-CTL-005); if the world no longer holds the
            session, open a new one, assuming nothing of the old survives (AWP-SES-008). True
            when the session resumed."""
            if conn.session_state == "closed":
                raise _SessionEnded
            try:
                await client.reconnect()
                say("  resumed the session")
                return True
            except AwpError as err:
                if err.code != ErrorCode.SESSION_UNKNOWN:
                    raise
            say("  the world no longer holds the session")
            await open_session()
            return False

        await open_session()
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

        async def run(target: tuple[float, float, float]) -> ActionRecord | None:
            """Move to `target` and wait for the outcome; None if the action was lost with its
            session."""
            action = await move(target)
            while True:
                try:
                    while lockstep and not conn.actions[action].terminal:
                        await client.advance(10)
                    return await client.wait_terminal(action)
                except (ConnectionError, ConnectionClosed):
                    if not await recover():
                        return None

        async def script() -> None:
            for target in TARGETS:
                record = await run(target)
                say(f"  move to {target}: {record.state if record else 'lost with the session'}")

            if not lockstep:
                action = await move(TARGETS[0])
                await client.wait_for(lambda e: (conn.actions[action].progress or 0) > 0.2, 5)
                await client.cancel(action)
                record = await client.wait_terminal(action)
                aborted = record.status.get("aborted_at_progress")
                say(f"  cancelled move: {record.state} at {aborted}")

            try:
                await move((0.9, 0.0, 0.4))
            except AwpError as err:
                say(f"  outside the envelope: {err.message}")

            await client.close_session()

        try:
            await script()
        except _SessionEnded:
            say("  the session ended after a protocol error")

    metrics = {
        "admission_p50_ms": _pct(admission_ms, 0.5),
        "admission_p95_ms": _pct(admission_ms, 0.95),
    }
    say(
        f"admission latency p50 {metrics['admission_p50_ms']:.2f} ms, "
        f"p95 {metrics['admission_p95_ms']:.2f} ms"
    )
    return metrics


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(
        prog="awp-demo", description="A scripted agent for the awp-sim reference world."
    )
    p.add_argument("--version", action="version", version=f"awp-demo {__version__}")
    p.add_argument("--url", default=os.environ.get("AWP_URL", "ws://127.0.0.1:8710"))
    p.add_argument("--token", default=os.environ.get("AWP_TOKEN"))
    args = p.parse_args(argv)
    try:
        asyncio.run(run_demo(args.url, token=args.token))
    except (OSError, InvalidHandshake, AwpError, TimeoutError) as err:
        print(f"awp-demo: {err}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
