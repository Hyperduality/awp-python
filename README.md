# awp-python

The Python agent SDK for the [Agent World Protocol](https://www.agentworldprotocol.com). `ClientConnection` is the agent side of AWP as a sans-IO state machine, and `awp.aio.AsyncClient` drives it over a WebSocket.

It targets specification revision **`0.1-draft.9`**, pinned as the `spec/` submodule. This is an alpha, so the API will change along with the draft. The reference world, [awp-sim](https://github.com/Hyperduality/awp-sim), is a separate package built on this one.

## Status

![AWP: Core Agent, AWP-conformant against 0.1-draft.9](https://img.shields.io/badge/AWP-Core_Agent%2C_conformant_0.1--draft.9-555)

In both time models, the demo agent built on the client is **Core Agent: AWP-conformant against 0.1-draft.9**. [awp-conformance](https://github.com/Hyperduality/awp-conformance) reports no failure and nothing untested, and [`conformance/`](conformance/README.md) holds the reports and the evidence for their manual rows. CI runs the suite against the demo agent on every change.

| Implemented | Not implemented |
|---|---|
| Lockstep and streaming sessions; `world.tick` advances that wait for every per-tick channel | Multi-bind (`embodiments`) |
| Inline and `ws` stream bindings; binary frame codec (every spec vector) | Other stream bindings (`webrtc`, `webtransport`, `shm`, `grpc`) |
| Action lifecycle checked against the spec's transition table; idempotent resubmission | |
| Heartbeats, clock synchronization, receiver reports, resumption with replay and acknowledgement | |
| Canonical schema validation in receiver and sender forms | |
| Task, approval and standing approvals, transfer, reset, snapshots, and command channels | |

## Install

```bash
pip install --pre awp-python
```

Requires Python 3.11+.

## Using the client

With asyncio:

```python
from awp import ClientConnection
from awp.aio import AsyncClient

conn = ClientConnection({"name": "my-agent", "version": "0.1.0", "vendor": "me"}, ["proprio/json"])
async with AsyncClient(conn, "ws://127.0.0.1:8710") as client:
    await client.initialize()
    await client.open_session("streaming", embodiment="arm_01", subscribe=["proprio"])
    await client.wait_for(lambda e: "proprio" in client.latest)
    record = await client.submit(
        "move_to_pose",
        {"pose": {"frame": "base", "p_m": [0.3, 0.2, 0.5], "q": [0, 0, 0, 1]}},
        basis=client.latest["proprio"].frame,  # the observation this intent rests on
        valid_for_ms=200,
    )
    print((await client.wait_terminal(record.action_id)).state)
    await client.close_session()
```

Without asyncio, feed `ClientConnection` the decoded messages and send whatever it queues:

```python
events = conn.receive(message)  # typed events: ActionUpdated, FrameReceived, ...
for out in conn.outgoing():  # messages to send, in order
    transport.send(json.dumps(out))
```

The connection:

- tracks the action lifecycle against the spec's transition table;
- deduplicates replayed statuses;
- keeps the clock offset from heartbeats;
- reports any violation by the world as a `ProtocolViolation` event.

## The demo agent

`awp-demo` is a scripted agent for awp-sim's arm. It moves, cancels a move, and triggers an envelope refusal. It rides out a lost connection by resuming the session, or by opening a new one if the world no longer holds it.

```bash
pip install --pre awp-sim
awp-sim serve &                      # streaming world on ws://127.0.0.1:8710
awp-demo                             # or --url, --token ($AWP_URL, $AWP_TOKEN)
```

## Layout

```
src/awp/            client protocol layer (sans-IO), asyncio adapter, frame codec, schemas, demo agent
src/awp/_spec/      schemas and lifecycle table bundled from spec/ (scripts/sync_spec.py)
tests/              unit tests, and a scripted world that serves over WebSockets
conformance/        conformance reports, the manifests they were run against, and evidence
spec/               agent-world-protocol, pinned at spec-v0.1-draft.9
```

## Development

```bash
git clone --recurse-submodules https://github.com/Hyperduality/awp-python
cd awp-python
uv sync
uv run ruff check && uv run ruff format --check
uv run mypy
uv run pytest --cov
uv run python scripts/sync_spec.py --check
```

To move to a new draft revision:

1. Check out its tag in `spec/`.
2. Run `scripts/sync_spec.py`.
3. Update `SPEC_REVISION` in `src/awp/__init__.py`.
4. Fix whatever the tests report.

## License

Apache-2.0. See [LICENSE](LICENSE).
