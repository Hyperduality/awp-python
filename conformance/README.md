# Conformance

![AWP: Core Agent, AWP-conformant against 0.1-draft.9](https://img.shields.io/badge/AWP-Core_Agent%2C_conformant_0.1--draft.9-555)

| Class | Configuration | Claim | Report |
|---|---|---|---|
| Core Agent | `awp-demo`, [`manifest-lockstep.json`](manifest-lockstep.json) | Core Agent (lockstep): AWP-conformant against 0.1-draft.9 (awp-conformance 0.1.0a3) | [`core-agent-lockstep.json`](core-agent-lockstep.json) |
| Core Agent | `awp-demo`, [`manifest-streaming.json`](manifest-streaming.json) | Core Agent (streaming): AWP-conformant against 0.1-draft.9 (awp-conformance 0.1.0a3) | [`core-agent-streaming.json`](core-agent-streaming.json) |

The reports come from awp-conformance 0.1.0a3 run against awp-python 0.1.0a4. Neither has a failure or anything untested. The evidence for their `manual` rows is below (AWP-CNF-005).

## Reproduce

The suite serves each manifest from its harness world and runs the demo agent against it. The manifests are awp-sim's (`awp-sim manifest --mode lockstep`, `--mode streaming`). [`frames.json`](frames.json) gives the harness one sample payload per channel.

```bash
pip install --pre awp-python awp-conformance
awp-conformance agent --manifest conformance/manifest-lockstep.json --frames conformance/frames.json \
  --mode lockstep --out report/ -- awp-demo --url '{url}' --token '{token}'
```

For streaming, use `manifest-streaming.json` with `--mode streaming`. CI runs both on every change. The tests cited below run in CI too:

```bash
uv run pytest tests/test_frames.py tests/test_evidence.py
```

## Manual evidence

### AWP-DAT-001, AWP-DAT-009: loss accounting at the receiver (lockstep)

In streaming, the suite reads the agent's loss accounting from its `obs.report`. Lockstep sessions send no `obs.report`, so these rows are `manual` there.

- `test_the_receiver_counts_seq_gaps_as_loss_but_not_the_gap_before_a_resync` (`tests/test_evidence.py`) runs in each time model:
  - on one channel, frames that skip two `seq` values count two missing frames in `ClientConnection.delivery()`;
  - on another, a resync frame after a gap of five counts none.
- The client holds no delta state. Each frame reaches the application whole, with its `keyframe` and `resync` flags.

### AWP-DAT-008: frame test vectors

`test_vectors` (`tests/test_frames.py`) runs `awp.frames` over every vector in `schemas/test-vectors/frames.json` of the `spec/` submodule, which is pinned at `spec-v0.1-draft.9`:

- the 10 valid vectors decode to their listed fields;
- the 7 marked `expect_error` are rejected with their listed error.

`test_roundtrip_without_vendor` re-encodes the 8 valid vectors that have no unknown extension or reserved bits, and each matches byte for byte.

### AWP-VER-009: the draft revision is named

- The [README](../README.md) and the [Python SDK page](https://www.agentworldprotocol.com/sdks/python) name `0.1-draft.9`.
- `awp.SPEC_REVISION` is `"0.1-draft.9"`.
- The `spec/` submodule is pinned at the tag `spec-v0.1-draft.9`.
- Each report records `"specification": "0.1-draft.9"`, and its claim names the revision.
