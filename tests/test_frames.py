from __future__ import annotations

import json

import pytest

from awp import frames
from awp.errors import AwpError, ErrorCode


def vectors(spec_dir):
    return json.loads((spec_dir / "schemas" / "test-vectors" / "frames.json").read_text())[
        "vectors"
    ]


def test_vectors(spec_dir):
    for v in vectors(spec_dir):
        data = bytes.fromhex(v["hex"])
        if "expect_error" in v:
            with pytest.raises(AwpError) as exc:
                frames.decode(data)
            assert exc.value.message == v["expect_error"], v["name"]
            continue
        f = frames.decode(data)
        got = {
            "channel_id": f.channel_id,
            "seq": f.seq,
            "ts_mono_ns": f.ts_mono_ns,
            "flags": f.flags,
            "keyframe": f.keyframe,
            "end_of_burst": f.end_of_burst,
            "resync": f.resync,
            "payload_len": len(f.payload),
            "payload_hex": f.payload.hex(),
            "tick": f.tick,
            "ts_sim_ns": f.ts_sim_ns,
            "ts_send_ns": f.ts_send_ns,
        }
        for key, want in v["expect"].items():
            if key == "flags":  # the vector's flags include bit 2 from the binary header
                want &= ~frames.HAS_EXTENSIONS
            if key == "vendor":
                assert [(t, list(b)) for t, b in f.vendor] == [
                    (e["type"], e["value"]) for e in want
                ]
                continue
            assert got[key] == want, (v["name"], key)


def test_roundtrip_without_vendor(spec_dir):
    for v in vectors(spec_dir):
        if "expect_error" in v or v["name"] in {"unknown_extension", "reserved_bits_set"}:
            continue
        data = bytes.fromhex(v["hex"])
        assert frames.encode(frames.decode(data)) == data, v["name"]


def test_inline_roundtrip():
    f = frames.Frame(
        channel_id=2, seq=9, ts_mono_ns=100, payload=b"{}", keyframe=True, resync=True, tick=3
    )
    params = frames.to_inline(f)
    assert params["flags"] == 9
    assert frames.from_inline(params) == f


def test_inline_rejects_bad_base64():
    with pytest.raises(AwpError) as exc:
        frames.from_inline(
            {"channel_id": 1, "seq": 1, "ts_mono_ns": 0, "flags": 0, "payload_b64": "@@"}
        )
    assert exc.value.code == ErrorCode.MALFORMED
