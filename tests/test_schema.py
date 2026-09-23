from __future__ import annotations

import json

import pytest

from awp import schema
from awp.errors import AwpError

from .conftest import TRACES


def test_examples_validate_as_sender(spec_dir):
    for path in sorted((spec_dir / "examples" / "v0.1").glob("*.json")):
        inst = json.loads(path.read_text())
        name = inst.pop("$schema").removeprefix(schema.BASE).removesuffix(".schema.json")
        assert schema.errors(name, inst, sender=True) == [], path.name


def test_receiver_accepts_unknown_fields_sender_does_not():
    ping = {"origin_ns": 1, "future_field": True}
    assert schema.errors("ping", ping) == []
    assert schema.errors("ping", ping, sender=True)
    assert schema.errors("ping", {"origin_ns": 1, "x-acme.note": 1}, sender=True) == []


def test_lint_applies_sender_constraints():
    frame = {"channel_id": 1, "seq": 1, "ts_mono_ns": 0, "flags": 8, "payload_b64": ""}
    assert schema.errors("frame-inline", frame) == []
    assert schema.errors("frame-inline", frame, sender=True)


def test_check_raises_malformed():
    with pytest.raises(AwpError):
        schema.check("ping", {})


def test_params_validator_resolves_manifest_defs(spec_dir):
    manifest = json.loads((spec_dir / "examples" / "v0.1" / "world-manifest.json").read_text())
    v = schema.ParamsValidator(manifest)
    good = {"pose": {"frame": "base", "p_m": [0.1, 0.2, 0.3], "q": [0, 0, 0, 1]}}
    assert v.errors("move_to_pose", good) == []
    assert v.errors("move_to_pose", {"pose": {"frame": "base"}})
    assert v.errors("gripper_set", {"width_m": 1.0})


def test_trace_messages_validate(spec_dir):
    for path in sorted(TRACES.glob("*.jsonl")):
        for line in path.read_text().splitlines():
            entry = json.loads(line)
            if "msg" in entry and entry["msg"].get("method") == "initialize":
                assert schema.errors("agent-manifest", entry["msg"]["params"], sender=True) == []


def test_bundled_spec_matches_the_pinned_revision(spec_dir):
    import subprocess

    import awp

    tag = subprocess.run(
        ["git", "-C", str(spec_dir), "describe", "--tags", "--exact-match"],
        capture_output=True,
        text=True,
    )
    if tag.returncode != 0:
        pytest.skip("spec checkout is not at a tag")
    assert tag.stdout.strip() == f"spec-v{awp.SPEC_REVISION}"
