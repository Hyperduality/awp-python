"""Validation against the canonical schemas bundled from the pinned spec.

The canonical schemas are receiver schemas: they accept unknown fields (AWP-VER-003). The sender
form applies the `x-awp-closed` and `x-awp-lint` annotations, exactly as the spec's CI does.
"""

from __future__ import annotations

import json
from functools import cache
from importlib.resources import files
from typing import Any

from jsonschema import Draft202012Validator
from referencing import Registry, Resource
from referencing.jsonschema import DRAFT202012

from .errors import AwpError, ErrorCode

BASE = "https://agentworldprotocol.com/schemas/v0.1/"
VENDOR_FIELD = r"^x-[a-z0-9]+\."
_META = "https://json-schema.org/draft/2020-12/schema"


def _load() -> dict[str, dict[str, Any]]:
    root = files("awp") / "_spec" / "schemas"
    out: dict[str, dict[str, Any]] = {}

    def walk(node: Any, prefix: str) -> None:
        for child in node.iterdir():
            if child.is_dir():
                walk(child, f"{prefix}{child.name}/")
            elif child.name.endswith(".schema.json"):
                out[prefix + child.name.removesuffix(".schema.json")] = json.loads(
                    child.read_text()
                )

    walk(root, "")
    return out


SCHEMAS = _load()

# Method → schema of its params, result, and notification params, as the spec's validator maps them.
METHODS: dict[str, dict[str, str]] = {
    "initialize": {"params": "agent-manifest", "result": "world-manifest"},
    "world.manifest": {"params": "empty-result", "result": "world-manifest"},
    "ping": {"params": "ping", "result": "ping-result"},
    "session.open": {"params": "session-open", "result": "session-ready"},
    "session.resume": {"params": "session-resume", "result": "session-ready"},
    "session.close": {"params": "empty-result", "result": "empty-result"},
    "session.transfer": {"params": "session-transfer", "result": "session-transfer-result"},
    "session.state": {"notification": "session-state"},
    "session.telemetry": {"notification": "session-telemetry"},
    "task.update": {"params": "task-update", "result": "empty-result"},
    "obs.subscribe": {"params": "subscribe", "result": "subscribe-result"},
    "obs.unsubscribe": {"params": "unsubscribe", "result": "subscribe-result"},
    "obs.frame": {"notification": "frame-inline"},
    "obs.report": {"notification": "obs-report"},
    "cmd.frame": {"notification": "frame-inline"},
    "action.submit": {"params": "action-submit", "result": "action-submit-result"},
    "action.cancel": {"params": "action-ref", "result": "action-cancel-result"},
    "action.status": {
        "params": "action-ref",
        "result": "action-status",
        "notification": "action-status",
    },
    "world.tick": {"params": "tick", "result": "tick-result"},
    "world.snapshot": {"params": "empty-result", "result": "snapshot-result"},
    "world.restore": {"params": "restore", "result": "reset-result"},
    "world.reset": {"params": "reset", "result": "reset-result"},
    "world.event": {"notification": "world-event"},
    "safety.approval_requested": {"notification": "approval-requested"},
    "safety.approval.respond": {"params": "approval-respond", "result": "empty-result"},
}


def schema_for(method: str, part: str) -> str | None:
    """The schema name for `part` ("params", "result", or "notification") of `method`."""
    return METHODS.get(method, {}).get(part)


def lint(node: Any) -> Any:
    """The sender form of a canonical schema."""
    if isinstance(node, list):
        return [lint(v) for v in node]
    if not isinstance(node, dict):
        return node
    out = {k: lint(v) for k, v in node.items() if k not in ("x-awp-closed", "x-awp-lint")}
    if "x-awp-lint" in node:
        out.update(lint(node["x-awp-lint"]))
    if node.get("x-awp-closed") is True:
        out["additionalProperties"] = False
        out["patternProperties"] = {VENDOR_FIELD: {}, **out.get("patternProperties", {})}
    return out


@cache
def _registry(sender: bool) -> Registry:
    resources = [
        (
            s["$id"],
            Resource.from_contents(lint(s) if sender else s, default_specification=DRAFT202012),
        )
        for s in SCHEMAS.values()
    ]
    return Registry().with_resources(resources)


@cache
def validator(name: str, *, sender: bool = False) -> Draft202012Validator:
    """Validator for `name` (e.g. "action-submit" or "safety-policy#/$defs/envelope")."""
    file, _, fragment = name.partition("#")
    if file not in SCHEMAS:
        raise KeyError(f"no schema {file!r}")
    ref = BASE + file + ".schema.json" + (f"#{fragment}" if fragment else "")
    return Draft202012Validator({"$ref": ref}, registry=_registry(sender))


def errors(name: str, instance: Any, *, sender: bool = False) -> list[str]:
    return [
        f"{'/'.join(map(str, e.absolute_path)) or '<root>'}: {e.message}"
        for e in validator(name, sender=sender).iter_errors(instance)
    ]


def check(name: str, instance: Any, *, sender: bool = False) -> None:
    """Raise AwpError(MALFORMED) if `instance` does not validate."""
    problems = errors(name, instance, sender=sender)
    if problems:
        raise AwpError(ErrorCode.MALFORMED, f"{name}: " + "; ".join(problems[:3]))


class ParamsValidator:
    """Validates action params against a manifest's params_schema (AWP-ACT-002, AWP-MAN-002).

    Local `$ref`s resolve against the manifest's own `$defs`; shared primitives against the
    published common schema.
    """

    _URI = "urn:awp:manifest"

    def __init__(self, manifest: dict[str, Any]) -> None:
        doc = {**manifest, "$schema": _META}
        registry = _registry(False).with_resource(
            self._URI, Resource.from_contents(doc, default_specification=DRAFT202012)
        )
        self._validators: dict[str, Draft202012Validator] = {}
        for i, decl in enumerate(manifest["action_schemas"]):
            Draft202012Validator.check_schema(decl["params_schema"])
            ref = f"{self._URI}#/action_schemas/{i}/params_schema"
            self._validators[decl["type"]] = Draft202012Validator({"$ref": ref}, registry=registry)

    def errors(self, action_type: str, params: Any) -> list[str]:
        return [
            f"{'/'.join(map(str, e.absolute_path)) or '<root>'}: {e.message}"
            for e in self._validators[action_type].iter_errors(params)
        ]
