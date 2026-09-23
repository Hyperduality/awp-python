"""The normative action lifecycle, loaded from the spec's transition table (AWP-LIF-001)."""

from __future__ import annotations

import json
from dataclasses import dataclass
from enum import StrEnum
from importlib.resources import files
from typing import Any


class ActionState(StrEnum):
    SUBMITTED = "submitted"
    PENDING_APPROVAL = "pending_approval"
    QUEUED = "queued"
    ACCEPTED = "accepted"
    EXECUTING = "executing"
    CANCELLING = "cancelling"
    REJECTED = "rejected"
    COMPLETED = "completed"
    FAILED = "failed"
    PREEMPTED = "preempted"
    CANCELLED = "cancelled"

    @property
    def terminal(self) -> bool:
        return _CLASSES[self] == "terminal"

    @property
    def pre_execution(self) -> bool:
        return _CLASSES[self] == "pre-execution"


@dataclass(frozen=True, slots=True)
class Transition:
    source: ActionState
    target: ActionState
    reasons: frozenset[str] | None
    via_error: bool


_TABLE = json.loads((files("awp") / "_spec" / "lifecycle.json").read_text())
_CLASSES: dict[ActionState, str] = {ActionState(s): c for s, c in _TABLE["states"].items()}
TRANSITIONS: dict[tuple[ActionState, ActionState], Transition] = {
    (ActionState(t["from"]), ActionState(t["to"])): Transition(
        ActionState(t["from"]),
        ActionState(t["to"]),
        frozenset(t["reasons"]) if "reasons" in t else None,
        t.get("wire") == "error",
    )
    for t in _TABLE["transitions"]
}


def permitted(source: ActionState, target: ActionState, reason: str | None = None) -> bool:
    """Whether a status notification may move an action from `source` to `target`."""
    t = TRANSITIONS.get((source, target))
    if t is None or t.via_error:
        return False
    if reason is None or t.reasons is None or reason.startswith("x-"):
        return True
    return reason in t.reasons


SUBMIT_FIELDS = (
    "type",
    "params",
    "embodiment_id",
    "preempt",
    "deadline_ms",
    "basis_ts_mono_ns",
    "valid_until_ns",
)


def same_submission(a: dict[str, Any], b: dict[str, Any]) -> bool:
    """AWP-ACT-009: absent never equals present; x- fields are not compared."""
    return all((f in a) == (f in b) and a.get(f) == b.get(f) for f in SUBMIT_FIELDS)
