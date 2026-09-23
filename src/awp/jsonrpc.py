"""JSON-RPC 2.0 framing for the control channel (AWP-CTL-001..009)."""

from __future__ import annotations

import json
from typing import Any

from .errors import AwpError, ErrorCode

MAX_SAFE_INT = 2**53 - 1

Message = dict[str, Any]


def request(id: int | str, method: str, params: dict[str, Any] | None = None) -> Message:
    msg: Message = {"jsonrpc": "2.0", "id": id, "method": method}
    if params is not None:
        msg["params"] = params
    return msg


def notification(method: str, params: dict[str, Any]) -> Message:
    return {"jsonrpc": "2.0", "method": method, "params": params}


def result(id: int | str | None, value: dict[str, Any]) -> Message:
    return {"jsonrpc": "2.0", "id": id, "result": value}


def error(id: int | str | None, err: AwpError) -> Message:
    return {"jsonrpc": "2.0", "id": id, "error": err.to_dict()}


def is_request(msg: Message) -> bool:
    return "method" in msg and "id" in msg


def is_notification(msg: Message) -> bool:
    return "method" in msg and "id" not in msg


def is_response(msg: Message) -> bool:
    return "method" not in msg and ("result" in msg or "error" in msg)


def encode(msg: Message) -> str:
    return json.dumps(msg, separators=(",", ":"), ensure_ascii=False, allow_nan=False)


def decode(text: str | bytes) -> Message:
    """Parse one control-channel message; raise AwpError for anything a receiver must reject."""
    try:
        msg = json.loads(text, parse_constant=_reject_constant)
    except (ValueError, UnicodeDecodeError) as exc:
        raise AwpError(ErrorCode.PARSE_ERROR, str(exc)) from None
    if isinstance(msg, list):
        raise AwpError(ErrorCode.INVALID_REQUEST, "batch requests are not permitted (AWP-CTL-006)")
    if not isinstance(msg, dict) or msg.get("jsonrpc") != "2.0":
        raise AwpError(ErrorCode.INVALID_REQUEST, "not a JSON-RPC 2.0 object")
    if not (is_request(msg) or is_notification(msg) or is_response(msg)):
        raise AwpError(ErrorCode.INVALID_REQUEST, "neither request, notification, nor response")
    if "method" in msg and not isinstance(msg["method"], str):
        raise AwpError(ErrorCode.INVALID_REQUEST, "method must be a string")
    check_integer_range(msg)
    return msg


def check_integer_range(value: Any) -> None:
    """AWP-CTL-009: JSON integers beyond ±(2^53 - 1) close the session."""
    if isinstance(value, bool):
        return
    if isinstance(value, int):
        if not -MAX_SAFE_INT <= value <= MAX_SAFE_INT:
            raise AwpError(ErrorCode.INTEGER_RANGE, f"integer {value} exceeds 2^53-1")
    elif isinstance(value, dict):
        for v in value.values():
            check_integer_range(v)
    elif isinstance(value, list):
        for v in value:
            check_integer_range(v)


def _reject_constant(name: str) -> Any:
    raise ValueError(f"{name} is not valid JSON")
