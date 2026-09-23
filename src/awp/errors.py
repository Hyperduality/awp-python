"""AWP error codes (spec/loop/events-and-errors) and the exception that carries them."""

from __future__ import annotations

from enum import IntEnum
from typing import Any


class ErrorCode(IntEnum):
    PARSE_ERROR = -32700
    INVALID_REQUEST = -32600
    METHOD_NOT_FOUND = -32601
    INVALID_PARAMS = -32602
    INTERNAL_ERROR = -32603

    VERSION_UNSUPPORTED = 1001
    MALFORMED = 1002
    EMBODIMENT_UNAVAILABLE = 2001
    TIME_MODEL_UNSUPPORTED = 2002
    SESSION_EXPIRED = 2003
    SESSION_EXISTS = 2004
    SESSION_UNKNOWN = 2005
    INTEGER_RANGE = 2006
    CHANNEL_UNKNOWN = 2007
    PARAMS_INVALID = 3001
    BUSY = 3002
    QUEUE_FULL = 3003
    ACTION_ID_CONFLICT = 3004
    ESTOP_ACTIVE = 3005
    TICK_NOT_AUTHORIZED = 3006
    STALE_INTENT = 3007
    ACTION_UNKNOWN = 3008
    TICK_MISMATCH = 3009
    FORBIDDEN = 4001
    ENVELOPE_EXCEEDED = 4002
    APPROVAL_DENIED = 4003
    APPROVAL_TIMEOUT = 4004

    @property
    def is_jsonrpc(self) -> bool:
        return -32768 <= self.value <= -32000

    @property
    def wire_name(self) -> str:
        if self.is_jsonrpc:
            return _JSONRPC_MESSAGES[self]
        return f"AWP_{self.name}"


_JSONRPC_MESSAGES = {
    ErrorCode.PARSE_ERROR: "Parse error",
    ErrorCode.INVALID_REQUEST: "Invalid request",
    ErrorCode.METHOD_NOT_FOUND: "Method not found",
    ErrorCode.INVALID_PARAMS: "Invalid params",
    ErrorCode.INTERNAL_ERROR: "Internal error",
}

# AWP_ENVELOPE_EXCEEDED is retryable only for rate limits; callers pass retryable explicitly.
RETRYABLE = frozenset(
    {
        ErrorCode.EMBODIMENT_UNAVAILABLE,
        ErrorCode.BUSY,
        ErrorCode.QUEUE_FULL,
        ErrorCode.ESTOP_ACTIVE,
        ErrorCode.STALE_INTENT,
        ErrorCode.APPROVAL_TIMEOUT,
    }
)


class AwpError(Exception):
    """A JSON-RPC error object with AWP data (AWP-ERR-001)."""

    def __init__(
        self,
        code: int,
        detail: str | None = None,
        *,
        retryable: bool | None = None,
        message: str | None = None,
        **data: Any,
    ) -> None:
        try:
            known: ErrorCode | None = ErrorCode(code)
        except ValueError:
            known = None
        self.code = code
        self.message = message or (known.wire_name if known else f"AWP_{code}")
        self.retryable = retryable if retryable is not None else known in RETRYABLE
        self.detail = detail
        self.data = data
        super().__init__(f"{self.message} ({code})" + (f": {detail}" if detail else ""))

    def to_dict(self) -> dict[str, Any]:
        err: dict[str, Any] = {"code": self.code, "message": self.message}
        if not (-32768 <= self.code <= -32000) or self.detail or self.data:
            data: dict[str, Any] = {"retryable": self.retryable, **self.data}
            if self.detail:
                data["detail"] = self.detail
            err["data"] = data
        return err

    @classmethod
    def from_dict(cls, err: dict[str, Any]) -> AwpError:
        data = dict(err.get("data") or {})
        retryable = bool(data.pop("retryable", False))
        detail = data.pop("detail", None)
        return cls(err["code"], detail, retryable=retryable, message=err.get("message"), **data)


class ProtocolError(Exception):
    """The peer violated the protocol in a way the receiver cannot recover from."""
