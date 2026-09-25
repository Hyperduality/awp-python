"""Agent World Protocol client protocol layer.

`ClientConnection` is the agent side of AWP as a sans-IO state machine; `awp.aio.AsyncClient`
drives it over a WebSocket. This package targets the specification revision in `SPEC_REVISION`.
"""

from importlib.metadata import PackageNotFoundError, version

from .client import (
    ActionRecord,
    ActionUpdated,
    ApprovalRequested,
    ClientConnection,
    ErrorResponse,
    Event,
    FrameReceived,
    Initialized,
    ProtocolViolation,
    ReplayCompleted,
    Response,
    SessionOpened,
    SessionStateChanged,
    Telemetry,
    TickCompleted,
    WorldEvent,
)
from .errors import AwpError, ErrorCode, ProtocolError
from .frames import Frame
from .lifecycle import ActionState

SPEC_REVISION = "0.1-draft.6"
PROTOCOL_VERSION = "0.1"

try:
    __version__ = version("awp-python")
except PackageNotFoundError:  # pragma: no cover - running from a source tree without install
    __version__ = "0.0.0"

__all__ = [
    "PROTOCOL_VERSION",
    "SPEC_REVISION",
    "ActionRecord",
    "ActionState",
    "ActionUpdated",
    "ApprovalRequested",
    "AwpError",
    "ClientConnection",
    "ErrorCode",
    "ErrorResponse",
    "Event",
    "Frame",
    "FrameReceived",
    "Initialized",
    "ProtocolError",
    "ProtocolViolation",
    "ReplayCompleted",
    "Response",
    "SessionOpened",
    "SessionStateChanged",
    "Telemetry",
    "TickCompleted",
    "WorldEvent",
    "__version__",
]
