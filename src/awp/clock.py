"""Clock synchronization over the heartbeat (AWP-CLK-007, AWP-CLK-008)."""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class ClockSample:
    offset_ns: int
    rtt_ns: int


def sample(origin_ns: int, receive_ns: int, transmit_ns: int, destination_ns: int) -> ClockSample:
    rtt = (destination_ns - origin_ns) - (transmit_ns - receive_ns)
    offset = ((receive_ns - origin_ns) + (transmit_ns - destination_ns)) // 2
    return ClockSample(offset_ns=offset, rtt_ns=max(rtt, 0))


class ClockEstimator:
    """Keeps the minimum-RTT sample of the most recent eight exchanges."""

    WINDOW = 8

    def __init__(self) -> None:
        self._samples: deque[ClockSample] = deque(maxlen=self.WINDOW)

    def add(self, s: ClockSample) -> None:
        self._samples.append(s)

    @property
    def samples(self) -> int:
        return len(self._samples)

    @property
    def best(self) -> ClockSample | None:
        return min(self._samples, key=lambda s: s.rtt_ns) if self._samples else None

    @property
    def offset_ns(self) -> int | None:
        best = self.best
        return best.offset_ns if best else None

    @property
    def error_bound_ns(self) -> int | None:
        best = self.best
        return best.rtt_ns // 2 if best else None

    def to_session(self, agent_ns: int) -> int:
        """Map an agent-clock value to the session clock (AWP-CLK-009)."""
        offset = self.offset_ns
        if offset is None:
            raise RuntimeError("no clock sample yet; complete a ping exchange first (AWP-CLK-008)")
        return agent_ns + offset

    def to_agent(self, session_ns: int) -> int:
        offset = self.offset_ns
        if offset is None:
            raise RuntimeError("no clock sample yet")
        return session_ns - offset
