"""The frame envelope (spec/transport/frames): binary codec and the inline JSON form."""

from __future__ import annotations

import base64
import binascii
import struct
from dataclasses import dataclass, field
from typing import Any

from .errors import AwpError, ErrorCode
from .jsonrpc import MAX_SAFE_INT

MAGIC = b"AWPF"
VERSION = 1
HEADER = struct.Struct("<4sBBHQQI")

KEYFRAME = 0x01
END_OF_BURST = 0x02
HAS_EXTENSIONS = 0x04
RESYNC = 0x08

EXT_TICK = 0x01
EXT_TS_SIM_NS = 0x02
EXT_TS_SEND_NS = 0x03
_REGISTERED_LEN = {EXT_TICK: 8, EXT_TS_SIM_NS: 8, EXT_TS_SEND_NS: 8}


@dataclass(frozen=True, slots=True)
class Frame:
    channel_id: int
    seq: int
    ts_mono_ns: int
    payload: bytes = b""
    keyframe: bool = False
    end_of_burst: bool = False
    resync: bool = False
    tick: int | None = None
    ts_sim_ns: int | None = None
    ts_send_ns: int | None = None
    vendor: tuple[tuple[int, bytes], ...] = field(default=())

    @property
    def flags(self) -> int:
        """Flag bits 0, 1, and 3; bit 2 is a property of the binary encoding only."""
        return (
            (KEYFRAME if self.keyframe else 0)
            | (END_OF_BURST if self.end_of_burst else 0)
            | (RESYNC if self.resync else 0)
        )


def _malformed(detail: str) -> AwpError:
    return AwpError(ErrorCode.MALFORMED, detail)


def _bounded(value: int, name: str, *, signed: bool = False) -> int:
    if value > MAX_SAFE_INT or (signed and value < -MAX_SAFE_INT):
        raise AwpError(ErrorCode.INTEGER_RANGE, f"{name} exceeds 2^53-1")
    return value


def decode(data: bytes) -> Frame:
    """Decode a binary frame (AWP-DAT-001..009). Raises AwpError MALFORMED or INTEGER_RANGE."""
    if len(data) < HEADER.size:
        raise _malformed("frame shorter than 28-byte header")
    magic, version, flags, channel_id, seq, ts, payload_len = HEADER.unpack_from(data)
    if magic != MAGIC:
        raise _malformed("bad magic")
    if version != VERSION:
        raise _malformed(f"unsupported version {version}")
    ext: dict[str, int] = {}
    vendor: list[tuple[int, bytes]] = []
    offset = HEADER.size
    if flags & HAS_EXTENSIONS:
        if len(data) < offset + 2:
            raise _malformed("missing ext_len")
        (ext_len,) = struct.unpack_from("<H", data, offset)
        offset += 2
        end = offset + ext_len
        if end > len(data):
            raise _malformed("ext_len exceeds frame")
        seen: set[int] = set()
        while offset < end:
            if offset + 2 > end:
                raise _malformed("truncated TLV header")
            kind, length = data[offset], data[offset + 1]
            value = data[offset + 2 : offset + 2 + length]
            if offset + 2 + length > end:
                raise _malformed("TLV value exceeds ext_len")
            if kind in seen:
                raise _malformed(f"duplicate extension type {kind:#04x}")
            seen.add(kind)
            if kind in _REGISTERED_LEN and length != _REGISTERED_LEN[kind]:
                raise _malformed(f"extension {kind:#04x} has len {length}")
            if kind == EXT_TICK:
                ext["tick"] = _bounded(int.from_bytes(value, "little"), "tick")
            elif kind == EXT_TS_SIM_NS:
                ext["ts_sim_ns"] = _bounded(
                    int.from_bytes(value, "little", signed=True), "ts_sim_ns", signed=True
                )
            elif kind == EXT_TS_SEND_NS:
                ext["ts_send_ns"] = _bounded(int.from_bytes(value, "little"), "ts_send_ns")
            elif kind >= 0x80:
                vendor.append((kind, bytes(value)))
            offset += 2 + length
    if offset + payload_len != len(data):
        raise _malformed(f"frame length {len(data)} != {offset} + payload_len {payload_len}")
    return Frame(
        channel_id=channel_id,
        seq=_bounded(seq, "seq"),
        ts_mono_ns=_bounded(ts, "ts_mono_ns"),
        payload=bytes(data[offset:]),
        keyframe=bool(flags & KEYFRAME),
        end_of_burst=bool(flags & END_OF_BURST),
        resync=bool(flags & RESYNC),
        vendor=tuple(vendor),
        **ext,
    )


def encode(frame: Frame) -> bytes:
    entries = bytearray()
    for kind, value, fmt in (
        (EXT_TICK, frame.tick, "<Q"),
        (EXT_TS_SIM_NS, frame.ts_sim_ns, "<q"),
        (EXT_TS_SEND_NS, frame.ts_send_ns, "<Q"),
    ):
        if value is not None:
            entries += bytes((kind, 8)) + struct.pack(fmt, value)
    for kind, raw in frame.vendor:
        entries += bytes((kind, len(raw))) + raw
    flags = frame.flags | (HAS_EXTENSIONS if entries else 0)
    header = HEADER.pack(
        MAGIC, VERSION, flags, frame.channel_id, frame.seq, frame.ts_mono_ns, len(frame.payload)
    )
    ext = struct.pack("<H", len(entries)) + entries if entries else b""
    return header + ext + frame.payload


def to_inline(frame: Frame) -> dict[str, Any]:
    """params of an obs.frame / cmd.frame notification (AWP-DAT-004)."""
    params: dict[str, Any] = {
        "channel_id": frame.channel_id,
        "seq": frame.seq,
        "ts_mono_ns": frame.ts_mono_ns,
        "flags": frame.flags,
    }
    for name in ("tick", "ts_sim_ns", "ts_send_ns"):
        value = getattr(frame, name)
        if value is not None:
            params[name] = value
    params["payload_b64"] = base64.b64encode(frame.payload).decode("ascii")
    return params


def from_inline(params: dict[str, Any]) -> Frame:
    try:
        payload = base64.b64decode(params["payload_b64"], validate=True)
        flags = int(params["flags"])
        return Frame(
            channel_id=int(params["channel_id"]),
            seq=int(params["seq"]),
            ts_mono_ns=int(params["ts_mono_ns"]),
            payload=payload,
            keyframe=bool(flags & KEYFRAME),
            end_of_burst=bool(flags & END_OF_BURST),
            resync=bool(flags & RESYNC),
            tick=params.get("tick"),
            ts_sim_ns=params.get("ts_sim_ns"),
            ts_send_ns=params.get("ts_send_ns"),
        )
    except (KeyError, TypeError, ValueError, binascii.Error) as exc:
        raise _malformed(f"invalid inline frame: {exc}") from None
