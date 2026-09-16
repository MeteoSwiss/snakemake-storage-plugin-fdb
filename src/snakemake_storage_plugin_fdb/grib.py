"""GRIB helpers on eccodes: message splitting, MARS keys, variants (FR-STORE-001).

``eccodes`` is imported inside the functions, never at module import, so the package
imports without the native library and definitions configured through
``ECCODES_DEFINITION_PATH`` (architecture.md §8.3) are in place when eccodes loads.
This module never reads or changes that variable: keys are decoded with whatever
definitions the process has.
"""

from __future__ import annotations

import os
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import Any

MARS_NAMESPACE = "mars"
_INDICATOR = b"GRIB"
_END = b"7777"
_HEADER = 16  # section 0 of a GRIB2 message; GRIB1 needs the first 8 bytes
_GRIB1_LARGE = 0x800000  # ECMWF large-message convention of GRIB edition 1


class GribError(ValueError):
    """Not GRIB, non-GRIB bytes outside messages, or an undecodable message."""


@dataclass(frozen=True)
class GribMessage:
    """One message of a GRIB file; ``data`` is ``file[offset : offset + length]``."""

    offset: int
    length: int
    data: bytes
    mars: dict[str, str]  # eccodes "mars" namespace, values as strings
    param_id: str


def _eccodes() -> Any:
    import eccodes

    return eccodes


def _keys(ec: Any, handle: Any) -> tuple[dict[str, str], str]:
    names = []
    it = ec.codes_keys_iterator_new(handle, MARS_NAMESPACE)
    try:
        while ec.codes_keys_iterator_next(it):
            names.append(ec.codes_keys_iterator_get_name(it))
    finally:
        ec.codes_keys_iterator_delete(it)
    mars = {name: ec.codes_get_string(handle, name) for name in names}
    return mars, ec.codes_get_string(handle, "paramId")


def _check_padding(path: object, data: bytes, start: int, end: int) -> None:
    """Bytes outside messages must be NUL padding (FDB would drop anything else)."""
    gap = data[start:end]
    if gap.strip(b"\0"):
        bad = start + len(gap) - len(gap.lstrip(b"\0"))
        where = "trailing non-GRIB bytes" if end == len(data) else "non-GRIB bytes"
        raise GribError(f"{path}: {where} at offset {bad}")


def split_messages(path: str | os.PathLike[str]) -> list[GribMessage]:
    """Split a GRIB file into messages with their MARS keys and paramId.

    NUL padding between and after messages (GRIB1 record padding) is allowed; any other
    byte outside a message, a truncated message or a file without messages raises
    ``GribError``.
    """
    ec = _eccodes()
    data = Path(path).read_bytes()
    spans: list[tuple[int, int, dict[str, str], str]] = []
    try:
        with open(path, "rb") as f:
            while (handle := ec.codes_grib_new_from_file(f)) is not None:
                try:
                    offset = ec.codes_get_message_offset(handle)
                    length = ec.codes_get_message_size(handle)
                    spans.append((offset, length, *_keys(ec, handle)))
                finally:
                    ec.codes_release(handle)
    except ec.GribInternalError as e:
        where = f" after offset {spans[-1][0] + spans[-1][1]}" if spans else ""
        raise GribError(f"{path}: cannot read GRIB message{where}: {e}") from e
    if not spans:
        raise GribError(f"{path} is not GRIB (no GRIB message found)")

    messages = []
    pos = 0
    for offset, length, mars, param_id in spans:
        _check_padding(path, data, pos, offset)
        messages.append(
            GribMessage(offset, length, data[offset : offset + length], mars, param_id)
        )
        pos = offset + length
    _check_padding(path, data, pos, len(data))
    return messages


def _readinto_exactly(source: Any, view: memoryview) -> int:
    """Fill ``view`` from ``source.readinto``; the byte count, short at the end.

    ``readinto`` rather than ``read``: a pyfdb data handle's ``read(n)`` zero-pads a
    short result to ``n`` bytes, ``readinto`` reports the count (architecture.md §13.4).
    """
    got = 0
    while got < len(view) and (n := source.readinto(view[got:])):
        got += n
    return got


def _read_exactly(source: Any, size: int) -> bytes:
    buffer = bytearray(size)
    return bytes(buffer[: _readinto_exactly(source, memoryview(buffer))])


def _message_length(header: bytes, label: str) -> int:
    """Total message length from section 0 (``GRIB`` indicator + length + edition)."""
    if len(header) < 8:
        raise GribError(f"{label}: truncated GRIB message header")
    edition = header[7]
    if edition == 1:
        length = int.from_bytes(header[4:7], "big")
        # Large GRIB1 messages carry the length in units of 120 bytes.
        if length >= _GRIB1_LARGE:
            length = (length & (_GRIB1_LARGE - 1)) * 120
        return length
    if edition == 2:
        if len(header) < _HEADER:
            raise GribError(f"{label}: truncated GRIB2 message header")
        return int.from_bytes(header[8:_HEADER], "big")
    raise GribError(f"{label}: unsupported GRIB edition {edition}")


def stream_messages(source: Any, label: str = "the FDB stream") -> Iterator[bytes]:
    """Yield the complete GRIB messages of a binary ``source`` with ``readinto`` (an
    opened pyfdb data handle, a file, a ``BytesIO``), one at a time.

    Memory is bounded by one message: the length is taken from section 0 and only that
    message is held (FR-DIRECT-001). NUL padding between and after messages is
    allowed, as in ``split_messages``; any other byte outside a message, and a message
    without its final ``7777``, raise ``GribError``. Nothing is decoded (no eccodes).
    """
    head = b""  # bytes read past the previous message
    while True:
        # Section 0 of the next message, skipping NUL padding; empty at the end.
        while len(head := head.lstrip(b"\0")) < _HEADER:
            more = _read_exactly(source, _HEADER - len(head))
            if not more:
                break
            head += more
        if not head:
            return
        if not head.startswith(_INDICATOR):
            raise GribError(f"{label}: non-GRIB bytes where a message was expected")
        length = _message_length(head, label)
        message = bytearray(length)
        view = memoryview(message)
        n = min(len(head), length)
        view[:n] = head[:n]
        got = n + _readinto_exactly(source, view[n:])
        if got != length or not message.endswith(_END):
            raise GribError(
                f"{label}: truncated GRIB message ({got} of {length} bytes)"
            )
        head = head[n:]
        yield bytes(message)


def _handle(ec: Any, msg: bytes) -> Any:
    if not msg.startswith(_INDICATOR):
        raise GribError("not a GRIB message (no 'GRIB' indicator)")
    try:
        handle = ec.codes_new_from_message(msg)
    except ec.GribInternalError as e:
        raise GribError(f"cannot decode GRIB message: {e}") from e
    try:
        size = ec.codes_get_message_size(handle)
    except ec.GribInternalError:
        size = len(msg) + 1  # eccodes could not even size it: treat as truncated
    if size > len(msg) or msg[size - len(_END) : size] != _END:
        ec.codes_release(handle)
        raise GribError(f"truncated GRIB message ({len(msg)} bytes, no final '7777')")
    return handle


def mars_keys(msg: bytes) -> tuple[dict[str, str], str]:
    """``(mars namespace keys, paramId)`` of the GRIB message starting ``msg``."""
    ec = _eccodes()
    handle = _handle(ec, msg)
    try:
        return _keys(ec, handle)
    finally:
        ec.codes_release(handle)


def message_of(data: bytes) -> GribMessage:
    """One in-memory GRIB message as ``GribMessage`` (offset 0), keys decoded."""
    mars, param_id = mars_keys(data)
    return GribMessage(0, len(data), data, mars, param_id)


def variant(template: bytes, zero_values: bool = True, **keys: Any) -> bytes:
    """Copy of a GRIB message with ``keys`` set in order (e.g. ``stream="oper"``).

    With ``zero_values`` all data values are set to 0, which makes the message a small
    constant field. Pass reserved names as ``**{"class": "od"}``.
    """
    ec = _eccodes()
    handle = _handle(ec, template)
    try:
        for key, value in keys.items():
            try:
                ec.codes_set(handle, key, value)
            except ec.GribInternalError as e:
                raise GribError(f"cannot set GRIB key {key}={value!r}: {e}") from e
        if zero_values:
            import numpy as np

            ec.codes_set_values(handle, np.zeros(ec.codes_get_size(handle, "values")))
        return ec.codes_get_message(handle)
    finally:
        ec.codes_release(handle)
