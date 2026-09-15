"""GRIB helpers on eccodes: message splitting, MARS keys, variants (spec §7.7).

``eccodes`` is imported inside the functions, never at module import, so the package
imports without the native library and definitions configured through
``ECCODES_DEFINITION_PATH`` (spec §4.1) are in place when eccodes loads. This module
never reads or changes that variable: keys are decoded with whatever definitions the
process has.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

MARS_NAMESPACE = "mars"
_INDICATOR = b"GRIB"
_END = b"7777"


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
