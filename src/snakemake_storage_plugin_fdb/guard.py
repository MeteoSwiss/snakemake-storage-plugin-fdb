"""Identifier guard hook for identifier-mode archiving (FR-STORE-008, ADR-013).

A guard checks each GRIB message against the identifier the plugin built for it, before
anything is archived. v1 ships the hook only: ``identifier_check=none`` selects
``NoGuard``; ``strict`` is reserved (``StrictGuard`` is not implemented).
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable

if TYPE_CHECKING:
    from .grib import GribMessage
    from .query import ParsedQuery

IDENTIFIER_CHECKS = ("none", "strict")


class IdentifierMismatch(ValueError):
    """An identifier contradicts the GRIB metadata of its message."""

    def __init__(
        self,
        message_index: int,
        key: str,
        identifier_value: str,
        grib_value: str | None,
    ) -> None:
        self.message_index = message_index
        self.key = key
        self.identifier_value = identifier_value
        self.grib_value = grib_value
        super().__init__(
            f"message {message_index}: identifier {key}={identifier_value} does not "
            f"match the GRIB metadata ({key}={grib_value})"
        )


@runtime_checkable
class IdentifierGuard(Protocol):
    def check(
        self,
        message: GribMessage,
        identifier: Mapping[str, str],
        query: ParsedQuery,
    ) -> None:
        """Raise ``IdentifierMismatch`` if ``identifier`` contradicts ``message``."""
        ...


class NoGuard:
    """``identifier_check=none``: accepts every identifier."""

    def check(
        self,
        message: GribMessage,
        identifier: Mapping[str, str],
        query: ParsedQuery,
    ) -> None:
        return None


class StrictGuard:
    """``identifier_check=strict``: reserved, not implemented in this version."""

    def __init__(self) -> None:
        raise NotImplementedError("identifier_check=strict is reserved")

    def check(
        self,
        message: GribMessage,
        identifier: Mapping[str, str],
        query: ParsedQuery,
    ) -> None:  # pragma: no cover - cannot be constructed
        raise NotImplementedError("identifier_check=strict is reserved")


def make_guard(settings: Any) -> IdentifierGuard:
    """Guard for ``settings.identifier_check`` (``None`` means ``none``).

    Raises ``NotImplementedError`` for ``strict`` and ``ValueError`` for unknown values;
    the provider validates the setting first and reports both as ``WorkflowError``.
    """
    value = getattr(settings, "identifier_check", None) or "none"
    if value == "none":
        return NoGuard()
    if value == "strict":
        return StrictGuard()
    raise ValueError(f"unknown identifier_check {value!r}")
