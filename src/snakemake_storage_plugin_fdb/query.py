"""Query language, normalisation and local paths (requirements.md §2.1, §2.2).

Pure Python: must not import ``pyfdb``, ``eccodes`` or metkit, since
``is_valid_query`` and ``postprocess_query`` run for every query during DAG building.
"""

from __future__ import annotations

import hashlib
import re
from collections.abc import Iterable
from dataclasses import dataclass

from snakemake_interface_storage_plugins.io import WILDCARD_REGEX

SCHEME = "fdb://"
SUFFIX = ".grib"
NAME_MAX = 255  # bytes per path component (ext4/xfs/lustre)
HASH_CHARS = 24
HASH_MARK = "~"  # first character of a hashed path component (FR-PATH-003)

GENERIC_ORDER = [
    "class",
    "expver",
    "stream",
    "domain",
    "date",
    "time",
    "type",
    "levtype",
    "levelist",
    "step",
    "number",
    "param",
]

INT_RE = re.compile(r"-?\d+")  # integer MARS values (steps, numbers, to/by bounds)
ECMWF_PARAM_TABLE = 128  # table whose paramIds carry no table prefix

_KEY_RE = re.compile(r"[A-Za-z][A-Za-z0-9_]*")
_ICHAR_RE = re.compile(r"[A-Za-z0-9.\-:_]")
_PARAM_TABLE_RE = re.compile(r"(\d+)\.(\d+)")  # e.g. 167.128, 70.131
_DATE_RE = re.compile(r"\d{8}")  # YYYYMMDD
# eckit::StreamParser (used by fdb5 SchemaParser) skips "#" to end of line anywhere.
_SCHEMA_COMMENT_RE = re.compile(r"#[^\n]*")

# A value is a sequence of pieces: (is_wildcard, text). Wildcard tokens are atomic.
_Piece = tuple[bool, str]


class QueryError(ValueError):
    """Invalid FDB query."""


def _pieces(text: str) -> list[_Piece]:
    out: list[_Piece] = []
    pos = 0
    for m in WILDCARD_REGEX.finditer(text):
        if m.start() > pos:
            out.append((False, text[pos : m.start()]))
        out.append((True, m.group(0)))
        pos = m.end()
    if pos < len(text):
        out.append((False, text[pos:]))
    return out


def _split(pieces: list[_Piece], sep: str) -> list[list[_Piece]]:
    """Split on ``sep`` occurring in literal pieces only."""
    parts: list[list[_Piece]] = [[]]
    for is_wc, text in pieces:
        if is_wc:
            parts[-1].append((True, text))
            continue
        for i, chunk in enumerate(text.split(sep)):
            if i:
                parts.append([])
            if chunk:
                parts[-1].append((False, chunk))
    return parts


def _strip(pieces: list[_Piece]) -> list[_Piece]:
    """Strip whitespace at both ends (literal pieces only)."""
    out = list(pieces)
    if out and not out[0][0]:
        out[0] = (False, out[0][1].lstrip())
    if out and not out[-1][0]:
        out[-1] = (False, out[-1][1].rstrip())
    return [p for p in out if p[1]]


def _text(pieces: Iterable[_Piece]) -> str:
    return "".join(text for _, text in pieces)


def _describe(ch: str) -> str:
    return "whitespace" if ch.isspace() else repr(ch)


def _replace_literal(value: str, old: str, new: str) -> str:
    return "".join(
        text if is_wc else text.replace(old, new) for is_wc, text in _pieces(value)
    )


def comparable(key: str, value: str) -> int | str | None:
    """Light normalisation of a MARS value for comparisons (FR-STORE-005): integers
    compare numerically (``time`` of one or two digits as hours, ``12`` -> ``1200``),
    ``param`` as a paramId (``N.T`` -> ``N`` for table 128, else ``T*1000+N``),
    everything else case-insensitively. ``None`` (not comparable) for other ``param``
    spellings and for ``date`` values other than ``YYYYMMDD`` (relative dates)."""
    if key == "param" and (m := _PARAM_TABLE_RE.fullmatch(value)):
        number, table = int(m.group(1)), int(m.group(2))
        return number if table == ECMWF_PARAM_TABLE else table * 1000 + number
    if key == "date":
        return int(value) if _DATE_RE.fullmatch(value) else None
    if INT_RE.fullmatch(value):
        if key == "time" and len(value) <= 2:
            return int(value) * 100
        return int(value)
    return None if key == "param" else value.lower()


@dataclass(frozen=True)
class KeyOrder:
    """Canonical key order: listed keys first, all other keys alphabetically."""

    keys: tuple[str, ...]

    def __post_init__(self) -> None:
        seen: set[str] = set()
        for k in self.keys:
            if not _KEY_RE.fullmatch(k) or k != k.lower():
                raise QueryError(f"invalid key name in key order: {k!r}")
            if k in seen:
                raise QueryError(f"duplicate key in key order: {k!r}")
            seen.add(k)

    @classmethod
    def generic(cls) -> KeyOrder:
        """ECMWF default-schema keys in MARS request order."""
        return cls(tuple(GENERIC_ORDER))

    @classmethod
    def from_setting(cls, csv: str) -> KeyOrder:
        """From a comma list such as ``"date,time,class"`` (``key_order`` setting)."""
        keys = [k.strip().lower() for k in csv.split(",")]
        if not any(keys):
            raise QueryError("key order setting is empty")
        if "" in keys:
            raise QueryError(f"empty key name in key order {csv!r}")
        return cls(tuple(keys))

    @classmethod
    def from_schema(cls, schema_text: str) -> KeyOrder:
        """Keys in order of first appearance across the rules of an FDB schema.

        ``#`` comments and type declarations outside ``[...]`` are ignored, as are the
        ``?``, ``?default``, ``-``, ``=values`` and ``:Type`` decorations of rule keys.
        """
        text = _SCHEMA_COMMENT_RE.sub("", schema_text)
        keys: list[str] = []
        depth = 0
        token: list[str] = []

        def flush() -> None:
            m = _KEY_RE.match("".join(token).strip())
            if depth > 0 and m and m.group(0).lower() not in keys:
                keys.append(m.group(0).lower())
            token.clear()

        for ch in text:
            if ch in "[],":
                flush()
                depth += {"[": 1, "]": -1}.get(ch, 0)
            elif depth > 0:
                token.append(ch)
        flush()
        if not keys:
            raise QueryError("no rule keys found in FDB schema")
        return cls(tuple(keys))

    def sort_key(self, key: str) -> tuple[int, str]:
        try:
            return (self.keys.index(key), "")
        except ValueError:
            return (len(self.keys), key)

    def sorted(self, keys: Iterable[str]) -> list[str]:
        return sorted(keys, key=self.sort_key)


@dataclass(frozen=True)
class ParsedQuery:
    """A parsed query. ``pairs`` are in canonical key order, values verbatim."""

    pairs: tuple[tuple[str, str], ...]

    def keys(self) -> list[str]:
        return [k for k, _ in self.pairs]

    def value(self, key: str) -> str:
        """Raw value, e.g. ``"0/6/12"``."""
        for k, v in self.pairs:
            if k == key:
                return v
        raise KeyError(key)

    def items(self, key: str) -> list[str]:
        """Value split on ``/`` outside wildcard tokens."""
        return [_text(p) for p in _split(_pieces(self.value(key)), "/")]

    def has_wildcards(self) -> bool:
        return bool(self.wildcard_keys())

    def wildcard_keys(self) -> set[str]:
        return {k for k, v in self.pairs if WILDCARD_REGEX.search(v)}

    def constant_pairs(self) -> dict[str, str]:
        wild = self.wildcard_keys()
        return {k: v for k, v in self.pairs if k not in wild}

    def single_valued(self) -> dict[str, str]:
        """Keys with exactly one literal item (no list, range or wildcard)."""
        return {k: v for k, v in self.constant_pairs().items() if "/" not in v}

    def has_range(self, key: str) -> bool:
        """Whether the value uses ``to``/``by``."""
        return any(item.lower() in ("to", "by") for item in self.items(key))

    def to_query(self) -> str:
        return SCHEME + ",".join(f"{k}={v}" for k, v in self.pairs)

    def to_request(self) -> dict[str, str]:
        """Request for ``inspect``/``retrieve``; lists stay raw ``a/b`` strings."""
        return dict(self.pairs)

    def _components(self) -> list[tuple[str, str, str]]:
        """``(key, value, unhashed path component)`` per pair."""
        last = len(self.pairs) - 1
        return [
            (k, v, f"{k}={_replace_literal(v, '/', '+')}{SUFFIX if i == last else ''}")
            for i, (k, v) in enumerate(self.pairs)
        ]

    def oversized_components(self) -> list[tuple[str, int]]:
        """``(key, bytes)`` of path components longer than ``NAME_MAX`` bytes.

        These are the components ``local_suffix`` hashes (or rejects, with a wildcard).
        """
        return [
            (k, len(comp.encode()))
            for k, _, comp in self._components()
            if len(comp.encode()) > NAME_MAX
        ]

    def local_suffix(self) -> str:
        """``key=value/.../key=value.grib`` with ``/`` in values replaced by ``+``.

        A constant component longer than ``NAME_MAX`` bytes is replaced by
        ``key=~<sha256(value)[:24]>``; a long component with a wildcard is an error.
        """
        parts = []
        for k, v, comp in self._components():
            ext = SUFFIX if k == self.pairs[-1][0] else ""  # keys are unique
            if len(comp.encode()) > NAME_MAX:
                if WILDCARD_REGEX.search(v):
                    raise QueryError(
                        f"local path component for key {k!r} exceeds {NAME_MAX} bytes "
                        "and contains a wildcard (cannot be hashed)"
                    )
                digest = hashlib.sha256(v.encode()).hexdigest()[:HASH_CHARS]
                comp = f"{k}={HASH_MARK}{digest}{ext}"
                if len(comp.encode()) > NAME_MAX:
                    raise QueryError(f"key name too long for a path component: {k!r}")
            parts.append(comp)
        return "/".join(parts)


_COMPONENT_RE = re.compile(rf"({_KEY_RE.pattern})=(.+)")


def query_of_path(path: str) -> str:
    """The query whose ``local_suffix`` ends ``path``: the inverse of the mapping.

    ``<prefix>/class=ea/.../step=0+6+12/param=167.grib`` gives
    ``fdb://class=ea,...,step=0/6/12,param=167``. ``QueryError`` for a path without
    the suffix, with a hashed component (the value is lost) or whose pairs are not a
    valid query.
    """
    parts = path.replace("\\", "/").split("/")
    if not parts[-1].endswith(SUFFIX):
        raise QueryError(f"no {SUFFIX} suffix")
    parts[-1] = parts[-1][: -len(SUFFIX)]
    pairs: list[str] = []
    for part in reversed(parts):  # the prefix ends at the first non key=value part
        match = _COMPONENT_RE.fullmatch(part)
        if not match:
            break
        key, value = match.groups()
        if value.startswith(HASH_MARK):
            raise QueryError(f"hashed component {key}={HASH_MARK}...")
        pairs.append(f"{key}={value.replace('+', '/')}")
    query = SCHEME + ",".join(reversed(pairs))
    parse(query)
    return query


def _parse_value(key: str, pieces: list[_Piece]) -> str:
    if not pieces:
        raise QueryError(f"empty value for key {key!r}")
    for item in _split(pieces, "/"):
        if not item:
            raise QueryError(f"empty item ('/' misplaced) in value of key {key!r}")
        for is_wc, text in item:
            if is_wc:
                continue
            for ch in text:
                if not _ICHAR_RE.fullmatch(ch):
                    raise QueryError(
                        f"invalid character {_describe(ch)} in value of key {key!r}"
                    )
    return _text(pieces)


def parse(query: str, order: KeyOrder | None = None) -> ParsedQuery:
    """Parse ``query`` (FR-QUERY-001) and sort keys by ``order`` (generic if ``None``).

    Raises ``QueryError`` with a message naming the problem.
    """
    if not isinstance(query, str) or not query.startswith(SCHEME):
        raise QueryError(f"query must start with {SCHEME!r}")
    body = query[len(SCHEME) :]
    if not body.strip():
        raise QueryError("empty query")
    order = order or KeyOrder.generic()

    pairs: dict[str, str] = {}
    segments = _split(_pieces(body), ",")
    for n, segment in enumerate(segments):
        segment = _strip(segment)
        if not segment:
            if n == len(segments) - 1:
                raise QueryError("trailing comma")
            raise QueryError("empty key=value pair")
        key_and_value = _split(segment, "=")
        key_pieces = _strip(key_and_value[0])
        if len(key_and_value) == 1:
            raise QueryError(f"missing '=' in {_text(segment)!r}")
        if len(key_and_value) > 2:
            raise QueryError(f"invalid character '=' in {_text(segment)!r}")
        if any(is_wc for is_wc, _ in key_pieces):
            raise QueryError(f"wildcards are not allowed in keys: {_text(segment)!r}")
        raw_key = _text(key_pieces)
        if not raw_key:
            raise QueryError(f"empty key in {_text(segment)!r}")
        if not _KEY_RE.fullmatch(raw_key):
            bad = next(
                (c for c in raw_key if not re.fullmatch(r"[A-Za-z0-9_]", c)), None
            )
            detail = f" (invalid character {_describe(bad)})" if bad else ""
            raise QueryError(f"invalid key {raw_key!r}{detail}")
        key = raw_key.lower()
        if key in pairs:
            raise QueryError(f"duplicate key {key!r}")
        pairs[key] = _parse_value(key, _strip(key_and_value[1]))

    return ParsedQuery(tuple((k, pairs[k]) for k in order.sorted(pairs)))


def normalize(query: str, order: KeyOrder | None = None) -> str:
    """Syntactic normalisation (FR-QUERY-007): whitespace, key case, key order."""
    return parse(query, order).to_query()


def validate(query: str) -> tuple[bool, str | None]:
    """Order-independent validity check: ``(True, None)`` or ``(False, reason)``."""
    try:
        parse(query)
    except QueryError as e:
        return False, str(e)
    return True, None
