"""pyfdb access layer: config and schema resolution, per-thread handles, inspect, list,
retrieve, archive, timestamps, expansion and error mapping (architecture.md §5.4).

``pyfdb`` is imported lazily (first FDB handle or ``Backend.expand()``), never at module
import, so the provider can export environment variables (architecture.md §8.3) before
FDB, metkit and eccodes load. The config and schema helpers are pure Python. Nothing in
this module changes the process environment; ``resolve_schema_path`` only reads it.
"""

from __future__ import annotations

import ctypes
import logging
import math
import os
import re
import threading
import time
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Any

import yaml
from snakemake_interface_common.exceptions import WorkflowError

from .grib import GribError
from .query import INT_RE, KeyOrder, ParsedQuery

if TYPE_CHECKING:
    import pyfdb

ConfigValue = Path | str | dict[str, Any] | None

CHUNK = 8 * 1024 * 1024  # retrieve buffer (FR-READ-007)
PART_SUFFIX = ".part"

_TIMESTAMP_RE = re.compile(r"timestamp=(\d+)\s*$")
_EXPANDED_LINE_RE = re.compile(r"^\t?(\w+)=(.*)$")
_SCHEMA_COMMENT_RE = re.compile(r"#[^\n]*")
_SCHEMA_KEY_RE = re.compile(r"\s*([A-Za-z][A-Za-z0-9_]*)\s*(.*)", re.S)
_USER_ERROR_PREFIX = re.compile(r"^(?:(?:UserError|Serious bug):\s*)+")

# Substrings of pyfdb RuntimeError messages (architecture.md §8.4; pyfdb 5.21.4.23)
_SPLITTER = "Cannot find a metkit SplitterBuilder"
_SCHEMA_MISMATCH = ("Keywords not used", "Could not find [", "Could not find a rule")
_CONFIG = ("Cannot open", "No writable roots available")


@dataclass(frozen=True)
class Field:
    """One FDB field as seen by ``inspect``/``list`` (architecture.md §5.4)."""

    key: dict[str, str]  # combined key, canonical values
    length: int  # message length in bytes; 0 below schema level 3
    timestamp: int  # index flush time (POSIX s); 0 if unknown (architecture.md §13.3)
    uri_path: str | None  # data file path for local toc stores


try:  # libc time(): the clock FDB stamps indexes with (architecture.md §8.7)
    _c_time = ctypes.CDLL(None).time
    _c_time.restype = ctypes.c_long
    _c_time.argtypes = [ctypes.c_void_p]
except (OSError, AttributeError, TypeError):  # TypeError: Windows (unsupported)
    _c_time = None
    logging.getLogger(__name__).debug("libc time() unavailable; using int(time.time())")


def fdb_time() -> int:
    """The current second on FDB's index clock, libc ``time()``: just after a second
    boundary it can still give the previous second when ``int(time.time())`` already
    gives the new one (architecture.md §8.7). ``int(time.time())`` where libc
    cannot be loaded."""
    return _c_time(None) if _c_time is not None else int(time.time())


@dataclass(frozen=True)
class SchemaInfo:
    """Rule keys of an FDB schema and their decorations, merged over all rules."""

    keys: tuple[str, ...]  # order of first appearance (= ``KeyOrder.from_schema``)
    optional: frozenset[str]  # ``key?`` and ``key?default``
    removed: frozenset[str]  # ``key-``
    defaults: dict[str, str]  # ``key?default``


def resolve_config(value: str | None) -> Path | str | None:
    """Setting value -> argument for ``pyfdb.FDB`` (FR-CONF-002).

    An existing file becomes a ``Path``; otherwise the text must parse as a YAML (or
    JSON) mapping and is returned unchanged. Anything else raises ``WorkflowError``,
    because pyfdb ignores a path string it cannot parse (architecture.md §13.7).
    """
    if value is None:
        return None
    if _is_file(value):
        return Path(value)
    try:
        parsed = yaml.safe_load(value)
    except yaml.YAMLError as e:
        raise WorkflowError(
            f"FDB configuration error: {value!r} is neither an existing file nor "
            f"valid YAML: {e}"
        ) from e
    if not isinstance(parsed, dict):
        raise WorkflowError(
            f"FDB configuration error: {value!r} is neither an existing file nor an "
            "inline YAML mapping"
        )
    return value


def _is_file(value: str | os.PathLike[str]) -> bool:
    try:
        return Path(value).is_file()
    except (OSError, ValueError):  # e.g. inline YAML longer than NAME_MAX
        return False


def _load_mapping(config: ConfigValue) -> dict[str, Any]:
    try:
        if isinstance(config, dict):
            return config
        if isinstance(config, Path):
            loaded = yaml.safe_load(config.read_text())
        elif isinstance(config, str):
            loaded = yaml.safe_load(config)
        else:
            return {}
    except (OSError, yaml.YAMLError):
        return {}
    return loaded if isinstance(loaded, dict) else {}


def _fdb_config(config: ConfigValue, env: Mapping[str, str]) -> dict[str, Any]:
    """The config FDB would load, following fdb5 ``Config::expandConfig``."""
    if config is not None:
        return _load_mapping(config)
    for name in ("FDB_CONFIG", "FDB5_CONFIG"):
        if name in env:
            return _load_mapping(env[name])
    path = env.get("FDB_CONFIG_FILE") or env.get("FDB5_CONFIG_FILE")
    if path:
        return _load_mapping(Path(path)) if _is_file(path) else {}
    if env.get("FDB_HOME"):
        for name in ("config.yaml", "config.json"):
            candidate = Path(env["FDB_HOME"], "etc", "fdb", name)
            if candidate.is_file():
                return _load_mapping(candidate)
    return {}


def resolve_schema_path(
    config: ConfigValue, env: Mapping[str, str] | None = None
) -> Path | None:
    """Local schema file that ``pyfdb.FDB(config)`` would use, without importing pyfdb.

    Config: ``config`` if given, else ``FDB_CONFIG``/``FDB5_CONFIG`` (YAML text), else
    ``FDB_CONFIG_FILE``/``FDB5_CONFIG_FILE``, else
    ``$FDB_HOME/etc/fdb/config.{yaml,json}``. Schema: its ``schema`` key, else
    ``FDB_SCHEMA_FILE``, else ``~fdb/etc/fdb/schema``; ``~fdb`` expands to the
    config's ``fdb_home`` or ``$FDB_HOME``. Returns ``None`` if that is not an existing
    file or ``~fdb`` cannot be expanded here (FDB would use its library directory).
    """
    env = os.environ if env is None else env
    cfg = _fdb_config(config, env)
    schema = cfg.get("schema")
    if not isinstance(schema, str) or not schema:
        schema = env.get("FDB_SCHEMA_FILE") or "~fdb/etc/fdb/schema"
    if schema == "~fdb" or schema.startswith("~fdb/"):
        home = cfg.get("fdb_home") or env.get("FDB_HOME")
        if not home:
            return None
        schema = str(home) + schema[len("~fdb") :]
    path = Path(schema)
    return path if _is_file(path) else None


def parse_schema(schema_text: str) -> SchemaInfo:
    """Keys and decorations (``key?``, ``key?default``, ``key-``) of an FDB schema.

    ``#`` comments and declarations outside ``[...]`` are ignored, as are ``=values``
    and ``:Type`` on rule keys. Raises ``QueryError`` if the text has no rule keys.
    """
    keys = KeyOrder.from_schema(schema_text).keys
    optional: set[str] = set()
    removed: set[str] = set()
    defaults: dict[str, str] = {}
    depth = 0
    token: list[str] = []

    def flush() -> None:
        m = _SCHEMA_KEY_RE.fullmatch("".join(token))
        token.clear()
        if depth <= 0 or not m:
            return
        key, rest = m.group(1).lower(), m.group(2).strip()
        if rest.startswith("?"):
            optional.add(key)
            default = rest[1:].split(":", 1)[0].strip()
            if default:
                defaults[key] = default
        elif rest.startswith("-"):
            removed.add(key)

    for ch in _SCHEMA_COMMENT_RE.sub("", schema_text):
        if ch in "[],":
            flush()
            depth += {"[": 1, "]": -1}.get(ch, 0)
        elif depth > 0:
            token.append(ch)
    flush()
    return SchemaInfo(keys, frozenset(optional), frozenset(removed), defaults)


def fallback_expand(request: Mapping[str, str]) -> dict[str, list[str]]:
    """Pure-Python expansion when metkit's is unavailable (architecture.md §8.8).

    Splits ``/`` lists and expands ``a/to/b[/by/c]`` for integers and ``YYYYMMDD``
    dates (step in days). No alias resolution; other ranges keep their items verbatim,
    which over-counts fields (the conservative direction for ``exists``).
    """
    return {key: _expand_items(value.split("/")) for key, value in request.items()}


def _expand_items(items: list[str]) -> list[str]:
    out: list[str] = []
    i = 0
    while i < len(items):
        if i + 2 < len(items) and items[i + 1].lower() == "to":
            end, by = i + 3, "1"
            if end + 1 < len(items) and items[end].lower() == "by":
                end, by = end + 2, items[end + 1]
            values = _range(items[i], items[i + 2], by)
            if values is not None:
                out.extend(values)
                i = end
                continue
        out.append(items[i])
        i += 1
    return out


def _range(start: str, stop: str, by: str) -> list[str] | None:
    if not all(INT_RE.fullmatch(v) for v in (start, stop, by)) or int(by) == 0:
        return None
    step = int(by)
    first, last = _as_date(start), _as_date(stop)
    if first and last:
        n = (last - first).days // step
        if n < 0:
            return None
        days = (first + timedelta(days=step * k) for k in range(n + 1))
        return [d.strftime("%Y%m%d") for d in days]
    a, b = int(start), int(stop)
    if (b - a) * step < 0:
        return None
    return [str(v) for v in range(a, b + (1 if step > 0 else -1), step)]


def _as_date(value: str) -> date | None:
    if len(value) != 8:
        return None
    try:
        return datetime.strptime(value, "%Y%m%d").date()
    except ValueError:
        return None


def count_fields(expanded: Mapping[str, list[str]]) -> int:
    """Expected field count ``E`` of an expanded request: product of the distinct
    values per key (architecture.md §8.8)."""
    return math.prod(len(set(values)) for values in expanded.values())


def _detail(exc: BaseException) -> str:
    lines = [line for line in str(exc).splitlines() if line.strip()]
    return _USER_ERROR_PREFIX.sub("", lines[0].strip()) if lines else type(exc).__name__


def map_error(
    exc: BaseException, query: str, local: str | os.PathLike[str] | None = None
) -> WorkflowError | None:
    """``WorkflowError`` for a known pyfdb/GRIB failure, ``None`` otherwise.

    The caller raises the result ``from exc`` or re-raises ``exc`` when ``None``
    (mapping table: architecture.md §8.4).
    """
    if isinstance(exc, GribError):
        return WorkflowError(str(exc))
    if not isinstance(exc, RuntimeError):
        return None
    text = str(exc)
    detail = _detail(exc)
    if "UserError" in text:
        hint = ""
        if "cannot expand" in text:
            hint = (
                " (if this value is valid for your FDB, point metkit_home at a MARS "
                "language that defines it)"
            )
        return WorkflowError(f"Invalid MARS request {query}: {detail}{hint}")
    if _SPLITTER in text:
        return WorkflowError(f"{local if local is not None else query} is not GRIB")
    if any(marker in text for marker in _SCHEMA_MISMATCH):
        where = f" ({local})" if local is not None else ""
        return WorkflowError(
            f"GRIB keys do not match the FDB schema for {query}{where}: {detail}"
        )
    if any(marker in text for marker in _CONFIG):
        return WorkflowError(f"FDB configuration error: {detail}")
    return None


class Backend:
    """pyfdb access for one provider: one archiving ``pyfdb.FDB`` per thread and a
    fresh handle per read (architecture.md §8.6).

    Methods propagate pyfdb's ``RuntimeError``s unchanged; callers convert them with
    ``map_error`` so the message can name the query and local file.
    """

    def __init__(
        self,
        config: ConfigValue = None,
        user_config: ConfigValue = None,
        logger: logging.Logger | None = None,
        schema_info: SchemaInfo | None = None,
    ) -> None:
        self.config = config
        self.user_config = user_config
        self.logger = logger or logging.getLogger(__name__)
        self.schema_info = schema_info
        self._local = threading.local()

    def _open(self) -> pyfdb.FDB:
        """A new ``pyfdb.FDB``; pyfdb is imported here, after the environment is set."""
        import pyfdb

        return pyfdb.FDB(self.config, self.user_config)

    def handle(self) -> pyfdb.FDB:
        """This thread's archiving handle, opened on first use; reads never use it."""
        fdb = getattr(self._local, "fdb", None)
        if fdb is None:
            fdb = self._local.fdb = self._open()
        return fdb

    def reader(self) -> pyfdb.FDB:
        """A new handle for one read.

        A handle that has read a database keeps that catalogue: fields archived later,
        by any handle, stay invisible to its ``inspect``/``list``/``retrieve``
        (architecture.md §8.6). Opening a handle is cheap, so reads get a fresh one.
        """
        return self._open()

    def inspect(self, request: Mapping[str, str]) -> list[Field]:
        """Fields ``retrieve(request)`` would return; missing ones are omitted."""
        return [self._field(el) for el in self.reader().inspect(dict(request))]

    def list(
        self,
        selection: Mapping[str, str],
        level: int = 3,
        include_masked: bool = False,
    ) -> list[Field]:
        """``fdb.list``: omitted keys are wildcards (architecture.md §13.4)."""
        elements = self.reader().list(
            dict(selection), include_masked=include_masked, level=level
        )
        return [self._field(el) for el in elements]

    def retrieve_to(
        self,
        request: Mapping[str, str],
        dest: str | os.PathLike[str],
        expected: int | None = None,
    ) -> int:
        """Stream ``retrieve(request)`` into ``dest`` via ``<dest>.part``; return bytes.

        The part file is fsynced and renamed over ``dest``. On any error, including a
        byte count different from ``expected``, it is removed and ``dest`` is untouched.
        """
        dest = Path(dest)
        part = dest.with_name(dest.name + PART_SUFFIX)
        dest.parent.mkdir(parents=True, exist_ok=True)
        written = 0
        try:
            fdb = self.reader()  # referenced while the data handle is read
            handle = fdb.retrieve(dict(request))
            with handle, open(part, "wb") as f:
                view = memoryview(bytearray(CHUNK))
                while (n := handle.readinto(view)) > 0:
                    f.write(view[:n])
                    written += n
                f.flush()
                os.fsync(f.fileno())
            if expected is not None and written != expected:
                raise WorkflowError(
                    f"retrieved {written} bytes for {dest}, expected {expected}"
                )
            os.replace(part, dest)
        except BaseException:
            part.unlink(missing_ok=True)
            raise
        return written

    def archive(self, data: bytes, identifier: Mapping[str, str] | None = None) -> None:
        """Archive GRIB bytes, natively or under ``identifier`` (not checked by FDB)."""
        self.handle().archive(
            data, dict(identifier) if identifier is not None else None
        )

    def flush(self) -> None:
        """Flush this thread's handle."""
        self.handle().flush()

    def expand(self, request: Mapping[str, str]) -> dict[str, list[str]] | None:
        """Canonical expansion by metkit via pyfdb's internal ``FDBToolRequest``.

        Returns ``None`` if that internal API is unavailable or its output cannot be
        parsed (callers then use ``fallback_expand``). An invalid request raises pyfdb's
        ``RuntimeError`` (``UserError``), like ``inspect`` would.
        """
        try:
            from pyfdb._internal import FDBToolRequest
            from pyfdb.pyfdb_type import UserInputMapper
        except ImportError as e:
            self.logger.debug(f"FDB request expansion unavailable: {e}")
            return None
        if not request:
            return {}
        try:
            selection = UserInputMapper.map_selection_to_internal(dict(request))
            tool = FDBToolRequest.from_internal_mars_selection(selection)
            text = repr(tool.tool_request)
        except RuntimeError:
            raise
        except Exception as e:  # internal API changed shape
            self.logger.debug(f"FDB request expansion unavailable: {e!r}")
            return None
        expanded: dict[str, list[str]] = {}
        for line in text.splitlines():
            m = _EXPANDED_LINE_RE.match(line)
            if m:
                expanded[m.group(1)] = m.group(2).rstrip(",").split("/")
        if not expanded:
            self.logger.debug(f"cannot parse FDB request expansion: {text!r}")
            return None
        return expanded

    def expected_count(self, request: Mapping[str, str]) -> int:
        """Expected number of fields: product of distinct expanded values per key."""
        expanded = self.expand(request)
        if expanded is None:
            expanded = fallback_expand(request)
        return count_fields(expanded)

    def spelling_diffs(
        self, parsed: ParsedQuery, expanded: Mapping[str, list[str]] | None = None
    ) -> list[tuple[str, str, str]]:
        """``(key, given, canonical)`` for literal values FDB spells differently.

        ``expanded`` is the expansion of ``parsed.constant_pairs()`` if the caller has
        it already (else it is computed here). Keys with wildcards or ``to``/``by``
        ranges are exempt; lists are compared item by item. Empty (with a debug log)
        if expansion is unavailable (FR-SPELL-001).
        """
        request = parsed.constant_pairs()
        if expanded is None:
            expanded = self.expand(request)
        if expanded is None:
            self.logger.debug("canonical-spelling check skipped (no expansion)")
            return []
        diffs: list[tuple[str, str, str]] = []
        for key in request:
            if parsed.has_range(key) or key not in expanded:
                continue
            given, canonical = parsed.items(key), expanded[key]
            if len(given) == len(canonical):
                pairs = zip(given, canonical, strict=True)
                diffs += [(key, g, c) for g, c in pairs if g != c]
            elif "/".join(given) != "/".join(canonical):
                diffs.append((key, "/".join(given), "/".join(canonical)))
        return diffs

    @staticmethod
    def timestamp_of(element: object) -> int:
        """Flush time from a ``ListElement`` repr, else 0 (architecture.md §13.3)."""
        m = _TIMESTAMP_RE.search(repr(element))
        return int(m.group(1)) if m else 0

    @classmethod
    def _field(cls, element: Any) -> Field:
        located = element.has_location()
        uri = element.uri if located else None
        return Field(
            key=dict(element.combined_key()),
            length=(element.length() or 0) if located else 0,
            timestamp=cls.timestamp_of(element),
            uri_path=(uri.path() or None) if uri is not None else None,
        )
