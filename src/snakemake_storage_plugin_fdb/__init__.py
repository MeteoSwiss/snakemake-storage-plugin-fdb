"""Snakemake storage plugin for ECMWF's Fields DataBase (FDB).

Importing this module loads no FDB/eccodes native library: ``is_valid_query`` and
``postprocess_query`` are pure Python, and ``pyfdb``/``eccodes`` are imported in
``StorageProvider.__post_init__`` only after the environment has been prepared
(architecture.md §8.3).
"""

import contextlib
import itertools
import os
import re
import threading
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional

from snakemake_interface_common.exceptions import WorkflowError
from snakemake_interface_storage_plugins.common import Operation
from snakemake_interface_storage_plugins.io import IOCacheStorageInterface, Mtime
from snakemake_interface_storage_plugins.settings import StorageProviderSettingsBase
from snakemake_interface_storage_plugins.storage_object import (
    StorageObjectGlob,
    StorageObjectRead,
    StorageObjectWrite,
    retry_decorator,
)
from snakemake_interface_storage_plugins.storage_provider import (
    ExampleQuery,
    QueryType,
    StorageProviderBase,
    StorageQueryValidationResult,
)

from .backend import (
    Backend,
    Field,
    SchemaInfo,
    count_fields,
    fallback_expand,
    fdb_time,
    map_error,
    parse_schema,
    resolve_config,
)
from .backend import resolve_schema_path as _resolve_schema_path
from .grib import GribError, GribMessage, split_messages
from .guard import IDENTIFIER_CHECKS, IdentifierMismatch, make_guard
from .query import (
    NAME_MAX,
    KeyOrder,
    ParsedQuery,
    QueryError,
    comparable,
    normalize,
    parse,
)
from .query import validate as _validate_query

ARCHIVE_MODES = ("identifier", "native")
STORE_CHECKS = ("strict", "warn")
CANONICAL_SPELLINGS = ("warn", "error", "ignore")
REMOVE_POLICIES = ("warn", "ignore", "error")
LANGUAGE_FILE = Path("share", "metkit", "language.yaml")
MEMFS_PREFIX = "/MEMFS/"  # eccodes' in-memory definitions (bundled with the wheels)

_ENV_NAME_RE = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")
_KEY_NAME_RE = re.compile(r"[A-Za-z][A-Za-z0-9_]*")
_ENV_LOCK = threading.Lock()
# Values this process's providers applied from eccodes_definitions / metkit_home,
# to warn when two providers disagree (one process environment, architecture.md §8.3).
_APPLIED: dict[str, str] = {}
# Queries already warned about (once per process): non-canonical spelling
# (FR-SPELL-001) and remove_policy=warn (FR-REMOVE-001).
_SPELLING_WARNED: set[str] = set()
_REMOVE_WARNED: set[str] = set()
_WARNED_LOCK = threading.Lock()
MISSING_SHOWN = 10  # missing field combinations listed in a retrieve error
STAY_NOTE = "they stay in FDB until the next successful store masks them"


def _first_time(registry: set[str], query: str) -> bool:
    """Record ``query`` in ``registry``; whether it was not there yet."""
    with _WARNED_LOCK:
        if query in registry:
            return False
        registry.add(query)
        return True


def _retry_fdb_io(func):
    """The interface's ``retry_decorator`` (3 attempts, exponential wait from 3 s)
    raising the last attempt's own exception instead of tenacity's ``RetryError``,
    so it can be mapped (architecture.md §8.5)."""
    return retry_decorator(func).retry_with(reraise=True)


# typing.Optional, not "X | None": Snakemake unwraps only typing.Optional for the CLI.
@dataclass
class StorageProviderSettings(StorageProviderSettingsBase):
    config: Optional[str] = field(  # noqa: UP045
        default=None,
        metadata={
            "help": "FDB configuration: path to a YAML file or inline YAML/JSON text. "
            "Default: FDB's own environment (FDB_CONFIG, FDB_CONFIG_FILE, FDB_HOME).",
        },
    )
    user_config: Optional[str] = field(  # noqa: UP045
        default=None,
        metadata={
            "help": "FDB user configuration (e.g. 'useSubToc: true'): path to a YAML "
            "file or inline YAML/JSON text.",
        },
    )
    archive_mode: Optional[str] = field(  # noqa: UP045
        default="native",
        metadata={
            "help": "How outputs are archived: 'native' (default; FDB derives the keys "
            "from the GRIB) or 'identifier' (the plugin builds the FDB key of every "
            "message; use it only with schemas whose rules share one key set, or "
            "supply the other keys in the query).",
        },
    )
    identifier_check: Optional[str] = field(  # noqa: UP045
        default="none",
        metadata={
            "help": "Check of identifiers against GRIB metadata before archiving: "
            "'none'. 'strict' is reserved and not implemented in this version.",
        },
    )
    store_check: Optional[str] = field(  # noqa: UP045
        default="strict",
        metadata={
            "help": "'strict': a stored file must provide exactly the fields its query "
            "expands to; 'warn': fewer fields are allowed and logged.",
        },
    )
    canonical_spelling: Optional[str] = field(  # noqa: UP045
        default="warn",
        metadata={
            "help": "Query values FDB spells differently (e.g. param=2t vs 167): "
            "'warn', 'error' or 'ignore'.",
        },
    )
    remove_policy: Optional[str] = field(  # noqa: UP045
        default="warn",
        metadata={
            "help": "FDB cannot delete fields; what removing an output does: 'warn' "
            "(no-op with a warning), 'ignore' (silent no-op) or 'error'.",
        },
    )
    glob_required_keys: Optional[str] = field(  # noqa: UP045
        default="class",
        metadata={
            "help": "Comma list of keys that must be constant in glob_wildcards "
            "patterns.",
        },
    )
    eccodes_definitions: Optional[str] = field(  # noqa: UP045
        default=None,
        metadata={
            "help": "Colon-separated eccodes definitions directories, prepended in "
            "order to ECCODES_DEFINITION_PATH.",
        },
    )
    metkit_home: Optional[str] = field(  # noqa: UP045
        default=None,
        metadata={
            "help": "Directory exported as METKIT_HOME for a custom MARS language; "
            "must contain share/metkit/language.yaml.",
        },
    )
    key_order: Optional[str] = field(  # noqa: UP045
        default=None,
        metadata={
            "help": "Comma list of keys defining the canonical key order of queries "
            "and local paths. Default: the FDB schema's rule order, else a generic "
            "MARS order.",
        },
    )
    env: Optional[str] = field(  # noqa: UP045
        default=None,
        metadata={
            "help": "Environment overrides NAME=VALUE[,NAME=VALUE] exported before the "
            "FDB libraries load (e.g. FDB_HOME=/path).",
        },
    )


def _choice(settings: Any, name: str, allowed: tuple[str, ...]) -> str:
    value = getattr(settings, name, None)
    if value is None:
        return StorageProviderSettings.__dataclass_fields__[name].default
    if value not in allowed:
        raise WorkflowError(f"invalid {name} {value!r} (allowed: {', '.join(allowed)})")
    return value


def _parse_env(value: str | None) -> dict[str, str]:
    """``NAME=VALUE[,NAME=VALUE]`` -> dict; values cannot contain commas."""
    if not value:
        return {}
    out: dict[str, str] = {}
    for item in value.split(","):
        name, sep, val = item.partition("=")
        name = name.strip()
        if not sep or not _ENV_NAME_RE.fullmatch(name):
            raise WorkflowError(
                f"invalid env setting {value!r}: expected NAME=VALUE[,NAME=VALUE], "
                f"got {item!r}"
            )
        if name in out:
            raise WorkflowError(f"invalid env setting {value!r}: duplicate {name}")
        out[name] = val
    return out


def _definition_dirs(value: str | None) -> list[str]:
    """Absolute ``eccodes_definitions`` entries; each must be an existing directory."""
    dirs = []
    for entry in (value or "").split(":"):
        if not entry:
            continue
        if entry.startswith(MEMFS_PREFIX):
            dirs.append(entry)
            continue
        if not Path(entry).is_dir():
            raise WorkflowError(
                f"eccodes_definitions: {entry!r} is not an existing directory"
            )
        dirs.append(os.path.abspath(entry))
    return dirs


def _key_list(name: str, value: str | None) -> tuple[str, ...]:
    keys = [k.strip().lower() for k in (value or "").split(",") if k.strip()]
    bad = [k for k in keys if not _KEY_NAME_RE.fullmatch(k)]
    if bad:
        raise WorkflowError(f"invalid key name(s) in {name}: {', '.join(bad)}")
    return tuple(keys)


class StorageProvider(StorageProviderBase):
    """One FDB (per tag): settings, environment, key order and the pyfdb backend."""

    def __post_init__(self) -> None:
        settings = self.settings or StorageProviderSettings()
        self.archive_mode = _choice(settings, "archive_mode", ARCHIVE_MODES)
        self.store_check = _choice(settings, "store_check", STORE_CHECKS)
        self.canonical_spelling = _choice(
            settings, "canonical_spelling", CANONICAL_SPELLINGS
        )
        self.remove_policy = _choice(settings, "remove_policy", REMOVE_POLICIES)
        identifier_check = _choice(settings, "identifier_check", IDENTIFIER_CHECKS)
        if identifier_check == "strict":
            raise WorkflowError(
                "identifier_check=strict is reserved and not implemented in this "
                "version"
            )
        self.glob_required_keys = _key_list(
            "glob_required_keys", settings.glob_required_keys
        )
        self._normalised: set[str] = set()  # queries seen by postprocess_query

        self._prepare_environment(settings)  # before anything reads the environment

        self.config = self._absolute(resolve_config(settings.config))
        self.user_config = self._absolute(resolve_config(settings.user_config))
        self.schema_path = _resolve_schema_path(self.config)
        self.schema_info = self._read_schema(self.schema_path)
        if settings.key_order:
            try:
                self.key_order = KeyOrder.from_setting(settings.key_order)
            except QueryError as e:
                raise WorkflowError(f"invalid key_order: {e}") from e
        elif self.schema_info is not None:
            self.key_order = KeyOrder(self.schema_info.keys)
        else:
            self.key_order = KeyOrder.generic()

        try:  # lazy import: the environment is final now
            import eccodes  # noqa: F401
            import pyfdb  # noqa: F401
        except ImportError as e:
            raise WorkflowError(f"cannot import the FDB/eccodes bindings: {e}") from e
        self.backend = Backend(
            self.config, self.user_config, self.logger, self.schema_info
        )
        self.guard = make_guard(settings)

    # --- construction helpers ------------------------------------------------------

    def _prepare_environment(self, settings: Any) -> None:
        """Validate everything, then export in one go (architecture.md §8.3)."""
        planned = _parse_env(settings.env)

        def current(name: str) -> str | None:
            return planned.get(name, os.environ.get(name))

        applied: dict[str, str] = {}  # values coming from the two dedicated settings

        dirs = _definition_dirs(settings.eccodes_definitions)
        if dirs:
            joined = applied["ECCODES_DEFINITION_PATH"] = ":".join(dirs)
            existing = current("ECCODES_DEFINITION_PATH")
            # Idempotent: a second provider with the same setting, or a spawned job
            # inheriting the exported value, does not prepend the directories again.
            if not existing:
                planned["ECCODES_DEFINITION_PATH"] = joined
            elif existing != joined and not existing.startswith(joined + ":"):
                planned["ECCODES_DEFINITION_PATH"] = f"{joined}:{existing}"

        source = "the env setting" if "METKIT_HOME" in planned else "the environment"
        if settings.metkit_home:
            home = applied["METKIT_HOME"] = os.path.abspath(settings.metkit_home)
            before = current("METKIT_HOME")
            if before is not None and before != home:
                self.logger.info(
                    f"FDB storage: metkit_home overrides METKIT_HOME={before} "
                    f"with {home}"
                )
            planned["METKIT_HOME"] = home
            source = "metkit_home"
        home = current("METKIT_HOME")
        if home and not (Path(home) / LANGUAGE_FILE).is_file():
            raise WorkflowError(
                f"METKIT_HOME={home} (from {source}) has no {LANGUAGE_FILE}; "
                "FDB would hang instead of failing"
            )

        if current("ECKIT_EXCEPTION_IS_SILENT") is None:
            planned["ECKIT_EXCEPTION_IS_SILENT"] = "1"

        with _ENV_LOCK:
            for name, value in applied.items():
                if name in _APPLIED and _APPLIED[name] != value:
                    self.logger.warning(
                        f"FDB storage: providers in one process use different {name} "
                        f"settings ({_APPLIED[name]} vs {value}); the last one wins"
                    )
                _APPLIED[name] = value
            os.environ.update(planned)

    @staticmethod
    def _absolute(value: Path | str | None) -> Path | str | None:
        return value.absolute() if isinstance(value, Path) else value

    @staticmethod
    def _read_schema(path: Path | None) -> SchemaInfo | None:
        if path is None:
            return None
        try:
            return parse_schema(path.read_text())
        except (OSError, QueryError) as e:
            raise WorkflowError(f"FDB configuration error: schema {path}: {e}") from e

    # --- StorageProviderBase -------------------------------------------------------

    @classmethod
    def example_queries(cls) -> list[ExampleQuery]:
        return [
            ExampleQuery(
                query="fdb://class=od,expver=0001,stream=oper,date={date},time=0000,"
                "type=fc,levtype=sfc,step=0/6/12,param=167",
                description="2 m temperature at steps 0, 6 and 12 of the 00 UTC "
                "forecast of each date (one local file with 3 fields).",
                type=QueryType.ANY,
            ),
            ExampleQuery(
                query="fdb://class=ea,expver=0001,stream=enda,date=20200101,time=0000,"
                "domain=g,type=an,levtype=sfc,step=0,number=0,param=167",
                description="A single ensemble-member analysis field.",
                type=QueryType.INPUT,
            ),
            ExampleQuery(
                query="fdb://class=od,expver=0001,stream=oper,date={date},time={time},"
                "type=fc,levtype=sfc,step=0/to/48/by/6,param=167/165/166",
                description="A to/by step range for three parameters (27 fields).",
                type=QueryType.INPUT,
            ),
        ]

    def rate_limiter_key(self, query: str, operation: Operation) -> Any:
        return "fdb"

    def default_max_requests_per_second(self) -> float:
        return 10.0

    def use_rate_limiter(self) -> bool:
        return False

    @classmethod
    def is_valid_query(cls, query: str) -> StorageQueryValidationResult:
        valid, reason = _validate_query(query)
        return StorageQueryValidationResult(query=query, valid=valid, reason=reason)

    def postprocess_query(self, query: str) -> str:
        """Syntactic normalisation in this provider's key order (FR-QUERY-007).

        The result is recorded for the over-long wildcard guard (FR-PATH-004). Invalid
        queries are returned unchanged; the storage object reports the error on use.
        """
        try:
            normalised = normalize(query, self.key_order)
        except QueryError:
            return query
        self._normalised.add(normalised)
        return normalised

    def is_normalised(self, query: str) -> bool:
        """Whether ``query`` was returned by ``postprocess_query`` of this provider."""
        return query in self._normalised

    def safe_print(self, query: str) -> str:
        return query


class StorageObject(StorageObjectRead, StorageObjectWrite, StorageObjectGlob):
    """One query: a MARS request mapped to one local GRIB file (FR-QUERY-002)."""

    provider: StorageProvider

    def __post_init__(self) -> None:
        self._parsed_for: str | None = None
        self._parsed: ParsedQuery | None = None
        self._parse_error: QueryError | None = None
        self._expansion_for: str | None = None
        self._expansion: dict[str, list[str]] = {}
        self._no_mtime_warned = False
        parsed = self._parse()
        if parsed is None or self.provider.is_normalised(self.query):
            return
        # Built by Snakemake from apply_wildcards(), bypassing postprocess_query:
        # hashing a component here would break local_suffix commutation (FR-PATH-004).
        oversized = parsed.oversized_components()
        if oversized:
            key, size = oversized[0]
            raise WorkflowError(
                f"local path component for key {key!r} of {self.query} is {size} "
                f"bytes after wildcard substitution (limit {NAME_MAX}); wildcard "
                "values must be single MARS values (put lists in the query, not in "
                "wildcards)"
            )

    def _parse(self) -> ParsedQuery | None:
        """Parsed ``self.query``, cached per query text.

        Not precomputed once: Snakemake copies objects and rewrites ``query`` to inject
        wildcard constraints without calling ``__post_init__`` (``rules.py``).
        """
        if self._parsed_for != self.query:
            try:
                self._parsed, self._parse_error = (
                    parse(self.query, self.provider.key_order),
                    None,
                )
            except QueryError as e:
                self._parsed, self._parse_error = None, e
            self._parsed_for = self.query
        return self._parsed

    @property
    def parsed(self) -> ParsedQuery:
        """Parsed query; ``WorkflowError`` with the parser message if invalid."""
        parsed = self._parse()
        if parsed is None:
            raise WorkflowError(f"invalid FDB query {self.query}: {self._parse_error}")
        return parsed

    def local_suffix(self) -> str:
        try:
            return self.parsed.local_suffix()
        except QueryError as e:
            raise WorkflowError(f"invalid FDB query {self.query}: {e}") from e

    # --- read helpers ----------------------------------------------------------------

    def _request(self) -> dict[str, str]:
        """``inspect``/``retrieve`` request of the query; no wildcards allowed."""
        parsed = self.parsed
        if parsed.has_wildcards():
            raise WorkflowError(f"FDB query {self.query} has unresolved wildcards")
        return parsed.to_request()

    @contextmanager
    def _mapping_errors(
        self, local: str | os.PathLike[str] | None = None
    ) -> Iterator[None]:
        """Known pyfdb and GRIB failures as ``WorkflowError`` (architecture.md §8.4)."""
        try:
            yield
        except (RuntimeError, GribError) as e:
            mapped = map_error(e, self.query, local)
            if mapped is None:
                raise
            raise mapped from e

    def _expanded(self) -> dict[str, list[str]]:
        """Expanded request (architecture.md §8.8), cached per query text.

        The first computation runs the canonical-spelling check (FR-SPELL-001); an
        invalid request raises the mapped ``WorkflowError`` and is not cached.
        """
        if self._expansion_for != self.query:
            request = self._request()
            with self._mapping_errors():
                expanded = self.provider.backend.expand(request)
            self._check_spelling(expanded)
            if expanded is None:
                self.provider.logger.debug(
                    f"FDB storage: {self.query}: no metkit expansion; query values are "
                    "used as written (spelling check skipped, identifier values from "
                    "the query archived verbatim)"
                )
                expanded = fallback_expand(request)
            self._expansion, self._expansion_for = expanded, self.query
        return self._expansion

    def _expected(self) -> int:
        """Expected field count ``E`` of the query."""
        return count_fields(self._expanded())

    def _check_spelling(self, expanded: dict[str, list[str]] | None) -> None:
        """FR-SPELL-001 on ``expanded`` (metkit's expansion of the request, if any)."""
        policy = self.provider.canonical_spelling
        if policy == "ignore":
            return
        if expanded is None:
            return  # _expanded() logs the missing expansion
        diffs = self.provider.backend.spelling_diffs(self.parsed, expanded)
        if not diffs:
            return
        (key, given, canonical), *rest = diffs
        parts = [f"{key}={given} (canonical: {canonical})"]
        parts += [f"{k}={g} ({c})" for k, g, c in rest]
        message = (
            f"Query {self.query} uses non-canonical spelling: {', '.join(parts)}. "
            "Use canonical spellings to avoid duplicate local paths for the same field."
        )
        if policy == "error":
            raise WorkflowError(message)
        if _first_time(_SPELLING_WARNED, self.query):
            self.provider.logger.warning(message)

    @_retry_fdb_io
    def _inspect(self, request: Mapping[str, str]) -> list[Field]:
        return self.provider.backend.inspect(request)

    @_retry_fdb_io
    def _retrieve_to(
        self, request: Mapping[str, str], dest: Path, expected: int
    ) -> int:
        return self.provider.backend.retrieve_to(request, dest, expected)

    def _fields(self) -> list[Field]:
        """Fields FDB holds for the query: one ``inspect``, not cached (FR-READ-010)."""
        request = self._request()
        self._expanded()  # invalid requests and spelling errors before any FDB I/O
        with self._mapping_errors():
            return self._inspect(request)

    def _complete(self, fields: list[Field]) -> bool:
        expected = self._expected()
        return expected > 0 and len(fields) == expected

    @staticmethod
    def _total_length(fields: list[Field]) -> int:
        """Bytes of the fields' messages (= retrieved bytes, FR-READ-005)."""
        return sum(f.length for f in fields)

    def _mtime_of(self, fields: list[Field]) -> float:
        """Latest field time (FR-READ-004)."""
        return max(self._field_time(f) for f in fields)

    def _field_time(self, field: Field) -> float:
        """Index timestamp; ``os.stat`` of the data file if it is 0 (FR-READ-004)."""
        return float(field.timestamp) or self._stat_mtime(field.uri_path)

    def _stat_mtime(self, uri_path: str | None) -> float:
        if uri_path:
            try:
                return os.stat(uri_path).st_mtime
            except OSError:
                pass
        if not self._no_mtime_warned:
            self._no_mtime_warned = True
            self.provider.logger.warning(
                f"FDB storage: a field of {self.query} has no index timestamp and no "
                "local data file; its mtime is taken as 0"
            )
        return 0.0

    def _missing_message(self, fields: list[Field]) -> str:
        """Retrieve error: found/expected counts, the first missing combinations
        (keys with several values only) and, if nothing matched, the optional
        schema keys the query does not name (FR-READ-008)."""
        expanded = self._expanded()
        keys = self.provider.key_order.sorted(expanded)
        values = [list(dict.fromkeys(expanded[k])) for k in keys]
        varying = [i for i, v in enumerate(values) if len(v) > 1] or range(len(keys))
        present = {tuple(f.key.get(k, "") for k in keys) for f in fields}
        combos = (c for c in itertools.product(*values) if c not in present)
        shown = [
            ",".join(f"{keys[i]}={c[i]}" for i in varying)
            for c in itertools.islice(combos, MISSING_SHOWN)
        ]
        expected = self._expected()
        missing = expected - len(fields)
        message = f"{self.query}: {len(fields)} of {expected} fields found in FDB"
        if shown:
            message += f"; missing: {'; '.join(shown)}"
            if missing > len(shown):
                message += f" (and {missing - len(shown)} more)"
        info = self.provider.schema_info
        if not fields and info is not None and (optional := info.optional - set(keys)):
            message += (
                ". FDB matches keys exactly; optional schema keys not in the query: "
                + ", ".join(sorted(optional))
            )
        return message

    # --- read path (requirements.md §2.5) ---------------------------------------------

    async def inventory(self, cache: IOCacheStorageInterface) -> None:
        """One ``inspect`` fills existence, mtime and size (FR-READ-009)."""
        key = self.cache_key()
        if key in cache.exists_in_storage:
            return
        fields = self._fields()
        exists = self._complete(fields)
        cache.exists_in_storage[key] = exists
        if exists:
            cache.mtime[key] = Mtime(storage=self._mtime_of(fields))
            cache.size[key] = self._total_length(fields)

    def get_inventory_parent(self) -> str | None:
        return None

    def cleanup(self) -> None:
        """Nothing to clean: archiving handles are per thread and flushed after every
        store; reads use a fresh handle each."""

    def exists(self) -> bool:
        """All fields the query expands to are in FDB (FR-READ-001)."""
        return self._complete(self._fields())

    def mtime(self) -> float:
        fields = self._fields()
        if not fields:
            raise FileNotFoundError(f"no fields in FDB for {self.query}")
        return self._mtime_of(fields)

    def size(self) -> int:
        return self._total_length(self._fields())

    def checksum(self) -> str | None:
        """``None``: Snakemake hashes the local copy instead (FR-READ-006)."""
        return None

    def retrieve_object(self) -> None:
        fields = self._fields()
        if not self._complete(fields):
            raise WorkflowError(self._missing_message(fields))
        local = self.local_path()
        with self._mapping_errors(local):
            self._retrieve_to(self._request(), local, self._total_length(fields))

    # --- write path (requirements.md §2.6) --------------------------------------------

    def store_object(self) -> None:
        """Archive the local GRIB file under the query (FR-STORE-*); never retried.

        Everything that can be checked from the file (GRIB structure, field count,
        identifiers, duplicates, guard) is checked before the first ``archive()``;
        a post-check ``inspect`` then requires every message to be reachable by the
        query with a timestamp from this store.
        """
        expected = self._expected()  # wildcards, invalid request, spelling check
        local = self.local_path()
        with self._mapping_errors(local):
            messages = split_messages(local)
        n = len(messages)
        counts = f"{local} has {n} fields, the query expands to {expected}"
        if n > expected or (n < expected and self.provider.store_check == "strict"):
            raise WorkflowError(f"{self.query}: {counts}; nothing was archived")
        if n < expected:
            self.provider.logger.warning(
                f"FDB storage: {self.query}: {counts} (store_check=warn)"
            )

        if self.provider.archive_mode == "identifier":
            keyed = self._identifiers(messages, local)
            batch = list(zip((msg.data for msg in messages), keyed, strict=True))
        else:  # native: FDB derives the keys, the guard is not consulted
            keyed = [msg.mars for msg in messages]
            batch = [(b"".join(msg.data for msg in messages), None)]
        first_index: dict[tuple[tuple[str, str], ...], int] = {}
        for i, key in enumerate(keyed, 1):
            first = first_index.setdefault(tuple(sorted(key.items())), i)
            if first != i:
                raise WorkflowError(
                    f"{self.query}: {local} holds duplicate fields (messages {first} "
                    f"and {i}); nothing was archived"
                )

        t_start = fdb_time()  # FDB's index clock (architecture.md §8.7)
        self._archive(batch, local)
        fresh = [f for f in self._fields() if self._field_time(f) >= t_start]
        if len(fresh) < n:
            raise WorkflowError(
                f"{self.query}: {counts}; {n - len(fresh)} landed outside the query "
                f"or are duplicates ({STAY_NOTE})"
            )

    def _identifiers(
        self, messages: list[GribMessage], local: Path
    ) -> list[dict[str, str]]:
        """FDB identifiers of ``messages`` (numbered from 1), each pre-checked against
        the query's values and passed to the guard (FR-STORE-005, FR-STORE-008)."""
        parsed = self.parsed
        single = self._canonical_single_values()
        allowed = self._allowed_values()
        info = self.provider.schema_info
        identifiers = []
        for index, message in enumerate(messages, 1):
            values = {**message.mars, "param": message.param_id}
            self._precheck(index, values, allowed, local)
            if info is None:  # no schema knowledge: query keys and message keys
                keys = self.provider.key_order.sorted({*parsed.keys(), *values})
                optional = removed = frozenset()
            else:
                keys, optional, removed = info.keys, info.optional, info.removed
            identifier: dict[str, str] = {}
            for key in keys:
                if key in removed:
                    continue
                if key in single:
                    identifier[key] = single[key]
                elif key in values:
                    identifier[key] = values[key]
                elif key not in optional:
                    raise WorkflowError(
                        f"{self.query}: cannot determine {key} for message {index} of "
                        f"{local}; nothing was archived"
                    )
            try:
                self.provider.guard.check(message, identifier, parsed)
            except IdentifierMismatch as e:
                raise WorkflowError(
                    f"{self.query}: identifier check failed for {local}: {e}; nothing "
                    "was archived"
                ) from e
            identifiers.append(identifier)
        return identifiers

    def _canonical_single_values(self) -> dict[str, str]:
        """Single-valued query keys spelled as in the expansion (FR-STORE-006): FDB's
        canonical spelling with metkit, the value as written with the fallback."""
        expanded = self._expanded()
        return {
            key: expanded[key][0] if len(expanded.get(key, ())) == 1 else value
            for key, value in self.parsed.single_valued().items()
        }

    def _allowed_values(self) -> dict[str, list[int | str]]:
        """Pre-check values: the comparable items of every constant query key, single-
        or multi-valued. Keys with ``to``/``by`` are skipped, as are keys whose items
        are not all comparable and of one kind (all integers or all strings)."""
        parsed = self.parsed
        allowed = {}
        for key in parsed.constant_pairs():
            if parsed.has_range(key):
                continue
            comparables = [comparable(key, item) for item in parsed.items(key)]
            if None not in comparables and len({type(c) for c in comparables}) == 1:
                allowed[key] = comparables
        return allowed

    def _precheck(
        self,
        index: int,
        values: Mapping[str, str],
        allowed: Mapping[str, list[int | str]],
        local: Path,
    ) -> None:
        """Message values of the keys in ``allowed`` must be one of the listed ones.

        Keys the message does not carry are not checked: the query value labels it.
        """
        for key, items in allowed.items():
            if key not in values:
                continue
            given = comparable(key, values[key])
            # given is None or of the other kind: an alias such as step=0 vs 0m
            if given is None or type(given) is not type(items[0]) or given in items:
                continue
            query_value = self.parsed.value(key)
            expected = (
                f"but the query has {key}={query_value}"
                if len(items) == 1
                else f"not one of {query_value}"
            )
            raise WorkflowError(
                f"{self.query}: message {index} of {local} has {key}={values[key]}, "
                f"{expected}; nothing was archived"
            )

    def _archive(
        self, batch: list[tuple[bytes, dict[str, str] | None]], local: Path
    ) -> None:
        """Archive and flush once; a failure after an ``archive()`` succeeded says
        that those fields stay in FDB (FR-STORE-010)."""
        backend = self.provider.backend
        archived = 0
        try:
            with self._mapping_errors(local):
                for data, identifier in batch:
                    backend.archive(data, identifier)
                    archived += 1
                backend.flush()
        except Exception as e:
            if not archived:
                raise
            with contextlib.suppress(Exception):
                backend.flush()
            raise WorkflowError(
                f"{e} ({archived} of {len(batch)} archive calls succeeded before the "
                f"failure; {STAY_NOTE})"
            ) from e

    def remove(self) -> None:
        """Never deletes: FDB has no per-field deletion (FR-REMOVE-001)."""
        policy = self.provider.remove_policy
        if policy == "ignore":
            return
        message = (
            f"FDB cannot delete individual fields; existing fields for {self.query} "
            "will be masked by the next archive. Use `fdb purge` to reclaim space."
        )
        if policy == "error":
            raise WorkflowError(f"remove_policy=error: {message}")
        if _first_time(_REMOVE_WARNED, self.query):
            self.provider.logger.warning(message)

    # --- glob (requirements.md §2.8) --------------------------------------------------

    @_retry_fdb_io
    def _list(self, selection: Mapping[str, str]) -> list[Field]:
        return self.provider.backend.list(selection)

    def list_candidate_matches(self) -> list[str]:
        """Concrete queries for ``glob_wildcards`` (FR-GLOB-001), sorted.

        One ``list`` of the pattern's constant keys (omitted keys are wildcards). Each
        field gives the pattern with its wildcard-bearing values replaced by the
        field's canonical values; fields lacking such a key, or listing it empty, are
        skipped. Keys the pattern does not name do not appear, so fields differing
        only there give one candidate.
        """
        parsed = self.parsed
        selection = parsed.constant_pairs()
        required = self.provider.glob_required_keys
        unconstrained = [key for key in required if key not in selection]
        if unconstrained:
            raise WorkflowError(
                f"FDB glob pattern {self.query} needs constant values for "
                f"{', '.join(unconstrained)} (glob_required_keys)"
            )
        wild = parsed.wildcard_keys()
        with self._mapping_errors():
            fields = self._list(selection)
        candidates = set()
        for f in fields:
            if all(f.key.get(k) for k in wild):
                pairs = tuple(
                    (k, f.key[k] if k in wild else v) for k, v in parsed.pairs
                )
                candidates.add(ParsedQuery(pairs).to_query())
        return sorted(candidates)
