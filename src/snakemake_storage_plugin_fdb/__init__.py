"""Snakemake storage plugin for ECMWF's Fields DataBase (FDB).

Importing this module loads no FDB/eccodes native library: ``is_valid_query`` and
``postprocess_query`` are pure Python, and ``pyfdb``/``eccodes`` are imported in
``StorageProvider.__post_init__`` only after the environment has been prepared
(architecture.md §8.3).
"""

import contextlib
import itertools
import logging
import os
import re
import sys
import threading
from collections.abc import Iterable, Iterator, Mapping
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
    StorageObjectTouch,
    StorageObjectWrite,
    retry_decorator,
)
from snakemake_interface_storage_plugins.storage_provider import (
    ExampleQuery,
    QueryType,
    StorageProviderBase,
    StorageQueryValidationResult,
)
from tenacity import retry_if_exception

from . import api as api  # the direct-access API (FR-DIRECT-001/002)
from . import summary
from .api import Marker, read_marker
from .backend import (
    NO_CONFIG_HINT,
    Backend,
    Field,
    SchemaInfo,
    config_text,
    count_fields,
    distinct_values,
    fallback_expand,
    fdb_time,
    is_transient,
    local_roots,
    map_error,
    missing_default_schema,
    parse_schema,
    resolve_config,
)
from .backend import resolve_schema_path as _resolve_schema_path
from .frames import clean_errors
from .grib import GribError, GribMessage, split_messages
from .guard import IdentifierMismatch, make_guard
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
from .rerun import install_lookup_input_tracking

ARCHIVE_MODES = ("identifier", "native")
CANONICAL_SPELLINGS = ("warn", "error", "ignore")
REMOVE_POLICIES = ("warn", "ignore", "error")
INPUT_TRACKINGS = ("lookup", "query")
LANGUAGE_FILE = Path("share", "metkit", "language.yaml")
MEMFS_PREFIX = "/MEMFS/"  # eccodes' in-memory definitions (bundled with the wheels)

_ENV_NAME_RE = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")
_KEY_NAME_RE = re.compile(r"[A-Za-z][A-Za-z0-9_]*")
_ENV_LOCK = threading.Lock()
# Values this process's providers applied from eccodes_definitions / metkit_home,
# to warn when two providers disagree (one process environment, architecture.md §8.3).
_APPLIED: dict[str, str] = {}
# Queries already warned about (once per process): non-canonical spelling
# (FR-SPELL-001), remove_policy=warn (FR-REMOVE-001) and partially present
# queries (FR-READ-008).
_SPELLING_WARNED: set[str] = set()
_REMOVE_WARNED: set[str] = set()
_PARTIAL_WARNED: set[str] = set()
# The FDB configuration this process exported for direct access (FR-DIRECT-003), as
# {variable: value}, and whether providers disagreed on it (then nothing is exported).
# In fdb5's own precedence order (architecture.md §13.7).
FDB_CONFIG_VARS = ("FDB_CONFIG", "FDB5_CONFIG", "FDB_CONFIG_FILE", "FDB5_CONFIG_FILE")
_FDB_EXPORTED: dict[str, str] | None = None
_FDB_EXPORT_CONFLICT = False
# The four variables as this process found them, to restore on a conflict (ADR-038).
_FDB_ENV_BEFORE: dict[str, str] | None = None
_FDB_REPLACED_WARNED = False
# Whether ``--touch`` was already explained in this process (FR-IFACE-006).
_TOUCH_NOTED = False
# Whether the startup line was already logged in this process (FR-CONF-010).
_STARTUP_LOGGED = False
# Queries whose missing-key hint was already given (FR-ERR-008).
_KEY_HINT_WARNED: set[str] = set()
_ALIAS_WARNED: set[str] = set()
_WARNED_LOCK = threading.Lock()
FieldId = tuple[tuple[str, str], ...]  # one field of a query: its key=value pairs
COVERAGE_MAX = 100_000  # fields enumerated for the input-set trigger (§8.10)
MISSING_SHOWN = 10  # missing field combinations listed in a retrieve error
OFFENDERS_SHOWN = 3  # messages named in a post-check error
STAY_NOTE = "they stay in FDB until the next successful store masks them"


def _spawned_job() -> bool:
    """Whether this process is a Snakemake job process rather than the main one.

    Snakemake spawns jobs with ``--mode remote|subprocess``
    (``snakemake_interface_executor_plugins/executors/real.py``); such a process builds
    its own provider from the same settings and must not repeat what the run has
    already said, nor summarise a run it does not see the end of (FR-CONF-010).
    """
    return any(arg == "--mode" or arg.startswith("--mode=") for arg in sys.argv[1:])


def _first_time(registry: set[str], query: str) -> bool:
    """Record ``query`` in ``registry``; whether it was not there yet."""
    with _WARNED_LOCK:
        if query in registry:
            return False
        registry.add(query)
        return True


def _contradicts(key: str, value: str, items: list[int | str]) -> bool:
    """Whether a message value is comparable with the query's ``items`` (all of one
    kind, FR-STORE-005) and none of them. A value that is not comparable or of the
    other kind (``step=0`` vs ``0m``) is not a contradiction (ADR-012)."""
    given = comparable(key, value)
    return given is not None and type(given) is type(items[0]) and given not in items


def _retry_fdb_io(func):
    """The interface's ``retry_decorator`` (3 attempts, exponential wait from 3 s)
    raising the last attempt's own exception instead of tenacity's ``RetryError``,
    so it can be mapped, and retrying only failures that may be transient
    (architecture.md §8.5, ADR-033)."""
    return retry_decorator(func).retry_with(
        reraise=True, retry=retry_if_exception(is_transient)
    )


# typing.Optional, not "X | None": Snakemake unwraps only typing.Optional for the CLI.
@dataclass
class StorageProviderSettings(StorageProviderSettingsBase):
    config: Optional[str] = field(  # noqa: UP045
        default=None,
        metadata={
            "help": "FDB configuration: path to a YAML file or inline YAML/JSON text. "
            "(default: unset, FDB's own environment: FDB_CONFIG, FDB_CONFIG_FILE, "
            "FDB_HOME)",
            "env_var": True,
        },
    )
    user_config: Optional[str] = field(  # noqa: UP045
        default=None,
        metadata={
            "help": "FDB user configuration (e.g. 'useSubToc: true'): path to a YAML "
            "file or inline YAML/JSON text. (default: unset)",
            "env_var": True,
        },
    )
    archive_mode: Optional[str] = field(  # noqa: UP045
        default="native",
        metadata={
            "help": "How outputs are archived: 'native' (default; FDB derives the keys "
            "from the GRIB - use this) or 'identifier' (the plugin builds the FDB key "
            "of every message; see the reference). (default: native)",
            "env_var": True,  # reaches jobs for the direct API (FR-DIRECT-003)
        },
    )
    canonical_spelling: Optional[str] = field(  # noqa: UP045
        default="warn",
        metadata={
            "help": "Query values FDB spells differently (e.g. param=2t vs 167): "
            "'warn', 'error' or 'ignore'. (default: warn)",
            "env_var": True,
        },
    )
    remove_policy: Optional[str] = field(  # noqa: UP045
        default="warn",
        metadata={
            "help": "FDB cannot delete fields; what removing an output does: 'warn' "
            "(no-op with a warning), 'ignore' (silent no-op) or 'error'. "
            "(default: warn)",
        },
    )
    input_tracking: Optional[str] = field(  # noqa: UP045
        default="lookup",
        metadata={
            "help": "What makes a rule with FDB inputs rerun: 'lookup' (only the FDB "
            "lookup, so editing a query does not trigger a rerun by itself) or 'query' "
            "(Snakemake's default: the recorded set of input queries too). "
            "(default: lookup)",
        },
    )
    glob_required_keys: Optional[str] = field(  # noqa: UP045
        default="class",
        metadata={
            "help": "(site setup) Comma list of keys that must be constant in "
            "glob_wildcards patterns. (default: class)",
            "env_var": True,
        },
    )
    eccodes_definitions: Optional[str] = field(  # noqa: UP045
        default=None,
        metadata={
            "help": "(site setup) Colon-separated eccodes definitions directories, "
            "prepended in order to ECCODES_DEFINITION_PATH. (default: unset)",
            "env_var": True,
        },
    )
    metkit_home: Optional[str] = field(  # noqa: UP045
        default=None,
        metadata={
            "help": "(site setup) Directory exported as METKIT_HOME for a custom MARS "
            "language; must contain share/metkit/language.yaml. (default: unset)",
            "env_var": True,
        },
    )
    key_order: Optional[str] = field(  # noqa: UP045
        default=None,
        metadata={
            "help": "(site setup) Comma list of keys defining the canonical key order "
            "of queries and local paths. (default: unset, the FDB schema's rule "
            "order, else a generic MARS order)",
            "env_var": True,
        },
    )
    env: Optional[str] = field(  # noqa: UP045
        default=None,
        metadata={
            "help": "(site setup) Environment overrides NAME=VALUE[,NAME=VALUE] "
            "exported before the FDB libraries load (e.g. FDB_HOME=/path). "
            "(default: unset)",
            "env_var": True,
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
        self.canonical_spelling = _choice(
            settings, "canonical_spelling", CANONICAL_SPELLINGS
        )
        self.remove_policy = _choice(settings, "remove_policy", REMOVE_POLICIES)
        self.glob_required_keys = _key_list(
            "glob_required_keys", settings.glob_required_keys
        )
        self.input_tracking = _choice(settings, "input_tracking", INPUT_TRACKINGS)
        if self.input_tracking == "lookup":  # FR-RERUN-001, before the DAG is built
            install_lookup_input_tracking(self.logger)
        self._normalised: set[str] = set()  # queries seen by postprocess_query
        self._field_sets: dict[str, frozenset[FieldId] | None] = {}

        self.config = self._absolute(resolve_config(settings.config))
        self.user_config = self._absolute(resolve_config(settings.user_config))
        self._prepare_environment(settings)  # before anything reads the environment
        self._require_a_configuration()  # before pyfdb is touched (FR-CONF-010)
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
        # Reference time of the empty-output convention (FR-DIRECT-004, L-31): fields a
        # directly archiving job produced carry an index timestamp from after this.
        self.run_time = fdb_time()
        summary.RUN.archive_modes.add(self.archive_mode)
        self._log_startup()
        if not _spawned_job():  # one summary, from the process that ends the run
            summary.install(self.logger)

    # --- construction helpers ------------------------------------------------------

    def _require_a_configuration(self) -> None:
        """Fail on "no FDB at all" before pyfdb is imported (FR-CONF-010).

        With no ``config`` setting and nothing in FDB's own environment, ``pyfdb.FDB()``
        loads the schema bundled with its wheel, which does not exist: eckit dumps a
        40-line backtrace no environment variable silences (L-7) and the plugin's own
        sentence is the last line of it. The same sentence is raised here instead, where
        Snakemake shows it at the Snakefile line (FR-ERR-005). A site whose compiled-in
        default really exists keeps working (FR-CONF-002).
        """
        if self.config is not None:
            return
        schema = missing_default_schema()
        if schema is None:
            return
        raise WorkflowError(
            f"FDB configuration error: Cannot open {schema} (No such file or "
            f"directory){NO_CONFIG_HINT}"
        )

    def _log_startup(self) -> None:
        """One line naming the FDB this process opened (FR-CONF-010).

        Once per process, at info level in the main Snakemake process and at debug
        level in a spawned job, which inherits the same configuration (FR-DIRECT-003)
        and would otherwise repeat the line for every job.
        """
        global _STARTUP_LOGGED
        with _WARNED_LOCK:
            first, _STARTUP_LOGGED = not _STARTUP_LOGGED, True
        if not first:
            return
        if isinstance(self.config, Path):
            where = str(self.config)
        elif self.config is not None:
            where = "inline configuration"
        else:
            where = "FDB's own environment"
        roots = local_roots(self.config) or None
        parts = [
            f"roots: {', '.join(str(r) for r in roots)}"
            if roots
            else "roots: none in the configuration",
            f"schema: {self.schema_path or 'not readable here'}",
            f"input tracking: {self.input_tracking}",
        ]
        line = f"FDB storage: using {where} ({'; '.join(parts)})"
        if _spawned_job():
            self.logger.debug(line)
        else:
            self.logger.info(line)

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
            if self.config is not None:
                self._export_fdb_config()

    def _export_fdb_config(self) -> None:
        """Put this provider's FDB configuration in the environment (FR-DIRECT-003);
        called under ``_ENV_LOCK`` after the other variables are exported.

        The configuration goes out as YAML text in ``FDB5_CONFIG``, the one variable
        both pyfdb and earthkit-data's ``fdb`` source read (architecture.md §13.7,
        §13.13), and a configuration file also as ``FDB_CONFIG_FILE`` for tools that
        want the path; fdb5 reads the text first, so both name the same FDB. A job then
        opens it with an unconfigured ``pyfdb.FDB()`` or ``from_source("fdb", ...)``
        (fdb5 has no variable for the user configuration).

        An environment that names a *different* FDB is replaced: all four variables are
        unset (a left-over higher-precedence one would otherwise win over the exported
        ``FDB5_CONFIG``, architecture.md §13.7) and one warning per process says so, so
        that the jobs and the plugin cannot look at two databases (ADR-038,
        FR-DIRECT-003). Providers with different configurations (tagged providers)
        export nothing at all and put the environment back as they found it: one process
        has one environment (architecture.md §8.3).
        """
        global _FDB_EXPORTED, _FDB_EXPORT_CONFLICT, _FDB_ENV_BEFORE
        exports = {}
        if text := config_text(self.config):
            exports["FDB5_CONFIG"] = text
        if isinstance(self.config, Path):
            exports["FDB_CONFIG_FILE"] = str(self.config)
        if _FDB_EXPORT_CONFLICT:
            return
        foreign = {
            name: os.environ[name]
            for name in FDB_CONFIG_VARS
            if name in os.environ and os.environ[name] != exports.get(name)
        }
        if not foreign and all(os.environ.get(n) == v for n, v in exports.items()):
            return  # nothing to do: a spawned job, or the same configuration again
        if _FDB_EXPORTED is not None and _FDB_EXPORTED != exports:
            self._restore_fdb_config()
            _FDB_EXPORTED, _FDB_EXPORT_CONFLICT = None, True
            self.logger.debug(
                "FDB storage: providers in one process use different FDB "
                "configurations; none is exported for direct access, pass config= "
                "to the direct API"
            )
            return
        if _FDB_ENV_BEFORE is None:
            _FDB_ENV_BEFORE = {
                name: os.environ[name] for name in FDB_CONFIG_VARS if name in os.environ
            }
        if foreign:
            self._warn_replaced(foreign)
            for name in FDB_CONFIG_VARS:
                os.environ.pop(name, None)
        os.environ.update(exports)
        _FDB_EXPORTED = exports
        self.logger.debug(
            f"FDB storage: exported {', '.join(exports)} for direct FDB access"
        )

    @staticmethod
    def _restore_fdb_config() -> None:
        """The four configuration variables as this process found them (ADR-038)."""
        before = _FDB_ENV_BEFORE or {}
        for name in FDB_CONFIG_VARS:
            if name in before:
                os.environ[name] = before[name]
            else:
                os.environ.pop(name, None)

    def _warn_replaced(self, foreign: Mapping[str, str]) -> None:
        """One warning per process for a replaced foreign configuration (ADR-038)."""
        global _FDB_REPLACED_WARNED
        with _WARNED_LOCK:
            if _FDB_REPLACED_WARNED:
                return
            _FDB_REPLACED_WARNED = True
        named = ", ".join(
            name if name.endswith("CONFIG") else f"{name}={value}"
            for name, value in foreign.items()
        )
        mine = self.config if isinstance(self.config, Path) else "inline YAML"
        self.logger.warning(
            f"FDB storage: the environment named another FDB ({named}); replaced by "
            f"the workflow's configuration ({mine}) so that jobs and the plugin use "
            "the same FDB. Unset the variable to silence this."
        )

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
                "domain=g,type=fc,levtype=sfc,step=0/6/12,param=167",
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
                "domain=g,type=fc,levtype=sfc,step=0/to/48/by/6,param=167/165/166",
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

    def field_set(self, query: str) -> frozenset[FieldId] | None:
        """The fields ``query`` expands to, each as its sorted key=value pairs, or
        ``None`` if the query cannot be expanded or names more than ``COVERAGE_MAX``
        fields (architecture.md §8.10). Cached per query text for the process: the
        recorded queries of a job repeat across its outputs and across jobs. Snakemake
        asks from its event loop; a concurrent miss would only expand twice.
        """
        if query not in self._field_sets:
            self._field_sets[query] = self._expand_fields(query)
        return self._field_sets[query]

    def _expand_fields(self, query: str) -> frozenset[FieldId] | None:
        # A recorded query may be one the Snakefile no longer has, so it is expanded
        # without a storage object: no spelling warning, no normalisation record.
        try:
            request = parse(query, self.key_order).to_request()
            expanded = self.backend.expand(request) or fallback_expand(request)
        except Exception as e:
            self.logger.debug(
                f"FDB storage: {query} is not expanded for the input-set trigger "
                f"({e}); it covers no fields"
            )
            return None
        if count_fields(expanded) > COVERAGE_MAX:
            self.logger.debug(
                f"FDB storage: {query} names more than {COVERAGE_MAX} fields; it "
                "covers no fields"
            )
            return None
        keys = sorted(expanded)
        return frozenset(
            tuple(zip(keys, combination, strict=True))
            for combination in itertools.product(*distinct_values(expanded, keys))
        )

    def is_normalised(self, query: str) -> bool:
        """Whether ``query`` was returned by ``postprocess_query`` of this provider."""
        return query in self._normalised

    def safe_print(self, query: str) -> str:
        return query

    def __str__(self) -> str:
        """The plugin name: Snakemake formats the provider into user-facing text
        such as the catalogue URL of an invalid query (``snakemake/storage.py``)."""
        return "fdb"


class StorageObject(
    StorageObjectRead, StorageObjectWrite, StorageObjectGlob, StorageObjectTouch
):
    """One query: a MARS request mapped to one local GRIB file (FR-QUERY-002)."""

    provider: StorageProvider

    @property
    def tracks_input_changes(self) -> bool:
        """Whether the text of this query takes part in Snakemake's input-set rerun
        trigger (FR-RERUN-002). False means the trigger asks ``covered_by`` instead.
        Name and meaning of the upstream hook proposed in requirements.md D-011; until
        it exists, ``rerun.py`` reads it (ADR-034)."""
        return self.provider.input_tracking == "query"

    def covered_by(self, recorded: Iterable[str]) -> bool:
        """Whether every field of this query is a field of one of the ``recorded``
        queries (FR-RERUN-001, architecture.md §8.10).

        A recorded query with the same text covers this one without any expansion.
        Otherwise fields are compared as the key=value pairs of the expanded requests,
        so the order of keys and values, list or range notation and non-canonical
        spellings do not matter, while a key only one of the queries names makes their
        fields differ. A query that cannot be expanded covers nothing, and an
        unexpandable own query is covered by its own text only (the trigger then
        behaves as Snakemake's own).
        """
        recorded = set(recorded)
        if self.query in recorded:
            return True
        mine = self.provider.field_set(self.query) if recorded else None
        if mine is None:
            return False
        sets = [s for query in recorded if (s := self.provider.field_set(query))]
        return any(mine <= s for s in sets) or mine <= frozenset().union(*sets)

    def __post_init__(self) -> None:
        self._parsed_for: str | None = None
        self._parsed: ParsedQuery | None = None
        self._parse_error: QueryError | None = None
        self._expansion_for: str | None = None
        self._expansion: dict[str, list[str]] = {}
        self._no_mtime_warned = False
        parsed = self._parse()
        self._check_spelling_early()
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

    def __repr__(self) -> str:
        """The query: Snakemake formats storage objects into user-facing text, e.g.
        the flags of a pattern given to ``expand()`` (FR-IFACE-002)."""
        return f"<{self.query}>"

    @contextmanager
    def _mapping_errors(
        self, local: str | os.PathLike[str] | None = None
    ) -> Iterator[None]:
        """Known pyfdb and GRIB failures as ``WorkflowError`` (architecture.md §8.4);
        the message keeps a short detail, the full text goes to the debug log."""
        try:
            yield
        except (RuntimeError, GribError) as e:
            info = self.provider.schema_info
            parsed = self._parse()
            mapped = map_error(
                e,
                self.query,
                local,
                request=parsed.to_request() if parsed is not None else None,
                schema_keys=info.keys if info is not None else None,
            )
            if mapped is None:
                raise
            self.provider.logger.debug(f"FDB storage: full error text: {e}")
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

    def _check_spelling_early(self) -> None:
        """FR-SPELL-001 at construction for a query without wildcards (FR-ERR-006).

        Snakemake then reports a spelling error against the Snakefile line that wrote
        the query instead of raising it from ``exists()`` during DAG building, where it
        arrives wrapped in a task group. Only metkit's expansion of the request is
        needed, which is language, not data, so no FDB is opened (FR-CONF-005); a query
        with wildcards is checked on first use, when its values are known, and anything
        the expansion itself refuses stays on the lazy path (FR-ERR-003).
        """
        if self.provider.canonical_spelling == "ignore":
            return
        parsed = self._parse()
        if parsed is None or parsed.has_wildcards():
            return
        try:
            expanded = self.provider.backend.expand(parsed.to_request())
        except Exception as e:
            self.provider.logger.debug(
                f"FDB storage: {self.query} is not expanded at construction ({e}); "
                "the spelling check runs on first use"
            )
            return
        self._check_spelling(expanded)
        if expanded is not None:  # what _expanded() would cache
            self._expansion, self._expansion_for = expanded, self.query

    def _check_spelling(self, expanded: dict[str, list[str]] | None) -> None:
        """FR-SPELL-001 on ``expanded`` (metkit's expansion of the request, if any)."""
        policy = self.provider.canonical_spelling
        if policy == "ignore":
            return
        if expanded is None:
            return  # _expanded() logs the missing expansion
        self._check_key_aliases(expanded)
        diffs = self.provider.backend.spelling_diffs(self.parsed, expanded)
        if not diffs:
            return
        (key, given, canonical), *rest = diffs
        parts = [f"{key}={given} (canonical: {canonical})"]
        parts += [f"{k}={g} ({c})" for k, g, c in rest]
        message = (
            f"FDB storage: query {self.query} uses non-canonical spelling: "
            f"{', '.join(parts)}. Use canonical spellings to avoid duplicate local "
            "paths for the same field."
        )
        if policy == "error":
            raise WorkflowError(message)
        if _first_time(_SPELLING_WARNED, self.query):
            self.provider.logger.warning(message)

    def _check_key_aliases(self, expanded: dict[str, list[str]] | None) -> None:
        """Warn once per query about MARS key aliases (FR-ERR-008, L-27).

        metkit accepts ``levtyp`` for ``levtype`` and maps it silently, so the query
        matches fields although it names a key the user did not mean to name; the plugin
        keeps the text, which gives the same fields a second local path. Keys of the
        expansion the query does not have, against keys of the query the expansion
        dropped, name both spellings.
        """
        if expanded is None:
            return
        parsed = self._parse()
        if parsed is None:
            return
        mine = set(parsed.keys())
        dropped = sorted(mine - set(expanded))
        added = sorted(set(expanded) - mine)
        if not dropped or not added:
            return
        pairs = ", ".join(
            f"{key} (canonical: {added[i] if i < len(added) else '/'.join(added)})"
            for i, key in enumerate(dropped)
        )
        if _first_time(_ALIAS_WARNED, self.query):
            self.provider.logger.warning(
                f"FDB storage: query {self.query} uses MARS key alias(es): {pairs}. "
                "FDB matched the canonical key; write it in the query, or the same "
                "fields get a second local path."
            )

    @_retry_fdb_io
    def _inspect(self, request: Mapping[str, str]) -> list[Field]:
        return self.provider.backend.inspect(request)

    @_retry_fdb_io
    def _retrieve_to(
        self, request: Mapping[str, str], dest: Path, expected: int
    ) -> int:
        return self.provider.backend.retrieve_to(request, dest, expected)

    def _fields(self) -> list[Field]:
        """Fields FDB holds for the query: one ``inspect``, not cached (FR-READ-010).

        Fields lacking a query key FDB indexes are dropped: ``inspect`` matches
        through keys the indexed fields do not have (L-22), while the values of the
        keys a field carries are matched by FDB itself (architecture.md §13.4).
        """
        request = self._request()
        self._expanded()  # invalid requests and spelling errors before any FDB I/O
        with self._mapping_errors():
            fields = self._inspect(request)
        required = self._indexed_keys()
        matching = [f for f in fields if all(f.key.get(k) for k in required)]
        if len(matching) < self._expected():
            self.provider.backend.check_roots(self.query)  # FR-ERR-004
        if len(matching) < len(fields):
            self.provider.logger.debug(
                f"FDB storage: {self.query}: {len(fields) - len(matching)} of "
                f"{len(fields)} inspected fields lack a query key and are not counted "
                "(FR-READ-001)"
            )
        return matching

    def _indexed_keys(self) -> list[str]:
        """Query keys FDB keeps in the field keys: the schema's rule keys not marked
        ``key-``. Unknown without a schema, so nothing is required then (L-15)."""
        info = self.provider.schema_info
        if info is None:
            return []
        return [
            k for k in self.parsed.keys() if k in info.keys and k not in info.removed
        ]

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

    def _missing_combinations(self, fields: list[Field], limit: int) -> list[str]:
        """At most ``limit`` field combinations of the query that no field of
        ``fields`` has, each naming the keys with several values (all keys for a
        single-field query)."""
        expanded = self._expanded()
        keys = self.provider.key_order.sorted(expanded)
        values = distinct_values(expanded, keys)
        varying = [i for i, v in enumerate(values) if len(v) > 1] or range(len(keys))
        present = {tuple(f.key.get(k, "") for k in keys) for f in fields}
        combos = (c for c in itertools.product(*values) if c not in present)
        return [
            ",".join(f"{keys[i]}={c[i]}" for i in varying)
            for c in itertools.islice(combos, limit)
        ]

    def _missing_list(self, fields: list[Field], label: str) -> str:
        """``; <label>: <combination>; ...[ (and <k> more)]`` for the field
        combinations of the query that no field of ``fields`` has (keys with several
        values only); empty when the expansion names no such combination."""
        shown = self._missing_combinations(fields, MISSING_SHOWN)
        if not shown:
            return ""
        text = f"; {label}: {'; '.join(shown)}"
        if (missing := self._expected() - len(fields)) > len(shown):
            text += f" (and {missing - len(shown)} more)"
        return text

    def _missing_message(self, fields: list[Field]) -> str:
        """Retrieve error: found/expected counts, the first missing combinations
        (keys with several values only) and, if nothing matched, the optional
        schema keys the query does not name (FR-READ-008)."""
        message = (
            f"{self.query}: {len(fields)} of {self._expected()} fields found in FDB"
        )
        message += self._missing_list(fields, "missing")
        info = self.provider.schema_info
        if not fields and info is not None:
            keys = self.provider.key_order.sorted(self._expanded())
            if optional := info.optional - set(keys):
                message += (
                    ". FDB matches keys exactly; optional schema keys not in the "
                    "query: " + ", ".join(sorted(optional))
                )
        return message

    def _absent_keys(self) -> list[str]:
        """Keys of the schema's first rule level the query does not name (FR-ERR-008).

        Such a query cannot match a single field: FDB matches keys exactly and the
        first level is the database key. Empty without a readable schema (L-15).
        """
        info = self.provider.schema_info
        if info is None:
            return []
        return sorted(info.first_level - set(self._expanded()) - set(info.defaults))

    def _report_absent(self, fields: list[Field]) -> None:
        """Nothing found: at info level once per query when the query omits a key of
        the schema's first level, at debug level otherwise (FR-ERR-008).

        The gate keeps a healthy run silent: its outputs are looked up before they
        exist, and they name every key of the first level.
        """
        logger = self.provider.logger
        absent = self._absent_keys()
        if absent and _first_time(_KEY_HINT_WARNED, self.query):
            logger.info(
                f"FDB storage: {self.query}: no fields in FDB; the query does not name "
                f"{', '.join(absent)}, which the FDB schema's first rule level "
                "requires. FDB matches keys exactly, so nothing can match."
            )
        elif logger.isEnabledFor(logging.DEBUG):
            logger.debug(f"FDB storage: {self._missing_message(fields)}")

    def _exists(self, fields: list[Field]) -> bool:
        """``_complete``, reporting an incomplete result (FR-READ-008).

        Snakemake reports an incomplete input as missing without ever calling
        ``retrieve_object``, so the error there would never be seen. A partial answer
        is warned about once per query; nothing found is the normal case of an output
        that does not exist yet and stays quiet unless the query omits a key the schema
        requires (FR-ERR-008). Either way the answer is remembered for the end-of-run
        summary (FR-IFACE-007).
        """
        self._record_lookup(fields)
        if self._complete(fields):
            summary.RUN.record_complete(self.query)
            return True
        if fields:
            if _first_time(_PARTIAL_WARNED, self.query):
                self.provider.logger.warning(
                    f"FDB storage: {self._missing_message(fields)}"
                )
        else:
            self._report_absent(fields)
        self._record_incomplete(fields)
        return False

    def _record_lookup(self, fields: list[Field]) -> None:
        """Feed a lookup to the end-of-run summary (FR-IFACE-007).

        Fields older than this run's reference time are the ones a store of this run
        would mask; fields from this run under an output no store step sees (an output
        declared ``retrieve=False``, FR-DIRECT-005) are what its job archived itself.
        """
        since = self.provider.run_time
        fresh = sum(1 for f in fields if self._field_time(f) >= since)
        summary.RUN.record_lookup(self.query, len(fields) - fresh)
        if fresh:
            summary.RUN.record_archive(self.query, fresh)

    def _record_incomplete(self, fields: list[Field]) -> None:
        """Remember an incomplete lookup for the end-of-run summary (FR-IFACE-007).

        ``fresh`` counts the fields whose index timestamp is from this run: together
        with a plain declaration and a missing local file, that is the signature of a
        job that archived its output itself under an output Snakemake wants as a file
        (FR-ERR-009).
        """
        try:
            local = self.local_path()
        except Exception:  # pragma: no cover - a query without a path is not stored
            return
        since = self.provider.run_time
        summary.RUN.record_incomplete(
            self.query,
            summary.Incomplete(
                report=self._missing_message(fields),
                found=len(fields),
                expected=self._expected(),
                plain=bool(getattr(self, "retrieve", True)),
                local_file=local.exists(),
                fresh=sum(1 for f in fields if self._field_time(f) >= since),
            ),
        )

    # --- read path (requirements.md §2.5) ---------------------------------------------

    @clean_errors
    async def inventory(self, cache: IOCacheStorageInterface) -> None:
        """One ``inspect`` fills existence, mtime and size (FR-READ-009)."""
        key = self.cache_key()
        if key in cache.exists_in_storage:
            return
        fields = self._fields()
        exists = self._exists(fields)
        cache.exists_in_storage[key] = exists
        if exists:
            cache.mtime[key] = Mtime(storage=self._mtime_of(fields))
            cache.size[key] = self._total_length(fields)

    def get_inventory_parent(self) -> str | None:
        return None

    def cleanup(self) -> None:
        """Nothing to clean: archiving handles are per thread and flushed after every
        store; reads use a fresh handle each."""

    @clean_errors
    def exists(self) -> bool:
        """All fields the query expands to are in FDB (FR-READ-001)."""
        return self._exists(self._fields())

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

    @clean_errors
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
        message keys against the query, identifiers, duplicates, guard) is checked
        before the first ``archive()`` (ADR-032); a post-check ``inspect`` then
        requires every message to be reachable by the query with a timestamp from this
        store.

        An empty local file means the job archived the fields itself with plain pyfdb
        (FR-DIRECT-004), an archive marker that it used ``api.archive`` (FR-DIRECT-002):
        nothing is archived here, only the post-check runs.
        """
        local = self.local_path()
        if local.is_file() and local.stat().st_size == 0:
            self._post_check_empty(local)
            return
        marker = read_marker(local)
        if marker is not None:
            self._post_check_marker(marker, local)
            return
        self._expected()  # wildcards, invalid request, spelling check before the file
        with self._mapping_errors(local):
            messages = split_messages(local)
        self.archive_messages(messages, local)

    def archive_messages(
        self, messages: list[GribMessage], source: str | os.PathLike[str]
    ) -> int:
        """Check, archive, flush and post-check ``messages`` under the query: steps 2
        to 6 of architecture.md §6.4, shared with ``api.archive`` (FR-DIRECT-002).

        The field count, every message's keys against the query, the identifiers and
        duplicates are checked before the first ``archive()`` (ADR-032). ``source``
        names the messages in error texts (the local file, or the direct API's input).
        Returns the FDB clock second read before the first ``archive()`` (§8.7).
        """
        n = len(messages)
        counts = f"{source} has {n} fields, the query expands to {self._expected()}"
        if n != self._expected():
            raise WorkflowError(f"{self.query}: {counts}; nothing was archived")
        values = self._checked_values(messages, source)
        if self.provider.archive_mode == "identifier":
            keyed = self._identifiers(messages, values, source)
            batch = list(zip((msg.data for msg in messages), keyed, strict=True))
        else:  # native: FDB derives the keys, the guard is not consulted
            self._require_indexed_keys(values, source)
            keyed = values
            batch = [(b"".join(msg.data for msg in messages), None)]
        first_index: dict[tuple[tuple[str, str], ...], int] = {}
        for i, key in enumerate(keyed, 1):
            first = first_index.setdefault(tuple(sorted(key.items())), i)
            if first != i:
                raise WorkflowError(
                    f"{self.query}: {source} holds duplicate fields (messages {first} "
                    f"and {i}); nothing was archived"
                )

        t_start = fdb_time()  # FDB's index clock (architecture.md §8.7)
        self._archive(batch, source)
        self._post_check(n, t_start, counts, keyed)
        return t_start

    def _post_check(
        self,
        n: int,
        t_start: int,
        counts: str,
        keyed: list[dict[str, str]] | None = None,
        direct: bool = False,
    ) -> None:
        """FR-STORE-009: every archived message must be reachable by the query with a
        timestamp from this store. ``keyed`` (absent for a marker) names the offenders.
        """
        fresh, stale = self._fresh(t_start)
        if len(fresh) < n:
            raise WorkflowError(
                f"{self.query}: {counts}; {n - len(fresh)} landed outside the query "
                f"or are duplicates{self._offenders(keyed or [], fresh)} ({STAY_NOTE})"
            )
        # What the run archived (FR-IFACE-007); what it masked comes from the lookups
        # before the store, which are the only ones that still see the old fields.
        summary.RUN.record_archive(self.query, n, direct)

    def _fresh(self, since: float) -> tuple[list[Field], list[Field]]:
        """The query's fields in FDB with an index timestamp from ``since`` on, and
        the older ones: one ``inspect`` (architecture.md §8.7)."""
        fresh: list[Field] = []
        stale: list[Field] = []
        for f in self._fields():
            (fresh if self._field_time(f) >= since else stale).append(f)
        return fresh, stale

    def _post_check_empty(self, local: Path) -> None:
        """Post-check of fields a job archived with plain pyfdb (FR-DIRECT-004).

        An empty local file is the job's statement that it archived the query's fields
        itself. Nothing was checked before those archives, so everything is checked
        here: every field of the query must be in FDB with an index timestamp not older
        than the reference time of this run (the FDB clock when the provider of this
        process was constructed, L-31).
        """
        since = self.provider.run_time
        self.provider.logger.debug(
            f"FDB storage: {self.query}: {local} is empty; the fields must have index "
            f"timestamps >= {since} (this run's reference, pid {os.getpid()})"
        )
        fresh, stale = self._fresh(since)
        if len(fresh) >= self._expected():
            summary.RUN.record_archive(self.query, len(fresh), direct=True)
            return
        message = (
            f"{self.query}: {local} is empty, so the job is taken to have archived "
            f"the fields itself; {len(fresh)} of {self._expected()} found in FDB with "
            f"timestamps from this run{self._missing_list(fresh, 'missing or older')}"
        )
        if stale:
            message += f"; {len(stale)} of the query's fields are from before this run"
        raise WorkflowError(message)

    def _post_check_marker(self, marker: Marker, local: Path) -> None:
        """Post-check of fields a job archived itself (FR-DIRECT-002): the marker must
        name this query (normalised, FR-QUERY-007) and the expansion's field count."""
        normalise = self.provider.postprocess_query
        if normalise(marker.query) != normalise(self.query):
            raise WorkflowError(
                f"{self.query}: {local} is an archive marker for {marker.query}; the "
                f"job archived fields of another query ({STAY_NOTE})"
            )
        counts = (
            f"the job archived {marker.fields} fields directly, the query expands to "
            f"{self._expected()}"
        )
        if marker.fields != self._expected():
            raise WorkflowError(f"{self.query}: {counts}")
        self._post_check(marker.fields, marker.time, counts, direct=True)

    def _checked_values(
        self, messages: list[GribMessage], source: str | os.PathLike[str]
    ) -> list[dict[str, str]]:
        """MARS keys (``param`` from ``paramId``) of ``messages``, each pre-checked
        against the query (FR-STORE-005, both archive modes)."""
        allowed = self._allowed_values()
        values = [{**msg.mars, "param": msg.param_id} for msg in messages]
        for index, message in enumerate(values, 1):
            self._precheck(index, message, allowed, source)
        return values

    def _require_indexed_keys(
        self, values: list[dict[str, str]], source: str | os.PathLike[str]
    ) -> None:
        """Native archiving takes every key from the message, so a query key FDB
        indexes that a message lacks cannot be honoured (FR-STORE-003, L-23)."""
        required = self._indexed_keys()
        for index, message in enumerate(values, 1):
            if absent := next((k for k in required if not message.get(k)), None):
                raise WorkflowError(
                    f"{self.query}: message {index} of {source} lacks {absent}, which "
                    "native archiving takes from the message; use "
                    "archive_mode=identifier to label it, or drop the key from the "
                    "query; nothing was archived"
                )

    def _offenders(self, keyed: list[dict[str, str]], fresh: list[Field]) -> str:
        """``: message <i> (<keys>), ...`` for the messages whose key, on the indexed
        query keys, no fresh field has (FR-STORE-009); empty if every message has one.
        Shown are the keys that contradict the query and those the query gives several
        values (they tell messages apart), at most ``OFFENDERS_SHOWN`` messages."""
        keys = self._indexed_keys() or self.parsed.keys()

        def signature(key: Mapping[str, str]) -> tuple[int | str | None, ...]:
            return tuple(comparable(k, key.get(k, "")) for k in keys)

        found = {signature(f.key) for f in fresh}
        missing = [i for i, key in enumerate(keyed, 1) if signature(key) not in found]
        if not missing:
            return ""
        allowed = self._allowed_values()
        multi = {k for k, v in self._expanded().items() if len(set(v)) > 1}
        names = []
        for index in missing[:OFFENDERS_SHOWN]:
            key = keyed[index - 1]
            shown = [
                k
                for k in self.provider.key_order.sorted(key)
                if k in multi or (k in allowed and _contradicts(k, key[k], allowed[k]))
            ]
            label = ", ".join(f"{k}={key[k]}" for k in shown)
            names.append(f"message {index} ({label})" if label else f"message {index}")
        more = len(missing) - len(names)
        return ": " + ", ".join(names) + (f" and {more} more" if more else "")

    def _identifiers(
        self,
        messages: list[GribMessage],
        mars: list[dict[str, str]],
        source: str | os.PathLike[str],
    ) -> list[dict[str, str]]:
        """FDB identifiers of ``messages`` (numbered from 1) from their pre-checked
        MARS keys ``mars``, each passed to the guard (FR-STORE-004, FR-STORE-008)."""
        parsed = self.parsed
        single = self._canonical_single_values()
        info = self.provider.schema_info
        identifiers = []
        for index, (message, values) in enumerate(zip(messages, mars, strict=True), 1):
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
                        f"{source}; nothing was archived"
                    )
            try:
                self.provider.guard.check(message, identifier, parsed)
            except IdentifierMismatch as e:
                raise WorkflowError(
                    f"{self.query}: identifier check failed for {source}: {e}; nothing "
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
        source: str | os.PathLike[str],
    ) -> None:
        """Message values of the keys in ``allowed`` must be one of the listed ones
        (FR-STORE-005), in both archive modes.

        Keys the message does not carry are not checked here: in identifier mode the
        query value labels the message (FR-STORE-004), in native mode
        ``_require_indexed_keys`` rejects them.
        """
        for key, items in allowed.items():
            if key not in values or not _contradicts(key, values[key], items):
                continue
            query_value = self.parsed.value(key)
            expected = (
                f"but the query has {key}={query_value}"
                if len(items) == 1
                else f"not one of {query_value}"
            )
            example = (
                f" (e.g. grib_set -s {key}={query_value})" if len(items) == 1 else ""
            )
            raise WorkflowError(
                f"{self.query}: message {index} of {source} has {key}={values[key]}, "
                f"{expected}; nothing was archived - set the key in the GRIB before "
                f"archiving{example}, or declare the output under the keys the data "
                "carries"
            )

    def _archive(
        self,
        batch: list[tuple[bytes, dict[str, str] | None]],
        source: str | os.PathLike[str],
    ) -> None:
        """Archive and flush once; a failure after an ``archive()`` succeeded says
        that those fields stay in FDB (FR-STORE-010)."""
        backend = self.provider.backend
        archived = 0
        try:
            with self._mapping_errors(source):
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
        """Never deletes: FDB has no per-field deletion (FR-REMOVE-001).

        Snakemake asks for this for the outputs of a *failed* job and for
        ``--delete-all-output``; the plugin cannot tell the two apart, so the message
        says what is true of both and branches on what FDB holds (FR-REMOVE-002).
        """
        policy = self.provider.remove_policy
        if policy == "ignore":
            return
        if policy == "warn" and not _first_time(_REMOVE_WARNED, self.query):
            return  # once per query and process
        message, complete = self._remove_message()
        if policy == "error":
            raise WorkflowError(f"remove_policy=error: {message}")
        if complete is False and message.startswith("FDB storage: nothing to remove"):
            self.provider.logger.info(message)
        else:
            self.provider.logger.warning(message)

    def _remove_message(self) -> tuple[str, bool | None]:
        """What removing this output does, given what FDB holds (FR-REMOVE-002);
        ``(message, complete)``, ``complete`` ``None`` if FDB could not be asked."""
        head = f"FDB storage: {self.query}: "
        keep = (
            "Nothing was removed: FDB cannot delete individual fields. Later archives "
            "of the same fields mask these; `fdb purge` reclaims the space."
        )
        try:
            fields = self._fields()
            expected = self._expected()
        except Exception as e:  # a removal must not fail on a lookup
            self.provider.logger.debug(f"FDB storage: remove() cannot look up: {e}")
            return head + keep, None
        if not fields:
            return (
                f"FDB storage: nothing to remove: no field of {self.query} is in FDB.",
                False,
            )
        if len(fields) >= expected:
            return (
                head + f"all {expected} fields are in FDB. " + keep + " This output "
                "therefore still looks complete, and the rule that writes it will not "
                "be scheduled again: after a failed job, rerun it with `-R <rule>` "
                "(every job of the rule) or archive the retry under a fresh expver.",
                True,
            )
        return (
            head
            + f"{len(fields)} of {expected} fields are in FDB"
            + self._missing_list(fields, "missing")
            + ". "
            + keep,
            False,
        )

    def touch(self) -> None:
        """``--touch``: nothing to do in FDB (FR-IFACE-006, L-25).

        FDB index timestamps are set when a field is archived and cannot be changed,
        and re-archiving to refresh them would write data. The fields are therefore
        left as they are; Snakemake touches the local outputs of the same workflow,
        which is what ``--touch`` is reached for. Logged once per process.
        """
        global _TOUCH_NOTED
        with _WARNED_LOCK:
            note, _TOUCH_NOTED = not _TOUCH_NOTED, True
        if note:
            self.provider.logger.info(
                "FDB storage: --touch leaves FDB fields as they are; index timestamps "
                "cannot be changed"
            )

    # --- glob (requirements.md §2.8) --------------------------------------------------

    @_retry_fdb_io
    def _list(self, selection: Mapping[str, str]) -> list[Field]:
        return self.provider.backend.list(selection)

    @clean_errors
    def list_candidate_matches(self) -> list[str]:
        """Concrete queries for ``glob_wildcards`` (FR-GLOB-001), sorted.

        One ``list`` of the pattern's constant keys (omitted keys are wildcards; unlike
        ``inspect``, ``list`` does not match through keys a field lacks, architecture.md
        §13.4). Each field gives the pattern with its wildcard-bearing values replaced
        by the field's canonical values; fields lacking such a key, or listing it
        empty, are skipped. Keys the pattern does not name do not appear, so fields
        differing only there give one candidate.
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
