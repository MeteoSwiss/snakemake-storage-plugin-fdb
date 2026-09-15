"""Snakemake storage plugin for ECMWF's Fields DataBase (FDB).

Importing this module loads no FDB/eccodes native library: ``is_valid_query`` and
``postprocess_query`` are pure Python, and ``pyfdb``/``eccodes`` are imported in
``StorageProvider.__post_init__`` only after the environment has been prepared
(spec §4.1, §5).
"""

import os
import re
import threading
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional

from snakemake_interface_common.exceptions import WorkflowError
from snakemake_interface_storage_plugins.common import Operation
from snakemake_interface_storage_plugins.io import IOCacheStorageInterface
from snakemake_interface_storage_plugins.settings import StorageProviderSettingsBase
from snakemake_interface_storage_plugins.storage_object import (
    StorageObjectGlob,
    StorageObjectRead,
    StorageObjectWrite,
)
from snakemake_interface_storage_plugins.storage_provider import (
    ExampleQuery,
    QueryType,
    StorageProviderBase,
    StorageQueryValidationResult,
)

from .backend import Backend, SchemaInfo, parse_schema, resolve_config
from .backend import resolve_schema_path as _resolve_schema_path
from .guard import IDENTIFIER_CHECKS, make_guard
from .query import NAME_MAX, KeyOrder, ParsedQuery, QueryError, normalize, parse
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
# to warn when two providers disagree (one process environment, spec §4.1).
_APPLIED: dict[str, str] = {}


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
        default="identifier",
        metadata={
            "help": "How outputs are archived: 'identifier' (the plugin builds the "
            "FDB key of every message) or 'native' (FDB derives keys from the GRIB).",
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
        """Apply spec §4.1: validate everything first, then export in one go."""
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
        """Syntactic normalisation in this provider's key order (spec §3.2).

        The result is recorded for the over-long wildcard guard (spec §3.3). Invalid
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
    """One query: a MARS request mapped to one local GRIB file (spec §6)."""

    provider: StorageProvider

    def __post_init__(self) -> None:
        self._parsed_for: str | None = None
        self._parsed: ParsedQuery | None = None
        self._parse_error: QueryError | None = None
        parsed = self._parse()
        if parsed is None or self.provider.is_normalised(self.query):
            return
        # Built by Snakemake from apply_wildcards() (no postprocess_query, spec §2.7):
        # hashing a component here would break local_suffix commutation (spec §3.3).
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

    # --- read path (plan step 5) ---------------------------------------------------

    async def inventory(self, cache: IOCacheStorageInterface) -> None:
        raise NotImplementedError

    def get_inventory_parent(self) -> str | None:
        raise NotImplementedError

    def cleanup(self) -> None:
        raise NotImplementedError

    def exists(self) -> bool:
        raise NotImplementedError

    def mtime(self) -> float:
        raise NotImplementedError

    def size(self) -> int:
        raise NotImplementedError

    def retrieve_object(self) -> None:
        raise NotImplementedError

    # --- write path (plan step 6) --------------------------------------------------

    def store_object(self) -> None:
        raise NotImplementedError

    def remove(self) -> None:
        raise NotImplementedError

    # --- glob (plan step 7) --------------------------------------------------------

    def list_candidate_matches(self) -> list[str]:
        raise NotImplementedError
