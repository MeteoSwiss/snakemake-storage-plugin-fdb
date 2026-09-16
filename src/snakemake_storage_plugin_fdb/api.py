"""Direct FDB access from ``run:`` and ``script:`` rule bodies (requirements.md §2.15).

A job that names its inputs with ``storage.fdb(query, retrieve=False)`` receives the
query string instead of a local file (FR-DIRECT-001) and reads the fields here;
``archive`` writes fields straight into FDB and leaves a marker at the output's local
path, which ``StorageObject.store_object`` recognises (FR-DIRECT-002). No GRIB file is
written to the local filesystem either way (NFR-PERF-005).

The functions take the same query text as the Snakefile. Without ``config``/
``user_config`` they use the plugin settings in the environment
(``SNAKEMAKE_STORAGE_FDB_*``, tagged values ignored) and then FDB's own environment,
which the provider fills with its configuration (FR-DIRECT-003). ``pyfdb`` and
``eccodes`` are imported on first use, never at module import (NFR-PERF-001).
"""

from __future__ import annotations

import functools
import io
import logging
import os
import tempfile
from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any, BinaryIO

from snakemake_interface_common.exceptions import WorkflowError

from .backend import distinct_values
from .grib import message_of, stream_messages
from .query import QueryError, query_of_path

if TYPE_CHECKING:  # pragma: no cover - typing only
    from . import StorageObject, StorageProvider

MARKER_HEADER = "# snakemake-storage-plugin-fdb archived"
_ENV_PREFIX = "SNAKEMAKE_STORAGE_FDB_"  # the plugin's settings in the environment
_TAG_SEPARATOR = "::"  # tagged setting values (architecture.md §13.8): not usable here
_SOURCE = "the archive() input"  # names the messages in error texts, as the file does
_logger = logging.getLogger(__name__)


# --- the archive marker (FR-DIRECT-002) ----------------------------------------------


@dataclass(frozen=True)
class Marker:
    """An archive marker: what a job archived directly at an output's local path."""

    query: str
    fields: int
    time: int  # FDB's index clock before the first archive (architecture.md §8.7)

    def write(self, path: str | os.PathLike[str]) -> Path:
        """Write the marker at ``path`` atomically (via ``<path>.part``)."""
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        part = path.with_name(path.name + ".part")
        part.write_text(
            f"{MARKER_HEADER}\nquery: {self.query}\nfields: {self.fields}\n"
            f"time: {self.time}\n"
        )
        os.replace(part, path)
        return path


def read_marker(path: str | os.PathLike[str]) -> Marker | None:
    """The ``Marker`` at ``path``, or ``None`` if the file is not a marker.

    A file that starts with ``MARKER_HEADER`` but cannot be parsed raises
    ``WorkflowError``: it was written by this plugin, so a malformed one is an error,
    not GRIB.
    """
    try:
        with io.open(path, "rb") as f:  # noqa: UP020 - `open` is this module's function
            if f.read(len(MARKER_HEADER)) != MARKER_HEADER.encode():
                return None
            lines = f.read().decode(errors="replace").splitlines()
    except OSError:
        return None
    values: dict[str, str] = {}
    for line in lines:
        key, sep, value = line.partition(":")
        if sep:
            values[key.strip()] = value.strip()
    try:
        return Marker(values["query"], int(values["fields"]), int(values["time"]))
    except (KeyError, ValueError) as e:
        raise WorkflowError(f"{path} is a malformed FDB archive marker: {e}") from e


# --- provider and storage object ------------------------------------------------------


def _settings_from_env() -> dict[str, str]:
    """Plugin settings the environment carries (``env_var`` fields, FR-CONF-008)."""
    from . import StorageProviderSettings

    values = {}
    for name, field in StorageProviderSettings.__dataclass_fields__.items():
        if not field.metadata.get("env_var"):
            continue
        value = os.environ.get(_ENV_PREFIX + name.upper())
        if not value:
            continue
        if _TAG_SEPARATOR in value:  # a tagged value names no single FDB here (L-19)
            _logger.debug(f"FDB storage: ignoring tagged {_ENV_PREFIX}{name.upper()}")
            continue
        values[name] = value
    return values


@functools.cache
def _cached_provider(items: tuple[tuple[str, str], ...]) -> StorageProvider:
    """One provider per settings combination for the process.

    Constructed outside Snakemake: its local prefix is never written to (the API uses
    the caller's paths) and input tracking is left to Snakemake's own, so no job process
    installs the rerun patch (FR-RERUN-002).
    """
    from . import StorageProvider, StorageProviderSettings

    return StorageProvider(
        local_prefix=Path(tempfile.gettempdir()),
        logger=_logger,
        settings=StorageProviderSettings(**dict(items)),
    )


def _object(
    query: str, config: str | None, user_config: str | None, **settings: str | None
) -> StorageObject:
    """A storage object for ``query`` (normalised as in a Snakefile, so error messages
    and paths read the same) on the provider of the given settings."""
    from . import StorageObject

    if not isinstance(query, str):
        raise WorkflowError(
            f"FDB query must be a string, got {type(query).__name__}: {query!r}"
        )
    values = _settings_from_env()  # what Snakemake carries into the job
    given = {"config": config, "user_config": user_config, **settings}
    values.update({k: v for k, v in given.items() if v is not None})  # arguments win
    values["input_tracking"] = "query"
    provider = _cached_provider(tuple(sorted(values.items())))
    return StorageObject(
        query=provider.postprocess_query(query),
        keep_local=False,
        retrieve=True,
        provider=provider,
    )


# --- reading (FR-DIRECT-001) ----------------------------------------------------------


def request(
    query: str, *, config: str | None = None, user_config: str | None = None
) -> dict[str, list[str]]:
    """The MARS request of ``query``, expanded and in canonical spelling.

    Lists and ``to``/``by`` ranges become value lists, so the result is what pyfdb and
    earthkit-data take as a request (``pyfdb.FDB().retrieve(request)``).
    """
    expanded = _object(query, config, user_config)._expanded()
    keys = list(expanded)
    return dict(zip(keys, distinct_values(expanded, keys), strict=True))


def open(  # noqa: A001 - the module's public name; `io.open` is used internally
    query: str, *, config: str | None = None, user_config: str | None = None
) -> BinaryIO:
    """A readable binary stream of the GRIB messages FDB holds for ``query``.

    The fields are not counted: whatever FDB returns is streamed, so a query whose
    fields are not all in FDB gives a short stream. Use ``messages`` for the
    completeness guarantee of a retrieval (FR-READ-001).
    """
    obj = _object(query, config, user_config)
    with obj._mapping_errors():
        return _stream(obj)


class _Stream(io.RawIOBase):
    """A pyfdb retrieve handle as a readable binary stream; the FDB handle is kept
    alive while the data handle is read (architecture.md §8.6)."""

    def __init__(self, fdb: Any, handle: Any) -> None:
        self._fdb = fdb
        self._handle = handle
        handle.open()  # a pyfdb data handle must be opened before it is read

    def readable(self) -> bool:
        return True

    def readinto(self, buffer: Any) -> int:
        return self._handle.readinto(buffer)

    def close(self) -> None:
        try:
            if not self.closed:
                self._handle.close()
        finally:
            super().close()


def messages(
    query: str, *, config: str | None = None, user_config: str | None = None
) -> Iterator[bytes]:
    """The GRIB messages of ``query``, one complete message at a time.

    All fields must be in FDB: a short query raises the missing-field report of a
    retrieval (FR-READ-001, FR-READ-008) before anything is read, and a stream that
    ends early raises as well. Memory is bounded by one message (NFR-PERF-003).
    """
    obj = _object(query, config, user_config)
    fields = obj._fields()
    if not obj._complete(fields):
        raise WorkflowError(obj._missing_message(fields))
    return _checked_stream(obj, obj._expected())


def _stream(obj: StorageObject) -> _Stream:
    """An open stream of the fields of ``obj``'s query (a fresh handle, ADR-014)."""
    fdb = obj.provider.backend.reader()
    return _Stream(fdb, fdb.retrieve(obj._request()))


def _checked_stream(obj: StorageObject, expected: int) -> Iterator[bytes]:
    count = 0
    with obj._mapping_errors():
        with _stream(obj) as stream:
            for message in stream_messages(stream, f"the FDB stream for {obj.query}"):
                count += 1
                yield message
    if count != expected:
        raise WorkflowError(
            f"{obj.query}: FDB returned {count} messages, the query expands to "
            f"{expected} fields"
        )


def retrieve(
    query: str,
    path: str | os.PathLike[str],
    *,
    config: str | None = None,
    user_config: str | None = None,
) -> Path:
    """Retrieve the fields of ``query`` into the local GRIB file ``path``.

    The same retrieval Snakemake performs for an input without ``retrieve=False``: all
    fields must be in FDB, and the file appears atomically (FR-READ-007). For a job
    that needs a file after all, e.g. to hand it to an external program.
    """
    obj = _object(query, config, user_config)
    obj.set_local_path(Path(path))
    obj.retrieve_object()
    return Path(path)


def earthkit(
    query: str,
    *,
    config: str | None = None,
    user_config: str | None = None,
    **kwargs: Any,
) -> Any:
    """``earthkit.data.from_source("fdb", request(query), **kwargs)``.

    earthkit-data is an optional dependency and opens its own FDB from the process
    environment, which the provider configures (FR-DIRECT-003); ``config`` and
    ``user_config`` only resolve the request here.
    """
    try:
        from earthkit.data import from_source
    except ImportError as e:  # pragma: no cover - exercised where earthkit is absent
        raise WorkflowError(
            f"earthkit-data is not installed, so earthkit() cannot be used: {e}"
        ) from e
    return from_source(
        "fdb", request(query, config=config, user_config=user_config), **kwargs
    )


# --- writing (FR-DIRECT-002) ----------------------------------------------------------


def query_of(path: str | os.PathLike[str]) -> str:
    """The FDB query of a local storage path, the inverse of the path mapping
    (``query.query_of_path``); ``WorkflowError`` for a path that is not one of this
    plugin's, including one with a hashed component (FR-PATH-003)."""
    try:
        return query_of_path(os.fspath(path))
    except QueryError as e:
        raise WorkflowError(
            f"{path} is not an FDB storage path ({e}); pass query= to archive()"
        ) from e


def archive(
    output: str | os.PathLike[str],
    messages: Iterable[bytes] | bytes,
    *,
    query: str | None = None,
    config: str | None = None,
    user_config: str | None = None,
    archive_mode: str | None = None,
) -> Marker:
    """Archive GRIB ``messages`` into FDB under the query of the output ``output``.

    ``output`` is what the job holds for an FDB output (``output[0]``: the local path);
    ``query`` overrides the query derived from it. ``messages`` are complete GRIB
    messages, as an iterable of ``bytes`` or one ``bytes`` holding them all.

    The checks of ``store_object`` run first, so nothing is archived unless every
    message belongs to the query (field count, message keys, identifiers, duplicates:
    FR-STORE-002 to FR-STORE-007, ADR-032). An iterator is therefore consumed into
    memory before the first archive; pass a list or bytes if the caller holds the
    messages anyway. After archiving and flushing, a marker file is written at
    ``output``: Snakemake's store step recognises it, skips archiving and runs the
    post-check (FR-DIRECT-002). The marker is returned.
    """
    obj = _object(
        query if query is not None else query_of(output),
        config,
        user_config,
        archive_mode=archive_mode,
    )
    obj._expected()  # wildcards, invalid request, spelling: before touching messages
    decoded = [message_of(data) for data in _as_messages(messages)]
    marker = Marker(obj.query, len(decoded), obj.archive_messages(decoded, _SOURCE))
    marker.write(output)
    return marker


def _as_messages(messages: Iterable[bytes] | bytes) -> list[bytes]:
    """The complete GRIB messages of ``messages``; one ``bytes`` holding several
    messages is split (a concatenated retrieval is a natural thing to pass)."""
    if isinstance(messages, bytes | bytearray | memoryview):
        return list(stream_messages(io.BytesIO(messages), _SOURCE))
    return list(messages)
