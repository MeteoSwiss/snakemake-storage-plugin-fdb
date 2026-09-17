"""What the run did in FDB, said once at the end of it (FR-IFACE-007).

The plugin knows three things no other component does and that a run otherwise ends
without: how many fields it archived and how many of them masked fields that were
already there (FDB reclaims those only with ``fdb purge``), which declared queries FDB
holds only in part although no job produced them, and that a file-based output has
fields in FDB but never got a local file (the job archived it itself and the output
declaration does not say so).

The state is per process and the block is logged once, from
``snakemake.logging.LoggerManager.stop``: the last point at which a line still reaches
the console and the log file (an ``atexit`` handler runs after Snakemake has removed its
log handlers, architecture.md ADR-041). The hook is patched like the rerun one
(ADR-034); where it cannot be patched the plugin falls back to ``atexit`` and says so at
debug level.
"""

from __future__ import annotations

import atexit
import functools
import inspect
import threading
from dataclasses import dataclass, field
from typing import Any

MASK_NOTE = "masked fields are reclaimed only by `fdb purge`"
PREFIX = "FDB storage:"
_MARKER = "__fdb_run_summary__"
_LOCK = threading.Lock()


@dataclass(frozen=True)
class Incomplete:
    """A query FDB holds only in part, as the last lookup of the run saw it."""

    report: str  # the missing-field report (FR-READ-008)
    found: int
    expected: int
    plain: bool  # declared without retrieve=False, so Snakemake wants a local file
    local_file: bool  # a local file exists at the object's path
    fresh: int  # fields with an index timestamp from this run


@dataclass
class RunState:
    """What this process did in FDB during the run.

    Counts are kept per query and as the largest value seen, never as a sum: the same
    query is looked up several times (DAG build, the job's own check, a consumer) and
    each lookup sees the same fields. Fields are told apart by their index timestamp
    against the run's reference time (architecture.md §8.7, L-31): fields from this run
    are what it archived, the older ones are what its archives masked (FDB's own
    listings hide a masked field, so the masked count has to come from the lookup that
    happened before the store).
    """

    archived: dict[str, int] = field(default_factory=dict)
    before: dict[str, int] = field(default_factory=dict)
    incomplete: dict[str, Incomplete] = field(default_factory=dict)
    archive_modes: set[str] = field(default_factory=set)
    direct: set[str] = field(default_factory=set)  # archived by the job itself
    plugin: set[str] = field(default_factory=set)  # archived by the store step

    def record_archive(
        self, query: str, fields: int, direct: bool | None = None
    ) -> None:
        with _LOCK:
            self.archived[query] = max(self.archived.get(query, 0), fields)
            if direct is None:
                # Counted from a lookup: fields of this run under a query the plugin's
                # own store step did not touch were archived by the job itself
                # (FR-DIRECT-005, which leaves no local file and no store to record).
                if query not in self.plugin:
                    self.direct.add(query)
                return
            self.incomplete.pop(query, None)
            (self.direct if direct else self.plugin).add(query)

    def record_lookup(self, query: str, before: int) -> None:
        """``before`` fields of ``query`` are older than this run's reference time."""
        with _LOCK:
            self.before[query] = max(self.before.get(query, 0), before)

    def record_incomplete(self, query: str, incomplete: Incomplete) -> None:
        with _LOCK:
            self.incomplete[query] = incomplete

    def record_complete(self, query: str) -> None:
        with _LOCK:
            self.incomplete.pop(query, None)

    # --- the block ----------------------------------------------------------------

    def lines(self) -> list[str]:
        """The summary block, empty when there is nothing to say."""
        out: list[str] = []
        fields = sum(self.archived.values())
        masked = sum(
            min(n, self.before.get(query, 0)) for query, n in self.archived.items()
        )
        if fields:
            queries = f"{len(self.archived)} quer" + (
                "y" if len(self.archived) == 1 else "ies"
            )
            out.append(
                f"{fields} field{'' if fields == 1 else 's'} archived ({queries}), "
                f"{masked} of which masked fields already in FDB ({MASK_NOTE})."
            )
        unwritten = [
            (query, item) for query, item in self.incomplete.items() if _unwritten(item)
        ]
        for query, item in unwritten:
            out.append(
                f"{query} was declared as a file-based output, no local file was "
                f"written, and {item.found} of {item.expected} of its fields are in "
                "FDB. If the job archives into FDB itself, declare the output "
                "retrieve=False (or touch(), or use api.archive)."
            )
        rest = [item for query, item in self.incomplete.items() if not _unwritten(item)]
        if rest:
            out.append(
                f"{len(rest)} quer{'y was' if len(rest) == 1 else 'ies were'} "
                "incomplete in FDB and no job produced them:"
            )
            out += [f"  {item.report}" for item in rest]
            out.append("Run -R <rule> or --forceall to produce them.")
        if self.direct and not self.plugin:
            other = sorted(self.archive_modes - {"native"})
            if other:
                out.append(
                    f"archive_mode={other[0]} had no effect: every FDB output of this "
                    "run was archived by its job, which the plugin's store step does "
                    "not touch."
                )
        return out

    def emit(self, logger: Any) -> bool:
        """Log the block once; whether anything was logged."""
        with _LOCK:
            if getattr(self, "_emitted", False):
                return False
            self._emitted = True
        lines = self.lines()
        if not lines:
            return False
        logger.info("\n".join([f"{PREFIX} run summary:", *(f"  {x}" for x in lines)]))
        return True


def _unwritten(item: Incomplete) -> bool:
    """Whether this is the file-based output a job archived itself (FR-ERR-008)."""
    return item.plain and not item.local_file and item.fresh > 0


RUN = RunState()


def reset() -> None:
    """Forget the run (tests, and a second workflow in one process)."""
    global RUN
    RUN = RunState()


def install(logger: Any) -> str:
    """Log the summary at the end of the run; the mechanism used.

    ``"logger"``: patched ``LoggerManager.stop``, so the block reaches the console and
    the log file. ``"atexit"``: Snakemake's logging could not be patched, and the block
    is logged when the process exits, after the log handlers are gone.
    """
    with _LOCK:
        if getattr(RUN, "_installed", False):
            return getattr(RUN, "_mechanism", "logger")
        RUN._installed = True  # type: ignore[attr-defined]
    mechanism = "logger" if _patch_logger_manager(logger) else "atexit"
    if mechanism == "atexit":
        atexit.register(lambda: RUN.emit(logger))
    RUN._mechanism = mechanism  # type: ignore[attr-defined]
    return mechanism


def _patch_logger_manager(logger: Any) -> bool:
    """Wrap ``LoggerManager.stop`` once per process; whether it is applied."""
    try:
        from snakemake.logging import LoggerManager
    except Exception as e:  # pragma: no cover - snakemake is always installed in tests
        logger.debug(f"{PREFIX} no run summary hook (cannot import LoggerManager: {e})")
        return False
    original = getattr(LoggerManager, "stop", None)
    if original is None or not callable(original):
        logger.debug(f"{PREFIX} no run summary hook (LoggerManager.stop is missing)")
        return False
    if getattr(original, _MARKER, False):
        return True
    try:
        params = list(inspect.signature(original).parameters)
    except (TypeError, ValueError):  # pragma: no cover - a plain function has one
        params = []
    if params != ["self"]:
        logger.debug(
            f"{PREFIX} no run summary hook (LoggerManager.stop takes "
            f"({', '.join(params)}), expected (self))"
        )
        return False

    @functools.wraps(original)
    def stop(self: Any) -> Any:
        try:
            RUN.emit(logger)
        except Exception as e:  # a summary must never fail a workflow
            logger.debug(f"{PREFIX} run summary failed: {e}")
        return original(self)

    setattr(stop, _MARKER, True)
    LoggerManager.stop = stop
    return True
