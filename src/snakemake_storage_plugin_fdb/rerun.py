"""Input tracking by lookup: decide the input-set trigger for FDB inputs by fields.

Interim patch of the private `snakemake.persistence.PersistenceBase._input_changed`
(ADR-034, architecture.md §8.10, L-21): an FDB input whose object declares
`tracks_input_changes = False` counts as changed only when the fields its query expands
to are not all covered by the FDB queries recorded for the job, as the upstream hook
proposed in requirements.md D-011 would decide it. Snakemake is imported lazily, inside
the installer, so that the plugin stays importable without it.
"""

import functools
import inspect
import threading
from collections import Counter
from collections.abc import Iterable
from importlib.metadata import PackageNotFoundError, version
from typing import Any

_MARKER = "__fdb_lookup_input_tracking__"
_INSTALL_LOCK = threading.Lock()


def _covered_objects(job: Any) -> list[Any]:
    """Storage objects of ``job`` that decide the trigger by field coverage."""
    return [
        f.storage_object
        for f in job.input
        if f.is_storage and not getattr(f.storage_object, "tracks_input_changes", True)
    ]


def decide(objects: list[Any], current: Iterable[Any], recorded: Iterable[Any]) -> bool:
    """Whether the input set changed, given the coverage-tracked ``objects`` of a job,
    the input list Snakemake computes now and the recorded one (architecture.md §8.10).

    Everything that is not one of ``objects`` is compared as Snakemake does: the same
    entries with the same multiplicities. The recorded entries that comparison leaves
    unmatched must all be queries of the objects' providers; they are the pool every
    object must be covered by.
    """
    # One entry per object: two providers may share a query text (Counter arithmetic
    # keeps multiplicities and drops what is not there).
    rest = Counter(map(str, current)) - Counter(obj.query for obj in objects)
    recorded = Counter(map(str, recorded))
    if rest - recorded:
        return True  # an input Snakemake compares by text is new
    pool = list(recorded - rest)
    providers = {id(obj.provider): obj.provider for obj in objects}.values()
    if not all(any(p.is_valid_query(e).valid for p in providers) for e in pool):
        return True  # an input Snakemake compares by text is gone
    return not all(obj.covered_by(pool) for obj in objects)


def _unavailable(logger: Any, reason: str) -> bool:
    try:
        release = version("snakemake")
    except PackageNotFoundError:
        release = "unknown"
    logger.warning(
        f"FDB storage: input tracking by lookup is unavailable with snakemake "
        f"{release} ({reason}); falling back to query tracking. Set "
        "input_tracking=query to silence this warning."
    )
    return False


def install_lookup_input_tracking(logger: Any) -> bool:
    """Patch ``PersistenceBase._input_changed`` once per process; whether it is applied.

    Failures (no Snakemake, missing attribute, unexpected signature) are one warning per
    call, never an error (L-21).
    """
    with _INSTALL_LOCK:
        try:
            from snakemake.persistence import PersistenceBase
        except Exception as e:
            return _unavailable(logger, f"cannot import snakemake.persistence: {e}")

        original = getattr(PersistenceBase, "_input_changed", None)
        if original is None:
            return _unavailable(logger, "PersistenceBase._input_changed is missing")
        if getattr(original, _MARKER, False):
            return True
        try:
            params = list(inspect.signature(original).parameters)
        except (TypeError, ValueError) as e:
            return _unavailable(
                logger, f"PersistenceBase._input_changed has no signature: {e}"
            )
        if params != ["self", "job", "file"]:
            return _unavailable(
                logger,
                f"PersistenceBase._input_changed takes ({', '.join(params)}), "
                "expected (self, job, file)",
            )

        @functools.wraps(original)
        def _input_changed(self: Any, job: Any, file: Any = None) -> bool:
            # The original gates on the record format version and compares the recorded
            # input list with the current one; equal lists mean no change here either,
            # so only its True is re-decided by field coverage.
            if not original(self, job, file=file):
                return False
            objects = _covered_objects(job)
            if not objects:
                return True
            try:
                return decide(objects, self._input(job), self.input(file))
            except Exception as e:  # a plugin failure must not fail the workflow
                logger.debug(
                    f"FDB storage: input-set trigger falls back to the recorded "
                    f"queries for {getattr(job, 'name', job)}: {e}"
                )
                return True

        setattr(_input_changed, _MARKER, True)
        PersistenceBase._input_changed = _input_changed
        return True
