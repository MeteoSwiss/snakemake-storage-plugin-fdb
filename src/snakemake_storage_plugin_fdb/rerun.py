"""Input tracking by lookup: hide FDB inputs from Snakemake's input-set rerun trigger.

Interim patch of the private `snakemake.persistence.PersistenceBase._input` (ADR-031,
architecture.md §8.10, L-21): storage inputs whose object declares
`tracks_input_changes = False` are left out of the recorded input list, as the upstream
hook proposed in requirements.md D-011 would do. Snakemake is imported lazily, inside
the installer, so that the plugin stays importable without it.
"""

import functools
import inspect
import threading
from collections import Counter
from importlib.metadata import PackageNotFoundError, version
from typing import Any

_MARKER = "__fdb_lookup_input_tracking__"
_INSTALL_LOCK = threading.Lock()


def _skips_tracking(f: Any) -> bool:
    """Whether input ``f`` is a storage file that opted out of the trigger."""
    return f.is_storage and not getattr(f.storage_object, "tracks_input_changes", True)


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
    """Patch ``PersistenceBase._input`` once per process; whether the patch is in place.

    Failures (no Snakemake, missing attribute, unexpected signature) are one warning per
    call, never an error (L-21).
    """
    with _INSTALL_LOCK:
        try:
            from snakemake.persistence import PersistenceBase
        except Exception as e:
            return _unavailable(logger, f"cannot import snakemake.persistence: {e}")

        original = getattr(PersistenceBase, "_input", None)
        if original is None:
            return _unavailable(logger, "PersistenceBase._input is missing")
        if getattr(original, _MARKER, False):
            return True
        try:
            params = list(inspect.signature(original).parameters)
        except (TypeError, ValueError) as e:
            return _unavailable(logger, f"PersistenceBase._input has no signature: {e}")
        if params != ["self", "job"]:
            return _unavailable(
                logger,
                f"PersistenceBase._input takes ({', '.join(params)}), "
                "expected (self, job)",
            )

        # updated=(): the original is an lru_cache wrapper, whose own attributes
        # (cache_clear, cache_info) must not end up on the replacement.
        @functools.wraps(original, updated=())
        def _input(self: Any, job: Any) -> list[str]:
            # The original yields storage_object.query for storage inputs and is
            # lru_cached on (self, job); remove the hidden queries from its result,
            # one entry per hidden input (two providers may share a query text).
            hidden = Counter(
                f.storage_object.query for f in job.input if _skips_tracking(f)
            )
            recorded = original(self, job)
            if not hidden:
                return recorded
            kept = []
            for entry in recorded:
                if hidden[entry] > 0:
                    hidden[entry] -= 1
                else:
                    kept.append(entry)
            return kept

        setattr(_input, _MARKER, True)
        PersistenceBase._input = _input
        return True
