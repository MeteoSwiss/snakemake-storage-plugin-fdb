"""Plugin errors without the plugin's own traceback frames (FR-ERR-007).

Snakemake renders a ``WorkflowError`` together with every frame between the ``raise``
and its own call (``snakemake/exceptions.py``: ``cut_traceback`` keeps the frames it
sees before the first Snakemake one), so a one-value mistake in a query arrives with
five ``__init__.py`` frames that tell the user nothing and make the message look like a
bug in the plugin.

``clean_errors`` wraps the methods Snakemake calls: it catches a ``WorkflowError`` and
raises a fresh one with the same message, so the traceback Snakemake renders holds the
wrapper's frame only. That wrapper is compiled from source text under the synthetic file
name ``FILENAME`` and is called ``fdb_storage_error``, so the single line that is left
says where the error comes from instead of pointing into the plugin's internals:

    WorkflowError:
    Invalid MARS request fdb://...: levelist is not allowed with levtype=sfc
      File "<snakemake-storage-plugin-fdb>", line 6, in fdb_storage_error

(Snakemake keeps every frame between the raise and its own call, so one frame is the
minimum; the original error, traceback included, goes to the debug log.)
"""

from __future__ import annotations

import functools
import inspect
import logging
import traceback
from collections.abc import Callable
from typing import Any

from snakemake_interface_common.exceptions import WorkflowError

FILENAME = "<snakemake-storage-plugin-fdb>"

_SOURCE = """
def make(func, log, functools):
    @functools.wraps(func)
    def fdb_storage_error(self, *args, **kwargs):
        try:
            return func(self, *args, **kwargs)
        except WorkflowError as e:
            log(self, e)
            raise WorkflowError(*e.args) from None

    return fdb_storage_error


def make_async(func, log, functools):
    @functools.wraps(func)
    async def fdb_storage_error(self, *args, **kwargs):
        try:
            return await func(self, *args, **kwargs)
        except WorkflowError as e:
            log(self, e)
            raise WorkflowError(*e.args) from None

    return fdb_storage_error
"""


def _log(obj: Any, error: BaseException) -> None:
    """The dropped traceback at debug level, on the object's own logger."""
    logger = getattr(getattr(obj, "provider", None), "logger", None)
    if logger is None:  # pragma: no cover - every storage object has a provider
        logger = logging.getLogger(__name__)
    if logger.isEnabledFor(logging.DEBUG):
        text = "".join(traceback.format_exception(error))
        logger.debug(f"FDB storage: error raised without its frames:\n{text}")


_NAMESPACE: dict[str, Any] = {"WorkflowError": WorkflowError}
exec(compile(_SOURCE, FILENAME, "exec"), _NAMESPACE)  # noqa: S102


def clean_errors(func: Callable[..., Any]) -> Callable[..., Any]:
    """Decorator for a method Snakemake calls: its ``WorkflowError``s reach the user
    without the plugin's traceback frames (FR-ERR-007)."""
    maker = "make_async" if inspect.iscoroutinefunction(func) else "make"
    return _NAMESPACE[maker](func, _log, functools)
