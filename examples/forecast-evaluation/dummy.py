"""Dummy data for the forecast-evaluation example: GRIB fields with random values.

Plain libraries only — eccodes, numpy, pyfdb and earthkit-data — as a job should be:
nothing here imports the storage plugin. Fields are copies of a template message read
from FDB itself (reduced Gaussian N32, 5248 values, 64 rows) with the MARS keys and the
data values replaced. ``GRID`` reshapes the values to a 64 x 82 image for plotting (not
geographically meaningful, fine for a dummy).
"""

import eccodes
import numpy as np
import pyfdb

GRID = (64, 82)


def request_of(query):
    """The MARS request of a query string (docs/user-guide.md, "Declaring the fields").

    ``fdb://`` stripped, then ``,`` and ``=`` split; ``/`` lists stay strings, which
    pyfdb and earthkit-data take as MARS lists.
    """
    return dict(item.split("=", 1) for item in query.removeprefix("fdb://").split(","))


def read_message(query):
    """The bytes of the field of a single-field query, read with plain pyfdb.

    pyfdb reads the ``FDB5_CONFIG`` the provider exported, so it needs no configuration
    argument (docs/user-guide.md, "Direct access from run and script rules").
    """
    with pyfdb.FDB().retrieve(request_of(query)) as source:
        return source.read()


def build_fields(*, template, expver, date, time, type_, param, values_by_step):
    """One GRIB message per step (in the given order), in memory."""
    messages = []
    for step, values in values_by_step.items():
        h = eccodes.codes_new_from_message(template)
        eccodes.codes_set(h, "stream", "oper")
        eccodes.codes_set(h, "dataDate", int(date))
        eccodes.codes_set(h, "dataTime", int(time))
        eccodes.codes_set(h, "type", type_)
        eccodes.codes_set(h, "expver", expver)
        eccodes.codes_set(h, "step", int(step))
        eccodes.codes_set(h, "paramId", int(param))
        eccodes.codes_set_values(h, np.asarray(values, dtype=float).ravel())
        messages.append(eccodes.codes_get_message(h))
        eccodes.codes_release(h)
    return messages


def archive_fields(messages):
    """Archive GRIB messages with plain pyfdb, which the job environment configures.

    Nothing touches the filesystem, and the rule's output is declared
    ``storage.fdb(..., retrieve=False)``: after the job Snakemake looks the fields up in
    FDB instead of expecting a local file (docs/user-guide.md, "Direct access from run
    and script rules").
    """
    fdb = pyfdb.FDB()
    for message in messages:
        fdb.archive(message)
    fdb.flush()  # before the job ends: the store step looks the fields up


def read_query(query):
    """{step: values} of every field of a job's input query, read straight from FDB.

    earthkit-data's ``fdb`` source reads the ``FDB5_CONFIG`` the provider exported, so
    it needs no configuration argument either.
    """
    from earthkit.data import from_source

    return {
        int(field.metadata("step")): field.to_numpy().ravel()
        for field in from_source("fdb", request_of(query)).to_fieldlist()
    }


def truth_field(rng, param, step):
    """A smooth random 'truth': a few sine waves plus noise, drifting with the step."""
    y, x = np.mgrid[0 : GRID[0], 0 : GRID[1]]
    phase = rng.uniform(0, 2 * np.pi, size=3) + 0.1 * step
    field = np.sin(x / 8 + phase[0]) * np.cos(y / 6 + phase[1]) + 0.3 * np.sin(
        x / 3 + phase[2]
    )
    scale = {167: 10.0, 165: 5.0}.get(int(param), 1.0)
    return 270.0 * (int(param) == 167) + scale * field + rng.normal(0, 0.2, GRID)


def forecast_field(rng, initial, step):
    """The 'ML model': the initial state plus growing random error."""
    return initial + rng.normal(0, 0.05 * (1 + step / 6.0), initial.shape)
