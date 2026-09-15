"""MeteoSwiss site suite: prerequisites from ``SMK_FDB_TEST_*`` env vars (spec §9.2).

Each missing prerequisite is its own skip reason; with ``SMK_FDB_TEST_REQUIRE_SITES=1``
it is a failure instead. Nothing in ``src/`` knows about this suite.
"""

import os
from pathlib import Path

import pytest

REQUIRE = os.environ.get("SMK_FDB_TEST_REQUIRE_SITES") == "1"
MEMFS = "/MEMFS/"  # in-memory definitions bundled with the eccodes wheels


def _missing(reason: str):
    if REQUIRE:
        pytest.fail(f"site prerequisite missing: {reason}", pytrace=False)
    pytest.skip(reason)


@pytest.fixture(scope="session")
def mch_samples() -> Path:
    """Directory with the OGD ICON-CH2-EPS samples (``SMK_FDB_TEST_MCH_SAMPLES``)."""
    value = os.environ.get("SMK_FDB_TEST_MCH_SAMPLES")
    if not value:
        _missing("SMK_FDB_TEST_MCH_SAMPLES is not set (directory with OGD samples)")
    path = Path(value)
    if not any(path.glob("*.grib2")):
        _missing(f"SMK_FDB_TEST_MCH_SAMPLES={value} contains no *.grib2 samples")
    return path


@pytest.fixture(scope="session")
def eccodes_definitions() -> str:
    """Colon list of COSMO definitions dirs (``SMK_FDB_TEST_ECCODES_DEFINITIONS``)."""
    value = os.environ.get("SMK_FDB_TEST_ECCODES_DEFINITIONS")
    if not value:
        _missing(
            "SMK_FDB_TEST_ECCODES_DEFINITIONS is not set "
            "(colon list: eccodes-cosmo-mars, eccodes-cosmo-resources definitions)"
        )
    entries = [d for d in value.split(":") if d]
    bad = [d for d in entries if not d.startswith(MEMFS) and not Path(d).is_dir()]
    if not entries or bad:
        _missing(f"SMK_FDB_TEST_ECCODES_DEFINITIONS: not directories: {bad or value}")
    return value
