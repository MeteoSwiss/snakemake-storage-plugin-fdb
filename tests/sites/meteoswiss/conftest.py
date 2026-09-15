"""MeteoSwiss site suite: prerequisites from ``SMK_FDB_TEST_*`` env vars (spec §9.2).

Each missing prerequisite is its own skip reason; with ``SMK_FDB_TEST_REQUIRE_SITES=1``
it is a failure instead. Nothing in ``src/`` knows about this suite.
"""

import json
import os
import re
import subprocess
import sys
from collections.abc import Callable
from pathlib import Path

import pytest

REQUIRE = os.environ.get("SMK_FDB_TEST_REQUIRE_SITES") == "1"
SITE_ENV = ("ECCODES_DEFINITION_PATH", "METKIT_HOME")
MEMFS = "/MEMFS/"  # in-memory definitions bundled with the eccodes wheels
REPO = Path(__file__).resolve().parents[3]
DEFAULT_SCHEMA = REPO / "examples" / "meteoswiss" / "realtime-varda.schema"
LANGUAGE_FILE = Path("share", "metkit", "language.yaml")


def _missing(reason: str):
    if REQUIRE:
        pytest.fail(f"site prerequisite missing: {reason}", pytrace=False)
    pytest.skip(reason)


@pytest.fixture(scope="session")
def run_site() -> Callable[..., dict]:
    """Runner: ``run(script, job, tmp, env=None)`` executes ``script`` in a subprocess
    with the JSON ``job`` (plus ``local_prefix`` under ``tmp``) as ``sys.argv[1]`` and
    returns the JSON of its last stdout line. The site variables of this process are
    dropped; ``env`` adds variables for the subprocess."""

    def run(script: str, job: dict, tmp: Path, env: dict[str, str] | None = None):
        proc_env = {k: v for k, v in os.environ.items() if k not in SITE_ENV}
        proc_env.update(env or {})
        job = {"local_prefix": str(tmp / "local"), **job}
        proc = subprocess.run(
            [sys.executable, "-c", script, json.dumps(job)],
            env=proc_env,
            capture_output=True,
            text=True,
            timeout=300,
        )
        if proc.returncode != 0:
            pytest.fail(f"site subprocess failed:\n{proc.stderr}", pytrace=False)
        return json.loads(proc.stdout.splitlines()[-1])

    return run


@pytest.fixture(scope="session")
def mch_fdb_config(fdb_config_file, mch_schema) -> Callable[[Path], Path]:
    """Factory: empty toc FDB under ``root`` with the site schema; its config file."""
    return lambda root: fdb_config_file(root, mch_schema)


@pytest.fixture(scope="session")
def mch_query_base() -> Callable[[Path], str]:
    """Constant query keys of the step-6 fields in a sample file
    (``class=od,...,date=<d>,time=<t>,levtype=sfc,step=6``); date and time come from
    the file name, which ``test_conventions`` checks against the MARS keys."""

    def base(path: Path) -> str:
        stamp = re.search(r"_(\d{8})(\d{4})_step6_", path.name)
        assert stamp, path.name
        return (
            "class=od,expver=0001,stream=enfo,model=icon-ch2-eps,"
            f"date={stamp.group(1)},time={stamp.group(2)},levtype=sfc,step=6"
        )

    return base


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
def mch_sample(mch_samples) -> Callable[[str], Path]:
    """Finder: the newest sample file matching a glob pattern (fails if none)."""

    def find(pattern: str) -> Path:
        found = sorted(mch_samples.glob(pattern))
        if not found:
            pytest.fail(f"no sample matching {pattern} in {mch_samples}")
        return found[-1]

    return find


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


@pytest.fixture(scope="session")
def mch_schema() -> Path:
    """varda-style FDB schema (``SMK_FDB_TEST_MCH_SCHEMA``, default in examples/)."""
    value = os.environ.get("SMK_FDB_TEST_MCH_SCHEMA")
    path = Path(value) if value else DEFAULT_SCHEMA
    if not path.is_file():
        _missing(
            f"FDB schema {path} not found (set SMK_FDB_TEST_MCH_SCHEMA; default "
            "examples/meteoswiss/realtime-varda.schema)"
        )
    return path.resolve()


@pytest.fixture(scope="session")
def metkit_home() -> Path:
    """MARS language directory with the site models (``SMK_FDB_TEST_METKIT_HOME``)."""
    value = os.environ.get("SMK_FDB_TEST_METKIT_HOME")
    if not value:
        _missing(
            "SMK_FDB_TEST_METKIT_HOME is not set (directory with "
            f"{LANGUAGE_FILE} defining the site models)"
        )
    path = Path(value)
    if not (path / LANGUAGE_FILE).is_file():
        _missing(f"SMK_FDB_TEST_METKIT_HOME={value} has no {LANGUAGE_FILE}")
    return path.resolve()
