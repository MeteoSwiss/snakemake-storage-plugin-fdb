"""``api.exists`` and ``python -m snakemake_storage_plugin_fdb`` (FR-DIRECT-006,
FR-DEV-004): what the plugin can say about an FDB without a workflow.
"""

import subprocess
import sys
from pathlib import Path

import pytest
import yaml

from snakemake_storage_plugin_fdb import api

DATA = Path(__file__).resolve().parent / "data"
SAMPLES = DATA / "grib" / "ecmwf"
EA = (
    "class=ea,expver=0001,stream=oper,date=20200101,time=0000,domain=g,type=an,"
    "levtype=sfc"
)
COMPLETE = f"fdb://{EA},step=0/6/12,param=167"
PARTIAL = f"fdb://{EA},step=0/6/12/18,param=167"

pytestmark = pytest.mark.skipif(
    not (SAMPLES / "template.grib").exists(), reason="no ECMWF samples"
)


@pytest.fixture
def config_file(seeded_fdb, tmp_path, clean_env) -> Path:
    path = tmp_path / "config.yaml"
    path.write_text(yaml.safe_dump(seeded_fdb.config))
    return path


def _cli(config: Path, *args: str, env=None) -> subprocess.CompletedProcess:
    from .conftest import _subprocess_env

    return subprocess.run(
        [sys.executable, "-m", "snakemake_storage_plugin_fdb", *args],
        capture_output=True,
        text=True,
        env=_subprocess_env(env),
        timeout=300,
    )


# --- api.exists (FR-DIRECT-006) ------------------------------------------------------


def test_api_exists_complete(config_file):
    lookup = api.exists(COMPLETE, config=str(config_file))
    assert (lookup.found, lookup.expected) == (3, 3)
    assert lookup.complete is True and bool(lookup) is True
    assert lookup.missing == []
    assert {f.keys["step"] for f in lookup.fields} == {"0", "6", "12"}
    assert all(f.length > 0 for f in lookup.fields)
    assert lookup.query == COMPLETE


def test_api_exists_partial_names_the_missing_fields(config_file):
    lookup = api.exists(PARTIAL, config=str(config_file))
    assert (lookup.found, lookup.expected) == (3, 4)
    assert lookup.complete is False and not lookup
    assert lookup.missing == ["step=18"]


def test_api_exists_absent(config_file):
    lookup = api.exists(f"fdb://{EA},step=99,param=167", config=str(config_file))
    assert (lookup.found, lookup.expected, lookup.fields) == (0, 1, [])
    # a single-field query: the one missing combination names every key
    assert len(lookup.missing) == 1
    assert lookup.missing[0].startswith("class=ea,")
    assert lookup.missing[0].endswith("step=99,param=167")


# --- the command line (FR-DEV-004) ---------------------------------------------------


def test_cli_inspect_complete(config_file):
    done = _cli(config_file, "inspect", "--config", str(config_file), COMPLETE)
    assert done.returncode == 0, done.stderr
    assert done.stdout.splitlines()[0] == f"3 of 3 fields in FDB for {COMPLETE}"
    assert done.stdout.count("step=0,") == 1  # one line per field, with its keys
    assert "bytes" in done.stdout


def test_cli_inspect_incomplete_exits_1(config_file):
    done = _cli(config_file, "inspect", "--config", str(config_file), PARTIAL)
    assert done.returncode == 1
    assert done.stdout.splitlines()[0].startswith("3 of 4 fields in FDB")
    assert "  missing: step=18" in done.stdout


def test_cli_inspect_reports_an_invalid_query(config_file):
    done = _cli(config_file, "inspect", "--config", str(config_file), "fdb://class=")
    assert done.returncode == 2
    assert "error: invalid FDB query" in done.stderr


def test_cli_inspect_uses_the_environment(config_file):
    """FR-CONF-008: the plugin's own setting variables, as in a spawned job."""
    done = _cli(
        config_file,
        "inspect",
        COMPLETE,
        env={"SNAKEMAKE_STORAGE_FDB_CONFIG": str(config_file)},
    )
    assert done.returncode == 0, done.stderr


def test_cli_list_distinct_values(config_file):
    partial = f"fdb://{EA},param=167"  # no step: FDB lists what it holds
    done = _cli(config_file, "list", "--config", str(config_file), partial)
    assert done.returncode == 0, done.stderr
    lines = done.stdout.splitlines()
    assert lines[0].startswith("3 fields in FDB under ")
    assert "  step: 0/12/6" in lines
    assert "  class: ea" in lines


def test_cli_list_without_fields_exits_1(config_file):
    done = _cli(
        config_file,
        "list",
        "--config",
        str(config_file),
        f"fdb://{EA.replace('expver=0001', 'expver=9999')}",
    )
    assert done.returncode == 1
    assert "no fields in FDB under" in done.stdout


def test_cli_list_maps_fdb_errors(config_file):
    done = _cli(config_file, "list", "--config", str(config_file), "fdb://class=zz")
    assert done.returncode == 2
    assert "error: Invalid MARS request fdb://class=zz" in done.stderr
    assert "Traceback" not in done.stderr


def test_cli_help_lists_both_commands(config_file):
    done = _cli(config_file, "--help")
    assert done.returncode == 0
    assert "inspect" in done.stdout and "list" in done.stdout
