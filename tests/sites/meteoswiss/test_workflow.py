"""End-to-end ``snakemake`` run of ``examples/meteoswiss/`` on the OGD samples.

``scripts/init_dev_fdb.py`` creates and seeds ``.fdb-mch/`` with the site environment
(the documented MeteoSwiss dev-FDB command). A copy of the shipped example (Snakefile,
profile, ``grib_keys.py``) runs next to it, from ``examples/meteoswiss/`` as the README
says, with the shipped profile: tagged ``storage mch:`` provider, ``config`` and
``env`` from the profile. Only the two site paths the profile expects under
``.local/`` are replaced on the command line by the ``SMK_FDB_TEST_*`` values.
Snakemake runs without ``ECCODES_DEFINITION_PATH``/``METKIT_HOME``, so the site
reaches FDB only through the plugin settings.
"""

import shutil
import sys
from pathlib import Path

import pytest
import yaml

pytestmark = pytest.mark.site_meteoswiss

REPO = Path(__file__).resolve().parents[3]
EXAMPLE = REPO / "examples" / "meteoswiss"
INIT_DEV_FDB = REPO / "scripts" / "init_dev_fdb.py"
# The example's query in the varda schema's key order, as Snakemake logs it and lays
# out the local copy; deliberately spelled out rather than derived, as the expected
# value.
NORMALISED = (
    "fdb://date={date},time={time},stream=enfo,class=od,expver=0001,"
    "model=icon-ch2-eps,type=cf,levtype=sfc,step=6,param=500011"
)


def test_workflow_example_profile_is_tagged():
    # every value is for the tagged provider; the two paths the e2e test overrides
    # on the command line are there to be overridden (no site prerequisites needed)
    profile = yaml.safe_load((EXAMPLE / "profile" / "config.yaml").read_text())
    assert {"storage-fdb-eccodes-definitions", "storage-fdb-metkit-home"} <= set(
        profile
    )
    for key, values in profile.items():
        assert key.startswith("storage-fdb-"), key
        assert all(v.startswith("mch::") for v in values), (key, values)


@pytest.fixture(scope="module")
def site_workflow(
    tmp_path_factory,
    run_logged,
    mch_sample,
    mch_stamp,
    mch_schema,
    site_env,
    eccodes_definitions,
    metkit_home,
) -> dict:
    tmp = tmp_path_factory.mktemp("mch-workflow")
    run = run_logged(tmp / "logs")
    sample = mch_sample("*_step6_t_2m_ctrl.grib2")
    date, time = mch_stamp(sample)
    work = tmp / "examples" / "meteoswiss"
    ignore = shutil.ignore_patterns(".snakemake", "__pycache__", "t2m", "logs")
    shutil.copytree(EXAMPLE, work, ignore=ignore)
    out: dict = {"work": work, "sample": sample, "date": date, "time": time}

    site = {**site_env, "METKIT_HOME": str(metkit_home)}
    init = [sys.executable, INIT_DEV_FDB, "--root", ".fdb-mch", "--schema", mch_schema]
    out["init"] = run("init", [*init, "--seed", sample.parent], tmp, site)

    snakemake = [
        sys.executable,
        "-m",
        "snakemake",
        "--profile",
        "profile",
        "-c1",
        "--storage-fdb-eccodes-definitions",
        f"mch::{eccodes_definitions}",
        "--storage-fdb-metkit-home",
        f"mch::{metkit_home}",
    ]
    out["run1"] = run("run1", snakemake, work)
    suffix = NORMALISED.format(date=date, time=time).removeprefix("fdb://")
    local = (
        work / ".snakemake" / "storage" / "mch" / (suffix.replace(",", "/") + ".grib")
    )
    out["local_after_run1"] = local.exists()
    out["run2"] = run("run2", snakemake, work)
    return out


def test_workflow_init_dev_fdb_site_command(site_workflow):
    log = site_workflow["init"].ok()
    assert "4 messages archived" in log  # ctrl T_2M, ctrl TOT_PREC, members 1-2


def test_workflow_retrieves_control_field(site_workflow):
    log = site_workflow["run1"].ok()
    work, date, time = (site_workflow[k] for k in ("work", "date", "time"))
    query = NORMALISED.format(date=date, time=time)
    assert f"{query} (retrieve from storage)" in log
    stem = work / "t2m" / f"{date}{time}"
    grib = stem.with_suffix(".grib2").read_bytes()
    assert grib == site_workflow["sample"].read_bytes()  # byte-identical retrieve
    # the definitions from the eccodes_definitions setting reach the shell job
    assert stem.with_suffix(".txt").read_text().split() == ["T_2M", "6", "0"]
    assert site_workflow["local_after_run1"] is False  # local copy removed


def test_workflow_second_run_is_a_no_op(site_workflow):
    run2 = site_workflow["run2"]
    assert run2.NOTHING_TO_BE_DONE in run2.ok()
