"""End-to-end ``snakemake`` run on the OGD ICON-CH2-EPS samples (plan step 9).

``scripts/init_dev_fdb.py`` creates and seeds a varda-schema FDB with the site
environment (the documented MeteoSwiss dev-FDB command). The workflow is written into
a temporary directory: a Snakefile with a tagged ``storage mch:`` provider and a
profile setting ``config``, ``eccodes_definitions``, ``metkit_home`` and ``env`` as
``mch::VALUE``. Snakemake runs without ``ECCODES_DEFINITION_PATH``/``METKIT_HOME``, so
the site reaches FDB only through the plugin settings. The rule is a ``shell`` rule:
the local executor runs it in the main process, which avoids the upstream bug that
drops tagged settings in spawned jobs (``run:`` rules, spec §2.7).
"""

import sys
from pathlib import Path

import pytest
import yaml

pytestmark = pytest.mark.site_meteoswiss

REPO = Path(__file__).resolve().parents[3]
INIT_DEV_FDB = REPO / "scripts" / "init_dev_fdb.py"
QUERY = (
    "fdb://class=od,expver=0001,stream=enfo,model=icon-ch2-eps,date={date},"
    "time={time},type=cf,levtype=sfc,step=6,param=500011"
)
# QUERY in the varda schema's key order, as Snakemake logs it and lays out the local
# copy; deliberately spelled out rather than derived, as the expected value.
NORMALISED = (
    "fdb://date={date},time={time},stream=enfo,class=od,expver=0001,"
    "model=icon-ch2-eps,type=cf,levtype=sfc,step=6,param=500011"
)
# The target t2m/<date><time>.txt is passed on the command line; date and time are
# wildcards of the rule and of the storage query. The COSMO definitions open
# /dev/stderr while decoding, which truncates a stderr that is redirected to a file
# (the Snakemake log here), so keys.py writes to the rule log.
SNAKEFILE = f"""\
import sys

PYTHON = sys.executable


storage mch:
    provider="fdb"


wildcard_constraints:
    date=r"\\d{{8}}",
    time=r"\\d{{4}}",


rule t2m_control:
    input:
        storage.mch("{QUERY}"),
    output:
        grib="t2m/{{date}}{{time}}.grib2",
        listing="t2m/{{date}}{{time}}.txt",
    log:
        "logs/t2m_{{date}}{{time}}.log",
    shell:
        "cp {{input}} {{output.grib}} && "
        "{{PYTHON}} keys.py {{input}} > {{output.listing}} 2> {{log}}"
"""
KEYS_SCRIPT = """\
import sys

import eccodes

with open(sys.argv[1], "rb") as f:
    while (h := eccodes.codes_grib_new_from_file(f)) is not None:
        print(*(eccodes.codes_get_string(h, k) for k in ("shortName", "step")))
        eccodes.codes_release(h)
"""


@pytest.fixture(scope="module")
def site_workflow(
    tmp_path_factory,
    run_logged,
    mch_sample,
    mch_stamp,
    mch_schema,
    eccodes_definitions,
    metkit_home,
) -> dict:
    tmp = tmp_path_factory.mktemp("mch-workflow")
    run = run_logged(tmp / "logs")
    sample = mch_sample("*_step6_t_2m_ctrl.grib2")
    date, time = mch_stamp(sample)
    work = tmp / "work"
    work.mkdir()
    out: dict = {"work": work, "sample": sample, "date": date, "time": time}

    site = {
        "ECCODES_DEFINITION_PATH": eccodes_definitions,
        "METKIT_HOME": str(metkit_home),
        "ECCODES_VERSION_CHECK_OFF": "1",
    }
    init = [sys.executable, INIT_DEV_FDB, "--root", ".fdb-mch", "--schema", mch_schema]
    out["init"] = run("init", [*init, "--seed", sample.parent], work, site)

    (work / "Snakefile").write_text(SNAKEFILE)
    (work / "keys.py").write_text(KEYS_SCRIPT)
    (work / "profile").mkdir()
    profile = {
        "storage-fdb-config": ["mch::.fdb-mch/config.yaml"],
        "storage-fdb-eccodes-definitions": [f"mch::{eccodes_definitions}"],
        "storage-fdb-metkit-home": [f"mch::{metkit_home}"],
        "storage-fdb-env": ["mch::ECCODES_VERSION_CHECK_OFF=1"],
    }
    (work / "profile" / "config.yaml").write_text(yaml.safe_dump(profile))
    target = f"t2m/{date}{time}.txt"
    snakemake = [sys.executable, "-m", "snakemake", "--profile", "profile", "-c1"]
    out["run1"] = run("run1", [*snakemake, target], work)
    suffix = NORMALISED.format(date=date, time=time).removeprefix("fdb://")
    local = (
        work / ".snakemake" / "storage" / "mch" / (suffix.replace(",", "/") + ".grib")
    )
    out["local_after_run1"] = local.exists()
    out["run2"] = run("run2", [*snakemake, target], work)
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
    assert stem.with_suffix(".txt").read_text().split() == ["T_2M", "6"]
    assert site_workflow["local_after_run1"] is False  # local copy removed


def test_workflow_second_run_is_a_no_op(site_workflow):
    run2 = site_workflow["run2"]
    assert run2.NOTHING_TO_BE_DONE in run2.ok()
