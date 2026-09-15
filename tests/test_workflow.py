"""End-to-end: ``snakemake`` on ``examples/ecmwf/`` against a dev FDB (FR-IFACE-004).

``scripts/init_dev_fdb.py --seed --variants`` creates a fresh ``.fdb/`` in a temporary
directory next to a copy of ``examples/ecmwf/``; Snakemake runs as a subprocess with
the documented command (``--storage-fdb-config ../../.fdb/config.yaml -c1``, untagged,
default ``native`` archive mode). All runs happen once per module; logs are written to
``<tmp>/logs/``.
"""

import shutil
import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]
EXAMPLE = REPO / "examples" / "ecmwf"
INIT_DEV_FDB = REPO / "scripts" / "init_dev_fdb.py"

pytestmark = pytest.mark.skipif(
    not (REPO / ".raw" / "template.grib").exists(), reason="no .raw/ ECMWF samples"
)

OUTPUT_REQUEST = {
    "class": "ea",
    "expver": "0002",
    "stream": "oper",
    "date": "20200101",
    "time": "0000",
    "domain": "g",
    "type": "an",
    "levtype": "sfc",
    "step": "0/6/12",
    "param": "167",
}
OUTPUT_QUERY = "fdb://" + ",".join(f"{k}={v}" for k, v in OUTPUT_REQUEST.items())
# Local copies of the example's queries (FR-PATH-001)
LOCAL_PREFIX = (
    ".snakemake/storage/fdb/class=ea/expver=0002/stream=oper/date=20200101/time=0000/"
    "domain=g/type=an/levtype=sfc/"
)
OUTPUT_LOCAL = LOCAL_PREFIX + "step=0+6+12/param=167.grib"
REMOVE_WARNING = (
    f"FDB cannot delete individual fields; existing fields for {OUTPUT_QUERY} will be "
    "masked by the next archive. Use `fdb purge` to reclaim space."
)
GLOB_SNAKEFILE = """\
storage:
    provider="fdb"


PATTERN = (
    "fdb://class=ea,expver=0002,stream=oper,date=20200101,time=0000,domain=g,"
    "type=an,levtype=sfc,step={step},param=167"
)
STEPS = sorted(glob_wildcards(storage.fdb(PATTERN)).step, key=int)


rule steps:
    input:
        [storage.fdb(PATTERN.format(step=step)) for step in STEPS],
    output:
        "steps.txt",
    params:
        steps=" ".join(STEPS),
    shell:
        "echo {params.steps} > {output}"
"""


def _fields(config: Path) -> list[dict[str, str]]:
    from snakemake_storage_plugin_fdb.backend import Backend

    return [f.key for f in Backend(config).inspect(OUTPUT_REQUEST)]


@pytest.fixture(scope="module")
def workflow(tmp_path_factory, run_logged) -> dict:
    """Runs, in order: init, the example twice, a glob workflow (keeping local
    copies), ``--delete-all-output``. The ``Run`` of each stage, the paths, and what
    the later stages change: the ``done`` file, the local copy and the FDB fields
    after the first run."""
    tmp = tmp_path_factory.mktemp("workflow")
    run = run_logged(tmp / "logs")
    config = tmp / ".fdb" / "config.yaml"
    example = tmp / "examples" / "ecmwf"
    glob = tmp / "examples" / "glob"
    out: dict = {"tmp": tmp, "config": config, "example": example, "glob_dir": glob}

    def snakemake(name: str, cwd: Path, *args: str) -> None:
        cmd = [sys.executable, "-m", "snakemake", "--storage-fdb-config"]
        out[name] = run(name, [*cmd, "../../.fdb/config.yaml", "-c1", *args], cwd)

    init = [sys.executable, INIT_DEV_FDB, "--root", tmp / ".fdb"]
    out["init"] = run("init", [*init, "--seed", "--variants"], tmp)
    shutil.copytree(EXAMPLE, example, ignore=shutil.ignore_patterns(".snakemake"))

    snakemake("run1", example)
    out["fields_after_run1"] = _fields(config)
    out["local_after_run1"] = (example / OUTPUT_LOCAL).exists()
    out["done"] = (example / "done" / "20200101.txt").read_text()
    snakemake("run2", example)

    glob.mkdir()
    (glob / "Snakefile").write_text(GLOB_SNAKEFILE)
    snakemake("glob", glob, "--keep-storage-local-copies")

    snakemake("delete", example, "--delete-all-output")
    return out


def test_init_dev_fdb_seeds_raw_and_variants(workflow):
    log = workflow["init"].ok()
    root = workflow["tmp"] / ".fdb"
    assert (root / "schema").read_bytes() == (REPO / "tests/data/schema").read_bytes()
    assert (root / "root").is_dir()
    assert "10 messages archived" in log  # 4 .raw files + 6 template variants


def test_workflow_run_stores_and_cleans_local_copies(workflow):
    log = workflow["run1"].ok()
    assert f"Storing in storage: {OUTPUT_QUERY}" in log
    lines = workflow["done"].splitlines()
    assert lines[0] == OUTPUT_LOCAL  # the rule read the retrieved local copy
    assert lines[1:] == [f"0002 oper 20200101 {s} 167" for s in (0, 6, 12)]
    assert workflow["local_after_run1"] is False  # removed after the run


def test_workflow_output_fields_in_fdb(workflow):
    workflow["run1"].ok()
    fields = workflow["fields_after_run1"]
    assert sorted(int(f["step"]) for f in fields) == [0, 6, 12]
    assert {f["expver"] for f in fields} == {"0002"}


def test_workflow_second_run_nothing_to_be_done(workflow):
    run2 = workflow["run2"]
    assert run2.NOTHING_TO_BE_DONE in run2.ok()


def test_workflow_glob_wildcards_steps(workflow):
    workflow["glob"].ok()
    glob = workflow["glob_dir"]
    assert (glob / "steps.txt").read_text().split() == ["0", "6", "12"]
    for step in (0, 6, 12):  # --keep-storage-local-copies
        assert (glob / LOCAL_PREFIX / f"step={step}" / "param=167.grib").is_file()


def test_workflow_delete_all_output_leaves_fields(workflow):
    assert REMOVE_WARNING in workflow["delete"].ok()
    assert not (workflow["example"] / "done" / "20200101.txt").exists()
    assert _fields(workflow["config"]) == workflow["fields_after_run1"]
