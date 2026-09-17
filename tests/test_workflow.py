"""End-to-end: ``snakemake`` on ``examples/ecmwf/`` against a dev FDB (FR-IFACE-004).

``scripts/init_dev_fdb.py --seed --variants`` creates a fresh ``.fdb/`` in a temporary
directory next to a copy of ``examples/ecmwf/``; Snakemake runs as a subprocess with
the documented command (``--storage-fdb-config ../../.fdb/config.yaml -c1``, untagged,
default ``native`` archive mode). All runs happen once per module; logs are written to
``<tmp>/logs/``.

The stages that touch provenance metadata (the two runs, ``--summary`` and
``--delete-all-output``) run once per persistence backend (FR-IFACE-005); the stages
that do not (globbing, the configuration through the environment) run for the file
backend only, and their tests skip for the db one.
"""

import shutil
import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]
EXAMPLE = REPO / "examples" / "ecmwf"
INIT_DEV_FDB = REPO / "scripts" / "init_dev_fdb.py"
SAMPLES = REPO / "tests" / "data" / "grib" / "ecmwf"

pytestmark = pytest.mark.skipif(
    not (SAMPLES / "template.grib").exists(), reason="no ECMWF samples"
)

BACKENDS = ("file", "db")  # --persistence-backend (FR-IFACE-005)
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
# FR-REMOVE-002: --delete-all-output on a complete FDB output
REMOVE_WARNING = (
    f"FDB storage: {OUTPUT_QUERY}: all 3 fields are in FDB. Nothing was removed: FDB "
    "cannot delete individual fields."
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


SPELLING_SNAKEFILE = """\
storage:
    provider="fdb"


rule t2m:
    input:
        storage.fdb(
            "fdb://class=ea,expver=0002,stream=oper,date=20200101,time=0000,domain=g,"
            "type=an,levtype=sfc,step=0,param=2t"
        ),
    output:
        "t2m.grib",
    shell:
        "cp {input} {output}"
"""


def _cmd(config: Path) -> list:
    return [
        sys.executable, "-m", "snakemake",
        "--storage-fdb-config", str(config),
        "--storage-fdb-canonical-spelling", "error", "-c1", "--dry-run",
    ]  # fmt: skip


def test_workflow_spelling_error_names_the_snakefile(tmp_path, run_logged):
    """FR-ERR-006: with canonical_spelling=error a query without wildcards fails where
    it is written, as a plain WorkflowError, not as a task-group traceback out of the
    first lookup (the check needs metkit only, so a dry run is enough)."""
    run = run_logged(tmp_path / "logs")
    run("init", [sys.executable, INIT_DEV_FDB, "--root", tmp_path / ".fdb"], tmp_path)
    work = tmp_path / "work"
    work.mkdir()
    (work / "Snakefile").write_text(SPELLING_SNAKEFILE)
    spelling = run(
        "spelling",
        _cmd(tmp_path / ".fdb" / "config.yaml"),
        work,
    )
    assert spelling.returncode != 0
    assert "WorkflowError in file" in spelling.log
    assert "uses non-canonical spelling: param=2t (canonical: 167)" in spelling.log
    assert "ExceptionGroup" not in spelling.log
    assert len(spelling.log.splitlines()) < 20  # no traceback


QUERY_ERROR_SNAKEFILE = """\
storage:
    provider="fdb"


rule t2m:
    input:
        storage.fdb(
            "fdb://class=ea,expver=0002,stream=oper,date=20200101,time=0000,domain=g,"
            "type=an,levtype=sfc,levelist=500,step=0,param=167"
        ),
    output:
        "t2m.grib",
    shell:
        "cp {input} {output}"
"""

PARTIAL_SNAKEFILE = """\
storage:
    provider="fdb"


rule steps:
    input:
        storage.fdb(
            "fdb://class=ea,expver=0002,stream=oper,date=20200101,time=0000,domain=g,"
            "type=an,levtype=sfc,step=0/6/12/18,param=167"
        ),
    output:
        "steps.txt",
    shell:
        "ls -l {input} > {output}"
"""


def test_workflow_query_error_has_no_plugin_frames(tmp_path, run_logged):
    """FR-ERR-007: a query metkit refuses gives a sentence, not plugin frames."""
    run = run_logged(tmp_path / "logs")
    run("init", [sys.executable, INIT_DEV_FDB, "--root", tmp_path / ".fdb"], tmp_path)
    work = tmp_path / "work"
    work.mkdir()
    (work / "Snakefile").write_text(QUERY_ERROR_SNAKEFILE)
    bad = run("query_error", _cmd(tmp_path / ".fdb" / "config.yaml"), work)
    assert bad.returncode != 0
    assert "levelist is not allowed with levtype=sfc" in bad.log
    assert "snakemake_storage_plugin_fdb/__init__.py" not in bad.log
    assert "Context[" not in bad.log
    assert len(bad.log.splitlines()) < 30


def _fields(config: Path) -> list[dict[str, str]]:
    from snakemake_storage_plugin_fdb.backend import Backend

    return [f.key for f in Backend(config).inspect(OUTPUT_REQUEST)]


@pytest.fixture(scope="module", params=BACKENDS)
def workflow(request, tmp_path_factory, run_logged) -> dict:
    """Runs, in order: init, the example twice, ``--summary``, a glob workflow (keeping
    local copies), ``--delete-all-output``. The ``Run`` of each stage, the paths, and
    what the later stages change: the ``done`` file, the local copy and the FDB fields
    after the first run."""
    backend = request.param
    tmp = tmp_path_factory.mktemp(f"workflow-{backend}")
    run = run_logged(tmp / "logs")
    config = tmp / ".fdb" / "config.yaml"
    example = tmp / "examples" / "ecmwf"
    glob = tmp / "examples" / "glob"
    out: dict = {
        "tmp": tmp,
        "config": config,
        "example": example,
        "glob_dir": glob,
        "backend": backend,
    }

    def snakemake(
        name: str,
        cwd: Path,
        *args: str,
        config: str | None = "../../.fdb/config.yaml",
        env: dict[str, str] | None = None,
    ) -> None:
        flag = ["--storage-fdb-config", config] if config else []
        cmd = [sys.executable, "-m", "snakemake", *flag]
        cmd += ["--persistence-backend", backend, "-c1", *args]
        out[name] = run(name, cmd, cwd, env=env)

    init = [sys.executable, INIT_DEV_FDB, "--root", tmp / ".fdb"]
    out["init"] = run("init", [*init, "--seed", "--variants"], tmp)
    shutil.copytree(EXAMPLE, example, ignore=shutil.ignore_patterns(".snakemake"))

    snakemake("run1", example)
    out["fields_after_run1"] = _fields(config)
    out["local_after_run1"] = (example / OUTPUT_LOCAL).exists()
    out["done"] = (example / "done" / "20200101.txt").read_text()
    snakemake("run2", example)
    snakemake("summary", example, "--summary")

    if backend == "file":  # stages that do not touch the provenance metadata
        glob.mkdir()
        (glob / "Snakefile").write_text(GLOB_SNAKEFILE)
        snakemake("glob", glob, "--keep-storage-local-copies")

        # FR-CONF-008: the same configuration through the environment variable, and
        # the hint when there is none at all (FR-ERR-005).
        env = {"SNAKEMAKE_STORAGE_FDB_CONFIG": "../../.fdb/config.yaml"}
        snakemake("env_var", example, "--dry-run", config=None, env=env)
        no_config = tmp / "examples" / "no-config"
        no_config.mkdir()
        (no_config / "Snakefile").write_text(GLOB_SNAKEFILE)
        snakemake("no_config", no_config, "--dry-run", config=None)

        # FR-READ-008, FR-IFACE-007: a partially present input in a plain run
        partial = tmp / "examples" / "partial"
        partial.mkdir()
        (partial / "Snakefile").write_text(PARTIAL_SNAKEFILE)
        snakemake("partial", partial, "--dry-run")

    snakemake("delete", example, "--delete-all-output")
    return out


def _file_backend_only(workflow: dict) -> None:
    if workflow["backend"] != "file":
        pytest.skip("stage runs for the file backend only")


def test_init_dev_fdb_seeds_samples_and_variants(workflow):
    log = workflow["init"].ok()
    root = workflow["tmp"] / ".fdb"
    assert (root / "schema").read_bytes() == (REPO / "tests/data/schema").read_bytes()
    assert (root / "root").is_dir()
    assert "10 messages archived" in log  # 4 ECMWF samples + 6 template variants


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


def test_workflow_summary_lists_the_fdb_output(workflow):
    """FR-IFACE-005: on both backends ``--summary`` reads the record of the FDB output
    back, keyed by its query text, and reports it up to date."""
    rows = {
        line.split("\t")[0]: line.split("\t")
        for line in workflow["summary"].ok().splitlines()
        if len(line.split("\t")) == 6
    }
    assert rows[OUTPUT_QUERY][4:] == ["ok", "no update"]


def test_workflow_glob_wildcards_steps(workflow):
    _file_backend_only(workflow)
    workflow["glob"].ok()
    glob = workflow["glob_dir"]
    assert (glob / "steps.txt").read_text().split() == ["0", "6", "12"]
    for step in (0, 6, 12):  # --keep-storage-local-copies
        assert (glob / LOCAL_PREFIX / f"step={step}" / "param=167.grib").is_file()


def test_workflow_config_from_environment_variable(workflow):
    """FR-CONF-008: SNAKEMAKE_STORAGE_FDB_CONFIG replaces --storage-fdb-config."""
    _file_backend_only(workflow)
    log = workflow["env_var"].ok()
    assert "FDB configuration error" not in log
    assert workflow["env_var"].NOTHING_TO_BE_DONE in log


def test_workflow_without_any_configuration_hints(workflow):
    """FR-ERR-005: the bundled default schema names the missing configuration."""
    _file_backend_only(workflow)
    log = workflow["no_config"].log
    assert workflow["no_config"].returncode != 0
    assert "no FDB configuration was given: set --storage-fdb-config" in log
    # FR-CONF-010: raised before pyfdb is touched, so eckit dumps nothing
    assert "backtrace" not in log
    assert "WorkflowError in file" in log


def test_workflow_startup_line_names_the_fdb_once(workflow):
    """FR-CONF-010: every run says which FDB it opened, once, in the plain log."""
    log = workflow["run1"].ok()
    lines = [x for x in log.splitlines() if x.startswith("FDB storage: using ")]
    assert len(lines) == 1
    assert ".fdb/config.yaml" in lines[0]
    assert "input tracking: lookup" in lines[0]


def test_workflow_run_summary(workflow):
    """FR-IFACE-007: what the run archived, and nothing when there is nothing to say."""
    summary = [x for x in workflow["run1"].ok().splitlines() if "run summary" in x]
    assert len(summary) == 1
    log = workflow["run1"].log
    assert "3 fields archived (1 query), 0 of which masked fields already in FDB" in log
    assert "reclaimed only by `fdb purge`" in log
    assert (
        "run summary" not in workflow["run2"].ok()
    )  # nothing to be done, nothing said


def test_workflow_partial_input_is_reported_and_summarised(workflow):
    """FR-READ-008: the partial lookup reaches the default log, once, and the summary
    repeats it with what to do about it."""
    _file_backend_only(workflow)
    partial = workflow["partial"]
    assert partial.returncode != 0  # MissingInputException
    lines = [x for x in partial.log.splitlines() if "3 of 4 fields found in FDB" in x]
    assert len(lines) == 2  # the lookup, then the summary
    assert "missing: step=18" in lines[0]
    assert "1 query was incomplete in FDB and no job produced them:" in partial.log
    assert "Run -R <rule> or --forceall to produce them." in partial.log


def test_workflow_delete_all_output_leaves_fields(workflow):
    assert REMOVE_WARNING in workflow["delete"].ok()
    assert not (workflow["example"] / "done" / "20200101.txt").exists()
    assert _fields(workflow["config"]) == workflow["fields_after_run1"]
