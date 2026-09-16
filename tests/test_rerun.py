"""Input tracking by lookup (FR-RERUN-001, FR-RERUN-002, ADR-031).

The unit tests patch ``snakemake.persistence.PersistenceBase`` in this process and
restore it through ``monkeypatch``; the end-to-end tests run ``snakemake`` in
subprocesses against a dev FDB in a temporary directory, like ``test_workflow.py``.
"""

import inspect
import logging
import sys
import time
from pathlib import Path

import pytest
from snakemake.io import IOFile, flag
from snakemake.persistence import PersistenceBase

from snakemake_storage_plugin_fdb import rerun

REPO = Path(__file__).resolve().parents[1]
INIT_DEV_FDB = REPO / "scripts" / "init_dev_fdb.py"
SAMPLES = REPO / "tests" / "data" / "grib" / "ecmwf"

INPUT_CHANGED = "Set of input files has changed since last execution"
UPDATED_INPUT = "Updated input files"
QUERY = (
    "fdb://class=ea,expver=0001,stream=oper,date=20200101,time=0000,domain=g,"
    "type=an,levtype=sfc,step=0/6/12,param=167"
)

SNAKEFILE = """\
storage:
    provider="fdb"


PARAM = str(config.get("param", "167/165"))  # --config param=167 gives an int
QUERY = (
    "fdb://class=ea,expver=0001,stream=oper,date=20200101,time=0000,domain=g,"
    "type=an,levtype=sfc,step=0/6/12,param=" + PARAM
)
LOCAL = config.get("local", "a.txt").split()


rule fields:
    input:
        storage.fdb(QUERY),
    output:
        "out/fields.txt",
    shell:
        "wc -c < {input} > {output}"


rule fields_run:  # a run: rule executes in a spawned process (architecture.md §8.10)
    input:
        storage.fdb(QUERY),
    output:
        "out/fields_run.txt",
    run:
        shell("wc -c < {input} > {output}")


rule local_files:
    input:
        LOCAL,
    output:
        "out/local.txt",
    shell:
        "cat {input} > {output}"
"""
FDB_TARGETS = ["out/fields.txt", "out/fields_run.txt"]

LOGGER = logging.getLogger("fdb-test")


def _warnings(caplog) -> list[str]:
    return [r.getMessage() for r in caplog.records if r.levelno == logging.WARNING]


def _storage_input(name: str, storage_object) -> IOFile:
    """A storage input as Snakemake builds it (``_IOFile.is_storage``)."""
    return IOFile(flag(name, "storage_object", storage_object))


@pytest.fixture
def unpatched(monkeypatch) -> None:
    """``PersistenceBase._input`` without the patch: another test may have installed it
    by constructing a provider; monkeypatch puts the current value back afterwards."""
    current = PersistenceBase._input
    if getattr(current, rerun._MARKER, False):
        current = current.__wrapped__
    monkeypatch.setattr(PersistenceBase, "_input", current)


# --- the patch --------------------------------------------------------------------


def test_persistence_input_signature_is_stable():
    """The private API the interim patch relies on (L-21, R-14)."""
    assert list(inspect.signature(PersistenceBase._input).parameters) == ["self", "job"]


def test_install_is_idempotent(unpatched, caplog):
    assert rerun.install_lookup_input_tracking(LOGGER)
    patched = PersistenceBase._input
    assert rerun.install_lookup_input_tracking(LOGGER)
    assert PersistenceBase._input is patched
    assert _warnings(caplog) == []


def test_install_hides_only_opted_out_inputs(unpatched, monkeypatch, make_provider):
    seen = []

    def fake_input(self, job):  # the original: lru_cached on (self, job) upstream
        seen.append(job)
        return sorted(
            f.storage_object.query if f.is_storage else str(f) for f in job.input
        )

    monkeypatch.setattr(PersistenceBase, "_input", fake_input)
    assert rerun.install_lookup_input_tracking(LOGGER)

    class OtherObject:  # another plugin's object: no attribute, so tracked
        query = "other://x"

    hidden = _storage_input("fdb1", make_provider().object(QUERY))
    tracked = _storage_input(
        "fdb2", make_provider(input_tracking="query").object(QUERY)
    )
    other = _storage_input("other", OtherObject())
    plain = IOFile("plain.txt")
    job = type("Job", (), {"input": [hidden, tracked, other, plain]})()

    assert PersistenceBase._input(None, job) == [QUERY, "other://x", "plain.txt"]
    assert seen == [job]  # the original sees the real job (its cache key)
    # A job without such inputs is passed through unchanged.
    plain_job = type("Job", (), {"input": [plain]})()
    assert PersistenceBase._input(None, plain_job) == ["plain.txt"]


def test_fallback_when_attribute_is_missing(unpatched, monkeypatch, caplog):
    monkeypatch.delattr(PersistenceBase, "_input")
    assert not rerun.install_lookup_input_tracking(LOGGER)
    assert len(_warnings(caplog)) == 1
    assert "input tracking by lookup is unavailable" in _warnings(caplog)[0]
    assert "falling back to query tracking" in _warnings(caplog)[0]


def test_fallback_on_unexpected_signature(unpatched, monkeypatch, caplog):
    monkeypatch.setattr(PersistenceBase, "_input", lambda self, job, extra=None: [])
    assert not rerun.install_lookup_input_tracking(LOGGER)
    assert len(_warnings(caplog)) == 1
    assert "expected (self, job)" in _warnings(caplog)[0]


def test_provider_setting_query_does_not_patch(make_provider, monkeypatch):
    calls = []
    monkeypatch.setattr(
        "snakemake_storage_plugin_fdb.install_lookup_input_tracking",
        lambda logger: calls.append(logger) or True,
    )
    query = make_provider(input_tracking="query")
    assert query.input_tracking == "query"
    assert query.object(QUERY).tracks_input_changes
    assert calls == []
    lookup = make_provider()
    assert lookup.input_tracking == "lookup"
    assert not lookup.object(QUERY).tracks_input_changes
    assert calls == [lookup.logger]


# --- end to end -------------------------------------------------------------------


def _archive_param_166(config: Path) -> None:
    """Three fields the widened query needs, archived now (later than the output)."""
    from snakemake_storage_plugin_fdb.backend import Backend
    from snakemake_storage_plugin_fdb.grib import variant

    template = (SAMPLES / "template.grib").read_bytes()
    backend = Backend(config)
    for step in (0, 6, 12):
        backend.archive(variant(template, stream="oper", step=step, paramId=166))
    backend.flush()


@pytest.fixture(scope="module")
def reruns(tmp_path_factory, run_logged) -> dict:
    """One dev FDB, one workflow directory and the runs of the scenarios: a full run,
    dry runs after narrowing (with both settings), after changing the local input set
    and after widening to a param archived after the output."""
    if not (SAMPLES / "template.grib").exists():
        pytest.skip("no ECMWF samples")
    tmp = tmp_path_factory.mktemp("rerun")
    run = run_logged(tmp / "logs")
    root = tmp / ".fdb"
    work = tmp / "work"
    out: dict = {"tmp": tmp, "config": root / "config.yaml"}

    out["init"] = run(
        "init",
        [sys.executable, INIT_DEV_FDB, "--root", root, "--seed", "--variants"],
        tmp,
    )
    out["init"].ok()

    work.mkdir()
    (work / "Snakefile").write_text(SNAKEFILE)
    (work / "a.txt").write_text("a\n")
    (work / "b.txt").write_text("b\n")

    def snakemake(name: str, targets: list[str], param: str, local: str, *args: str):
        """One run; ``--config`` last, it swallows what follows."""
        cmd = [
            sys.executable,
            "-m",
            "snakemake",
            "--storage-fdb-config",
            str(root / "config.yaml"),
            "-c1",
            *targets,
            *args,
            "--config",
            f"param={param}",
            f"local={local}",
        ]
        out[name] = run(name, cmd, work)

    both, files = "167/165", "a.txt b.txt"
    snakemake("run1", [*FDB_TARGETS, "out/local.txt"], both, files)
    out["run1"].ok()

    snakemake("narrow", FDB_TARGETS, "167", files, "-n")
    snakemake(
        "narrow_query",
        FDB_TARGETS,
        "167",
        files,
        "-n",
        "--storage-fdb-input-tracking",
        "query",
    )
    snakemake("local", ["out/local.txt"], both, "a.txt", "-n")

    time.sleep(1.2)  # one-second index timestamps (L-3)
    _archive_param_166(root / "config.yaml")
    snakemake("widen", FDB_TARGETS, "167/165/166", files, "-n")
    return out


def test_rerun_narrowed_query_is_up_to_date(reruns):
    """FR-RERUN-001: the query text alone never triggers a rerun, also for the output
    of a ``run:`` rule, whose job ran in a spawned process."""
    log = reruns["narrow"].ok()
    assert reruns["narrow"].NOTHING_TO_BE_DONE in log
    assert INPUT_CHANGED not in log


def test_rerun_widened_query_with_newer_field(reruns):
    """A field archived after the output reruns the job, with the mtime reason."""
    log = reruns["widen"].ok()
    assert UPDATED_INPUT in log
    assert INPUT_CHANGED not in log


def test_rerun_local_input_set_still_triggers(reruns):
    """Only FDB inputs are exempt: local inputs keep Snakemake's behaviour."""
    assert INPUT_CHANGED in reruns["local"].ok()


def test_rerun_input_tracking_query_restores_the_trigger(reruns):
    """FR-RERUN-002: the setting restores Snakemake's default behaviour."""
    log = reruns["narrow_query"].ok()
    assert INPUT_CHANGED in log
    assert reruns["narrow_query"].NOTHING_TO_BE_DONE not in log
