"""Input tracking by lookup (FR-RERUN-001, FR-RERUN-002, ADR-034).

The unit tests patch ``snakemake.persistence.PersistenceBase`` in this process and
restore it through ``monkeypatch``; the end-to-end tests run ``snakemake`` in
subprocesses against a dev FDB in a temporary directory, like ``test_workflow.py``.
Each end-to-end fixture runs once per provenance backend (FR-IFACE-005): the file
backend and ``--persistence-backend db`` with its default SQLite URL.
"""

import inspect
import json
import logging
import shutil
import sqlite3
import sys
import time
from base64 import urlsafe_b64decode
from pathlib import Path
from types import SimpleNamespace

import pytest
from snakemake.io import IOFile, flag
from snakemake.persistence import PersistenceBase

from snakemake_storage_plugin_fdb import rerun

REPO = Path(__file__).resolve().parents[1]
INIT_DEV_FDB = REPO / "scripts" / "init_dev_fdb.py"
SAMPLES = REPO / "tests" / "data" / "grib" / "ecmwf"

INPUT_CHANGED = "Set of input files has changed since last execution"
UPDATED_INPUT = "Updated input files"
BASE = (
    "fdb://class=ea,expver=0001,stream=oper,date=20200101,time=0000,domain=g,"
    "type=an,levtype=sfc,step={step},param={param}"
)


def query(param: str = "167/165", step: str = "0/6/12") -> str:
    return BASE.format(param=param, step=step)


QUERY = query()

SNAKEFILE = """\
storage:
    provider="fdb"


BASE = (
    "fdb://class=ea,expver=0001,stream=oper,date=20200101,time=0000,domain=g,"
    "type=an,levtype=sfc,step={step},param={param}"
)


def query(param="167/165", step="0/6/12"):
    return BASE.format(param=param, step=step)


PARAM = str(config.get("param", "167/165"))  # --config param=167 gives an int
STEP = str(config.get("step", "0/6/12"))
LOCAL = config.get("local", "a.txt b.txt").split()
SPLIT_MS = int(config.get("split_ms", 0))  # recorded as one query, compared as two
SPLIT_SM = int(config.get("split_sm", 1))  # recorded as two queries, compared as one


def maybe_split(split):
    if split:
        return [storage.fdb(query(param="167")), storage.fdb(query(param="165"))]
    return storage.fdb(query(param="167/165"))


rule fields:
    input:
        storage.fdb(query(param=PARAM, step=STEP)),
    output:
        "out/fields.txt",
    shell:
        "wc -c < {input} > {output}"


rule fields_run:  # a run: rule executes in a spawned process (architecture.md §8.10)
    input:
        storage.fdb(query(param=PARAM, step=STEP)),
    output:
        "out/fields_run.txt",
    run:
        shell("wc -c < {input} > {output}")


rule merged_to_split:
    input:
        maybe_split(SPLIT_MS),
    output:
        "out/merged_to_split.txt",
    shell:
        "cat {input} > {output}"


rule split_to_merged:
    input:
        maybe_split(SPLIT_SM),
    output:
        "out/split_to_merged.txt",
    shell:
        "cat {input} > {output}"


rule local_files:
    input:
        LOCAL,
    output:
        "out/local.txt",
    shell:
        "cat {input} > {output}"
"""
BACKENDS = ("file", "db")  # --persistence-backend (FR-IFACE-005)
FDB_TARGETS = ["out/fields.txt", "out/fields_run.txt"]
REFACTOR_TARGETS = ["out/merged_to_split.txt", "out/split_to_merged.txt"]

CHAIN = """\
storage:
    provider="fdb"


SRC = (
    "fdb://class=ea,expver=0001,stream=oper,date=20200101,time=0000,domain=g,"
    "type=an,levtype=sfc,step=0/6/12,param={param}"
)
DST = SRC.replace("expver=0001", "expver=0002")
PARAMS = str(config.get("params", "167")).split("/")


rule all:
    input:
        "out/consumed.txt",


rule produce:
    input:
        storage.fdb(SRC),
    output:
        storage.fdb(DST),
    run:
        import eccodes

        with open(input[0], "rb") as fi, open(output[0], "wb") as fo:
            while (handle := eccodes.codes_grib_new_from_file(fi)) is not None:
                eccodes.codes_set(handle, "expver", "0002")
                eccodes.codes_write(handle, fo)
                eccodes.codes_release(handle)


rule consume:
    input:
        [storage.fdb(DST.format(param=p)) for p in PARAMS],
    output:
        "out/consumed.txt",
    shell:
        "cat {input} > {output}"
"""

PRODUCED = (  # the FDB output of ``rule produce`` for param 167
    "fdb://class=ea,expver=0002,stream=oper,date=20200101,time=0000,domain=g,"
    "type=an,levtype=sfc,step=0/6/12,param=167"
)
CLEANUP_FAILED = "Failed to clean up metadata for the following files"

LOGGER = logging.getLogger("fdb-test")


def _warnings(caplog) -> list[str]:
    return [r.getMessage() for r in caplog.records if r.levelno == logging.WARNING]


def _storage_input(name: str, storage_object) -> IOFile:
    """A storage input as Snakemake builds it (``_IOFile.is_storage``)."""
    return IOFile(flag(name, "storage_object", storage_object))


def _job(*inputs) -> object:
    return SimpleNamespace(input=list(inputs), name="job")


def _changed(recorded: list[str], current: list[str], job, version: int = 4) -> bool:
    """``PersistenceBase._input_changed`` for one output, on a fake persistence that
    holds the three things it reads."""
    state = SimpleNamespace(
        record_format_version=lambda file: version,
        input=lambda file: sorted(recorded),
        _input=lambda job: sorted(current),
    )
    return PersistenceBase._input_changed(state, job, file="out.txt")


@pytest.fixture
def unpatched(monkeypatch) -> None:
    """``PersistenceBase._input_changed`` without the patch: another test may have
    installed it by constructing a provider; monkeypatch restores the current value."""
    current = PersistenceBase._input_changed
    if getattr(current, rerun._MARKER, False):
        current = current.__wrapped__
    monkeypatch.setattr(PersistenceBase, "_input_changed", current)


# --- the patch --------------------------------------------------------------------


def test_persistence_private_api_is_stable(make_provider):
    """The private API the interim patch relies on (L-21, R-14): the hook's signature
    and the recorded form of a storage input, its query text verbatim."""
    assert list(inspect.signature(PersistenceBase._input_changed).parameters) == [
        "self",
        "job",
        "file",
    ]
    obj = make_provider().object(query(param="167"))
    job = _job(_storage_input("fdb", obj), IOFile("plain.txt"))
    assert PersistenceBase._input.__wrapped__(None, job) == sorted(
        [obj.query, "plain.txt"]
    )


def test_install_is_idempotent(unpatched, caplog):
    assert rerun.install_lookup_input_tracking(LOGGER)
    patched = PersistenceBase._input_changed
    assert rerun.install_lookup_input_tracking(LOGGER)
    assert PersistenceBase._input_changed is patched
    assert _warnings(caplog) == []


def test_fallback_when_attribute_is_missing(unpatched, monkeypatch, caplog):
    monkeypatch.delattr(PersistenceBase, "_input_changed")
    assert not rerun.install_lookup_input_tracking(LOGGER)
    assert len(_warnings(caplog)) == 1
    assert "input tracking by lookup is unavailable" in _warnings(caplog)[0]
    assert "falling back to query tracking" in _warnings(caplog)[0]


def test_fallback_on_unexpected_signature(unpatched, monkeypatch, caplog):
    monkeypatch.setattr(PersistenceBase, "_input_changed", lambda self, job: False)
    assert not rerun.install_lookup_input_tracking(LOGGER)
    assert len(_warnings(caplog)) == 1
    assert "expected (self, job, file)" in _warnings(caplog)[0]


def test_provider_setting_query_does_not_patch(make_provider, monkeypatch):
    calls = []
    monkeypatch.setattr(
        "snakemake_storage_plugin_fdb.install_lookup_input_tracking",
        lambda logger: calls.append(logger) or True,
    )
    query_provider = make_provider(input_tracking="query")
    assert query_provider.input_tracking == "query"
    assert query_provider.object(QUERY).tracks_input_changes
    assert calls == []
    lookup = make_provider()
    assert lookup.input_tracking == "lookup"
    assert not lookup.object(QUERY).tracks_input_changes
    assert calls == [lookup.logger]


def test_patched_input_changed_uses_coverage(unpatched, make_provider):
    """The wrapper re-decides only what Snakemake reports as changed, and only for
    this plugin's coverage-tracked inputs."""
    assert rerun.install_lookup_input_tracking(LOGGER)
    provider = make_provider()
    narrow = _job(_storage_input("fdb", provider.object(query("167"))), IOFile("p.txt"))
    wide = _job(_storage_input("fdb", provider.object(QUERY)), IOFile("p.txt"))
    assert not _changed([QUERY, "p.txt"], [query("167"), "p.txt"], narrow)
    assert _changed([query("167"), "p.txt"], [QUERY, "p.txt"], wide)
    # The original's format-version gate is consulted first.
    assert not _changed([query("167"), "p.txt"], [QUERY, "p.txt"], wide, version=3)


def test_patched_input_changed_without_fdb_inputs(unpatched, make_provider):
    """A provider with ``input_tracking=query`` keeps Snakemake's comparison."""
    assert rerun.install_lookup_input_tracking(LOGGER)
    tracked = make_provider(input_tracking="query").object(query(param="167"))
    job = _job(_storage_input("fdb", tracked), IOFile("plain.txt"))
    assert _changed([QUERY, "plain.txt"], [query("167"), "plain.txt"], job)


def test_patched_input_changed_survives_a_plugin_failure(
    unpatched, make_provider, monkeypatch
):
    """A failure inside the decision falls back to Snakemake's answer."""
    assert rerun.install_lookup_input_tracking(LOGGER)
    provider = make_provider()
    monkeypatch.setattr(
        type(provider),
        "field_set",
        lambda self, q: (_ for _ in ()).throw(RuntimeError("boom")),
    )
    job = _job(_storage_input("fdb", provider.object(query(param="167"))))
    assert _changed([QUERY], [query("167")], job)


# --- the coverage decision ----------------------------------------------------------


def test_decide_partitions_inputs(make_provider):
    """Entries that are not this plugin's queries are compared as Snakemake does; the
    unmatched recorded queries are the pool the tracked objects must be covered by."""
    obj = make_provider().object(query(param="167"))
    assert not rerun.decide([obj], [obj.query, "a.txt"], [QUERY, "a.txt"])
    assert rerun.decide([obj], [obj.query, "a.txt", "b.txt"], [QUERY, "a.txt"])
    assert rerun.decide([obj], [obj.query, "a.txt"], [QUERY, "a.txt", "b.txt"])
    # Nothing recorded for the job's FDB inputs (a record written by 0.2.0).
    assert rerun.decide([obj], [obj.query, "a.txt"], ["a.txt"])
    # A recorded entry that is not a query of this plugin is compared as text.
    assert rerun.decide([obj], [obj.query], [QUERY, "other://x"])
    assert rerun.decide([obj], [obj.query], [QUERY, "fdb://nonsense"])


def test_covered_by_narrowing_and_widening(make_provider):
    """Fewer fields, or the same fields split into per-field queries, are covered."""
    provider = make_provider()
    split = [query(param="167"), query(param="165")]
    assert provider.object(query(param="167")).covered_by([QUERY])
    assert provider.object(query(step="0/6")).covered_by([QUERY])
    assert provider.object(QUERY).covered_by(split)
    assert not provider.object(QUERY).covered_by([query(param="167")])
    assert not provider.object(QUERY).covered_by([])


def test_covered_by_ignores_notation(make_provider):
    """Order, ranges and non-canonical spellings name the same fields."""
    provider = make_provider()
    assert provider.object(query(param="165/167", step="12/6/0")).covered_by([QUERY])
    assert provider.object(query(step="0/to/12/by/6")).covered_by([QUERY])
    assert provider.object(query(param="2t")).covered_by([query(param="167")])


def test_covered_by_unexpandable_queries(make_provider):
    """A recorded entry that is no expandable query covers nothing, except itself: the
    same text is covered without expansion."""
    provider = make_provider()
    assert not provider.object(query(param="167")).covered_by(["fdb://nonsense"])
    assert provider.object(query(param="167")).covered_by(["fdb://nonsense", QUERY])
    assert provider.object("fdb://nonsense").covered_by(["fdb://nonsense"])


def test_covered_by_differing_key_sets(make_provider):
    """A key one query names and the other does not makes the fields differ."""
    provider = make_provider()
    without_domain = query(param="167").replace(",domain=g", "")
    assert not provider.object(without_domain).covered_by([query(param="167")])
    assert not provider.object(query(param="167")).covered_by([without_domain])


def test_field_set_is_cached_per_query(make_provider, monkeypatch):
    provider = make_provider()
    calls = []
    monkeypatch.setattr(
        type(provider), "_expand_fields", lambda self, q: calls.append(q) or frozenset()
    )
    provider.field_set("fdb://other")
    provider.field_set("fdb://other")
    assert calls == ["fdb://other"]


def test_field_set_refuses_oversized_queries(make_provider, monkeypatch):
    monkeypatch.setattr("snakemake_storage_plugin_fdb.COVERAGE_MAX", 2)
    provider = make_provider()
    assert provider.field_set(query(param="167", step="0/6")) is not None
    assert provider.field_set(QUERY) is None


# --- end to end -------------------------------------------------------------------


def _archive(config: Path, params: tuple[int, ...], steps: tuple[int, ...]) -> None:
    """Archive ``class=ea,stream=oper`` variants of the template now."""
    from snakemake_storage_plugin_fdb.backend import Backend
    from snakemake_storage_plugin_fdb.grib import variant

    template = (SAMPLES / "template.grib").read_bytes()
    backend = Backend(config)
    for param in params:
        for step in steps:
            backend.archive(variant(template, stream="oper", step=step, paramId=param))
    backend.flush()


def _drop_fdb_inputs(work: Path, backend: str) -> int:
    """Remove the FDB queries from the recorded input sets, as 0.2.0 wrote them.

    ``work`` is a copy of the directory the records were written in. The db backend
    keys its records by the absolute path of the workdir's ``.snakemake`` (namespace),
    so the copied records are rewritten to the copy's namespace as well; the file
    backend's records live in the copied directory and need no rewrite.
    """
    if backend == "db":
        return _drop_fdb_inputs_db(work / ".snakemake")
    dropped = 0
    for record in (work / ".snakemake" / "metadata").rglob("*"):
        if not record.is_file():
            continue
        data = json.loads(record.read_text())
        inputs = data.get("input")
        if not inputs:
            continue
        kept = [i for i in inputs if not i.startswith("fdb://")]
        dropped += len(inputs) - len(kept)
        data["input"] = kept
        record.write_text(json.dumps(data))
    return dropped


def _drop_fdb_inputs_db(snakemake_dir: Path) -> int:
    """``_drop_fdb_inputs`` for ``snakemake_metadata`` in the SQLite metadata db."""
    dropped = 0
    namespace = str(snakemake_dir.absolute())
    with sqlite3.connect(snakemake_dir / "metadata.db") as db:
        rows = db.execute(
            "SELECT namespace, target, input FROM snakemake_metadata"
        ).fetchall()
        for old_namespace, target, raw in rows:
            inputs = json.loads(raw) if raw else []
            kept = [i for i in inputs if not i.startswith("fdb://")]
            dropped += len(inputs) - len(kept)
            db.execute(
                "UPDATE snakemake_metadata SET namespace = ?, input = ? "
                "WHERE namespace = ? AND target = ?",
                (namespace, json.dumps(kept), old_namespace, target),
            )
    return dropped


def _recorded_targets(work: Path, backend: str) -> set[str]:
    """The keys the provenance records are stored under: the query text of a storage
    file (``PersistenceBase._get_key``), on either backend."""
    root = work / ".snakemake"
    if backend == "db":
        with sqlite3.connect(root / "metadata.db") as db:
            return {t for (t,) in db.execute("SELECT target FROM snakemake_metadata")}
    targets = set()
    for record in (root / "metadata").rglob("*"):
        if record.is_file():  # base64 of the key, split over directories at "@"
            parts = record.relative_to(root / "metadata").parts
            targets.add(urlsafe_b64decode("".join(p.lstrip("@") for p in parts)))
    return {t.decode() for t in targets}


def _workflow(tmp_path_factory, run_logged, name: str, snakefile: str, backend: str):
    """A seeded dev FDB and a workflow directory: the runs by name, and a runner.

    Every run uses ``backend`` as the provenance backend (FR-IFACE-005); ``db`` means
    ``--persistence-backend db`` with its default SQLite file under the workdir.
    """
    if not (SAMPLES / "template.grib").exists():
        pytest.skip("no ECMWF samples")
    tmp = tmp_path_factory.mktemp(f"{name}-{backend}")
    run = run_logged(tmp / "logs")
    config, work = tmp / ".fdb" / "config.yaml", tmp / "work"
    out: dict = {"config": config, "work": work, "backend": backend}
    init = [
        sys.executable,
        INIT_DEV_FDB,
        "--root",
        config.parent,
        "--seed",
        "--variants",
    ]
    out["init"] = run("init", init, tmp)
    out["init"].ok()
    work.mkdir()
    (work / "Snakefile").write_text(snakefile)

    def snakemake(name: str, *args: str, cwd: Path = work, **conf) -> None:
        """One run; ``--config`` last, it swallows what follows."""
        settings = [f"{k}={v}" for k, v in conf.items()]
        cmd = [sys.executable, "-m", "snakemake", "--storage-fdb-config", str(config)]
        cmd += ["--persistence-backend", backend]
        cmd += ["-c1", *args, *(["--config", *settings] if settings else [])]
        out[name] = run(name, cmd, cwd)

    return out, snakemake


@pytest.fixture(scope="module", params=BACKENDS)
def reruns(request, tmp_path_factory, run_logged) -> dict:
    """A full run, then the dry runs of the scenarios named below, per backend."""
    out, snakemake = _workflow(
        tmp_path_factory, run_logged, "rerun", SNAKEFILE, request.param
    )
    config, work = out["config"], out["work"]
    _archive(config, params=(166,), steps=(0, 6, 12))  # older than every output
    (work / "a.txt").write_text("a\n")
    (work / "b.txt").write_text("b\n")

    snakemake("run1", *FDB_TARGETS, *REFACTOR_TARGETS, "out/local.txt")
    out["run1"].ok()
    outputs_written = time.monotonic()

    legacy = work.parent / "legacy"
    shutil.copytree(work, legacy)
    out["dropped"] = _drop_fdb_inputs(legacy, out["backend"])
    snakemake("legacy", *FDB_TARGETS, "-n", cwd=legacy)

    snakemake("narrow", *FDB_TARGETS, "-n", param="167")
    snakemake(
        "narrow_query",
        *FDB_TARGETS,
        "-n",
        "--storage-fdb-input-tracking",
        "query",
        param="167",
    )
    snakemake("reorder", *FDB_TARGETS, "-n", param="165/167", step="12/6/0")
    snakemake("range", *FDB_TARGETS, "-n", step="0/to/12/by/6")
    snakemake("refactor", *REFACTOR_TARGETS, "-n", split_ms=1, split_sm=0)
    snakemake("local", "out/local.txt", "-n", local="a.txt")
    snakemake("widen", *FDB_TARGETS, "-n", param="167/165/166")

    # One-second index timestamps (L-3): the re-archive must follow the outputs.
    time.sleep(max(0.0, 1.2 - (time.monotonic() - outputs_written)))
    _archive(config, params=(167,), steps=(0,))
    snakemake("rearchived", *FDB_TARGETS, "-n")
    return out


@pytest.mark.parametrize("name", ["narrow", "reorder", "range", "refactor"])
def test_rerun_same_or_fewer_fields_is_up_to_date(reruns, name):
    """FR-RERUN-001: a query naming fewer fields (also for the output of a ``run:``
    rule, whose job ran in a spawned process), reordered values, a range for a list
    and one query split into per-param queries or merged back rerun nothing."""
    assert reruns[name].NOTHING_TO_BE_DONE in reruns[name].ok()


def test_rerun_widened_query_with_older_fields(reruns):
    """FR-RERUN-001: fields the recorded queries do not cover rerun the job, even when
    they are older than the output."""
    assert INPUT_CHANGED in reruns["widen"].ok()


def test_rerun_rearchived_field_uses_the_mtime_trigger(reruns):
    """A field of the same query archived after the output reruns by mtime alone."""
    log = reruns["rearchived"].ok()
    assert UPDATED_INPUT in log
    assert INPUT_CHANGED not in log


def test_rerun_local_input_set_still_triggers(reruns):
    """Only FDB inputs are decided by coverage: local inputs keep Snakemake's rule."""
    assert INPUT_CHANGED in reruns["local"].ok()


def test_rerun_input_tracking_query_restores_the_trigger(reruns):
    """FR-RERUN-002: the setting restores Snakemake's default behaviour."""
    assert INPUT_CHANGED in reruns["narrow_query"].ok()


def test_rerun_record_without_fdb_inputs(reruns):
    """A record written by 0.2.0 (FDB inputs hidden) reruns the job once."""
    assert reruns["dropped"] > 0
    assert INPUT_CHANGED in reruns["legacy"].ok()


@pytest.fixture(scope="module", params=BACKENDS)
def chain(request, tmp_path_factory, run_logged) -> dict:
    """A producer with one FDB output per parameter and a consumer of all of them:
    a full run, the run after a parameter is added, and a dry run after it; per
    backend, so that the metadata of FDB outputs is read back from both."""
    out, snakemake = _workflow(
        tmp_path_factory, run_logged, "chain", CHAIN, request.param
    )
    snakemake("first", params="167")
    out["first"].ok()
    out["recorded"] = _recorded_targets(out["work"], out["backend"])
    snakemake("added_plan", "-n", params="167/165")
    snakemake("added", params="167/165")
    snakemake("settled", "-n", params="167/165")
    snakemake("summary", "--summary", params="167/165")
    snakemake("cleanup_metadata", "--cleanup-metadata", PRODUCED, params="167/165")
    return out


def test_chain_added_parameter_is_planned(chain):
    """The playground scenario: a new parameter makes the consumer and the producer of
    its fields run, although the consumer's output exists."""
    log = chain["added_plan"].ok()
    assert INPUT_CHANGED in log
    assert "produce" in log and "consume" in log


def test_chain_added_parameter_is_archived(chain):
    """The new fields are archived and the consumer sees them; the next run is idle."""
    assert "Storing in storage" in chain["added"].ok()
    assert chain["settled"].NOTHING_TO_BE_DONE in chain["settled"].ok()


def test_chain_metadata_of_fdb_outputs(chain):
    """FR-IFACE-005: on both backends the record of an FDB output is keyed by its
    query text and read back by ``--summary`` as up to date."""
    assert PRODUCED in chain["recorded"]
    summary = [line.split("\t") for line in chain["summary"].ok().splitlines()]
    rows = {line[0]: line for line in summary if len(line) == 6}
    assert rows[PRODUCED][2] == "produce"
    assert rows[PRODUCED][4:] == ["ok", "no update"]


def test_chain_cleanup_metadata_of_an_fdb_output(chain):
    """L-25 holds for both backends: Snakemake path-normalises the ``fdb://`` argument
    of ``--cleanup-metadata`` to ``fdb:/``, so no record is found."""
    log = chain["cleanup_metadata"].log
    assert chain["cleanup_metadata"].returncode != 0
    assert CLEANUP_FAILED in log
    assert PRODUCED.replace("fdb://", "fdb:/") in log
    kept = _recorded_targets(chain["work"], chain["backend"])
    assert PRODUCED in kept  # the record itself is untouched
