"""Direct FDB access from ``run:`` and ``script:`` rules (requirements.md §2.15).

Unit tests of ``snakemake_storage_plugin_fdb.api`` against temporary FDBs, of the
archive marker and of the environment export, plus end-to-end workflows whose rules
read from and write to FDB without any local GRIB file.
"""

import io
import os
import sys
from pathlib import Path

import pytest
import yaml
from snakemake_interface_common.exceptions import WorkflowError

from snakemake_storage_plugin_fdb import api
from snakemake_storage_plugin_fdb.backend import Backend
from snakemake_storage_plugin_fdb.grib import (
    GribError,
    split_messages,
    stream_messages,
    variant,
)
from snakemake_storage_plugin_fdb.query import parse

REPO = Path(__file__).resolve().parents[1]
INIT_DEV_FDB = REPO / "scripts" / "init_dev_fdb.py"
SAMPLES = REPO / "tests" / "data" / "grib" / "ecmwf"
TEMPLATE = SAMPLES / "template.grib"

EA = (
    "class=ea,expver=0001,stream=oper,date=20200101,time=0000,domain=g,type=an,"
    "levtype=sfc"
)
QUERY = f"fdb://{EA},step=0/6/12,param=167"
SHORT = f"fdb://{EA},step=0/6/12/18,param=167"  # step=18 is not in the seeded FDB
OUT = f"fdb://{EA.replace('expver=0001', 'expver=0031')},step=0/6/12,param=167"

pytestmark = pytest.mark.skipif(not TEMPLATE.exists(), reason="no ECMWF samples")


@pytest.fixture
def seeded_config(seeded_fdb) -> str:
    """The seeded FDB as an inline YAML configuration (what the API settings take)."""
    return yaml.safe_dump(seeded_fdb.config)


def _messages(expver: str = "0031", params=(167,), steps=(0, 6, 12)) -> list[bytes]:
    """Zeroed ``stream=oper`` variants of the template, one per step and param."""
    template = TEMPLATE.read_bytes()
    return [
        variant(template, stream="oper", expver=expver, step=step, paramId=param)
        for param in params
        for step in steps
    ]


def _fdb_steps(config, query: str = OUT) -> list[int]:
    """The steps FDB holds for ``query`` (a config path or mapping)."""
    fields = Backend(config).inspect(parse(query).to_request())
    return sorted(int(f.key["step"]) for f in fields)


# --- streaming GRIB messages ---------------------------------------------------------


def test_stream_messages_splits_like_split_messages(tmp_path):
    blob = b"".join(_messages())
    (tmp_path / "all.grib").write_bytes(blob)
    assert list(stream_messages(io.BytesIO(blob))) == [
        message.data for message in split_messages(tmp_path / "all.grib")
    ]


def test_stream_messages_allows_nul_padding():
    messages = _messages(steps=(0, 6))
    padded = b"\0" * 3 + messages[0] + b"\0" * 5 + messages[1] + b"\0" * 2
    assert list(stream_messages(io.BytesIO(padded))) == messages


def test_stream_messages_rejects_foreign_bytes():
    with pytest.raises(GribError, match="non-GRIB bytes"):
        list(stream_messages(io.BytesIO(b"hello" + _messages()[0])))


def test_stream_messages_rejects_a_truncated_message():
    with pytest.raises(GribError, match="truncated GRIB message"):
        list(stream_messages(io.BytesIO(_messages()[0][:-20])))


def test_stream_messages_of_an_empty_stream():
    assert list(stream_messages(io.BytesIO(b""))) == []


# --- reading (FR-DIRECT-001) ---------------------------------------------------------


def test_request_expands_the_query(seeded_config, clean_env):
    request = api.request(QUERY, config=seeded_config)
    assert request["step"] == ["0", "6", "12"]
    assert request["param"] == ["167"]
    assert request["class"] == ["ea"]


def test_messages_equal_a_retrieval(seeded_config, clean_env, tmp_path):
    api.retrieve(QUERY, tmp_path / "all.grib", config=seeded_config)
    retrieved = [m.data for m in split_messages(tmp_path / "all.grib")]
    assert list(api.messages(QUERY, config=seeded_config)) == retrieved
    assert len(retrieved) == 3


def test_open_streams_the_same_bytes(seeded_config, clean_env):
    with api.open(QUERY, config=seeded_config) as stream:
        data = stream.read()
    assert data == b"".join(api.messages(QUERY, config=seeded_config))


def test_messages_reports_missing_fields(seeded_config, clean_env):
    with pytest.raises(WorkflowError, match="3 of 4 fields found in FDB"):
        list(api.messages(SHORT, config=seeded_config))


def test_messages_of_an_unknown_query_reports_the_error(seeded_config, clean_env):
    with pytest.raises(WorkflowError, match="0 of 3 fields found in FDB"):
        list(api.messages(OUT, config=seeded_config))


def test_retrieve_writes_the_file(seeded_config, clean_env, tmp_path):
    path = api.retrieve(QUERY, tmp_path / "sub" / "all.grib", config=seeded_config)
    assert path.is_file()
    assert len(split_messages(path)) == 3


def test_earthkit_from_source(seeded_config, clean_env):
    pytest.importorskip("earthkit.data", reason="earthkit-data is optional")
    data = api.earthkit(QUERY, config=seeded_config)
    assert len(data) == 3


# --- the local path of an output ------------------------------------------------------


def test_query_of_a_local_path():
    local = (
        ".snakemake/storage/fdb/class=ea/expver=0031/stream=oper/date=20200101/"
        "time=0000/domain=g/type=an/levtype=sfc/step=0+6+12/param=167.grib"
    )
    assert api.query_of(local) == OUT
    parsed = parse(OUT)  # generic key order: the inverse gives the normalised query
    assert api.query_of(parsed.local_suffix()) == parsed.to_query()


def test_query_of_a_hashed_path():
    with pytest.raises(WorkflowError, match="hashed component"):
        api.query_of("fdb/class=ea/param=~0123456789abcdef.grib")


def test_query_of_a_foreign_path():
    with pytest.raises(WorkflowError, match="not an FDB storage path"):
        api.query_of("results/mean.txt")


# --- the archive marker (FR-DIRECT-002) -----------------------------------------------


def test_marker_round_trip(tmp_path):
    marker = api.Marker(OUT, 3, 1700000000)
    path = marker.write(tmp_path / "out" / "param=167.grib")
    assert path.read_text().startswith(api.MARKER_HEADER)
    assert api.read_marker(path) == marker


def test_read_marker_of_a_grib_file(tmp_path):
    (tmp_path / "a.grib").write_bytes(_messages()[0])
    assert api.read_marker(tmp_path / "a.grib") is None
    assert api.read_marker(tmp_path / "missing.grib") is None


def test_read_marker_malformed(tmp_path):
    (tmp_path / "m").write_text(f"{api.MARKER_HEADER}\nquery: {OUT}\n")
    with pytest.raises(WorkflowError, match="malformed FDB archive marker"):
        api.read_marker(tmp_path / "m")


# --- writing (FR-DIRECT-002) ----------------------------------------------------------


@pytest.fixture
def write_config(temp_fdb_config) -> str:
    return yaml.safe_dump(temp_fdb_config)


def test_archive_puts_the_fields_in_fdb_and_writes_a_marker(
    write_config, temp_fdb_config, clean_env, tmp_path
):
    path = tmp_path / "out" / "param=167.grib"
    marker = api.archive(path, _messages(), query=OUT, config=write_config)
    assert marker == api.Marker(OUT, 3, marker.time)
    assert api.read_marker(path) == marker
    assert _fdb_steps(temp_fdb_config) == [0, 6, 12]


def test_archive_accepts_bytes_and_iterators(write_config, clean_env, tmp_path):
    blob = b"".join(_messages())
    marker = api.archive(tmp_path / "a.grib", blob, query=OUT, config=write_config)
    assert marker.fields == 3
    second = api.archive(
        tmp_path / "b.grib", iter(_messages()), query=OUT, config=write_config
    )
    assert second.fields == 3


def test_archive_uses_the_query_of_the_output_path(write_config, clean_env, tmp_path):
    local = tmp_path.joinpath(
        "class=ea/expver=0031/stream=oper/date=20200101/time=0000/domain=g/type=an/"
        "levtype=sfc/step=0+6+12/param=167.grib"
    )
    assert api.archive(local, _messages(), config=write_config).query == OUT


def test_archive_rejects_a_wrong_field_count(
    write_config, temp_fdb_config, clean_env, tmp_path
):
    path = tmp_path / "a.grib"
    with pytest.raises(WorkflowError, match="has 2 fields.*nothing was archived"):
        api.archive(path, _messages(steps=(0, 6)), query=OUT, config=write_config)
    assert not path.exists()
    assert Backend(temp_fdb_config).list({"class": "ea", "expver": "0031"}) == []


def test_archive_rejects_a_message_that_contradicts_the_query(
    write_config, temp_fdb_config, clean_env, tmp_path
):
    messages = _messages(steps=(0, 6))
    messages.append(
        variant(
            TEMPLATE.read_bytes(), stream="oper", expver="0031", step=18, paramId=167
        )
    )
    path = tmp_path / "a.grib"
    with pytest.raises(WorkflowError, match="has step=18.*nothing was archived"):
        api.archive(path, messages, query=OUT, config=write_config)
    assert not path.exists()
    assert Backend(temp_fdb_config).list({"class": "ea", "expver": "0031"}) == []


def test_archive_rejects_duplicates(write_config, clean_env, tmp_path):
    messages = _messages(steps=(0, 6))
    messages.append(messages[0])
    with pytest.raises(WorkflowError, match="duplicate fields"):
        api.archive(tmp_path / "a.grib", messages, query=OUT, config=write_config)


def test_archive_takes_the_archive_mode_from_the_environment(
    write_config, temp_fdb_config, clean_env, tmp_path
):
    """The workflow's setting, which Snakemake exports into jobs (FR-DIRECT-003):
    a query key the GRIB lacks is rejected natively and labelled in identifier mode."""
    query = OUT.replace("param=167", "quantile=1:10,param=167")
    with pytest.raises(WorkflowError, match="lacks quantile"):
        api.archive(tmp_path / "a.grib", _messages(), query=query, config=write_config)
    clean_env.setenv("SNAKEMAKE_STORAGE_FDB_ARCHIVE_MODE", "identifier")
    api.archive(tmp_path / "a.grib", _messages(), query=query, config=write_config)
    assert _fdb_steps(temp_fdb_config, query) == [0, 6, 12]


# --- store_object and the marker ------------------------------------------------------


def test_store_object_accepts_the_marker(make_provider, write_config):
    obj = make_provider(config=write_config).object(OUT)
    api.archive(obj.local_path(), _messages(), query=OUT, config=write_config)
    obj.store_object()  # post-check only: the fields are in FDB
    assert obj.exists()


def test_store_object_marker_with_a_wrong_count(make_provider, write_config):
    obj = make_provider(config=write_config).object(OUT)
    api.Marker(OUT, 2, 1).write(obj.local_path())
    with pytest.raises(WorkflowError, match="archived 2 fields directly"):
        obj.store_object()


def test_store_object_marker_of_another_query(make_provider, write_config):
    obj = make_provider(config=write_config).object(OUT)
    api.Marker(OUT.replace("expver=0031", "expver=0032"), 3, 1).write(obj.local_path())
    with pytest.raises(WorkflowError, match="archive marker for .*expver=0032"):
        obj.store_object()


def test_store_object_marker_without_the_fields(make_provider, write_config):
    """The post-check still runs: a marker whose fields are not in FDB fails."""
    obj = make_provider(config=write_config).object(OUT)
    api.Marker(OUT, 3, 1).write(obj.local_path())
    with pytest.raises(WorkflowError, match="landed outside the query"):
        obj.store_object()


# --- the environment export (FR-DIRECT-003) -------------------------------------------


def test_provider_exports_the_configuration_file(
    make_provider, fdb_config_file, tmp_path
):
    config = fdb_config_file(tmp_path / "fdb1")
    make_provider(config=str(config))
    assert os.environ["FDB_CONFIG_FILE"] == str(config.absolute())
    assert "FDB_CONFIG" not in os.environ


def test_provider_exports_inline_configuration(make_provider, write_config):
    make_provider(config=write_config)
    assert os.environ["FDB_CONFIG"] == write_config


def test_provider_keeps_a_configuration_in_the_environment(
    make_provider, clean_env, fdb_config_file, tmp_path
):
    clean_env.setenv("FDB_CONFIG_FILE", "/elsewhere/config.yaml")
    make_provider(config=str(fdb_config_file(tmp_path / "fdb1")))
    assert os.environ["FDB_CONFIG_FILE"] == "/elsewhere/config.yaml"


def test_providers_with_different_configurations_export_nothing(
    make_provider, fdb_config_file, tmp_path
):
    first = fdb_config_file(tmp_path / "fdb1")
    make_provider(config=str(first))
    assert os.environ["FDB_CONFIG_FILE"] == str(first.absolute())
    make_provider(config=str(fdb_config_file(tmp_path / "fdb2")))
    assert "FDB_CONFIG_FILE" not in os.environ


def test_conflicting_providers_leave_a_user_configuration(
    make_provider, clean_env, fdb_config_file, tmp_path
):
    """A configuration the user set is never removed, even when it equals the first
    provider's and a second provider disagrees (nothing was exported to retract)."""
    first = fdb_config_file(tmp_path / "fdb1")
    clean_env.setenv("FDB_CONFIG_FILE", str(first.absolute()))
    make_provider(config=str(first))
    make_provider(config=str(fdb_config_file(tmp_path / "fdb2")))
    assert os.environ["FDB_CONFIG_FILE"] == str(first.absolute())


def test_api_uses_the_exported_configuration(
    make_provider, fdb_config_file, tmp_path, seeded_fdb
):
    """With the configuration in the environment, the API needs no argument."""
    config = tmp_path / "exported.yaml"
    config.write_text(yaml.safe_dump(seeded_fdb.config))
    make_provider(config=str(config))
    assert len(list(api.messages(QUERY))) == 3


# --- end to end -----------------------------------------------------------------------

SNAKEFILE = '''\
"""Direct reads and direct archives: no GRIB file on the local filesystem."""

storage:
    provider="fdb"


BASE = (
    "fdb://class=ea,expver={expver},stream=oper,date=20200101,time=0000,domain=g,"
    "type=an,levtype=sfc,step=0/6/12,param=167"
)
IN = BASE.format(expver="0001")
OUT = BASE.format(expver="0041")
QUANTILE = BASE.format(expver="0043").replace("param=", "quantile=1:10,param=")


rule all:
    input:
        "steps.txt",
        "mean.txt",
        "mixed.txt",
        storage.fdb(OUT, retrieve=False),


rule steps:
    """A run: rule reading the fields straight from FDB."""
    input:
        storage.fdb(IN, retrieve=False),
    output:
        "steps.txt",
    run:
        import eccodes
        from snakemake_storage_plugin_fdb import api

        with open(output[0], "w") as f:
            for message in api.messages(input[0]):
                handle = eccodes.codes_new_from_message(message)
                print(eccodes.codes_get_string(handle, "step"), file=f)
                eccodes.codes_release(handle)


rule shift_expver:
    """A run: rule archiving straight into FDB."""
    input:
        storage.fdb(IN, retrieve=False),
    output:
        storage.fdb(OUT),
    run:
        import eccodes
        from snakemake_storage_plugin_fdb import api

        def shifted():
            for message in api.messages(input[0]):
                handle = eccodes.codes_new_from_message(message)
                eccodes.codes_set(handle, "expver", "0041")
                yield eccodes.codes_get_message(handle)
                eccodes.codes_release(handle)

        api.archive(output[0], shifted())


rule quantile:
    """Run with --storage-fdb-archive-mode identifier: the output names a key the
    GRIB lacks, which only identifier mode can label. The job inherits the setting."""
    input:
        storage.fdb(IN, retrieve=False),
    output:
        storage.fdb(QUANTILE),
    run:
        import eccodes
        from snakemake_storage_plugin_fdb import api

        def shifted():
            for message in api.messages(input[0]):
                handle = eccodes.codes_new_from_message(message)
                eccodes.codes_set(handle, "expver", "0043")
                yield eccodes.codes_get_message(handle)
                eccodes.codes_release(handle)

        api.archive(output[0], shifted())


rule mean:
    """A script: rule using plain pyfdb with the exported configuration."""
    input:
        storage.fdb(IN, retrieve=False),
    output:
        "mean.txt",
    script:
        "scripts/mean.py"


rule mixed:
    """One retrieved input for a shell command, one direct input."""
    input:
        retrieved=storage.fdb(IN),
        direct=storage.fdb(IN, retrieve=False),
    output:
        "mixed.txt",
    run:
        from pathlib import Path
        from snakemake_storage_plugin_fdb import api

        local = Path(input.retrieved).stat().st_size
        direct = sum(len(m) for m in api.messages(input.direct))
        Path(output[0]).write_text(f"{local} {direct}\\n")
'''

SCRIPT = '''\
"""The mean of every field, read with pyfdb configured by the environment."""

import eccodes
import numpy as np
import pyfdb

from snakemake_storage_plugin_fdb import api
from snakemake_storage_plugin_fdb.grib import stream_messages

request = api.request(snakemake.input[0])
with pyfdb.FDB().retrieve(request) as data, open(snakemake.output[0], "w") as f:
    for message in stream_messages(data):
        handle = eccodes.codes_new_from_message(message)
        print(f"{np.mean(eccodes.codes_get_values(handle)):.3f}", file=f)
        eccodes.codes_release(handle)
'''

OUT_QUERY = (
    "fdb://class=ea,expver=0041,stream=oper,date=20200101,time=0000,domain=g,"
    "type=an,levtype=sfc,step=0/6/12,param=167"
)
OUT_LOCAL = (
    ".snakemake/storage/fdb/class=ea/expver=0041/stream=oper/date=20200101/time=0000/"
    "domain=g/type=an/levtype=sfc/step=0+6+12/param=167.grib"
)
QUANTILE_QUERY = OUT_QUERY.replace("expver=0041", "expver=0043").replace(
    "param=", "quantile=1:10,param="
)


def _files(work: Path) -> list[Path]:
    storage = work / ".snakemake" / "storage"
    if not storage.is_dir():
        return []
    return sorted(p for p in storage.rglob("*") if p.is_file())


@pytest.fixture(scope="module")
def direct(tmp_path_factory, run_logged) -> dict:
    """The direct-access workflow: first run, second run, a re-archived input, a run
    keeping the local copies and ``--delete-all-output``."""
    tmp = tmp_path_factory.mktemp("direct")
    run = run_logged(tmp / "logs")
    config = tmp / ".fdb" / "config.yaml"
    work = tmp / "work"
    (work / "scripts").mkdir(parents=True)
    (work / "Snakefile").write_text(SNAKEFILE)
    (work / "scripts" / "mean.py").write_text(SCRIPT)
    out: dict = {"tmp": tmp, "work": work, "config": config}

    init = [sys.executable, INIT_DEV_FDB, "--root", tmp / ".fdb", "--variants"]
    out["init"] = run("init", init, tmp)
    out["init"].ok()

    def snakemake(name: str, *args: str) -> None:
        cmd = [
            sys.executable, "-m", "snakemake",
            "--storage-fdb-config", str(config), "-c1", *args,
        ]  # fmt: skip
        out[name] = run(name, cmd, work)

    snakemake("run1")
    out["files_after_run1"] = _files(work)
    out["texts"] = {
        name: (work / name).read_text()
        for name in ("steps.txt", "mean.txt", "mixed.txt")
        if (work / name).is_file()
    }
    snakemake("run2")
    # The target first: the setting takes several (tagged) values.
    snakemake("identifier", "quantile", "--storage-fdb-archive-mode", "identifier")

    # A re-archived input field must make the direct rules rerun (mtime trigger).
    backend = Backend(config)
    backend.archive(variant(TEMPLATE.read_bytes(), stream="oper", step=0, paramId=167))
    backend.flush()
    snakemake("rerun", "--dry-run")
    snakemake("keep", "--keep-storage-local-copies")
    out["kept"] = _files(work)
    snakemake("delete", "--delete-all-output")
    out["after_delete"] = _files(work)
    return out


def test_direct_workflow_runs(direct):
    log = direct["run1"].ok()
    assert f"Storing in storage: {OUT_QUERY}" in log
    texts = direct["texts"]
    assert texts["steps.txt"].split() == ["0", "6", "12"]
    assert len(texts["mean.txt"].splitlines()) == 3
    local, streamed = texts["mixed.txt"].split()
    assert local == streamed  # the retrieved file and the streamed messages agree


def test_direct_workflow_archives_into_fdb(direct):
    direct["run1"].ok()
    assert _fdb_steps(direct["config"], OUT_QUERY) == [0, 6, 12]


def test_direct_workflow_writes_no_data_file(direct):
    """Nothing but the marker is ever written under .snakemake/storage (NFR-PERF-005).

    The first run keeps no local copies at all; the run with
    ``--keep-storage-local-copies`` keeps the marker of the FDB output and the one
    retrieved input of the mixed rule.
    """
    direct["run1"].ok()
    assert direct["files_after_run1"] == []
    kept = {p.relative_to(direct["work"]).as_posix() for p in direct["kept"]}
    assert OUT_LOCAL in kept
    marker = api.read_marker(direct["work"] / OUT_LOCAL)
    assert marker is not None
    assert (marker.query, marker.fields) == (OUT_QUERY, 3)
    retrieved = kept - {OUT_LOCAL}
    assert all("expver=0001" in name for name in retrieved)  # the mixed rule's input


def test_direct_workflow_inherits_the_archive_mode(direct):
    """FR-DIRECT-003: a run: job's api.archive follows --storage-fdb-archive-mode."""
    log = direct["identifier"].ok()
    assert f"Storing in storage: {QUANTILE_QUERY}" in log
    assert _fdb_steps(direct["config"], QUANTILE_QUERY) == [0, 6, 12]


def test_direct_workflow_second_run_is_idle(direct):
    run2 = direct["run2"]
    assert run2.NOTHING_TO_BE_DONE in run2.ok()


def test_direct_workflow_rearchived_input_reruns(direct):
    log = direct["rerun"].ok()
    assert direct["rerun"].NOTHING_TO_BE_DONE not in log
    assert "shift_expver" in log


def test_direct_workflow_delete_all_output(direct):
    log = direct["delete"].ok()
    assert "FDB cannot delete individual fields" in log
    assert not (direct["work"] / "steps.txt").exists()
    # FDB fields are never deleted (FR-REMOVE-001); kept local copies, marker included,
    # are Snakemake's business and stay where --keep-storage-local-copies put them.
    assert direct["after_delete"] == direct["kept"]
    assert _fdb_steps(direct["config"], OUT_QUERY) == [0, 6, 12]
