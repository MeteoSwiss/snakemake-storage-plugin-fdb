"""Direct FDB access from ``run:`` and ``script:`` rules (requirements.md §2.15).

Unit tests of ``snakemake_storage_plugin_fdb.api`` against temporary FDBs, of the
archive marker (FR-DIRECT-002), of the empty-output convention (FR-DIRECT-004) and of
the environment export, plus an end-to-end workflow whose jobs use plain pyfdb and
eccodes and write no GRIB file anywhere.
"""

import importlib.util
import io
import os
import re
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


def test_query_is_the_inverse_of_request(seeded_config, clean_env):
    """FR-DIRECT-001: a Snakefile keeps the request as a dict and derives the query."""
    assert (
        api.query(api.request(QUERY, config=seeded_config)) == parse(QUERY).to_query()
    )


def test_query_joins_values_and_keeps_wildcards():
    request = {
        "class": "ea",
        "expver": "0001",
        "stream": "oper",
        "date": "{date}",
        "time": "0000",
        "domain": "g",
        "type": "an",
        "levtype": "sfc",
        "step": [0, 6, 12],
        "param": 167,
    }
    built = api.query(request)  # the generic key order, as the parser gives it
    assert built == parse(QUERY.replace("date=20200101", "date={date}")).to_query()
    assert parse(built).items("step") == ["0", "6", "12"]


def test_query_rejects_an_invalid_request():
    with pytest.raises(WorkflowError, match="invalid FDB request"):
        api.query({"class": "ea", "step": "0,6"})  # a comma separates keys, not values
    with pytest.raises(WorkflowError, match="empty value for key 'step'"):
        api.query({"class": "ea", "step": []})


def _request_of(query: str) -> dict[str, str]:
    """The user guide's two-line parse: a job's MARS request from the query string it
    holds as its input, ``/`` lists left as they are (FR-DIRECT-001)."""
    body = query.removeprefix("fdb://")
    return dict(item.split("=", 1) for item in body.split(","))


def test_a_job_reads_with_plain_pyfdb_from_its_input(seeded_config, clean_env):
    """pyfdb takes the parsed query as a MARS request, lists and ranges as strings, and
    returns what api.messages gives (FR-DIRECT-001, FR-DIRECT-003)."""
    import pyfdb

    request = _request_of(QUERY)
    assert request["step"] == "0/6/12"
    fdb = pyfdb.FDB(yaml.safe_load(seeded_config))
    with fdb.retrieve(request) as source:
        retrieved = list(stream_messages(io.BytesIO(source.read())))
    assert list(api.messages(QUERY, config=seeded_config)) == retrieved
    assert len(retrieved) == 3
    with fdb.retrieve({**request, "step": "0/to/12/by/6"}) as source:
        assert list(stream_messages(io.BytesIO(source.read()))) == retrieved


def test_earthkit_reads_the_exported_configuration(make_provider, tmp_path, seeded_fdb):
    """FR-DIRECT-003: earthkit-data's ``fdb`` source reads ``FDB5_CONFIG``, which the
    provider exports (architecture.md §13.13), so ``from_source("fdb", request)`` needs
    no argument in a job; it takes the parsed query with its ``/`` lists as well."""
    from_source = pytest.importorskip(
        "earthkit.data", reason="earthkit-data is optional"
    ).from_source
    config = tmp_path / "exported.yaml"
    config.write_text(yaml.safe_dump(seeded_fdb.config))
    make_provider(config=str(config))
    fields = from_source("fdb", _request_of(QUERY)).to_fieldlist()
    assert sorted(f.metadata("step") for f in fields) == [0, 6, 12]


def test_messages_reports_missing_fields(seeded_config, clean_env):
    with pytest.raises(WorkflowError, match="3 of 4 fields found in FDB"):
        list(api.messages(SHORT, config=seeded_config))


def test_messages_of_an_unknown_query_reports_the_error(seeded_config, clean_env):
    with pytest.raises(WorkflowError, match="0 of 3 fields found in FDB"):
        list(api.messages(OUT, config=seeded_config))


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


# --- store_object and the empty output (FR-DIRECT-004) --------------------------------


def _local_file(obj, data: bytes = b"") -> Path:
    """The local path of ``obj`` holding ``data``; empty, as ``touch()`` leaves it."""
    local = obj.local_path()
    local.parent.mkdir(parents=True, exist_ok=True)
    local.write_bytes(data)
    return local


def _archive(backend: Backend, messages: list[bytes]) -> None:
    """Archive and flush ``messages``, as a job's plain ``pyfdb.FDB()`` would."""
    for message in messages:
        backend.archive(message)
    backend.flush()


def test_store_object_accepts_an_empty_output(make_provider, write_config, empty_fdb):
    """FR-DIRECT-004: an empty file means the job archived the fields itself."""
    obj = make_provider(config=write_config).object(OUT)
    _archive(empty_fdb(), _messages())
    _local_file(obj)
    obj.store_object()  # post-check only
    assert obj.exists()


@pytest.mark.parametrize("steps, found", [((), 0), ((0, 6), 2)])
def test_store_object_empty_output_with_missing_fields(
    make_provider, write_config, empty_fdb, steps, found
):
    obj = make_provider(config=write_config).object(OUT)
    _archive(empty_fdb(), _messages(steps=steps))
    local = _local_file(obj)
    with pytest.raises(WorkflowError) as error:
        obj.store_object()
    text = str(error.value)
    assert f"{local} is empty, so the job is taken to have archived" in text
    assert f"{found} of 3 found in FDB with timestamps from this run" in text
    assert "missing or older: " in text and "step=12" in text
    assert "from before this run" not in text


def test_store_object_empty_output_with_stale_fields(
    make_provider, write_config, empty_fdb
):
    """Fields older than the reference time of this run do not count (L-31)."""
    provider = make_provider(config=write_config)
    obj = provider.object(OUT)
    _archive(empty_fdb(), _messages())
    provider.run_time += 3600  # as if the run had started later
    _local_file(obj)
    with pytest.raises(WorkflowError) as error:
        obj.store_object()
    assert "0 of 3 found in FDB with timestamps from this run" in str(error.value)
    assert "3 of the query's fields are from before this run" in str(error.value)


def test_store_object_of_a_non_empty_non_grib_file(make_provider, write_config):
    """Only an empty file is the convention; anything else is GRIB or an error."""
    obj = make_provider(config=write_config).object(OUT)
    _local_file(obj, b"not grib at all\n")
    with pytest.raises(WorkflowError, match="is not GRIB"):
        obj.store_object()


# --- the environment export (FR-DIRECT-003) -------------------------------------------


def test_provider_exports_the_configuration_file(
    make_provider, fdb_config_file, tmp_path
):
    """A file goes out as text in FDB5_CONFIG (what pyfdb reads first and earthkit
    reads at all, architecture.md §13.7, §13.13) and as its path in FDB_CONFIG_FILE."""
    config = fdb_config_file(tmp_path / "fdb1")
    make_provider(config=str(config))
    assert os.environ["FDB_CONFIG_FILE"] == str(config.absolute())
    assert yaml.safe_load(os.environ["FDB5_CONFIG"]) == yaml.safe_load(
        config.read_text()
    )
    assert "FDB_CONFIG" not in os.environ


def test_provider_exports_relative_config_paths_as_absolute(
    make_provider, clean_env, tmp_path
):
    """fdb5 resolves a configuration's relative paths against the working directory
    (§13.7); the exported text carries them absolute, so a job elsewhere agrees."""
    clean_env.chdir(tmp_path)
    config = tmp_path / "config.yaml"
    config.write_text(
        "type: local\nengine: toc\nschema: ~fdb/etc/fdb/schema\n"
        "spaces:\n- handler: Default\n  roots:\n  - path: db\n"
    )
    make_provider(config="config.yaml")
    exported = yaml.safe_load(os.environ["FDB5_CONFIG"])
    assert exported["spaces"][0]["roots"][0]["path"] == str(tmp_path / "db")
    assert exported["schema"] == "~fdb/etc/fdb/schema"  # expanded by fdb5 itself
    assert os.environ["FDB_CONFIG_FILE"] == str(config)


def test_provider_exports_inline_configuration(make_provider, write_config):
    make_provider(config=write_config)
    assert os.environ["FDB5_CONFIG"] == write_config
    assert "FDB_CONFIG_FILE" not in os.environ


def test_provider_keeps_a_configuration_in_the_environment(
    make_provider, clean_env, fdb_config_file, tmp_path
):
    clean_env.setenv("FDB_CONFIG_FILE", "/elsewhere/config.yaml")
    make_provider(config=str(fdb_config_file(tmp_path / "fdb1")))
    assert os.environ["FDB_CONFIG_FILE"] == "/elsewhere/config.yaml"
    assert "FDB5_CONFIG" not in os.environ


def test_providers_with_different_configurations_export_nothing(
    make_provider, fdb_config_file, tmp_path
):
    first = fdb_config_file(tmp_path / "fdb1")
    make_provider(config=str(first))
    assert os.environ["FDB_CONFIG_FILE"] == str(first.absolute())
    make_provider(config=str(fdb_config_file(tmp_path / "fdb2")))
    assert "FDB_CONFIG_FILE" not in os.environ
    assert "FDB5_CONFIG" not in os.environ


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
"""Plugin-free jobs: plain pyfdb and eccodes in the bodies, no GRIB file anywhere.

Inputs are declared ``retrieve=False``, so a job holds the query string and parses its
MARS request from it (FR-DIRECT-001); outputs a job archives itself are declared
``touch(storage.fdb(...))``: the empty file tells the store step to check instead of
archive (FR-DIRECT-004). Only the ``api.archive`` rules touch the plugin.
"""

import os
import sys

print("SNAKEFILE PID", os.getpid(), file=sys.stderr)

storage:
    provider="fdb"


BASE = (
    "fdb://class=ea,expver={expver},stream=oper,date=20200101,time=0000,domain=g,"
    "type=an,levtype=sfc,step=0/6/12,param=167"
)
IN = BASE.format(expver="0001")
MARKER = BASE.format(expver="0041")           # archived with api.archive (a marker)
PLAIN = BASE.format(expver="0042")            # archived with plain pyfdb (touch())
QUANTILE = BASE.format(expver="0043").replace("param=", "quantile=1:10,param=")
BAD = BASE.format(expver="0046")


def request_of(query):
    """A job's MARS request from the query string it holds as input."""
    body = query.removeprefix("fdb://")
    return dict(item.split("=", 1) for item in body.split(","))


def fields_of(query):
    """The GRIB messages of a job's input query, with plain pyfdb and eccodes."""
    import eccodes
    import pyfdb

    with pyfdb.FDB().retrieve(request_of(query)) as source:
        data = source.read()
    return list(eccodes.MemoryReader(data))


def relabelled(query, expver, steps=None):
    """The fields of ``query`` with another ``expver``, as bytes (some steps only)."""
    for message in fields_of(query):
        if steps is None or message.get("step") in steps:
            message.set("expver", expver)
            yield message.get_buffer()


def archive_plain(messages):
    """Archive with plain pyfdb, flushed before the job ends (the store looks up)."""
    import pyfdb

    print("JOB PID", os.getpid(), file=sys.stderr)
    fdb = pyfdb.FDB()
    for message in messages:
        fdb.archive(message)
    fdb.flush()


rule all:
    input:
        "steps.txt",
        "mean.txt",
        "mixed.txt",
        storage.fdb(MARKER, retrieve=False),
        storage.fdb(PLAIN, retrieve=False),


rule steps:
    """A run: rule reading with plain pyfdb from the query string it holds."""
    input:
        storage.fdb(IN, retrieve=False),
    output:
        "steps.txt",
    run:
        with open(output[0], "w") as f:
            for message in fields_of(input[0]):
                print(message.get("step"), file=f)


rule plain_shift:
    """A run: rule archiving with plain pyfdb; the empty output says so."""
    input:
        storage.fdb(IN, retrieve=False),
    output:
        touch(storage.fdb(PLAIN)),
    run:
        archive_plain(relabelled(input[0], "0042"))


rule bad_count:
    """A job that archives two of the three fields: the store step must say so."""
    input:
        storage.fdb(IN, retrieve=False),
    output:
        touch(storage.fdb(BAD)),
    run:
        archive_plain(relabelled(input[0], "0046", steps={0, 6}))


rule marker_shift:
    """The optional api.archive: pre-checks, then a marker instead of an empty file."""
    input:
        storage.fdb(IN, retrieve=False),
    output:
        storage.fdb(MARKER),
    run:
        from snakemake_storage_plugin_fdb import api

        api.archive(output[0], relabelled(input[0], "0041"))


rule quantile:
    """Run with --storage-fdb-archive-mode identifier: the output names a key the
    GRIB lacks, which only identifier mode can label. The job inherits the setting."""
    input:
        storage.fdb(IN, retrieve=False),
    output:
        storage.fdb(QUANTILE),
    run:
        from snakemake_storage_plugin_fdb import api

        api.archive(output[0], relabelled(input[0], "0043"))


rule mean:
    """A script: rule using earthkit-data where installed, plain pyfdb otherwise."""
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

        local = Path(input.retrieved).stat().st_size
        direct = sum(len(m.get_buffer()) for m in fields_of(input.direct))
        Path(output[0]).write_text(f"{local} {direct}\\n")
'''

MEAN_SCRIPT = '''\
"""The mean of every field of the job's input query, read straight from FDB.

Nothing configures the libraries here: the provider exported the configuration
(FDB5_CONFIG, which both pyfdb and earthkit-data's fdb source read).
"""

import sys

query = snakemake.input[0]
request = dict(item.split("=", 1) for item in query.removeprefix("fdb://").split(","))

try:
    from earthkit.data import from_source
except ImportError:
    import eccodes
    import pyfdb

    with pyfdb.FDB().retrieve(request) as source:
        data = source.read()
    means = [m.get_array("values").mean() for m in eccodes.MemoryReader(data)]
    print("READER: pyfdb", file=sys.stderr)
else:
    fields = from_source("fdb", request).to_fieldlist()
    means = [field.to_numpy().mean() for field in fields]
    print("READER: earthkit", file=sys.stderr)

with open(snakemake.output[0], "w") as f:
    for mean in means:
        print(f"{mean:.3f}", file=f)
'''

BASE_QUERY = (
    "fdb://class=ea,expver={expver},stream=oper,date=20200101,time=0000,domain=g,"
    "type=an,levtype=sfc,step=0/6/12,param=167"
)
MARKER_QUERY = BASE_QUERY.format(expver="0041")
PLAIN_QUERY = BASE_QUERY.format(expver="0042")
QUANTILE_QUERY = BASE_QUERY.format(expver="0043").replace(
    "param=", "quantile=1:10,param="
)
LOCAL = (
    ".snakemake/storage/fdb/class=ea/expver={expver}/stream=oper/date=20200101/"
    "time=0000/domain=g/type=an/levtype=sfc/step=0+6+12/param=167.grib"
)
MARKER_LOCAL = LOCAL.format(expver="0041")
PLAIN_LOCAL = LOCAL.format(expver="0042")


def _files(work: Path) -> list[Path]:
    storage = work / ".snakemake" / "storage"
    if not storage.is_dir():
        return []
    return sorted(p for p in storage.rglob("*") if p.is_file())


@pytest.fixture(scope="module")
def direct(tmp_path_factory, run_logged) -> dict:
    """The plugin-free workflow: first run (verbose, for the process check), second
    run, a job that archives too few fields, a re-archived input, a run keeping the
    local copies and ``--delete-all-output``."""
    tmp = tmp_path_factory.mktemp("direct")
    run = run_logged(tmp / "logs")
    config = tmp / ".fdb" / "config.yaml"
    work = tmp / "work"
    (work / "scripts").mkdir(parents=True)
    (work / "Snakefile").write_text(SNAKEFILE)
    (work / "scripts" / "mean.py").write_text(MEAN_SCRIPT)
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

    snakemake("run1", "--verbose")  # the store step's debug line names its process
    out["files_after_run1"] = _files(work)
    out["texts"] = {
        name: (work / name).read_text()
        for name in ("steps.txt", "mean.txt", "mixed.txt")
        if (work / name).is_file()
    }
    snakemake("run2")
    # The target first: the setting takes several (tagged) values.
    snakemake("identifier", "quantile", "--storage-fdb-archive-mode", "identifier")
    snakemake("bad", "bad_count")

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
    for query in (MARKER_QUERY, PLAIN_QUERY):
        assert f"Storing in storage: {query}" in log
    texts = direct["texts"]
    assert texts["steps.txt"].split() == ["0", "6", "12"]
    assert len(texts["mean.txt"].splitlines()) == 3
    # The script read with earthkit-data where it is installed, through the exported
    # FDB5_CONFIG and with no argument (FR-DIRECT-003), with plain pyfdb otherwise.
    reader = "earthkit" if importlib.util.find_spec("earthkit.data") else "pyfdb"
    assert f"READER: {reader}" in log
    local, streamed = texts["mixed.txt"].split()
    assert local == streamed  # the retrieved file and the streamed messages agree


def test_direct_workflow_archives_into_fdb(direct):
    """The api.archive rule and the plain pyfdb rule both landed."""
    direct["run1"].ok()
    for query in (MARKER_QUERY, PLAIN_QUERY):
        assert _fdb_steps(direct["config"], query) == [0, 6, 12]


def test_direct_workflow_writes_no_data_file(direct):
    """Nothing but markers and empty files is written under .snakemake/storage
    (NFR-PERF-005).

    The first run keeps no local copies at all; the run with
    ``--keep-storage-local-copies`` keeps the marker of the api.archive output, the
    empty file of the plain pyfdb output and the one retrieved input of the mixed rule.
    """
    direct["run1"].ok()
    assert direct["files_after_run1"] == []
    kept = {p.relative_to(direct["work"]).as_posix(): p for p in direct["kept"]}
    marker = api.read_marker(kept[MARKER_LOCAL])
    assert marker is not None
    assert (marker.query, marker.fields) == (MARKER_QUERY, 3)
    assert kept[PLAIN_LOCAL].stat().st_size == 0
    retrieved = set(kept) - {MARKER_LOCAL, PLAIN_LOCAL}
    assert all("expver=0001" in name for name in retrieved)  # the mixed rule's input


def test_direct_workflow_store_runs_in_the_main_process(direct):
    """L-31: the store step, and with it the reference time of the empty-output
    convention, belongs to the process that parsed the Snakefile first (the main
    Snakemake process), not to the spawned job."""
    log = direct["run1"].ok()
    snakefile_pids = re.findall(r"SNAKEFILE PID (\d+)", log)
    job_pid = re.search(r"JOB PID (\d+)", log)[1]  # plain_shift, the only plain archive
    store_line = re.compile(r"expver=0042.* is empty; .*reference, pid (\d+)\)")
    store_pid = store_line.search(log)[1]
    assert store_pid == snakefile_pids[0]  # the main process
    assert job_pid != store_pid  # a run: job is spawned
    assert job_pid in snakefile_pids[1:]  # and re-parses the Snakefile


def test_direct_workflow_too_few_fields_is_reported(direct):
    """FR-DIRECT-004: nothing was pre-checked, so the post-check names the problem."""
    bad = direct["bad"]
    assert bad.returncode != 0
    assert "is empty, so the job is taken to have archived the fields itself" in bad.log
    assert "2 of 3 found in FDB with timestamps from this run" in bad.log
    assert "missing or older: step=12" in bad.log


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
    assert "plain_shift" in log


def test_direct_workflow_delete_all_output(direct):
    log = direct["delete"].ok()
    assert "FDB cannot delete individual fields" in log
    assert not (direct["work"] / "steps.txt").exists()
    # FDB fields are never deleted (FR-REMOVE-001); kept local copies, markers and
    # empty files included, stay where --keep-storage-local-copies put them.
    assert direct["after_delete"] == direct["kept"]
    assert _fdb_steps(direct["config"], PLAIN_QUERY) == [0, 6, 12]
