"""Backend: config/schema resolution, expansion, error mapping, and temp FDB access."""

import logging
import subprocess
import sys
import threading
from pathlib import Path

import pytest
import yaml
from snakemake_interface_common.exceptions import WorkflowError

from snakemake_storage_plugin_fdb.backend import (
    Backend,
    SchemaInfo,
    fallback_expand,
    map_error,
    parse_schema,
    resolve_config,
    resolve_schema_path,
)
from snakemake_storage_plugin_fdb.grib import GribError, split_messages, variant
from snakemake_storage_plugin_fdb.query import KeyOrder, QueryError, parse

REPO = Path(__file__).resolve().parents[1]
RAW = REPO / ".raw"
DATA = Path(__file__).resolve().parent / "data"
TEST_SCHEMA = DATA / "schema"

_raw_present = pytest.mark.skipif(
    not (RAW / "template.grib").exists(), reason="no .raw/ ECMWF samples"
)


def needs_raw(func):
    return pytest.mark.needs_raw(_raw_present(func))


# class=ea,stream=oper variants seeded by conftest.seeded_fdb
EA_OPER = {
    "class": "ea",
    "expver": "0001",
    "stream": "oper",
    "date": "20200101",
    "time": "0000",
    "domain": "g",
    "type": "an",
    "levtype": "sfc",
}
REQ_2X2 = {**EA_OPER, "step": "0/6", "param": "167/165"}


# --- pure Python: no pyfdb --------------------------------------------------------


def test_import_and_helpers_do_not_load_pyfdb():
    code = (
        "import sys\n"
        "from snakemake_storage_plugin_fdb import backend\n"
        f"backend.resolve_config({str(TEST_SCHEMA)!r})\n"
        f"backend.resolve_schema_path('schema: {TEST_SCHEMA}', env={{}})\n"
        f"backend.parse_schema(open({str(TEST_SCHEMA)!r}).read())\n"
        "backend.Backend({'type': 'local'})\n"
        "bad = [m for m in ('pyfdb', 'eccodes', 'gribapi') if m in sys.modules]\n"
        "assert not bad, bad\n"
    )
    subprocess.run([sys.executable, "-c", code], check=True)


def test_resolve_config_forms(tmp_path):
    assert resolve_config(None) is None
    path = tmp_path / "config.yaml"
    path.write_text("type: local\n")
    assert resolve_config(str(path)) == path
    assert isinstance(resolve_config(str(path)), Path)
    inline = "type: local\nengine: toc\n"
    assert resolve_config(inline) == inline
    assert resolve_config('{"type": "local"}') == '{"type": "local"}'
    long_inline = "{type: local, schema: " + "x" * 400 + "}"
    assert resolve_config(long_inline) == long_inline  # no ENAMETOOLONG


@pytest.mark.parametrize(
    "value, message",
    [
        ("/no/such/config.yaml", "neither an existing file nor an inline YAML mapping"),
        ("- a\n- b\n", "inline YAML mapping"),
        ("", "inline YAML mapping"),
        ("type: [unclosed", "valid YAML"),
    ],
)
def test_resolve_config_errors(value, message):
    with pytest.raises(WorkflowError, match="FDB configuration error") as e:
        resolve_config(value)
    assert message in str(e.value)


def test_parse_schema_test_schema():
    text = TEST_SCHEMA.read_text()
    info = parse_schema(text)
    assert info == SchemaInfo(
        keys=(
            "class", "expver", "stream", "date", "time", "domain",
            "type", "levtype", "step", "quantile", "number", "levelist", "param",
        ),
        optional=frozenset({"domain", "quantile", "number", "levelist"}),
        removed=frozenset(),
        defaults={},
    )  # fmt: skip
    assert info.keys == KeyOrder.from_schema(text).keys


SCHEMA_DECORATED = """
# rules [ with, brackets ] in a comment: ignored
param: Param;
[ class, expver, stream=oper/dcda, date, time, domain?
    [ type, levtype
        [ step, levelist?, param ]]]

[ Class, expver, stream=enfo, date: Date, time, domain-  # trailing comment
    [ type=pf/cf, levtype
        [ step, number?0, quantile?, grid-, param ]]]
"""


def test_parse_schema_decorations():
    info = parse_schema(SCHEMA_DECORATED)
    assert info.keys == (
        "class", "expver", "stream", "date", "time", "domain",
        "type", "levtype", "step", "levelist", "param", "number", "quantile", "grid",
    )  # fmt: skip
    assert info.keys == KeyOrder.from_schema(SCHEMA_DECORATED).keys
    assert info.optional == {"domain", "levelist", "number", "quantile"}
    assert info.removed == {"domain", "grid"}  # merged over both rule groups
    assert info.defaults == {"number": "0"}


@needs_raw
def test_parse_schema_raw():
    info = parse_schema((RAW / "schema").read_text())
    assert info.optional == {"domain", "levelist"}
    assert info.keys[:6] == ("class", "expver", "stream", "date", "time", "domain")


def test_parse_schema_without_rules():
    with pytest.raises(QueryError, match="no rule keys"):
        parse_schema("param: Param;\n# [ a, b ]\n")


def _yaml(schema: Path | str, **extra) -> str:
    return yaml.safe_dump({"type": "local", "schema": str(schema), **extra})


def test_resolve_schema_path_explicit_config(tmp_path, monkeypatch):
    config_file = tmp_path / "config.yaml"
    config_file.write_text(_yaml(TEST_SCHEMA))
    other = tmp_path / "other.yaml"
    other.write_text(_yaml(tmp_path / "missing"))
    env = {"FDB_CONFIG_FILE": str(other), "FDB_HOME": str(tmp_path)}
    assert resolve_schema_path(_yaml(TEST_SCHEMA), env) == TEST_SCHEMA
    assert resolve_schema_path(config_file, env) == TEST_SCHEMA
    assert resolve_schema_path({"schema": str(TEST_SCHEMA)}, env) == TEST_SCHEMA
    assert resolve_schema_path(_yaml(tmp_path / "missing"), env) is None
    # default env is os.environ
    monkeypatch.setenv("FDB_CONFIG", _yaml(TEST_SCHEMA))
    assert resolve_schema_path(None) == TEST_SCHEMA


def test_resolve_schema_path_env_fallbacks(tmp_path):
    config_file = tmp_path / "config.yaml"
    config_file.write_text(_yaml(TEST_SCHEMA))
    assert resolve_schema_path(None, {"FDB_CONFIG": _yaml(TEST_SCHEMA)}) == TEST_SCHEMA
    assert resolve_schema_path(None, {"FDB5_CONFIG": _yaml(TEST_SCHEMA)}) == TEST_SCHEMA
    assert (
        resolve_schema_path(
            None,
            {"FDB_CONFIG": _yaml(TEST_SCHEMA), "FDB_CONFIG_FILE": "/nonexistent.yaml"},
        )
        == TEST_SCHEMA
    )  # FDB_CONFIG wins
    for name in ("FDB_CONFIG_FILE", "FDB5_CONFIG_FILE"):
        assert resolve_schema_path(None, {name: str(config_file)}) == TEST_SCHEMA

    home = tmp_path / "home"
    (home / "etc" / "fdb").mkdir(parents=True)
    (home / "etc" / "fdb" / "schema").write_text(TEST_SCHEMA.read_text())
    default_schema = home / "etc" / "fdb" / "schema"
    # no config file under FDB_HOME -> ~fdb/etc/fdb/schema
    assert resolve_schema_path(None, {"FDB_HOME": str(home)}) == default_schema
    # FDB_CONFIG_FILE naming a missing file -> skeleton config -> default schema
    env = {"FDB_CONFIG_FILE": str(tmp_path / "nope.yaml"), "FDB_HOME": str(home)}
    assert resolve_schema_path(None, env) == default_schema
    # config.json under FDB_HOME, with a ~fdb schema path
    (home / "etc" / "fdb" / "config.json").write_text(
        '{"type": "local", "schema": "~fdb/etc/fdb/schema"}'
    )
    assert resolve_schema_path(None, {"FDB_HOME": str(home)}) == default_schema
    # FDB_SCHEMA_FILE when the config has no schema key
    env = {"FDB_CONFIG": "type: local", "FDB_SCHEMA_FILE": str(TEST_SCHEMA)}
    assert resolve_schema_path(None, env) == TEST_SCHEMA
    # fdb_home in the config expands ~fdb
    cfg = {"schema": "~fdb/etc/fdb/schema", "fdb_home": str(home)}
    assert resolve_schema_path(cfg, {}) == default_schema


def test_resolve_schema_path_unresolvable():
    assert resolve_schema_path(None, {}) is None  # ~fdb without FDB_HOME
    assert resolve_schema_path("type: remote\nhost: x\n", {}) is None


def test_fallback_expand():
    assert fallback_expand({"step": "0/to/12/by/6", "param": "2t/167"}) == {
        "step": ["0", "6", "12"],
        "param": ["2t", "167"],
    }
    assert fallback_expand({"number": "1/to/3"}) == {"number": ["1", "2", "3"]}
    assert fallback_expand({"step": "12/to/0/by/-6"}) == {"step": ["12", "6", "0"]}
    assert fallback_expand({"date": "20200130/TO/20200202"}) == {
        "date": ["20200130", "20200131", "20200201", "20200202"]
    }
    assert fallback_expand({"x": "1/2/to/4/5"}) == {"x": ["1", "2", "3", "4", "5"]}
    # not expandable: items kept verbatim (over-counts)
    assert fallback_expand({"step": "0/to/60/by/10m"}) == {
        "step": ["0", "to", "60", "by", "10m"]
    }
    assert fallback_expand({"step": "12/to/0"}) == {"step": ["12", "to", "0"]}


SPLITTER = (
    "Serious bug: Cannot find a metkit SplitterBuilder for "
    "PeekHandle[BufferedHandle[MemoryHandle[size=4]]] test - 74657374\n"
)


@pytest.mark.parametrize(
    "exc, expected",
    [
        (
            RuntimeError("UserError: UserError: TypeEnum[name=class]: cannot expand "
                         "'zz' request=retrieve,class=zz, expanded=retrieve,"),
            "Invalid MARS request fdb://q: TypeEnum[name=class]: cannot expand 'zz' "
            "request=retrieve,class=zz, expanded=retrieve, (if this value is valid for "
            "your FDB, point metkit_home at a MARS language that defines it)",
        ),
        (
            RuntimeError("UserError: UserError: TypeToByListInt[name=number]: Key "
                         "[number] not acceptable with context: Context[...]"),
            "Invalid MARS request fdb://q: TypeToByListInt[name=number]: Key [number] "
            "not acceptable with context: Context[...]",
        ),
        (
            RuntimeError("UserError: Only one value possible for 'class'\nsecond"),
            "Invalid MARS request fdb://q: Only one value possible for 'class'",
        ),
        (RuntimeError(SPLITTER), "out.grib is not GRIB"),
        (
            RuntimeError("Serious bug: Keywords not used: {number} for key={...}"),
            "GRIB keys do not match the FDB schema for fdb://q (out.grib): "
            "Keywords not used: {number} for key={...}",
        ),
        (
            RuntimeError("Serious bug: Could not find [model] in {class=ea}"),
            "GRIB keys do not match the FDB schema for fdb://q (out.grib): "
            "Could not find [model] in {class=ea}",
        ),
        (
            RuntimeError("Serious bug: FDB: Could not find a rule to archive "
                         "{class=ea}"),
            "GRIB keys do not match the FDB schema for fdb://q (out.grib): "
            "FDB: Could not find a rule to archive {class=ea}",
        ),
        (
            RuntimeError("Cannot open /x/schema  (No such file or directory)"),
            "FDB configuration error: Cannot open /x/schema  "
            "(No such file or directory)",
        ),
        (
            RuntimeError("Unexpected state: No writable roots available. "
                         "Configured roots: [/x]"),
            "FDB configuration error: Unexpected state: No writable roots available. "
            "Configured roots: [/x]",
        ),
        (GribError("f.grib: trailing non-GRIB bytes at offset 7"),
         "f.grib: trailing non-GRIB bytes at offset 7"),
    ],
)  # fmt: skip
def test_map_error_table(exc, expected):
    mapped = map_error(exc, "fdb://q", local="out.grib")
    assert isinstance(mapped, WorkflowError)
    assert str(mapped) == expected


def test_map_error_without_local_and_unknown():
    assert str(map_error(RuntimeError(SPLITTER), "fdb://q")) == "fdb://q is not GRIB"
    assert map_error(RuntimeError("something else"), "fdb://q") is None
    assert map_error(KeyError("UserError"), "fdb://q") is None


def test_timestamp_of():
    class Element:
        def __init__(self, text):
            self.text = text

        def __repr__(self):
            return self.text

    tail = "{step=0,param=167},TocFieldLocation[...],length=236,"
    assert Backend.timestamp_of(Element(tail + "timestamp=1789488488")) == 1789488488
    assert Backend.timestamp_of(Element("{class=ea},length=0,timestamp=0\n")) == 0
    assert Backend.timestamp_of(Element("no timestamp")) == 0


# --- against a temporary FDB --------------------------------------------------------


@needs_raw
def test_inspect_2x2(seeded_fdb):
    fields = seeded_fdb.backend.inspect(REQ_2X2)
    assert len(fields) == 4
    assert {(f.key["step"], f.key["param"]) for f in fields} == {
        ("0", "167"), ("0", "165"), ("6", "167"), ("6", "165"),
    }  # fmt: skip
    for f in fields:
        assert 200 <= f.length <= 300  # zeroed variants
        assert seeded_fdb.flush_start <= f.timestamp <= seeded_fdb.flush_end
        assert abs(f.timestamp - seeded_fdb.flush_end) <= 1
        assert f.uri_path and Path(f.uri_path).is_file()


@needs_raw
def test_inspect_aliases_and_missing(seeded_fdb):
    backend = seeded_fdb.backend
    aliases = {
        **EA_OPER,
        "date": "2020-01-01",
        "time": "0",
        "type": "AN",
        "step": "0/to/12/by/6",
        "param": "2t",
    }
    assert len(backend.inspect(aliases)) == 3
    assert len(backend.inspect({**REQ_2X2, "step": "0/6/18"})) == 4  # 18 omitted
    assert backend.inspect({**REQ_2X2, "expver": "0002"}) == []


@needs_raw
def test_invalid_request(seeded_fdb):
    with pytest.raises(RuntimeError) as e:
        seeded_fdb.backend.inspect({**REQ_2X2, "class": "zz"})
    mapped = map_error(e.value, "fdb://class=zz")
    assert isinstance(mapped, WorkflowError)
    assert str(mapped).startswith("Invalid MARS request fdb://class=zz: ")
    assert "cannot expand 'zz'" in str(mapped)


@needs_raw
def test_list_levels(seeded_fdb):
    backend = seeded_fdb.backend
    fields = backend.list({"class": "ea", "stream": "oper"})
    assert len(fields) == 6
    assert all(f.length > 0 and f.timestamp > 0 and f.uri_path for f in fields)
    assert len(backend.list({})) == 10  # 4 raw files + 6 variants
    (db,) = backend.list({"class": "ea", "stream": "oper"}, level=1)
    assert db.key["stream"] == "oper" and "step" not in db.key
    assert (db.length, db.timestamp, db.uri_path) == (0, 0, None)


@needs_raw
def test_retrieve_to(seeded_fdb, tmp_path):
    backend = seeded_fdb.backend
    total = sum(f.length for f in backend.inspect(REQ_2X2))
    dest = tmp_path / "sub" / "param=167+165.grib"
    assert backend.retrieve_to(REQ_2X2, dest, expected=total) == total
    assert dest.stat().st_size == total
    assert not dest.with_name(dest.name + ".part").exists()
    messages = split_messages(dest)
    # request order, outer key first (architecture.md §13.4)
    assert [(m.mars["step"], m.param_id) for m in messages] == [
        ("0", "167"), ("0", "165"), ("6", "167"), ("6", "165"),
    ]  # fmt: skip
    # an existing destination is replaced
    one = {**REQ_2X2, "step": "0", "param": "167"}
    assert backend.retrieve_to(one, dest) == messages[0].length
    assert dest.read_bytes() == messages[0].data


@needs_raw
def test_retrieve_to_errors_leave_no_part(seeded_fdb, tmp_path):
    backend = seeded_fdb.backend
    dest = tmp_path / "x.grib"
    part = tmp_path / "x.grib.part"
    with pytest.raises(WorkflowError, match="expected 1"):
        backend.retrieve_to(REQ_2X2, dest, expected=1)
    assert not dest.exists() and not part.exists()
    with pytest.raises(RuntimeError, match="cannot expand"):
        backend.retrieve_to({**REQ_2X2, "class": "zz"}, dest)
    assert not dest.exists() and not part.exists()
    dest.write_bytes(b"old")
    with pytest.raises(WorkflowError):
        backend.retrieve_to(REQ_2X2, dest, expected=0)
    assert dest.read_bytes() == b"old" and not part.exists()


@needs_raw
def test_retrieve_to_nothing_found(seeded_fdb, tmp_path):
    dest = tmp_path / "none.grib"
    assert seeded_fdb.backend.retrieve_to({**REQ_2X2, "expver": "0002"}, dest) == 0
    assert dest.read_bytes() == b""


@needs_raw
def test_expand(seeded_fdb):
    request = {"param": "2t/165", "step": "0/to/12/by/6", "date": "2020-01-01"}
    assert seeded_fdb.backend.expand(request) == {
        "param": ["167", "165"],
        "step": ["0", "6", "12"],
        "date": ["20200101"],
    }
    assert seeded_fdb.backend.expand({}) == {}
    with pytest.raises(RuntimeError, match="UserError"):
        seeded_fdb.backend.expand({"class": "zz"})


@needs_raw
def test_expected_count(seeded_fdb):
    backend = seeded_fdb.backend
    assert backend.expected_count(REQ_2X2) == 4
    # aliases of one field count once with metkit expansion ("167/167")
    assert backend.expected_count({"param": "2t/167", "step": "0/to/12/by/6"}) == 3


@needs_raw
def test_expansion_fallback(seeded_fdb, monkeypatch, caplog):
    backend = seeded_fdb.backend
    monkeypatch.setitem(sys.modules, "pyfdb._internal", None)  # import fails
    with caplog.at_level(logging.DEBUG, logger="snakemake_storage_plugin_fdb"):
        assert backend.expand({"param": "2t"}) is None
        assert backend.expected_count({"param": "2t/167", "step": "0/to/12/by/6"}) == 6
        assert backend.spelling_diffs(parse("fdb://class=EA,param=2t")) == []
    assert "skipped" in caplog.text


@needs_raw
def test_spelling_diffs(seeded_fdb):
    backend = seeded_fdb.backend
    parsed = parse("fdb://param=2t,class=EA,step=0/to/6/by/6")
    # parsed (canonical key order), not the order written in the query
    assert backend.spelling_diffs(parsed) == [
        ("class", "EA", "ea"),
        ("param", "2t", "167"),
    ]
    canonical = parse(
        "fdb://class=ea,expver=0001,stream=oper,date=20200101,time=0000,domain=g,"
        "type=an,levtype=sfc,step=0/6,param=167/165"
    )
    assert backend.spelling_diffs(canonical) == []
    mixed = parse("fdb://class=ea,time=0,expver=1,date={date},param=2t/167")
    assert backend.spelling_diffs(mixed) == [
        ("expver", "1", "0001"), ("time", "0", "0000"), ("param", "2t", "167"),
    ]  # fmt: skip


@needs_raw
def test_archive_identifier_and_masking(empty_fdb):
    backend = empty_fdb()
    template = (RAW / "template.grib").read_bytes()
    message = variant(template, stream="oper", step=1)
    identifier = {**EA_OPER, "step": "1", "param": "167"}
    backend.archive(message, identifier)
    backend.flush()
    (field,) = backend.inspect(identifier)
    assert field.length == len(message)
    backend.archive(message, identifier)
    backend.flush()
    assert len(backend.list(identifier)) == 1
    assert len(backend.list(identifier, include_masked=True)) == 2


@needs_raw
def test_archive_native_errors_map(empty_fdb, tmp_path):
    raw_schema = empty_fdb(RAW / "schema")
    with pytest.raises(RuntimeError) as e:
        raw_schema.archive((RAW / "template.grib").read_bytes())  # number not in schema
    mapped = map_error(e.value, "fdb://q", tmp_path / "t.grib")
    assert "GRIB keys do not match the FDB schema" in str(mapped)
    assert "Keywords not used: {number}" in str(mapped)
    with pytest.raises(RuntimeError) as e:
        raw_schema.archive(b"test")
    assert str(map_error(e.value, "fdb://q", "t.grib")) == "t.grib is not GRIB"


def _toc_config(root: Path, schema: Path) -> dict:
    return {
        "type": "local",
        "engine": "toc",
        "schema": str(schema),
        "spaces": [{"handler": "Default", "roots": [{"path": str(root)}]}],
    }


@needs_raw
def test_config_errors_at_first_use(tmp_path):
    (tmp_path / "db").mkdir()
    backend = Backend(_toc_config(tmp_path / "db", tmp_path / "missing-schema"))
    backend.handle()  # construction does not validate
    with pytest.raises(RuntimeError) as e:
        backend.inspect(REQ_2X2)
    assert str(map_error(e.value, "fdb://q")).startswith(
        "FDB configuration error: Cannot open"
    )
    config = _toc_config(tmp_path / "no-root", TEST_SCHEMA)
    with pytest.raises(RuntimeError) as e:
        Backend(config).inspect(REQ_2X2)
    assert "No writable roots available" in str(map_error(e.value, "fdb://q"))


@needs_raw
def test_config_forms(seeded_fdb, tmp_path):
    config_file = tmp_path / "config.yaml"
    config_file.write_text(yaml.safe_dump(seeded_fdb.config))
    for value in (str(config_file), yaml.safe_dump(seeded_fdb.config)):
        backend = Backend(resolve_config(value), resolve_config("useSubToc: false"))
        assert len(backend.inspect(REQ_2X2)) == 4


def test_handle_per_thread():
    backend = Backend({"type": "local", "engine": "toc"})
    handles = []

    def grab():
        handles.append(backend.handle())

    threads = [threading.Thread(target=grab) for _ in range(2)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert len(handles) == 2 and handles[0] is not handles[1]
    assert backend.handle() is backend.handle()


@needs_raw
def test_reads_see_archives_after_an_earlier_read(empty_fdb):
    # a handle that has read a database keeps its catalogue (architecture.md §13.5)
    backend = empty_fdb()
    template = (RAW / "template.grib").read_bytes()
    identifier = {**EA_OPER, "step": "0", "param": "167"}
    old = variant(template, stream="oper", step=0, paramId=167)
    new = variant(template, False, stream="oper", step=0, paramId=167)
    backend.archive(old, identifier)
    backend.flush()
    assert [f.length for f in backend.inspect(identifier)] == [len(old)]
    backend.archive(new, identifier)
    backend.flush()
    assert [f.length for f in backend.inspect(identifier)] == [len(new)]
    assert len(backend.list(identifier, include_masked=True)) == 2
