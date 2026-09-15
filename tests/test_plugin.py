"""Provider and storage object (provider: plan step 4, read path: step 5, write
path: step 6).

Glob tests are added in plan step 7.
"""

import asyncio
import copy
import itertools
import logging
import os
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest
import yaml
from snakemake.io import IOCache, IOFile, apply_wildcards, flag
from snakemake_interface_common.exceptions import WorkflowError
from snakemake_interface_storage_plugins.tests import TestStorageBase

from snakemake_storage_plugin_fdb import (
    StorageObject,
    StorageProvider,
    StorageProviderSettings,
)
from snakemake_storage_plugin_fdb.backend import Backend, Field, map_error
from snakemake_storage_plugin_fdb.grib import split_messages, variant
from snakemake_storage_plugin_fdb.guard import IdentifierMismatch, NoGuard
from snakemake_storage_plugin_fdb.query import NAME_MAX

REPO = Path(__file__).resolve().parents[1]
RAW = REPO / ".raw"
TEST_SCHEMA = Path(__file__).resolve().parent / "data" / "schema"
BASE = "class=od,expver=0001,stream=oper,date=20240101,time=0000,type=fc,levtype=sfc"
# class=ea,stream=oper variants of template.grib seeded by conftest.seeded_fdb
EA = (
    "class=ea,expver=0001,stream=oper,date=20200101,time=0000,domain=g,type=an,"
    "levtype=sfc"
)
EA2 = EA.replace("expver=0001", "expver=0002")  # not seeded; written by store tests

_raw_present = pytest.mark.skipif(
    not (REPO / ".raw" / "template.grib").exists(), reason="no .raw/ ECMWF samples"
)


def needs_raw(obj):
    return pytest.mark.needs_raw(_raw_present(obj))


SCHEMA_ORDER = (
    "class", "expver", "stream", "date", "time", "domain",
    "type", "levtype", "step", "quantile", "number", "levelist", "param",
)  # fmt: skip


# --- queries ------------------------------------------------------------------------


@pytest.mark.parametrize(
    "query",
    [
        f"fdb://{BASE},step=0,param=167",
        f"fdb://{BASE},step=0/to/12/by/6,param=167/165",
        "fdb://class=od,date={date},param={param,[0-9]+}",
        "fdb://class=od,date={date,\\d{8}},step={s}/{t}",
        "fdb:// class = od ,\n param = 167 ",
    ],
)
def test_is_valid_query_accepts(query):
    assert StorageProvider.is_valid_query(query)


@pytest.mark.parametrize(
    "query, reason",
    [
        ("s3://x", "must start with"),
        ("test/x.txt", "must start with"),
        ("fdb://class=od/expver=0001", "invalid character '='"),
        ("fdb://", "empty query"),
        ("fdb://a=", "empty value"),
    ],
)
def test_is_valid_query_rejects(query, reason):
    result = StorageProvider.is_valid_query(query)
    assert not result
    assert reason in result.reason


def test_example_queries_valid():
    examples = StorageProvider.example_queries()
    assert len(examples) == 3
    for example in examples:
        assert StorageProvider.is_valid_query(example.query), example.query
        assert example.description


def test_valid_query_and_postprocess_load_no_native_library():
    code = (
        "import sys\n"
        "import snakemake_storage_plugin_fdb as p\n"
        "assert p.StorageProvider.is_valid_query('fdb://class=od,param=167')\n"
        "p.query.normalize('fdb://param=167,class=od')\n"
        "p.StorageProvider.example_queries()\n"
        "bad = [m for m in ('pyfdb', 'eccodes', 'gribapi') if m in sys.modules]\n"
        "assert not bad, bad\n"
    )
    subprocess.run([sys.executable, "-c", code], check=True, timeout=120)


# --- provider -----------------------------------------------------------------------


def test_provider_settings_schema_key_order(make_provider):
    provider = make_provider()
    assert provider.schema_path == TEST_SCHEMA
    assert provider.key_order.keys == SCHEMA_ORDER
    assert provider.schema_info.optional == {"domain", "quantile", "number", "levelist"}
    assert provider.backend.schema_info is provider.schema_info
    query = (
        "fdb://param=167,step=0,levtype=sfc,type=fc,time=0000,date=20240101,class=od"
    )
    assert provider.postprocess_query(query) == (
        "fdb://class=od,date=20240101,time=0000,type=fc,levtype=sfc,step=0,param=167"
    )


def test_provider_settings_key_order_setting(make_provider):
    provider = make_provider(key_order="date, time,CLASS")
    assert provider.key_order.keys == ("date", "time", "class")
    assert provider.postprocess_query("fdb://param=167,class=od,time=00,date=1") == (
        "fdb://date=1,time=00,class=od,param=167"
    )


def test_provider_settings_generic_order_without_schema(make_provider, tmp_path):
    config = yaml.safe_dump({"type": "local", "schema": str(tmp_path / "missing")})
    provider = make_provider(config=config)
    assert provider.schema_path is None and provider.schema_info is None
    assert provider.key_order.keys[:3] == ("class", "expver", "stream")


def test_provider_settings_env_fallback_config(make_provider, clean_env):
    clean_env.setenv("FDB_CONFIG", yaml.safe_dump({"schema": str(TEST_SCHEMA)}))
    provider = make_provider(config=None)
    assert provider.config is None
    assert provider.key_order.keys == SCHEMA_ORDER


def test_provider_settings_schema_without_rules(make_provider, tmp_path):
    schema = tmp_path / "schema"
    schema.write_text("param: Param;\n")
    with pytest.raises(WorkflowError, match="FDB configuration error.*no rule keys"):
        make_provider(schema=schema)


def test_provider_settings_config_file_made_absolute(
    make_provider, clean_env, tmp_path
):
    (tmp_path / "config.yaml").write_text(yaml.safe_dump({"schema": str(TEST_SCHEMA)}))
    clean_env.chdir(tmp_path)
    provider = make_provider(config="config.yaml")
    assert provider.config == tmp_path / "config.yaml"
    assert provider.config.is_absolute()


def test_provider_settings_rate_limiter_and_safe_print(make_provider):
    provider = make_provider()
    assert provider.use_rate_limiter() is False
    assert provider.default_max_requests_per_second() == 10.0
    assert provider.rate_limiter_key("fdb://class=od", None) == "fdb"
    assert provider.safe_print("fdb://class=od") == "fdb://class=od"


def test_provider_settings_postprocess_invalid_unchanged(make_provider):
    provider = make_provider()
    assert provider.postprocess_query("s3://x") == "s3://x"
    assert not provider.is_normalised("s3://x")


def test_provider_settings_unreachable_fdb_raises_at_first_use(make_provider, tmp_path):
    config = yaml.safe_dump(
        {
            "type": "local",
            "engine": "toc",
            "schema": str(TEST_SCHEMA),
            "spaces": [{"roots": [{"path": str(tmp_path / "no" / "root")}]}],
        }
    )
    provider = make_provider(config=config)  # no error at construction
    query = f"fdb://{BASE},step=0,param=167"
    with pytest.raises(RuntimeError) as e:
        provider.backend.inspect(provider.object(query).parsed.to_request())
    mapped = map_error(e.value, query)
    assert mapped is not None and "FDB configuration error" in str(mapped)


# --- storage object -----------------------------------------------------------------


def test_storage_object_local_suffix(make_provider):
    provider = make_provider()
    obj = provider.object(f"fdb://param=167,{BASE},step=0/6/12")
    assert isinstance(obj, StorageObject)
    assert obj.local_suffix() == (
        "class=od/expver=0001/stream=oper/date=20240101/time=0000/type=fc/"
        "levtype=sfc/step=0+6+12/param=167.grib"
    )
    assert obj.local_path() == provider.local_prefix / obj.local_suffix()


def test_storage_object_invalid_query_raises_on_use(make_provider):
    provider = make_provider()
    obj = StorageObject(
        query="fdb://a=", keep_local=False, retrieve=True, provider=provider
    )
    with pytest.raises(WorkflowError, match="invalid FDB query.*empty value"):
        obj.local_suffix()


def test_storage_object_follows_query_rewrite(make_provider):
    # Snakemake copies the object and injects wildcard constraints into .query
    # without calling __post_init__ (rules.py update_wildcard_constraints).
    provider = make_provider()
    obj = provider.object(f"fdb://{BASE},step={{step}},param=167")
    assert "step={step}/" in obj.local_suffix()
    clone = copy.copy(obj)
    clone.query = obj.query.replace("{step}", "{step,\\d+}")
    assert "step={step,\\d+}/" in clone.local_suffix()
    assert "step={step}/" in obj.local_suffix()


def _substitute(obj: StorageObject, **wildcards: str) -> StorageObject:
    """What Snakemake does for a job: ``_IOFile.apply_wildcards`` (spec §2.7)."""
    iofile = IOFile(flag(str(obj.local_path()), "storage_object", obj))
    return iofile.apply_wildcards(wildcards).storage_object


def test_wildcard_guard_long_substituted_value(make_provider):
    provider = make_provider()
    pattern = provider.object(f"fdb://{BASE},step=0,param={{p}}")
    short = _substitute(pattern, p="167")
    assert short.local_suffix() == apply_wildcards(pattern.local_suffix(), {"p": "167"})
    value = "1" * NAME_MAX
    with pytest.raises(WorkflowError) as e:
        _substitute(pattern, p=value)
    message = str(e.value)
    assert "'param'" in message
    assert f"{len(f'param={value}.grib')} bytes" in message
    assert "wildcard values must be single MARS values" in message


def test_wildcard_guard_constant_long_list_is_hashed(make_provider):
    provider = make_provider()
    params = "/".join(str(500000 + i) for i in range(60))  # > NAME_MAX bytes
    obj = provider.object(f"fdb://{BASE},step=0,param={params}")
    assert obj.local_suffix().endswith(".grib")
    assert "/param=~" in obj.local_suffix()
    # Snakemake rebuilds constant inputs through apply_wildcards too: still accepted
    assert _substitute(obj).local_suffix() == obj.local_suffix()


def test_wildcard_guard_unrecorded_short_query_accepted(make_provider):
    provider = make_provider()
    obj = StorageObject(
        query=f"fdb://{BASE},step=0,param=167",
        keep_local=False,
        retrieve=True,
        provider=provider,
    )
    assert obj.local_suffix().endswith("param=167.grib")


# --- read path (plan step 5) ----------------------------------------------------------


@needs_raw
class TestStorageRead(TestStorageBase):
    """Interface conformance on pre-archived fields (store is not testable here:
    ``TestStorageBase`` stores the text ``test``, never valid GRIB; spec §9.4)."""

    __test__ = True
    retrieve_only = True
    delete = False
    files_only = True

    @pytest.fixture(autouse=True)
    def _seeded(self, seeded_fdb):
        self.seeded = seeded_fdb

    def get_storage_provider_cls(self):
        return StorageProvider

    def get_storage_provider_settings(self):
        return StorageProviderSettings(config=yaml.safe_dump(self.seeded.config))

    def get_query(self, tmp_path) -> str:
        return f"fdb://{EA},step=0/6/12,param=167"

    def get_query_not_existing(self, tmp_path) -> str:
        return f"fdb://{EA2},step=0/6/12,param=167"


@pytest.fixture
def seeded_provider(seeded_fdb, make_provider):
    """Factory for a provider on the session's seeded FDB (clean environment)."""

    def make(**settings) -> StorageProvider:
        return make_provider(config=yaml.safe_dump(seeded_fdb.config), **settings)

    return make


def _warnings(caplog) -> list[str]:
    return [r.getMessage() for r in caplog.records if r.levelno == logging.WARNING]


@needs_raw
def test_exists_size_checksum_complete(seeded_provider, seeded_fdb):
    obj = seeded_provider().object(f"fdb://{EA},step=0/6/12,param=167/165")
    assert obj.exists() is True
    lengths = [f.length for f in seeded_fdb.backend.inspect(obj.parsed.to_request())]
    assert len(lengths) == 6
    assert obj.size() == sum(lengths) == obj.local_footprint()
    assert obj.checksum() is None
    assert obj.get_inventory_parent() is None
    assert obj.cleanup() is None


@needs_raw
def test_exists_partial_retrieve_names_missing(seeded_provider):
    obj = seeded_provider().object(f"fdb://{EA},step=0/6/12/18,param=167")
    assert obj.exists() is False
    assert obj.size() > 0  # the three present fields
    with pytest.raises(WorkflowError) as e:
        obj.retrieve_object()
    message = str(e.value)
    assert "3 of 4 fields found" in message
    assert message.endswith("missing: step=18")  # only keys with several values
    assert "optional schema keys" not in message
    assert not obj.local_path().exists()
    assert not obj.local_path().with_name(obj.local_path().name + ".part").exists()


@needs_raw
def test_exists_missing_optional_key_and_mtime_not_found(seeded_provider):
    # the seeded fields carry domain=g; inspect needs it named (spec §2.3, §7.1)
    obj = seeded_provider().object(
        f"fdb://{EA.replace(',domain=g', '')},step=0,param=167"
    )
    assert obj.exists() is False
    with pytest.raises(FileNotFoundError):
        obj.mtime()
    with pytest.raises(
        WorkflowError,
        match=r"0 of 1 fields found.*optional schema "
        r"keys not in the query: domain, levelist, number, quantile",
    ):
        obj.retrieve_object()


@needs_raw
def test_exists_invalid_request_raises(seeded_provider):
    obj = seeded_provider().object(
        f"fdb://{EA.replace('class=ea', 'class=zz')},param=167"
    )
    with pytest.raises(WorkflowError, match="Invalid MARS request"):
        obj.exists()


@needs_raw
def test_exists_wildcard_query_rejected(seeded_provider):
    obj = seeded_provider().object(f"fdb://{EA},step={{step}},param=167")
    with pytest.raises(WorkflowError, match="unresolved wildcards"):
        obj.exists()


@needs_raw
def test_exists_retries_transient_inspect_error(seeded_provider, monkeypatch):
    monkeypatch.setattr(StorageObject._inspect.retry, "sleep", lambda seconds: None)
    provider = seeded_provider()
    real, calls = provider.backend.inspect, []

    def flaky(request):
        calls.append(request)
        if len(calls) == 1:
            raise RuntimeError("transient")
        return real(request)

    monkeypatch.setattr(provider.backend, "inspect", flaky)
    assert provider.object(f"fdb://{EA},step=0,param=167").exists() is True
    assert len(calls) == 2


@needs_raw
def test_mtime_is_flush_time(seeded_provider, seeded_fdb):
    mtime = seeded_provider().object(f"fdb://{EA},step=0/6/12,param=167").mtime()
    assert isinstance(mtime, float)
    assert int(seeded_fdb.flush_start) <= mtime <= seeded_fdb.flush_end


@needs_raw
def test_mtime_timestamp_fallback_os_stat(seeded_provider, monkeypatch, caplog):
    obj = seeded_provider().object(f"fdb://{EA},step=0/6/12,param=167")
    paths = {f.uri_path for f in obj._fields()}
    monkeypatch.setattr(Backend, "timestamp_of", staticmethod(lambda element: 0))
    assert all(f.timestamp == 0 for f in obj._fields())
    assert obj.mtime() == max(os.stat(p).st_mtime for p in paths)
    assert not _warnings(caplog)
    # no timestamp and no local data file: 0.0, warned once per object
    blind = [Field(key={}, length=1, timestamp=0, uri_path=None)] * 2
    assert obj._mtime_of(blind) == 0.0
    assert obj._mtime_of(blind) == 0.0
    assert len(_warnings(caplog)) == 1


@needs_raw
def test_retrieve_request_order_and_size(seeded_provider):
    obj = seeded_provider().object(f"fdb://{EA},step=0/6/12,param=167/165")
    obj.local_path().parent.mkdir(parents=True, exist_ok=True)
    obj.local_path().write_bytes(b"stale")
    obj.retrieve_object()
    local = obj.local_path()
    assert local.stat().st_size == obj.size()
    keys = [(m.mars["step"], m.param_id) for m in split_messages(local)]
    assert keys == list(itertools.product(("0", "6", "12"), ("167", "165")))
    assert not local.with_name(local.name + ".part").exists()


@needs_raw
def test_inventory_fills_cache_with_one_inspect(seeded_provider, monkeypatch):
    provider = seeded_provider()
    real, calls = provider.backend.inspect, []
    monkeypatch.setattr(
        provider.backend, "inspect", lambda r: calls.append(r) or real(r)
    )
    obj = provider.object(f"fdb://{EA},step=0/6/12,param=167")
    cache = IOCache(max_wait_time=10)
    asyncio.run(obj.inventory(cache))
    assert len(calls) == 1
    key = obj.cache_key()
    assert dict(cache.exists_in_storage) == {key: True}
    assert list(cache.mtime) == [key] and list(cache.size) == [key]
    assert cache.mtime[key].storage() == obj.mtime()
    assert cache.size[key] == obj.size()
    assert not cache.checksum
    asyncio.run(obj.inventory(cache))  # already present: no FDB call
    assert len(calls) == 3  # inventory, then the mtime() and size() calls above

    missing = provider.object(f"fdb://{EA},step=18,param=167")
    asyncio.run(missing.inventory(cache))
    assert cache.exists_in_storage[missing.cache_key()] is False
    assert missing.cache_key() not in cache.mtime
    assert missing.cache_key() not in cache.size


@needs_raw
def test_canonical_spelling_warns_once(seeded_provider, caplog):
    provider = seeded_provider()
    query = f"fdb://{EA},step=0,param=2t"
    obj = provider.object(query)
    assert obj.exists() is True
    obj.mtime()
    obj.size()
    assert provider.object(query).exists() is True  # another object, same query
    warnings = _warnings(caplog)
    assert len(warnings) == 1
    assert "param=2t (canonical: 167)" in warnings[0]
    assert obj.local_suffix().endswith("/param=2t.grib")  # local path unchanged


@needs_raw
def test_canonical_spelling_error_raises(seeded_provider):
    obj = seeded_provider(canonical_spelling="error").object(
        f"fdb://{EA},step=0,param=2t"
    )
    for _ in range(2):
        with pytest.raises(WorkflowError, match=r"canonical: 167"):
            obj.exists()


@needs_raw
@pytest.mark.parametrize(
    "setting, fields",
    [("ignore", "step=0,param=2t"), ("warn", "step=0/to/12/by/6,param=167")],
    ids=["ignore", "range-exempt"],
)
def test_canonical_spelling_silent(seeded_provider, caplog, setting, fields):
    obj = seeded_provider(canonical_spelling=setting).object(f"fdb://{EA},{fields}")
    assert obj.exists() is True
    assert not _warnings(caplog)


# --- write path (plan step 6) ---------------------------------------------------------

# expver=0002 variants of template.grib built per test (class=ea, stream=oper)
STORE_QUERY = f"fdb://{EA2},step=0/6/12,param=167"
# template.grib as is (stream=enda, number=0; NUL-padded GRIB1, spec §2.1)
TEMPLATE_QUERY = (
    "fdb://class=ea,expver=0001,stream=enda,date=20200101,time=0000,domain=g,"
    "type=an,levtype=sfc,step=0,number=0,param=167"
)


def _grib(steps=(0, 6, 12), params=(167,), zero_values=True) -> bytes:
    template = (RAW / "template.grib").read_bytes()
    return b"".join(
        variant(template, zero_values, stream="oper", expver="0002", step=s, paramId=p)
        for s in steps
        for p in params
    )


def _store(provider: StorageProvider, query: str, data: bytes) -> StorageObject:
    obj = provider.object(query)
    obj.local_path().parent.mkdir(parents=True, exist_ok=True)
    obj.local_path().write_bytes(data)
    obj.store_object()
    return obj


def _in_fdb(provider: StorageProvider, query: str) -> int:
    return len(provider.backend.inspect(provider.object(query).parsed.to_request()))


@needs_raw
@pytest.mark.parametrize("archive_mode", ["identifier", "native"])
def test_store_roundtrip(make_provider, archive_mode):
    provider = make_provider(archive_mode=archive_mode)
    assert isinstance(provider.guard, NoGuard)
    data = _grib()
    t_start = int(time.time())
    obj = _store(provider, STORE_QUERY, data)
    assert obj.exists() is True
    assert obj.mtime() >= t_start
    lengths = [m.length for m in split_messages(obj.local_path())]
    assert obj.size() == sum(lengths) == len(data)  # unpadded messages
    obj.local_path().unlink()
    obj.retrieve_object()
    keys = [(m.mars["step"], m.param_id) for m in split_messages(obj.local_path())]
    assert keys == [("0", "167"), ("6", "167"), ("12", "167")]


@needs_raw
@pytest.mark.parametrize(
    "schema, archive_mode, error",
    [
        (TEST_SCHEMA, "identifier", None),
        (RAW / "schema", "identifier", None),  # number is not a schema key: dropped
        (RAW / "schema", "native", "GRIB keys do not match the FDB schema"),
    ],
    ids=["test-schema-identifier", "raw-schema-identifier", "raw-schema-native"],
)
def test_store_template(make_provider, schema, archive_mode, error):
    provider = make_provider(schema=schema, archive_mode=archive_mode)
    data = (RAW / "template.grib").read_bytes()
    if error:
        with pytest.raises(WorkflowError, match=error):
            _store(provider, TEMPLATE_QUERY, data)
        assert _in_fdb(provider, TEMPLATE_QUERY) == 0
        return
    obj = _store(provider, TEMPLATE_QUERY, data)
    assert obj.exists() is True
    (message,) = split_messages(obj.local_path())
    assert obj.size() == message.length < len(data)  # NUL padding not stored (§7.2)
    (field,) = provider.backend.list({"class": "ea", "stream": "enda"})
    assert ("number" in field.key) == (schema == TEST_SCHEMA)


@needs_raw
@pytest.mark.parametrize("archive_mode", ["identifier", "native"])
@pytest.mark.parametrize(
    "case",
    ["missing", "foreign", "duplicate", "trailing-garbage", "text"],
)
def test_store_strict_rejects(make_provider, archive_mode, case):
    provider = make_provider(archive_mode=archive_mode)
    native_foreign = case == "foreign" and archive_mode == "native"
    data, error = {
        "missing": (_grib((0, 6)), "has 2 fields, the query expands to 3; nothing"),
        "foreign": (
            _grib((0, 6, 18)),
            "1 landed outside the query or are duplicates"
            if native_foreign
            else "message 3 of .* has step=18, not one of 0/6/12; nothing",
        ),
        "duplicate": (_grib((0, 6, 6)), r"duplicate fields \(messages 2 and 3\)"),
        "trailing-garbage": (_grib() + b"GARBAGE", "trailing non-GRIB bytes"),
        "text": (b"test", "is not GRIB"),
    }[case]
    with pytest.raises(WorkflowError, match=error) as e:
        _store(provider, STORE_QUERY, data)
    if native_foreign:  # archived before the post-check; the message says so
        assert "stay in FDB" in str(e.value)
        assert _in_fdb(provider, STORE_QUERY) == 2
    else:
        assert _in_fdb(provider, STORE_QUERY) == 0


@needs_raw
@pytest.mark.parametrize("archive_mode", ["identifier", "native"])
def test_store_warn_fewer_fields(make_provider, caplog, archive_mode):
    provider = make_provider(archive_mode=archive_mode, store_check="warn")
    obj = _store(provider, STORE_QUERY, _grib((0, 6)))
    warnings = _warnings(caplog)
    assert len(warnings) == 1
    assert "has 2 fields, the query expands to 3 (store_check=warn)" in warnings[0]
    assert obj.exists() is False
    assert _in_fdb(provider, STORE_QUERY) == 2
    # foreign fields still fail (steps without earlier fields: the post-check has
    # one-second resolution, spec §7.7)
    error = "outside the query" if archive_mode == "native" else "has step=0"
    with pytest.raises(WorkflowError, match=error):
        _store(provider, f"fdb://{EA2},step=12/18/24,param=167", _grib((0, 18)))


@needs_raw
def test_store_wildcard_query_rejected(make_provider):
    with pytest.raises(WorkflowError, match="unresolved wildcards"):
        _store(make_provider(), f"fdb://{EA2},step={{step}},param=167", _grib((0,)))


@needs_raw
def test_store_partial_archive_failure_says_fields_stay(make_provider, monkeypatch):
    provider = make_provider()
    real, calls = provider.backend.archive, []

    def failing(data, identifier=None):
        calls.append(identifier)
        if len(calls) == 2:
            raise RuntimeError("disk full")
        real(data, identifier)

    monkeypatch.setattr(provider.backend, "archive", failing)
    with pytest.raises(
        WorkflowError, match=r"disk full \(1 of 3 archive calls succeeded.*stay in FDB"
    ):
        _store(provider, STORE_QUERY, _grib())
    assert len(calls) == 2  # not retried
    assert _in_fdb(provider, STORE_QUERY) == 1  # flushed; masked by the next store


class RecordingGuard:
    """Records guard calls into a shared event list; optionally fails on one."""

    def __init__(self, events: list, fail_on: int | None = None):
        self.events, self.fail_on, self.calls = events, fail_on, 0

    def check(self, message, identifier, query):
        self.calls += 1
        self.events.append(("check", message.mars["step"], dict(identifier)))
        if self.calls == self.fail_on:
            raise IdentifierMismatch(self.calls, "step", identifier["step"], "7")


@needs_raw
def test_store_guard_sees_every_message_before_archive(make_provider, monkeypatch):
    provider = make_provider()
    events: list = []
    provider.guard = RecordingGuard(events)
    real = provider.backend.archive

    def archive(data, identifier=None):
        events.append(("archive", identifier["step"], None))
        real(data, identifier)

    monkeypatch.setattr(provider.backend, "archive", archive)
    obj = _store(provider, STORE_QUERY, _grib())
    assert [(kind, step) for kind, step, _ in events] == [
        ("check", "0"), ("check", "6"), ("check", "12"),
        ("archive", "0"), ("archive", "6"), ("archive", "12"),
    ]  # fmt: skip
    identifier = events[1][2]
    assert identifier == {
        "class": "ea", "expver": "0002", "stream": "oper", "date": "20200101",
        "time": "0000", "domain": "g", "type": "an", "levtype": "sfc",
        "step": "6", "param": "167",
    }  # fmt: skip
    assert obj.exists() is True


@needs_raw
def test_store_guard_mismatch_leaves_fdb_unchanged(make_provider, monkeypatch):
    provider = make_provider()
    provider.guard = RecordingGuard([], fail_on=2)
    archived = []
    monkeypatch.setattr(provider.backend, "archive", lambda *a: archived.append(a))
    with pytest.raises(
        WorkflowError, match="identifier check failed.*message 2: identifier step=6"
    ):
        _store(provider, STORE_QUERY, _grib())
    assert not archived
    assert _in_fdb(provider, STORE_QUERY) == 0


@needs_raw
def test_store_masking_rerun(make_provider):
    provider = make_provider()
    query = f"fdb://{EA2},step=0,param=167"
    first = _store(provider, query, _grib((0,)))
    mtime = first.mtime()
    time.sleep(max(0.0, mtime + 1.05 - time.time()))  # next index flush second
    new = _grib((0,), zero_values=False)
    second = _store(provider, query, new)
    assert second.mtime() > mtime
    second.local_path().unlink()
    second.retrieve_object()
    assert second.local_path().read_bytes() == new
    request = second.parsed.to_request()
    assert len(provider.backend.list(request)) == 1
    assert len(provider.backend.list(request, include_masked=True)) == 2


@needs_raw
def test_store_threads(make_provider):
    provider = make_provider()
    steps = (0, 6, 12, 18)

    def store(step: int) -> bool:
        query = f"fdb://{EA2},step={step},param=167"
        return _store(provider, query, _grib((step,))).exists()

    with ThreadPoolExecutor(max_workers=len(steps)) as pool:
        assert list(pool.map(store, steps)) == [True] * len(steps)
    assert _in_fdb(provider, f"fdb://{EA2},step=0/6/12/18,param=167") == 4


@pytest.mark.parametrize("policy", ["warn", "ignore", "error"])
def test_remove_policy(make_provider, caplog, policy):
    provider = make_provider(remove_policy=policy)
    query = f"fdb://{EA2},step=0,param=167"
    obj = provider.object(query)
    if policy == "error":
        with pytest.raises(WorkflowError, match="remove_policy=error: FDB cannot"):
            obj.remove()
        return
    obj.remove()
    provider.object(query).remove()  # warned once per query
    warnings = _warnings(caplog)
    if policy == "ignore":
        assert not warnings
        return
    assert warnings == [
        f"FDB cannot delete individual fields; existing fields for {query} will be "
        "masked by the next archive. Use `fdb purge` to reclaim space."
    ]
