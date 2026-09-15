"""Provider and storage object (provider part: plan step 4, read path: plan step 5).

Write and glob tests are added in plan steps 6-7.
"""

import asyncio
import copy
import itertools
import logging
import os
import subprocess
import sys
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
from snakemake_storage_plugin_fdb.grib import split_messages
from snakemake_storage_plugin_fdb.query import NAME_MAX

REPO = Path(__file__).resolve().parents[1]
TEST_SCHEMA = Path(__file__).resolve().parent / "data" / "schema"
BASE = "class=od,expver=0001,stream=oper,date=20240101,time=0000,type=fc,levtype=sfc"
# class=ea,stream=oper variants of template.grib seeded by conftest.seeded_fdb
EA = (
    "class=ea,expver=0001,stream=oper,date=20200101,time=0000,domain=g,type=an,"
    "levtype=sfc"
)

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
        return f"fdb://{EA.replace('expver=0001', 'expver=0002')},step=0/6/12,param=167"


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
