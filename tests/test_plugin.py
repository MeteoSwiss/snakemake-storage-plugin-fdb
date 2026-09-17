"""Provider and storage object: settings, read, write, glob and interface conformance
(requirements.md §2.1–§2.10, §2.12).
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
from dataclasses import replace
from pathlib import Path

import pytest
import yaml
from snakemake.io import IOCache, IOFile, apply_wildcards, flag, glob_wildcards
from snakemake_interface_common.exceptions import WorkflowError
from snakemake_interface_storage_plugins.exceptions import FileOrDirectoryNotFoundError
from snakemake_interface_storage_plugins.registry import StoragePluginRegistry
from snakemake_interface_storage_plugins.storage_object import (
    StorageObjectGlob,
    StorageObjectTouch,
)
from snakemake_interface_storage_plugins.tests import TestStorageBase

from snakemake_storage_plugin_fdb import (
    StorageObject,
    StorageProvider,
    StorageProviderSettings,
)
from snakemake_storage_plugin_fdb import backend as backend_module
from snakemake_storage_plugin_fdb.backend import Backend, Field, fdb_time, map_error
from snakemake_storage_plugin_fdb.grib import split_messages, variant
from snakemake_storage_plugin_fdb.guard import IdentifierMismatch, NoGuard
from snakemake_storage_plugin_fdb.query import NAME_MAX

DATA = Path(__file__).resolve().parent / "data"
SAMPLES = DATA / "grib" / "ecmwf"
TEST_SCHEMA = DATA / "schema"
ECMWF_SCHEMA = DATA / "ecmwf-fdb-tests.schema"  # multi-rule
PYFDB_SCHEMA = DATA / "pyfdb-tests.schema"
SYNTH11_QUERY = (  # synth11.grib sample as is (architecture.md §13.2)
    "fdb://class=od,expver=0001,stream=oper,date=20230508,time=1200,domain=g,"
    "type=fc,levtype=sfc,step=1,param=151130"
)
BASE = "class=od,expver=0001,stream=oper,date=20240101,time=0000,type=fc,levtype=sfc"
# class=ea,stream=oper variants of template.grib seeded by conftest.seeded_fdb
EA = (
    "class=ea,expver=0001,stream=oper,date=20200101,time=0000,domain=g,type=an,"
    "levtype=sfc"
)
EA2 = EA.replace("expver=0001", "expver=0002")  # not seeded; written by store tests

_samples_present = pytest.mark.skipif(
    not (SAMPLES / "template.grib").exists(), reason="no ECMWF samples"
)


def needs_samples(obj):
    return pytest.mark.needs_samples(_samples_present(obj))


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


def test_interface_conformance():
    # loaded the way Snakemake loads storage plugins (module name prefix)
    plugin = StoragePluginRegistry().get_plugin("fdb")
    assert plugin.storage_provider is StorageProvider
    assert plugin.storage_object is StorageObject
    assert plugin.settings_cls is StorageProviderSettings
    assert plugin.is_read_write()  # StorageObjectRead and StorageObjectWrite
    assert not StorageProvider.__abstractmethods__
    assert not StorageObject.__abstractmethods__
    assert issubclass(StorageObject, StorageObjectGlob)
    # a no-op touch, so that --touch works for the local outputs (FR-IFACE-006)
    assert issubclass(StorageObject, StorageObjectTouch)


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
    """Like Snakemake for a job: ``_IOFile.apply_wildcards`` (architecture.md §13.8)."""
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


# --- read path (requirements.md §2.5) -------------------------------------------------


class FDBStorageBase(TestStorageBase):
    """``TestStorageBase`` on a provider for the FDB configured in ``self.config``
    (an empty temp FDB unless a test replaces it)."""

    files_only = True  # directories are not supported (FR-IFACE-001)
    touch = False  # no StorageObjectTouch (FR-IFACE-001)
    config: dict

    @pytest.fixture(autouse=True)
    def _config(self, temp_fdb_config, clean_env):
        self.config = temp_fdb_config

    def get_storage_provider_cls(self):
        return StorageProvider

    def get_storage_provider_settings(self):
        return StorageProviderSettings(config=yaml.safe_dump(self.config))


class TestStorageRead(FDBStorageBase):
    """Base tests on pre-archived fields (architecture.md §8.9). ``test_storage`` and
    ``test_storage_not_existing`` read the seeded FDB and need the ECMWF samples;
    ``test_query_validation`` and ``test_example_queries`` run without data."""

    __test__ = True
    retrieve_only = True  # the base store sequence writes text (TestStorageWrite)

    @needs_samples
    def test_storage(self, tmp_path, seeded_fdb):
        self.config = seeded_fdb.config
        super().test_storage(tmp_path)

    @needs_samples
    def test_storage_not_existing(self, tmp_path, seeded_fdb):
        self.config = seeded_fdb.config
        super().test_storage_not_existing(tmp_path)

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


def _write_local(obj: StorageObject, data: bytes) -> None:
    obj.local_path().parent.mkdir(parents=True, exist_ok=True)
    obj.local_path().write_bytes(data)


@needs_samples
def test_exists_size_checksum_complete(seeded_provider, seeded_fdb):
    obj = seeded_provider().object(f"fdb://{EA},step=0/6/12,param=167/165")
    assert obj.exists() is True
    lengths = [f.length for f in seeded_fdb.backend.inspect(obj.parsed.to_request())]
    assert len(lengths) == 6
    assert obj.size() == sum(lengths) == obj.local_footprint()
    assert obj.checksum() is None
    assert obj.get_inventory_parent() is None
    assert obj.cleanup() is None


@needs_samples
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


@needs_samples
def test_exists_partial_warns_once(seeded_provider, caplog):
    """FR-READ-008: a partially present input is warned about by ``exists`` and
    ``inventory``, once per query."""
    obj = seeded_provider().object(f"fdb://{EA},step=0/6/12/18,param=167")
    cache = IOCache(max_wait_time=10)
    with caplog.at_level(logging.WARNING):
        assert obj.exists() is False
        asyncio.run(obj.inventory(cache))
    assert cache.exists_in_storage[obj.cache_key()] is False
    warnings = _warnings(caplog)
    assert len(warnings) == 1
    assert "3 of 4 fields found in FDB" in warnings[0]
    assert warnings[0].endswith("missing: step=18")


@needs_samples
def test_exists_absent_object_does_not_warn(seeded_provider, caplog):
    """Nothing found is the normal case of an output that does not exist yet: the
    report, with the optional-schema-key hint, goes to the debug log (FR-READ-008).
    A query that omits a key of the schema's first level is the exception
    (FR-ERR-008, ``tests/test_messages.py``)."""
    obj = seeded_provider().object(f"fdb://{EA},step=99,param=167")
    with caplog.at_level(logging.DEBUG, logger="fdb-test"):
        assert obj.exists() is False
    assert not _warnings(caplog)
    assert not [r for r in caplog.records if r.levelno == logging.INFO]
    debug = [r.getMessage() for r in caplog.records if r.levelno == logging.DEBUG]
    assert any("0 of 1 fields found in FDB" in m for m in debug)
    assert any(
        "optional schema keys not in the query: levelist, number, quantile" in m
        for m in debug
    )


@needs_samples
def test_exists_missing_optional_key_and_mtime_not_found(seeded_provider):
    # the seeded fields carry domain=g; inspect needs it named (FR-READ-001)
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


@needs_samples
def test_exists_invalid_request_raises(seeded_provider):
    obj = seeded_provider().object(
        f"fdb://{EA.replace('class=ea', 'class=zz')},param=167"
    )
    with pytest.raises(WorkflowError, match="Invalid MARS request"):
        obj.exists()


@needs_samples
def test_exists_wildcard_query_rejected(seeded_provider):
    obj = seeded_provider().object(f"fdb://{EA},step={{step}},param=167")
    with pytest.raises(WorkflowError, match="unresolved wildcards"):
        obj.exists()


def _failing(
    monkeypatch,
    provider: StorageProvider,
    method: str,
    exc: Exception | None = None,
    *,
    always: bool = False,
) -> list:
    """Make ``provider.backend.<method>`` raise ``exc`` (default: a transient error)
    on its first call or on every call, and skip the retry sleep of the
    ``StorageObject`` wrapper; returns the call log."""
    wrapper = {"inspect": StorageObject._inspect, "list": StorageObject._list}[method]
    monkeypatch.setattr(wrapper.retry, "sleep", lambda seconds: None)
    real, calls = getattr(provider.backend, method), []

    def flaky(request):
        calls.append(request)
        if always or len(calls) == 1:
            raise exc or RuntimeError("transient")
        return real(request)

    monkeypatch.setattr(provider.backend, method, flaky)
    return calls


@needs_samples
def test_exists_retries_transient_inspect_error(seeded_provider, monkeypatch):
    provider = seeded_provider()
    calls = _failing(monkeypatch, provider, "inspect")
    assert provider.object(f"fdb://{EA},step=0,param=167").exists() is True
    assert len(calls) == 2


@needs_samples
@pytest.mark.parametrize(
    "exc, attempts, raises, message",
    [
        (RuntimeError("transient"), 3, RuntimeError, "transient"),
        (
            RuntimeError("Cannot open /x/schema  (No such file or directory)"),
            1,
            WorkflowError,
            "FDB configuration error: Cannot open /x/schema",
        ),
        (
            RuntimeError("Failed system call: opendir (Success)"),
            1,
            WorkflowError,
            "FDB I/O error for fdb://",
        ),
        (
            RuntimeError("UserError: TypeEnum[name=class]: cannot expand 'zz'"),
            1,
            WorkflowError,
            "Invalid MARS request fdb://",
        ),
    ],
)
def test_exists_retries_only_transient_errors(
    seeded_provider, monkeypatch, caplog, exc, attempts, raises, message
):
    """ADR-033: permanent failures are mapped and raised on the first attempt, with
    the full pyfdb text in the debug log (FR-ERR-001)."""
    provider = seeded_provider()
    calls = _failing(monkeypatch, provider, "inspect", exc, always=True)
    with caplog.at_level(logging.DEBUG, logger="fdb-test"):
        with pytest.raises(raises) as e:
            provider.object(f"fdb://{EA},step=0,param=167").exists()
    assert str(e.value).startswith(message)
    assert len(calls) == attempts
    debug = [r.getMessage() for r in caplog.records if r.levelno == logging.DEBUG]
    assert (f"FDB storage: full error text: {exc}" in debug) is (
        raises is WorkflowError
    )


@needs_samples
def test_exists_unreadable_root_is_an_io_error(make_provider, tmp_path):
    """FR-ERR-004: an unreadable FDB root is a mapped I/O error, not a raw
    ``RuntimeError``."""
    if os.geteuid() == 0:
        pytest.skip("running as root: permissions are not enforced")
    provider = make_provider()
    root = tmp_path / "fdb" / "db"  # created by make_provider's default config
    root.chmod(0o000)
    try:
        with pytest.raises(WorkflowError) as e:
            provider.object(f"fdb://{EA},step=0,param=167").exists()
    finally:
        root.chmod(0o755)
    message = str(e.value)
    assert message.startswith("FDB I/O error for fdb://")
    assert "(Success)" not in message
    assert message.endswith(
        "(check permissions, free space and the roots in the FDB configuration)"
    )


@needs_samples
def test_exists_unreadable_root_without_fdb_error(make_provider, tmp_path, monkeypatch):
    """FR-ERR-004: FDB 5.23 returns no fields for an unreadable root instead of
    failing; the plugin's own root check reports it on every FDB version."""
    if os.geteuid() == 0:
        pytest.skip("running as root: permissions are not enforced")
    provider = make_provider()
    monkeypatch.setattr(provider.backend, "inspect", lambda request: [])
    root = tmp_path / "fdb" / "db"
    root.chmod(0o000)
    try:
        with pytest.raises(WorkflowError) as e:
            provider.object(f"fdb://{EA},step=0,param=167").exists()
    finally:
        root.chmod(0o755)
    assert str(e.value).startswith(f"FDB I/O error for fdb://{EA}")
    assert f"FDB root {root} is not readable" in str(e.value)
    # readable again: the same lookup is simply missing
    assert provider.object(f"fdb://{EA},step=0,param=167").exists() is False


def test_provider_str_is_the_plugin_name(make_provider):
    """Snakemake formats the provider into user-facing text (snakemake/storage.py)."""
    assert str(make_provider()) == "fdb"


@needs_samples
def test_mtime_is_flush_time(seeded_provider, seeded_fdb):
    mtime = seeded_provider().object(f"fdb://{EA},step=0/6/12,param=167").mtime()
    assert isinstance(mtime, float)
    assert seeded_fdb.flush_start <= mtime <= seeded_fdb.flush_end


@needs_samples
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


@needs_samples
def test_retrieve_request_order_and_size(seeded_provider):
    obj = seeded_provider().object(f"fdb://{EA},step=0/6/12,param=167/165")
    _write_local(obj, b"stale")
    obj.retrieve_object()
    local = obj.local_path()
    assert local.stat().st_size == obj.size()
    keys = [(m.mars["step"], m.param_id) for m in split_messages(local)]
    assert keys == list(itertools.product(("0", "6", "12"), ("167", "165")))
    assert not local.with_name(local.name + ".part").exists()


@needs_samples
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


@needs_samples
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


@needs_samples
def test_canonical_spelling_error_raises(seeded_provider):
    """FR-ERR-006: a query without wildcards is checked when the object is built, so
    Snakemake blames the Snakefile line instead of the first lookup; a wildcard query
    keeps the lazy check, on every call."""
    provider = seeded_provider(canonical_spelling="error")
    with pytest.raises(WorkflowError, match=r"canonical: 167"):
        provider.object(f"fdb://{EA},step=0,param=2t")
    obj = provider.object(f"fdb://{EA},step={{step}},param=2t")
    for _ in range(2):
        with pytest.raises(WorkflowError, match=r"canonical: 167"):
            obj.query = obj.query.replace("{step}", "0")
            obj.exists()


@needs_samples
@pytest.mark.parametrize(
    "setting, fields",
    [("ignore", "step=0,param=2t"), ("warn", "step=0/to/12/by/6,param=167")],
    ids=["ignore", "range-exempt"],
)
def test_canonical_spelling_silent(seeded_provider, caplog, setting, fields):
    obj = seeded_provider(canonical_spelling=setting).object(f"fdb://{EA},{fields}")
    assert obj.exists() is True
    assert not _warnings(caplog)


# --- write path (requirements.md §2.6) ------------------------------------------------

# expver=0002 variants of template.grib built per test (class=ea, stream=oper)
STORE_QUERY = f"fdb://{EA2},step=0/6/12,param=167"
# template.grib as is (stream=enda, number=0; NUL-padded GRIB1, architecture.md §13.2)
TEMPLATE_QUERY = (
    "fdb://class=ea,expver=0001,stream=enda,date=20200101,time=0000,domain=g,"
    "type=an,levtype=sfc,step=0,number=0,param=167"
)


def _message(zero_values=True, **keys) -> bytes:
    """One ``expver=0002`` variant of template.grib; ``keys`` override the defaults."""
    template = (SAMPLES / "template.grib").read_bytes()
    keys = {"stream": "oper", "expver": "0002", "step": 0, "paramId": 167, **keys}
    return variant(template, zero_values, **keys)


def _grib(steps=(0, 6, 12), params=(167,), zero_values=True) -> bytes:
    return b"".join(
        _message(zero_values, step=s, paramId=p) for s in steps for p in params
    )


def _store(provider: StorageProvider, query: str, data: bytes) -> StorageObject:
    obj = provider.object(query)
    _write_local(obj, data)
    obj.store_object()
    return obj


def _in_fdb(provider: StorageProvider, query: str) -> int:
    return len(provider.backend.inspect(provider.object(query).parsed.to_request()))


def _stored_key(provider: StorageProvider) -> dict[str, str]:
    """Key of the only field stored by the test (in the ``expver=0002`` variants)."""
    (field,) = provider.backend.list({"class": "ea", "expver": "0002"})
    return field.key


@needs_samples
@pytest.mark.parametrize("archive_mode", ["identifier", "native"])
def test_store_roundtrip(make_provider, archive_mode):
    provider = make_provider(archive_mode=archive_mode)
    assert isinstance(provider.guard, NoGuard)
    data = _grib()
    t_start = fdb_time()
    obj = _store(provider, STORE_QUERY, data)
    assert obj.exists() is True
    assert obj.mtime() >= t_start
    lengths = [m.length for m in split_messages(obj.local_path())]
    assert obj.size() == sum(lengths) == len(data)  # unpadded messages
    obj.local_path().unlink()
    obj.retrieve_object()
    keys = [(m.mars["step"], m.param_id) for m in split_messages(obj.local_path())]
    assert keys == [("0", "167"), ("6", "167"), ("12", "167")]


@needs_samples
@pytest.mark.parametrize(
    "schema, archive_mode, error",
    [
        (TEST_SCHEMA, "identifier", None),
        (PYFDB_SCHEMA, "identifier", None),  # number is not a schema key: dropped
        (PYFDB_SCHEMA, "native", "GRIB keys do not match the FDB schema"),
    ],
    ids=["test-schema-identifier", "pyfdb-schema-identifier", "pyfdb-schema-native"],
)
def test_store_template(make_provider, schema, archive_mode, error):
    provider = make_provider(schema=schema, archive_mode=archive_mode)
    data = (SAMPLES / "template.grib").read_bytes()
    if error:
        with pytest.raises(WorkflowError, match=error):
            _store(provider, TEMPLATE_QUERY, data)
        assert _in_fdb(provider, TEMPLATE_QUERY) == 0
        return
    obj = _store(provider, TEMPLATE_QUERY, data)
    assert obj.exists() is True
    (message,) = split_messages(obj.local_path())
    assert obj.size() == message.length < len(data)  # padding not stored (FR-READ-005)
    (field,) = provider.backend.list({"class": "ea", "stream": "enda"})
    assert ("number" in field.key) == (schema == TEST_SCHEMA)


@needs_samples
@pytest.mark.parametrize("archive_mode", ["identifier", "native"])
@pytest.mark.parametrize(
    "case",
    ["missing", "extra", "foreign", "duplicate", "trailing-garbage", "text"],
)
def test_store_strict_rejects(make_provider, archive_mode, case):
    provider = make_provider(archive_mode=archive_mode)
    data, error = {
        "missing": (_grib((0, 6)), "has 2 fields, the query expands to 3; nothing"),
        "extra": (_grib((0, 6, 12, 18)), "has 4 fields, the query expands to 3"),
        "foreign": (
            _grib((0, 6, 18)),
            "message 3 of .* has step=18, not one of 0/6/12; nothing",
        ),
        "duplicate": (_grib((0, 6, 6)), r"duplicate fields \(messages 2 and 3\)"),
        "trailing-garbage": (_grib() + b"GARBAGE", "trailing non-GRIB bytes"),
        "text": (b"test", "is not GRIB"),
    }[case]
    with pytest.raises(WorkflowError, match=error):
        _store(provider, STORE_QUERY, data)
    assert _in_fdb(provider, STORE_QUERY) == 0  # nothing was archived (FR-STORE-003)


@needs_samples
@pytest.mark.parametrize("archive_mode", ["identifier", "native"])
@pytest.mark.parametrize(
    "wrong, error",
    [
        ({"expver": "0001"}, "has expver=0001, but the query has expver=0002"),
        ({"step": 18}, "has step=18, not one of 0/6/12"),
        ({"paramId": 165}, "has param=165, but the query has param=167"),
    ],
    ids=["expver", "step", "param"],
)
def test_store_precheck_rejects_wrong_key(make_provider, archive_mode, wrong, error):
    """A message contradicting the query is rejected in both modes, before archiving:
    natively it would be archived under its own keys (FR-STORE-003, FR-STORE-005)."""
    provider = make_provider(archive_mode=archive_mode)
    data = _grib((0, 6)) + _message(**wrong)
    with pytest.raises(WorkflowError, match=f"message 3 of .* {error}; nothing"):
        _store(provider, STORE_QUERY, data)
    assert _in_fdb(provider, STORE_QUERY) == 0
    assert not provider.backend.list({"class": "ea"})  # not even under its own keys


@needs_samples
def test_store_native_rejects_key_absent_from_message(make_provider):
    """Native archiving takes every key from the message, so a query key the message
    lacks cannot be honoured (FR-STORE-003, L-23); identifier mode labels it."""
    query = f"fdb://{EA2},step=0,quantile=1:10,param=167"
    provider = make_provider()  # native is the default
    with pytest.raises(
        WorkflowError,
        match=(
            "message 1 of .* lacks quantile, which native archiving takes from the "
            "message; use archive_mode=identifier"
        ),
    ):
        _store(provider, query, _grib((0,)))
    assert _in_fdb(provider, query) == 0
    labelled = make_provider(archive_mode="identifier")
    assert _store(labelled, query, _grib((0,))).exists() is True


@needs_samples
def test_no_schema_requires_no_key(make_provider):
    """Without a readable schema the plugin cannot tell which keys FDB indexes, so no
    key is required of a message or a field (FR-STORE-003, FR-READ-001, L-15)."""
    provider = make_provider()
    provider.schema_info = None  # as with a remote FDB whose schema is not local
    query = f"fdb://{EA2},step=0,quantile=1:10,param=167"
    assert _store(provider, query, _grib((0,))).exists() is True  # quantile dropped


@needs_samples
def test_exists_does_not_match_through_absent_key(make_provider):
    """``inspect`` matches through a key the fields do not have (L-22); such fields
    do not count (FR-READ-001)."""
    provider = make_provider()
    stored = _store(provider, STORE_QUERY, _grib())
    assert stored.exists() is True
    obj = provider.object(f"fdb://{EA2},step=0/6/12,quantile=1:10,param=167")
    request = obj.parsed.to_request()
    assert len(provider.backend.inspect(request)) == 3  # FDB matches through quantile
    assert not provider.backend.list(request)  # list does not
    assert obj.exists() is False
    with pytest.raises(WorkflowError, match="0 of 3 fields found in FDB"):
        obj.retrieve_object()


@needs_samples
def test_store_post_check_names_offending_messages(make_provider):
    """The pre-check skips ``to``/``by`` keys (FR-STORE-005), so a foreign step is
    archived natively and the post-check names the message the query cannot reach
    (FR-STORE-009)."""
    provider = make_provider()
    query = f"fdb://{EA2},step=0/to/12/by/6,param=167"
    with pytest.raises(WorkflowError, match="1 landed outside the query") as e:
        _store(provider, query, _grib((0, 6, 18)))
    assert "or are duplicates: message 3 (step=18) (they stay in FDB" in str(e.value)
    assert _in_fdb(provider, query) == 2


@needs_samples
def test_store_default_native_under_multi_rule_schema(make_provider):
    """Native is the default (ADR-009): identifier mode needs a value for every key
    that is mandatory in any rule of a multi-rule schema."""
    provider = make_provider(schema=ECMWF_SCHEMA)
    data = (SAMPLES / "synth11.grib").read_bytes()
    obj = _store(provider, SYNTH11_QUERY, data)
    assert obj.exists() is True
    obj.local_path().unlink()
    obj.retrieve_object()
    assert obj.local_path().read_bytes() == data  # unpadded single message

    identifier = make_provider(schema=ECMWF_SCHEMA, archive_mode="identifier")
    with pytest.raises(
        WorkflowError,
        match=r"cannot determine \w+ for message 1 of .*; nothing was archived",
    ):
        _store(identifier, SYNTH11_QUERY, data)


@needs_samples
@pytest.mark.parametrize(
    "fields, messages, error",  # message 1 matches, message 2 contradicts the query
    [
        (
            "step=6,param=167/165",
            [(6, 167), (0, 165)],
            "step=0, but the query has step=6",
        ),
        (
            "step=0/6,param=165",
            [(0, 165), (6, 167)],
            "param=167, but the query has param=165",
        ),
    ],
    ids=["step", "param"],
)
def test_store_identifier_single_value_mismatch(make_provider, fields, messages, error):
    provider = make_provider(archive_mode="identifier")
    query = f"fdb://{EA2},{fields}"
    data = b"".join(_grib((step,), (param,)) for step, param in messages)
    with pytest.raises(
        WorkflowError, match=f"message 2 of .* has {error}; nothing was archived"
    ):
        _store(provider, query, data)
    assert _in_fdb(provider, query) == 0


@needs_samples
@pytest.mark.parametrize(
    "key, given, canonical",
    [
        ("param", "167", "167"),
        ("param", "167.128", "167"),  # FDB stores it verbatim (architecture.md §13.5)
        ("time", "0", "0000"),  # FDB would reject it as not canonical
        ("time", "00", "0000"),
    ],
)
def test_store_identifier_archives_canonical_spelling(
    make_provider, key, given, canonical
):
    # the GRIB says param=167.128 (paramId 167), time=0000: the pre-check passes and
    # the identifier uses the canonical spelling (FR-STORE-006)
    provider = make_provider(archive_mode="identifier")
    canonical_query = f"fdb://{EA2},step=0,param=167"
    query = canonical_query.replace(f"{key}={canonical}", f"{key}={given}")
    _store(provider, query, _grib((0,)))  # includes the post-check
    assert _stored_key(provider)[key] == canonical
    obj = provider.object(canonical_query)
    assert obj.exists() is True
    obj.retrieve_object()
    (message,) = split_messages(obj.local_path())
    assert (message.param_id, message.mars["time"]) == ("167", "0000")


@needs_samples
def test_store_identifier_verbatim_without_expansion(
    make_provider, monkeypatch, caplog
):
    provider = make_provider(archive_mode="identifier")
    monkeypatch.setattr(provider.backend, "expand", lambda request: None)
    with caplog.at_level(logging.DEBUG, logger="fdb-test"):
        obj = _store(provider, f"fdb://{EA2},step=0,param=167.128", _grib((0,)))
    assert obj.exists() is True
    assert _stored_key(provider)["param"] == "167.128"
    assert "archived verbatim" in caplog.text


@needs_samples
def test_store_post_check_uses_fdb_clock(make_provider, monkeypatch):
    # FDB's clock can lag int(time.time()) by a second (architecture.md §8.7): with
    # both the clock and the index timestamps at a past second, the store passes its
    # post-check only if t_start is taken from fdb_time() (FR-STORE-009)
    provider = make_provider(archive_mode="identifier")
    stamp = 1_700_000_000  # below int(time.time()) for good
    real_inspect = provider.backend.inspect
    monkeypatch.setattr(backend_module, "_c_time", lambda _: stamp)
    monkeypatch.setattr(
        provider.backend,
        "inspect",
        lambda request: [replace(f, timestamp=stamp) for f in real_inspect(request)],
    )
    _store(provider, f"fdb://{EA2},step=0,param=167", _grib((0,)))


def test_fdb_time_is_c_time(monkeypatch):
    assert backend_module._c_time is not None  # libc time() loaded, no fallback
    assert abs(fdb_time() - time.time()) < 2
    monkeypatch.setattr(time, "time", lambda: 1_800_000_000.9)
    monkeypatch.setattr(backend_module, "_c_time", lambda _: 1_799_999_999)
    assert fdb_time() == 1_799_999_999  # libc's second, not int(time.time())
    monkeypatch.setattr(backend_module, "_c_time", None)  # no loadable libc
    assert fdb_time() == 1_800_000_000


@needs_samples
def test_store_identifier_key_absent_from_message_takes_query_value(make_provider):
    # the variants carry no quantile (an optional key of tests/data/schema)
    provider = make_provider(archive_mode="identifier")
    query = f"fdb://{EA2},step=0,quantile=1:10,param=167"
    obj = _store(provider, query, _grib((0,)))
    assert obj.exists() is True
    assert _stored_key(provider)["quantile"] == "1:10"


@needs_samples
def test_store_wildcard_query_rejected(make_provider):
    with pytest.raises(WorkflowError, match="unresolved wildcards"):
        _store(make_provider(), f"fdb://{EA2},step={{step}},param=167", _grib((0,)))


@needs_samples
def test_store_partial_archive_failure_says_fields_stay(make_provider, monkeypatch):
    provider = make_provider(archive_mode="identifier")  # one archive call per message
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


@needs_samples
def test_store_guard_sees_every_message_before_archive(make_provider, monkeypatch):
    provider = make_provider(archive_mode="identifier")  # the guard runs only there
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


@needs_samples
def test_store_guard_mismatch_leaves_fdb_unchanged(make_provider, monkeypatch):
    provider = make_provider(archive_mode="identifier")
    provider.guard = RecordingGuard([], fail_on=2)
    archived = []
    monkeypatch.setattr(provider.backend, "archive", lambda *a: archived.append(a))
    with pytest.raises(
        WorkflowError, match="identifier check failed.*message 2: identifier step=6"
    ):
        _store(provider, STORE_QUERY, _grib())
    assert not archived
    assert _in_fdb(provider, STORE_QUERY) == 0


@needs_samples
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


@needs_samples
def test_store_threads(make_provider):
    provider = make_provider()
    steps = (0, 6, 12, 18)

    def store(step: int) -> bool:
        query = f"fdb://{EA2},step={step},param=167"
        return _store(provider, query, _grib((step,))).exists()

    with ThreadPoolExecutor(max_workers=len(steps)) as pool:
        assert list(pool.map(store, steps)) == [True] * len(steps)
    assert _in_fdb(provider, f"fdb://{EA2},step=0/6/12/18,param=167") == 4


@needs_samples
class TestStorageWrite(FDBStorageBase):
    """The base store sequence on an empty FDB: store, delete the local copy, exists,
    mtime, size, checksum, inventory, retrieve, remove (a no-op with a warning,
    FR-REMOVE-001). ``TestStorageBase`` writes the text ``test`` before ``store_object``
    (architecture.md §8.9), so the object replaces it with GRIB for the query first."""

    __test__ = True

    def _get_obj(self, tmp_path, query):
        obj = super()._get_obj(tmp_path, query)

        def store_object():
            obj.local_path().write_bytes(_grib())
            StorageObject.store_object(obj)

        obj.store_object = store_object
        return obj

    def get_query(self, tmp_path) -> str:
        return STORE_QUERY

    def get_query_not_existing(self, tmp_path) -> str:
        return STORE_QUERY  # the FDB is empty


@needs_samples
def test_managed_wrappers_without_rate_limiter(make_provider):
    # the managed_* coroutines Snakemake calls pass through the no-op rate limiter
    # and return what the plain methods return (values: test_store_roundtrip etc.)
    provider = make_provider()
    obj = provider.object(STORE_QUERY)
    assert obj.print_query == obj.query  # safe_print is the identity
    with pytest.raises(FileOrDirectoryNotFoundError):
        asyncio.run(obj.managed_mtime())  # nothing stored yet
    data = _grib()
    _write_local(obj, data)
    asyncio.run(obj.managed_store())
    obj.local_path().unlink()
    for name in ("exists", "mtime", "size", "local_footprint", "checksum"):
        assert asyncio.run(getattr(obj, f"managed_{name}")()) == getattr(obj, name)()
    asyncio.run(obj.managed_retrieve())
    assert obj.local_path().read_bytes() == data
    asyncio.run(obj.managed_remove())  # the warning: test_remove_policy


@pytest.mark.parametrize("policy", ["warn", "ignore", "error"])
def test_remove_policy(make_provider, caplog, policy):
    provider = make_provider(remove_policy=policy)
    query = f"fdb://{EA2},step=0,param=167"
    obj = provider.object(query)
    # nothing of this query is in the provider's empty FDB (FR-REMOVE-002)
    if policy == "error":
        with pytest.raises(WorkflowError, match="remove_policy=error: FDB storage:"):
            obj.remove()
        return
    with caplog.at_level(logging.INFO):
        obj.remove()
        provider.object(query).remove()  # said once per query
    assert not _warnings(caplog)
    infos = [r.getMessage() for r in caplog.records if r.levelno == logging.INFO]
    if policy == "ignore":
        assert not infos
        return
    assert infos == [f"FDB storage: nothing to remove: no field of {query} is in FDB."]


# --- glob (requirements.md §2.8) ------------------------------------------------------


def _glob(obj: StorageObject) -> dict[str, list[str]]:
    """Snakemake's own ``glob_wildcards`` on a pattern (architecture.md §13.8)."""
    return glob_wildcards(flag(obj.query, "storage_object", obj))._asdict()


@needs_samples
@pytest.mark.parametrize("step", ["{step}", "{step,\\d+}"], ids=["plain", "constraint"])
def test_glob_step_candidates_match_pattern(seeded_provider, step):
    obj = seeded_provider().object(f"fdb://param=167,{EA},step={step}")
    candidates = obj.list_candidate_matches()
    assert candidates == [f"fdb://{EA},step={s},param=167" for s in ("0", "12", "6")]
    # glob_wildcards keeps the candidates matching regex_from_filepattern(obj.query)
    assert _glob(obj)["step"] == ["0", "12", "6"]


@needs_samples
def test_glob_keys_absent_from_pattern_collapse(seeded_provider):
    # param and stream are wildcards for list (architecture.md §13.4): 6 oper variants
    # and the enda template give three candidates
    obj = seeded_provider().object("fdb://class=ea,step={step}")
    assert obj.list_candidate_matches() == [
        "fdb://class=ea,step=0",
        "fdb://class=ea,step=12",
        "fdb://class=ea,step=6",
    ]


@needs_samples
def test_glob_skips_fields_without_the_wildcard_key(seeded_provider):
    # only the enda template carries number; the oper variants lack it
    obj = seeded_provider().object("fdb://class=ea,step=0,param=167,number={n}")
    assert obj.list_candidate_matches() == ["fdb://class=ea,step=0,number=0,param=167"]
    assert _glob(obj) == {"n": ["0"]}


@needs_samples
def test_glob_wildcard_inside_value_and_constant_list(seeded_provider):
    pattern = EA.replace("date=20200101", "date={year}0101")
    obj = seeded_provider().object(f"fdb://{pattern},step=0/6,param={{p}}")
    # the constant step list is copied verbatim: 4 fields give 2 candidates
    assert _glob(obj) == {"year": ["2020", "2020"], "p": ["165", "167"]}


@needs_samples
@pytest.mark.parametrize(
    "settings, query, missing",
    [
        ({}, "fdb://class={c},step={step}", "class"),
        ({}, "fdb://expver=0001,step={step}", "class"),
        ({"glob_required_keys": "class,expver"}, "fdb://class=ea,step={s}", "expver"),
    ],
    ids=["wildcard", "absent", "setting"],
)
def test_glob_required_keys_enforced(seeded_provider, settings, query, missing):
    obj = seeded_provider(**settings).object(query)
    with pytest.raises(
        WorkflowError, match=f"needs constant values for {missing} \\(glob_required"
    ):
        obj.list_candidate_matches()


@needs_samples
def test_glob_required_keys_empty_allows_any_pattern(seeded_provider):
    obj = seeded_provider(glob_required_keys="").object("fdb://class={c},stream=enda")
    assert obj.list_candidate_matches() == ["fdb://class=ea,stream=enda"]


@needs_samples
def test_glob_invalid_value(seeded_provider):
    obj = seeded_provider().object("fdb://class=zz,step={s}")
    with pytest.raises(WorkflowError, match="Invalid MARS request"):
        obj.list_candidate_matches()


@needs_samples
def test_glob_retries_transient_list_error(seeded_provider, monkeypatch):
    provider = seeded_provider()
    calls = _failing(monkeypatch, provider, "list")
    assert len(provider.object(f"fdb://{EA},step={{s}}").list_candidate_matches()) == 3
    assert len(calls) == 2
