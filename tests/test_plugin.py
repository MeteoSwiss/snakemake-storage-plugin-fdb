"""Provider and storage object (provider part: plan step 4).

Read, write and glob tests are added in plan steps 5-7.
"""

import copy
import subprocess
import sys
from pathlib import Path

import pytest
import yaml
from snakemake.io import IOFile, apply_wildcards, flag
from snakemake_interface_common.exceptions import WorkflowError

from snakemake_storage_plugin_fdb import StorageObject, StorageProvider
from snakemake_storage_plugin_fdb.backend import map_error
from snakemake_storage_plugin_fdb.query import NAME_MAX

TEST_SCHEMA = Path(__file__).resolve().parent / "data" / "schema"
BASE = "class=od,expver=0001,stream=oper,date=20240101,time=0000,type=fc,levtype=sfc"
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
