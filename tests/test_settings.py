"""Provider settings: fields, validation, guard selection and environment precedence.

No FDB is used: providers get an inline config for a temp FDB that is never opened.
Every test runs under ``clean_env``, so exported variables are restored afterwards.
"""

import logging
import os
import typing
from dataclasses import fields
from pathlib import Path

import pytest
from snakemake_interface_common.exceptions import WorkflowError

from snakemake_storage_plugin_fdb import LANGUAGE_FILE, StorageProviderSettings
from snakemake_storage_plugin_fdb.guard import (
    IdentifierGuard,
    IdentifierMismatch,
    NoGuard,
    StrictGuard,
    make_guard,
)
from snakemake_storage_plugin_fdb.query import parse

# reference.md settings table: name -> default
SETTINGS = {
    "config": None,
    "user_config": None,
    "archive_mode": "native",  # ADR-009
    "identifier_check": "none",
    "canonical_spelling": "warn",
    "remove_policy": "warn",
    "input_tracking": "lookup",  # FR-RERUN-002
    "glob_required_keys": "class",
    "eccodes_definitions": None,
    "metkit_home": None,
    "key_order": None,
    "env": None,
}

# reference.md "Environment variables": settings the interface reads from
# SNAKEMAKE_STORAGE_FDB_<NAME> (FR-CONF-008).
ENV_VAR_SETTINGS = {
    "config",
    "user_config",
    "eccodes_definitions",
    "metkit_home",
    "key_order",
    "env",
    "glob_required_keys",
}


def _metkit_home(path: Path) -> Path:
    (path / LANGUAGE_FILE).parent.mkdir(parents=True)
    (path / LANGUAGE_FILE).write_text("")
    return path


def _dirs(tmp_path: Path, *names: str) -> list[str]:
    out = []
    for name in names:
        (tmp_path / name).mkdir()
        out.append(str(tmp_path / name))
    return out


# --- fields -----------------------------------------------------------------------


def test_settings_fields():
    own = {f.name: f for f in fields(StorageProviderSettings)}
    own.pop("max_requests_per_second")  # inherited, unused
    assert {name: f.default for name, f in own.items()} == SETTINGS
    hints = typing.get_type_hints(StorageProviderSettings)
    for name, f in own.items():
        assert hints[name] == typing.Optional[str], name  # noqa: UP045
        assert f.metadata.get("help"), name
        for key in ("nargs", "parse_func"):
            assert key not in f.metadata, (name, key)
        assert bool(f.metadata.get("env_var")) == (name in ENV_VAR_SETTINGS), name


def test_settings_help_names_the_default():
    """FR-CONF-001: a plugin setting's help text ends with ``(default: ...)``."""
    for f in fields(StorageProviderSettings):
        if f.name == "max_requests_per_second":  # inherited, no default of ours
            continue
        assert f"(default: {f.default or 'unset'}" in f.metadata["help"], f.name
        assert f.metadata["help"].endswith(")"), f.name


def test_settings_defaults_construct(make_provider):
    provider = make_provider()
    assert provider.archive_mode == "native"
    assert provider.canonical_spelling == "warn"
    assert provider.remove_policy == "warn"
    assert provider.glob_required_keys == ("class",)
    assert provider.input_tracking == "lookup"
    assert isinstance(provider.guard, NoGuard)


def test_settings_none_means_default(make_provider):
    provider = make_provider(archive_mode=None, remove_policy=None)
    assert provider.archive_mode == "native"
    assert provider.remove_policy == "warn"


@pytest.mark.parametrize(
    "name, value",
    [
        ("archive_mode", "copy"),
        ("canonical_spelling", "fix"),
        ("remove_policy", "wipe"),
        ("identifier_check", "loose"),
        ("input_tracking", "text"),
    ],
)
def test_settings_invalid_choice(make_provider, name, value):
    with pytest.raises(WorkflowError, match=f"invalid {name} '{value}'"):
        make_provider(**{name: value})


def test_settings_other_choices(make_provider):
    provider = make_provider(
        archive_mode="identifier",
        canonical_spelling="error",
        remove_policy="ignore",
        glob_required_keys=" class , Stream ",
    )
    assert provider.archive_mode == "identifier"
    assert (provider.canonical_spelling, provider.remove_policy) == ("error", "ignore")
    assert provider.glob_required_keys == ("class", "stream")
    assert make_provider(glob_required_keys="").glob_required_keys == ()


@pytest.mark.parametrize(
    "name, value, message",
    [
        ("glob_required_keys", "class,1x", "invalid key name.*1x"),
        ("key_order", "date,,time", "invalid key_order"),
        ("key_order", "date,da-te", "invalid key_order"),
        ("config", "- not\n- a mapping\n", "FDB configuration error"),
        ("user_config", "/no/such/user_config.yaml", "FDB configuration error"),
    ],
)
def test_settings_invalid_values(make_provider, name, value, message):
    with pytest.raises(WorkflowError, match=message):
        make_provider(**{name: value})


# --- guard ------------------------------------------------------------------------


def test_identifier_check_strict_is_reserved(make_provider):
    with pytest.raises(WorkflowError, match="reserved") as e:
        make_provider(identifier_check="strict")
    assert "identifier_check=strict" in str(e.value)


def test_guard_hook():
    guard = NoGuard()
    assert isinstance(guard, IdentifierGuard)
    query = parse("fdb://class=od,step=7,param=167")
    assert guard.check(object(), {"step": "7"}, query) is None
    with pytest.raises(NotImplementedError, match="reserved"):
        StrictGuard()
    assert isinstance(make_guard(StorageProviderSettings()), NoGuard)
    assert isinstance(make_guard(None), NoGuard)
    with pytest.raises(NotImplementedError):
        make_guard(StorageProviderSettings(identifier_check="strict"))
    with pytest.raises(ValueError, match="unknown identifier_check"):
        make_guard(StorageProviderSettings(identifier_check="loose"))


def test_guard_identifier_mismatch():
    e = IdentifierMismatch(2, "step", "7", "1")
    assert isinstance(e, ValueError)
    assert (e.message_index, e.key, e.identifier_value, e.grib_value) == (
        2,
        "step",
        "7",
        "1",
    )
    assert "message 2" in str(e) and "step=7" in str(e) and "step=1" in str(e)


# --- environment precedence (architecture.md §8.3) ------------------------------------


def test_settings_eccodes_definitions_prepend(make_provider, clean_env, tmp_path):
    a, b = _dirs(tmp_path, "a", "b")
    clean_env.setenv("ECCODES_DEFINITION_PATH", "/x")
    make_provider(eccodes_definitions=f"{a}:{b}")
    assert _env("ECCODES_DEFINITION_PATH") == f"{a}:{b}:/x"
    # idempotent: a second provider (or a spawned job inheriting it) adds nothing
    make_provider(eccodes_definitions=f"{a}:{b}")
    assert _env("ECCODES_DEFINITION_PATH") == f"{a}:{b}:/x"


def test_settings_eccodes_definitions_unset_env(make_provider, tmp_path):
    (a,) = _dirs(tmp_path, "a")
    make_provider(eccodes_definitions=f"{a}::/MEMFS/definitions")
    assert _env("ECCODES_DEFINITION_PATH") == f"{a}:/MEMFS/definitions"


def test_settings_eccodes_definitions_relative(make_provider, clean_env, tmp_path):
    _dirs(tmp_path, "defs")
    clean_env.chdir(tmp_path)
    make_provider(eccodes_definitions="defs")
    assert _env("ECCODES_DEFINITION_PATH") == str(tmp_path / "defs")


def test_settings_no_env_settings_leave_env_untouched(
    make_provider, clean_env, tmp_path
):
    home = _metkit_home(tmp_path / "mk")
    clean_env.setenv("ECCODES_DEFINITION_PATH", "/x")
    clean_env.setenv("METKIT_HOME", str(home))
    make_provider()
    assert _env("ECCODES_DEFINITION_PATH") == "/x"
    assert _env("METKIT_HOME") == str(home)


def test_settings_eccodes_definitions_missing_dir(make_provider, clean_env, tmp_path):
    (a,) = _dirs(tmp_path, "a")
    clean_env.setenv("ECCODES_DEFINITION_PATH", "/x")
    missing = str(tmp_path / "missing")
    with pytest.raises(WorkflowError, match="not an existing directory") as e:
        make_provider(eccodes_definitions=f"{a}:{missing}")
    assert missing in str(e.value)
    assert _env("ECCODES_DEFINITION_PATH") == "/x"


def test_settings_metkit_home_wins(make_provider, clean_env, tmp_path, caplog):
    old = _metkit_home(tmp_path / "old")
    new = _metkit_home(tmp_path / "new")
    clean_env.setenv("METKIT_HOME", str(old))
    with caplog.at_level(logging.INFO, logger="fdb-test"):
        make_provider(metkit_home=str(new))
    assert _env("METKIT_HOME") == str(new)
    assert "overrides METKIT_HOME" in caplog.text


def test_settings_metkit_home_without_language(make_provider, clean_env, tmp_path):
    (a,) = _dirs(tmp_path, "defs")
    (tmp_path / "mk" / "share" / "metkit").mkdir(parents=True)
    with pytest.raises(WorkflowError, match="language.yaml") as e:
        make_provider(metkit_home=str(tmp_path / "mk"), eccodes_definitions=a)
    assert "from metkit_home" in str(e.value)
    # validation happens before anything is exported
    assert _env("METKIT_HOME") is None
    assert _env("ECCODES_DEFINITION_PATH") is None
    assert _env("ECKIT_EXCEPTION_IS_SILENT") is None


@pytest.mark.parametrize("via_env_setting", [False, True])
def test_settings_invalid_metkit_home_from_env(
    make_provider, clean_env, tmp_path, via_env_setting
):
    bad = str(tmp_path / "empty")
    Path(bad).mkdir()
    if via_env_setting:
        source, kwargs = "the env setting", {"env": f"METKIT_HOME={bad}"}
    else:
        clean_env.setenv("METKIT_HOME", bad)
        source, kwargs = "the environment", {}
    with pytest.raises(WorkflowError, match=f"from {source}"):
        make_provider(**kwargs)


def test_settings_config_leaves_fdb_env_untouched(make_provider, clean_env):
    values = {
        "FDB_CONFIG": "type: local",
        "FDB_CONFIG_FILE": "/elsewhere/config.yaml",
        "FDB_HOME": "/elsewhere",
    }
    for name, value in values.items():
        clean_env.setenv(name, value)
    make_provider()  # explicit inline config
    assert {name: _env(name) for name in values} == values
    assert _env("FDB5_CONFIG") is None


def test_settings_env_overrides(make_provider, clean_env, tmp_path):
    clean_env.setenv("FDB_HOME", "/x")
    clean_env.delenv("SMK_FDB_TEST_DUMMY", raising=False)
    make_provider(env="FDB_HOME=/y, SMK_FDB_TEST_DUMMY=a=b,ECKIT_EXCEPTION_IS_SILENT=0")
    assert _env("FDB_HOME") == "/y"
    assert _env("SMK_FDB_TEST_DUMMY") == "a=b"
    assert _env("ECKIT_EXCEPTION_IS_SILENT") == "0"  # env setting beats the default


def test_settings_eckit_silent_default(make_provider, clean_env):
    make_provider()
    assert _env("ECKIT_EXCEPTION_IS_SILENT") == "1"
    clean_env.setenv("ECKIT_EXCEPTION_IS_SILENT", "0")
    make_provider()
    assert _env("ECKIT_EXCEPTION_IS_SILENT") == "0"


def test_settings_env_setting_then_dedicated_settings(make_provider, tmp_path):
    (a,) = _dirs(tmp_path, "a")
    home = _metkit_home(tmp_path / "mk")
    make_provider(
        env="ECCODES_DEFINITION_PATH=/x,METKIT_HOME=/nowhere",
        eccodes_definitions=a,
        metkit_home=str(home),
    )
    assert _env("ECCODES_DEFINITION_PATH") == f"{a}:/x"
    assert _env("METKIT_HOME") == str(home)


@pytest.mark.parametrize(
    "value", ["FDB_HOME", "=x", "1X=y", "FDB_HOME=/a,FDB_HOME=/b", "A=1,,B=2"]
)
def test_settings_env_syntax(make_provider, value):
    with pytest.raises(WorkflowError, match="invalid env setting"):
        make_provider(env=value)


def test_settings_different_definitions_warn(make_provider, tmp_path, caplog):
    a, b = _dirs(tmp_path, "a", "b")
    make_provider(eccodes_definitions=a)
    with caplog.at_level(logging.WARNING, logger="fdb-test"):
        make_provider(eccodes_definitions=b)
    assert "different ECCODES_DEFINITION_PATH settings" in caplog.text
    assert _env("ECCODES_DEFINITION_PATH") == f"{b}:{a}"


def _env(name: str) -> str | None:
    return os.environ.get(name)
