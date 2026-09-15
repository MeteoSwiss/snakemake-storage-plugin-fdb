"""Shared fixtures: ECMWF samples and temporary toc FDBs (spec §9.1, §9.4).

``ECKIT_EXCEPTION_IS_SILENT`` is defaulted before any test module imports the plugin,
so pyfdb loads with it. FDBs live under pytest's temporary directories, never ``.fdb/``.
"""

import os

os.environ.setdefault("ECKIT_EXCEPTION_IS_SILENT", "1")

import logging  # noqa: E402
import time  # noqa: E402
from collections.abc import Callable  # noqa: E402
from dataclasses import dataclass  # noqa: E402
from pathlib import Path  # noqa: E402
from typing import Any  # noqa: E402

import pytest  # noqa: E402
import yaml  # noqa: E402

REPO = Path(__file__).resolve().parents[1]
RAW = REPO / ".raw"
DATA = Path(__file__).resolve().parent / "data"
TEST_SCHEMA = DATA / "schema"
RAW_FILES = ("template.grib", "steprange.grib", "quantile.grib", "synth11.grib")
VARIANT_STEPS = (0, 6, 12)
VARIANT_PARAMS = (167, 165)


def fdb_config(root: Path, schema: Path = TEST_SCHEMA) -> dict[str, Any]:
    """Local toc FDB config with its database root under ``root``."""
    (root / "db").mkdir(parents=True, exist_ok=True)
    return {
        "type": "local",
        "engine": "toc",
        "schema": str(schema),
        "spaces": [{"handler": "Default", "roots": [{"path": str(root / "db")}]}],
    }


@pytest.fixture(scope="session")
def fdb_config_file() -> Callable[..., Path]:
    """Factory: ``fdb_config(root, schema)`` written to ``root/config.yaml``."""

    def write(root: Path, schema: Path = TEST_SCHEMA) -> Path:
        config = root / "config.yaml"
        config.write_text(yaml.safe_dump(fdb_config(root, schema)))
        return config

    return write


@dataclass
class SeededFDB:
    config: dict[str, Any]
    backend: Any  # snakemake_storage_plugin_fdb.backend.Backend
    flush_start: float
    flush_end: float


@pytest.fixture(scope="session")
def seeded_fdb(tmp_path_factory) -> SeededFDB:
    """Session FDB under ``tests/data/schema``: the four ``.raw`` files archived
    natively plus zeroed ``class=ea,stream=oper`` variants of ``template.grib`` for
    ``step`` 0/6/12 x ``param`` 167/165 (about 12 KB in total).
    """
    if not (RAW / "template.grib").exists():
        pytest.skip("no .raw/ ECMWF samples")
    from snakemake_storage_plugin_fdb.backend import Backend
    from snakemake_storage_plugin_fdb.grib import variant

    config = fdb_config(tmp_path_factory.mktemp("seeded-fdb"))
    backend = Backend(config)
    for name in RAW_FILES:
        backend.archive((RAW / name).read_bytes())
    template = (RAW / "template.grib").read_bytes()
    for step in VARIANT_STEPS:
        for param in VARIANT_PARAMS:
            backend.archive(variant(template, stream="oper", step=step, paramId=param))
    start = time.time()
    backend.flush()
    return SeededFDB(config, backend, start, time.time())


# Variables the provider reads or exports (spec §4.1); restored after each test.
PROVIDER_ENV_VARS = (
    "ECCODES_DEFINITION_PATH",
    "METKIT_HOME",
    "ECKIT_EXCEPTION_IS_SILENT",
    "FDB_CONFIG",
    "FDB5_CONFIG",
    "FDB_CONFIG_FILE",
    "FDB5_CONFIG_FILE",
    "FDB_HOME",
    "FDB_SCHEMA_FILE",
)


@pytest.fixture
def clean_env(monkeypatch) -> pytest.MonkeyPatch:
    """Unset the provider's environment variables; monkeypatch restores them.

    Also resets the plugin's per-process records (applied settings, queries warned
    about spelling), so tests do not see each other's warnings.
    """
    import snakemake_storage_plugin_fdb as plugin

    for name in PROVIDER_ENV_VARS:
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setattr(plugin, "_APPLIED", {})
    monkeypatch.setattr(plugin, "_SPELLING_WARNED", set())
    monkeypatch.setattr(plugin, "_REMOVE_WARNED", set())
    return monkeypatch


@pytest.fixture
def make_provider(tmp_path, clean_env) -> Callable[..., Any]:
    """Factory for a ``StorageProvider`` in a clean environment.

    Without an explicit ``config`` it gets an inline YAML config for a temp FDB under
    ``schema``; pass ``config=None`` to use FDB's environment fallback.
    """
    from snakemake_storage_plugin_fdb import StorageProvider, StorageProviderSettings

    def make(schema: Path = TEST_SCHEMA, **settings: Any) -> StorageProvider:
        if "config" not in settings:
            settings["config"] = yaml.safe_dump(fdb_config(tmp_path / "fdb", schema))
        return StorageProvider(
            local_prefix=tmp_path / "local",
            logger=logging.getLogger("fdb-test"),
            settings=StorageProviderSettings(**settings),
        )

    return make


@pytest.fixture
def temp_fdb_config(tmp_path) -> dict[str, Any]:
    """Config of an empty toc FDB under ``tmp_path`` (``tests/data/schema``)."""
    return fdb_config(tmp_path / "fdb")


@pytest.fixture
def empty_fdb(tmp_path) -> Callable[..., Any]:
    """Factory for a fresh ``Backend`` on an empty FDB (optionally another schema)."""
    from snakemake_storage_plugin_fdb.backend import Backend

    def make(schema: Path = TEST_SCHEMA) -> Backend:
        return Backend(fdb_config(tmp_path / "fdb", schema))

    return make
