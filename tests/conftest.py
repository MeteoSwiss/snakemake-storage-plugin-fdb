"""Shared fixtures: ECMWF samples and temporary toc FDBs (spec §9.1, §9.4).

``ECKIT_EXCEPTION_IS_SILENT`` is defaulted before any test module imports the plugin,
so pyfdb loads with it. FDBs live under pytest's temporary directories, never ``.fdb/``.
"""

import os

os.environ.setdefault("ECKIT_EXCEPTION_IS_SILENT", "1")

import time  # noqa: E402
from collections.abc import Callable  # noqa: E402
from dataclasses import dataclass  # noqa: E402
from pathlib import Path  # noqa: E402
from typing import Any  # noqa: E402

import pytest  # noqa: E402

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


@pytest.fixture
def empty_fdb(tmp_path) -> Callable[..., Any]:
    """Factory for a fresh ``Backend`` on an empty FDB (optionally another schema)."""
    from snakemake_storage_plugin_fdb.backend import Backend

    def make(schema: Path = TEST_SCHEMA) -> Backend:
        return Backend(fdb_config(tmp_path / "fdb", schema))

    return make
