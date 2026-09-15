"""Read path on the OGD ICON-CH2-EPS samples under the varda schema (plan step 5).

The samples are archived natively into a temporary FDB and read back through the
plugin's provider and storage objects. Everything that touches FDB runs in a
subprocess: the definitions must be active before eccodes loads and metkit reads its
MARS language once per process. The site is configured only through generic means,
either the ``eccodes_definitions``/``metkit_home`` settings or the plain
``ECCODES_DEFINITION_PATH``/``METKIT_HOME`` variables.
"""

import hashlib
import json
import os
import subprocess
import sys
from pathlib import Path

import pytest
import yaml

pytestmark = pytest.mark.site_meteoswiss

SITE_ENV = ("ECCODES_DEFINITION_PATH", "METKIT_HOME")

SCRIPT = """
import hashlib, json, logging, sys, time
from pathlib import Path
from snakemake_storage_plugin_fdb import StorageProvider, StorageProviderSettings
from snakemake_storage_plugin_fdb.grib import split_messages

job = json.loads(sys.argv[1])
provider = StorageProvider(
    local_prefix=Path(job["local_prefix"]),
    logger=logging.getLogger("site"),
    settings=StorageProviderSettings(**job["settings"]),
)
out = {"archived": [], "results": {}}
if job.get("archive"):
    start = time.time()
    for path in job["archive"]:
        provider.backend.archive(Path(path).read_bytes())
        out["archived"] += [m.mars for m in split_messages(path)]
    provider.backend.flush()
    out["flush"] = [start, time.time()]
for name, query in job.get("queries", {}).items():
    res = out["results"][name] = {}
    try:
        obj = provider.object(query)
        res["exists"] = obj.exists()
        if res["exists"]:
            res["mtime"] = obj.mtime()
            res["size"] = obj.size()
            obj.retrieve_object()
            res["sha256"] = hashlib.sha256(obj.local_path().read_bytes()).hexdigest()
    except Exception as e:
        res["error"] = f"{type(e).__name__}: {e}"
print(json.dumps(out))
"""

SAMPLES = {
    "t2m_cf": "*_step6_t_2m_ctrl.grib2",
    "tp_cf": "*_step6_tot_prec_ctrl.grib2",
    "t2m_pf": "*_step6_t_2m_pert_m1-2.grib2",
}


def _run(job: dict, tmp: Path, env: dict[str, str] | None = None) -> dict:
    proc_env = {k: v for k, v in os.environ.items() if k not in SITE_ENV}
    proc_env.update(env or {})
    job = {"local_prefix": str(tmp / "local"), **job}
    proc = subprocess.run(
        [sys.executable, "-c", SCRIPT, json.dumps(job)],
        env=proc_env,
        capture_output=True,
        text=True,
        timeout=300,
    )
    if proc.returncode != 0:
        pytest.fail(f"site subprocess failed:\n{proc.stderr}", pytrace=False)
    return json.loads(proc.stdout.splitlines()[-1])


@pytest.fixture(scope="module")
def site_fdb(
    tmp_path_factory, mch_sample, mch_schema, eccodes_definitions, metkit_home
):
    """Temp FDB (config file) with the four sample fields archived natively, and
    the queries addressing them (``t2m_pf_1to3`` and ``tp_cf_no_timespan`` match
    nothing: member 3 is absent, accumulations need ``timespan=fs``)."""
    tmp = tmp_path_factory.mktemp("mch-fdb")
    (tmp / "db").mkdir()
    config = tmp / "config.yaml"
    config.write_text(
        yaml.safe_dump(
            {
                "type": "local",
                "engine": "toc",
                "schema": str(mch_schema),
                "spaces": [
                    {"handler": "Default", "roots": [{"path": str(tmp / "db")}]}
                ],
            }
        )
    )
    files = {name: mch_sample(pattern) for name, pattern in SAMPLES.items()}
    settings = {
        "config": str(config),
        "archive_mode": "native",
        "eccodes_definitions": eccodes_definitions,
        "metkit_home": str(metkit_home),
    }
    out = _run({"settings": settings, "archive": [str(p) for p in files.values()]}, tmp)
    assert len(out["archived"]) == 4
    first = out["archived"][0]
    base = (
        "fdb://class=od,expver=0001,stream=enfo,model=icon-ch2-eps,"
        f"date={first['date']},time={first['time']},levtype=sfc,step=6"
    )
    return {
        "config": str(config),
        "files": files,
        "flush": out["flush"],
        "queries": {
            "t2m_cf": f"{base},type=cf,param=500011",
            "tp_cf": f"{base},type=cf,timespan=fs,param=500041",
            "t2m_pf": f"{base},type=pf,number=1/2,param=500011",
            "t2m_pf_1to3": f"{base},type=pf,number=1/to/3,param=500011",
            "tp_cf_no_timespan": f"{base},type=cf,param=500041",
        },
    }


@pytest.mark.parametrize("configured_by", ["settings", "env"])
def test_read_samples(
    site_fdb, eccodes_definitions, metkit_home, tmp_path, configured_by
):
    if configured_by == "settings":
        settings = {
            "config": site_fdb["config"],
            "eccodes_definitions": eccodes_definitions,
            "metkit_home": str(metkit_home),
        }
        env = {}
    else:  # plain environment variables, no site settings at all
        settings = {"config": site_fdb["config"]}
        env = {
            "ECCODES_DEFINITION_PATH": eccodes_definitions,
            "METKIT_HOME": str(metkit_home),
        }
    job = {"settings": settings, "queries": site_fdb["queries"]}
    results = _run(job, tmp_path, env)["results"]
    start, end = site_fdb["flush"]
    for name, path in site_fdb["files"].items():
        res = results[name]
        assert res.get("exists") is True, (name, res)
        data = path.read_bytes()
        assert res["size"] == len(data)
        assert res["sha256"] == hashlib.sha256(data).hexdigest()  # byte-identical
        assert int(start) <= res["mtime"] <= end
    assert results["t2m_pf_1to3"] == {"exists": False}
    assert results["tp_cf_no_timespan"] == {"exists": False}


def test_read_model_requires_metkit_home(site_fdb, eccodes_definitions, tmp_path):
    settings = {
        "config": site_fdb["config"],
        "eccodes_definitions": eccodes_definitions,
    }
    job = {
        "settings": settings,
        "queries": {"t2m_cf": site_fdb["queries"]["t2m_cf"]},
    }
    error = _run(job, tmp_path)["results"]["t2m_cf"].get("error", "")
    assert error.startswith("WorkflowError: Invalid MARS request"), error
    assert "icon-ch2-eps" in error
    assert "metkit_home" in error  # the mapped hint
