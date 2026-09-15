"""Read path on the OGD ICON-CH2-EPS samples under the varda schema (plan step 5).

The samples are archived natively into a temporary FDB and read back through the
plugin's provider and storage objects. Everything that touches FDB runs in a
subprocess: the definitions must be active before eccodes loads and metkit reads its
MARS language once per process. The site is configured only through generic means,
either the ``eccodes_definitions``/``metkit_home`` settings or the plain
``ECCODES_DEFINITION_PATH``/``METKIT_HOME`` variables.
"""

import hashlib

import pytest

pytestmark = pytest.mark.site_meteoswiss

SCRIPT = """
import hashlib, json, logging, sys, time
from pathlib import Path
from snakemake_storage_plugin_fdb import StorageProvider, StorageProviderSettings
from snakemake_storage_plugin_fdb.backend import fdb_time
from snakemake_storage_plugin_fdb.grib import split_messages

job = json.loads(sys.argv[1])
out = {"archived": [], "results": {}, "warnings": []}


class Collect(logging.Handler):
    def emit(self, record):
        out["warnings"].append(record.getMessage())


logger = logging.getLogger("site")
logger.addHandler(Collect(logging.WARNING))
logger.propagate = False
provider = StorageProvider(
    local_prefix=Path(job["local_prefix"]),
    logger=logger,
    settings=StorageProviderSettings(**job["settings"]),
)
if job.get("archive"):
    start = fdb_time()
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


@pytest.fixture(scope="module")
def site_fdb(
    tmp_path_factory,
    run_site,
    mch_fdb_config,
    mch_sample,
    mch_query_base,
    eccodes_definitions,
    metkit_home,
):
    """Temp FDB (config file) with the four sample fields archived natively, and
    the queries addressing them (``t2m_pf_1to3`` and ``tp_cf_no_timespan`` match
    nothing: member 3 is absent, accumulations need ``timespan=fs``)."""
    tmp = tmp_path_factory.mktemp("mch-fdb")
    files = {name: mch_sample(pattern) for name, pattern in SAMPLES.items()}
    settings = {
        "config": str(mch_fdb_config(tmp)),
        "eccodes_definitions": eccodes_definitions,
        "metkit_home": str(metkit_home),
    }
    job = {
        "settings": {**settings, "archive_mode": "native"},
        "archive": [str(p) for p in files.values()],
    }
    out = run_site(SCRIPT, job, tmp)
    assert len(out["archived"]) == 4
    base = "fdb://" + mch_query_base(files["t2m_cf"])
    return {
        "settings": settings,
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


@pytest.fixture
def read_site(site_fdb, run_site, tmp_path):
    """``read(queries, settings=site settings, env=None)``: the script's output
    (``results`` per query name and the ``warnings`` logged)."""

    def read(queries: dict, settings: dict | None = None, env=None) -> dict:
        job = {"settings": settings or site_fdb["settings"], "queries": queries}
        return run_site(SCRIPT, job, tmp_path, env)

    return read


@pytest.mark.parametrize("configured_by", ["settings", "env"])
def test_read_samples(site_fdb, read_site, site_env, metkit_home, configured_by):
    if configured_by == "settings":
        results = read_site(site_fdb["queries"])["results"]
    else:  # plain environment variables, no site settings at all
        settings = {"config": site_fdb["settings"]["config"]}
        env = {**site_env, "METKIT_HOME": str(metkit_home)}
        results = read_site(site_fdb["queries"], settings, env)["results"]
    start, end = site_fdb["flush"]
    for name, path in site_fdb["files"].items():
        res = results[name]
        assert res.get("exists") is True, (name, res)
        data = path.read_bytes()
        assert res["size"] == len(data)
        assert res["sha256"] == hashlib.sha256(data).hexdigest()  # byte-identical
        assert start <= res["mtime"] <= end
    assert results["t2m_pf_1to3"] == {"exists": False}
    assert results["tp_cf_no_timespan"] == {"exists": False}


def test_read_model_requires_metkit_home(site_fdb, read_site):
    settings = {k: v for k, v in site_fdb["settings"].items() if k != "metkit_home"}
    query = site_fdb["queries"]["t2m_cf"]
    error = read_site({"t2m_cf": query}, settings)["results"]["t2m_cf"].get("error", "")
    assert error.startswith("WorkflowError: Invalid MARS request"), error
    assert "icon-ch2-eps" in error
    assert "metkit_home" in error  # the mapped hint


def test_number_context(site_fdb, read_site):
    # metkit accepts number only with type=pf (spec §8)
    query = site_fdb["queries"]["t2m_cf"] + ",number=0"
    error = read_site({"cf": query})["results"]["cf"].get("error", "")
    assert error.startswith("WorkflowError: Invalid MARS request"), error
    assert "number" in error


def test_canonical_spelling_mch(site_fdb, read_site):
    query = (
        site_fdb["queries"]["t2m_cf"]
        .replace("model=icon-ch2-eps", "model=ICON-CH2-EPS")
        .replace("param=500011", "param=T_2M")
    )
    out = read_site({"q": query})
    assert out["results"]["q"].get("exists") is True, out["results"]  # same field
    (warning,) = [w for w in out["warnings"] if "non-canonical spelling" in w]
    assert "model=ICON-CH2-EPS" in warning and "icon-ch2-eps" in warning, warning
    assert "param=T_2M" in warning and "500011" in warning, warning
