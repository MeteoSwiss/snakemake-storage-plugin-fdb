"""Write path on the OGD ICON-CH2-EPS samples under the varda schema (plan step 6).

Each test stores samples through ``StorageObject.store_object`` into an empty
temporary FDB, in both archive modes, and reads them back through the plugin. As in
``test_read.py``, FDB access runs in a subprocess and the site is configured only
through the generic ``eccodes_definitions``/``metkit_home`` settings.
"""

import hashlib

import pytest

pytestmark = pytest.mark.site_meteoswiss

SCRIPT = """
import hashlib, json, logging, shutil, sys, time
from pathlib import Path
from snakemake_storage_plugin_fdb import StorageProvider, StorageProviderSettings

job = json.loads(sys.argv[1])
out = {"warnings": [], "results": {}}


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
for name, spec in job["stores"].items():
    res = out["results"][name] = {}
    obj = provider.object(spec["query"])
    request = obj.parsed.to_request()
    try:
        local = obj.local_path()
        local.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(spec["file"], local)
        res["t_start"] = int(time.time())
        obj.store_object()
        local.unlink()  # Snakemake may drop the local copy; read back from FDB
        res["exists"] = obj.exists()
        res["mtime"] = obj.mtime()
        res["size"] = obj.size()
        obj.retrieve_object()
        res["sha256"] = hashlib.sha256(local.read_bytes()).hexdigest()
        res["listed"] = [f.key for f in provider.backend.list(request)]
        for _ in range(spec.get("remove", 0)):
            obj.remove()
            res["exists_after_remove"] = obj.exists()
    except Exception as e:
        res["error"] = f"{type(e).__name__}: {e}"
        res["found"] = len(provider.backend.inspect(request))
print(json.dumps(out))
"""

MODES = ["native", "identifier"]


@pytest.fixture
def write_job(
    run_site,
    mch_fdb_config,
    mch_sample,
    mch_query_base,
    eccodes_definitions,
    metkit_home,
    tmp_path,
):
    """Runner: store ``{name: (sample pattern, query fields, extra)}`` into a fresh
    FDB; the query is the sample's constant keys plus ``fields``."""

    def run(archive_mode: str, stores: dict, **settings) -> dict:
        settings = {
            "config": str(mch_fdb_config(tmp_path / "fdb")),
            "archive_mode": archive_mode,
            "eccodes_definitions": eccodes_definitions,
            "metkit_home": str(metkit_home),
            **settings,
        }
        job_stores, files = {}, {}
        for name, (pattern, fields, extra) in stores.items():
            path = files[name] = mch_sample(pattern)
            query = f"fdb://{mch_query_base(path)},{fields}"
            job_stores[name] = {"query": query, "file": str(path), **extra}
        out = run_site(SCRIPT, {"settings": settings, "stores": job_stores}, tmp_path)
        return {"files": files, **out}

    return run


def _assert_round_trip(res: dict, path) -> None:
    assert "error" not in res, res.get("error")
    data = path.read_bytes()
    assert res["exists"] is True
    assert res["size"] == len(data)
    assert res["sha256"] == hashlib.sha256(data).hexdigest()  # byte-identical
    assert res["mtime"] >= res["t_start"]


@pytest.mark.parametrize("archive_mode", MODES)
def test_write_ctrl(write_job, archive_mode):
    out = write_job(
        archive_mode,
        {"t2m_cf": ("*_step6_t_2m_ctrl.grib2", "type=cf,param=500011", {})},
    )
    res = out["results"]["t2m_cf"]
    _assert_round_trip(res, out["files"]["t2m_cf"])
    (key,) = res["listed"]
    assert key["model"] == "icon-ch2-eps"
    assert key["number"] == ""
    assert key["timespan"] == "none"
    assert key["param"] == "500011"
    assert key["domain"] == ""  # removed by the schema's domain- (spec §2.8)


@pytest.mark.parametrize("archive_mode", MODES)
def test_write_members(write_job, archive_mode):
    fields = "type=pf,number=1/2,param=500011"
    out = write_job(
        archive_mode, {"t2m_pf": ("*_step6_t_2m_pert_m1-2.grib2", fields, {})}
    )
    res = out["results"]["t2m_pf"]
    _assert_round_trip(res, out["files"]["t2m_pf"])
    assert sorted(key["number"] for key in res["listed"]) == ["1", "2"]


@pytest.mark.parametrize("archive_mode", MODES)
def test_write_strict_rejects_foreign_member(write_job, archive_mode):
    fields = "type=pf,number=1/3,param=500011"  # the file holds members 1 and 2
    out = write_job(
        archive_mode, {"t2m_pf": ("*_step6_t_2m_pert_m1-2.grib2", fields, {})}
    )
    res = out["results"]["t2m_pf"]
    if archive_mode == "identifier":  # rejected before archiving
        assert "has number=2, not one of 1/3; nothing was archived" in res["error"]
        assert res["found"] == 0
    else:  # FDB derives the keys; the post-check finds member 2 outside the query
        assert "1 landed outside the query" in res["error"]
        assert res["found"] == 1


def test_write_identifier_param_mismatch(write_job):
    # single-valued query keys are checked against the GRIB (spec §7.7)
    fields = "type=cf,param=500041"  # the file holds T_2M (500011)
    out = write_job("identifier", {"t2m_cf": ("*_step6_t_2m_ctrl.grib2", fields, {})})
    res = out["results"]["t2m_cf"]
    assert (
        "has param=500011, but the query has param=500041; nothing was archived"
        in res["error"]
    )
    assert res["found"] == 0


def test_write_remove_policy_warn(write_job):
    extra = {"remove": 2}
    out = write_job(
        "identifier",
        {"t2m_cf": ("*_step6_t_2m_ctrl.grib2", "type=cf,param=500011", extra)},
    )
    res = out["results"]["t2m_cf"]
    _assert_round_trip(res, out["files"]["t2m_cf"])
    assert res["exists_after_remove"] is True  # nothing is deleted
    removes = [w for w in out["warnings"] if "cannot delete" in w]
    assert len(removes) == 1  # once per query
    assert "Use `fdb purge` to reclaim space." in removes[0]
