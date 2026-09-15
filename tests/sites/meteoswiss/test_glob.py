"""Glob on the OGD ICON-CH2-EPS samples under the varda schema (requirements.md §2.8).

The samples are archived natively into a temporary FDB, then patterns are globbed
through ``StorageObject.list_candidate_matches`` and Snakemake's own
``glob_wildcards``. As in ``test_read.py``, FDB access runs in a subprocess and the
site is configured only through the generic ``eccodes_definitions``/``metkit_home``
settings.
"""

import pytest
from snakemake.io import apply_wildcards

pytestmark = pytest.mark.site_meteoswiss

SCRIPT = """
import json, logging, sys
from pathlib import Path
from snakemake.io import flag, glob_wildcards
from snakemake_storage_plugin_fdb import StorageProvider, StorageProviderSettings

job = json.loads(sys.argv[1])
provider = StorageProvider(
    local_prefix=Path(job["local_prefix"]),
    logger=logging.getLogger("site"),
    settings=StorageProviderSettings(**job["settings"]),
)
for path in job["archive"]:
    provider.backend.archive(Path(path).read_bytes())
provider.backend.flush()
out = {}
for name, pattern in job["patterns"].items():
    obj = provider.object(pattern)
    wildcards = glob_wildcards(flag(obj.query, "storage_object", obj))
    out[name] = {
        "query": obj.query,
        "candidates": obj.list_candidate_matches(),
        "wildcards": wildcards._asdict(),
    }
print(json.dumps(out))
"""

SAMPLES = (
    "*_step6_t_2m_ctrl.grib2",
    "*_step6_tot_prec_ctrl.grib2",
    "*_step6_t_2m_pert_m1-2.grib2",
)


@pytest.fixture(scope="module")
def globbed(
    tmp_path_factory,
    run_site,
    mch_fdb_config,
    mch_sample,
    mch_query_base,
    eccodes_definitions,
    metkit_home,
):
    """Per pattern: the normalised query, its candidates and the ``glob_wildcards``
    values."""
    tmp = tmp_path_factory.mktemp("mch-glob")
    files = [mch_sample(pattern) for pattern in SAMPLES]
    base = mch_query_base(files[0])
    job = {
        "settings": {
            "config": str(mch_fdb_config(tmp)),
            "eccodes_definitions": eccodes_definitions,
            "metkit_home": str(metkit_home),
        },
        "archive": [str(p) for p in files],
        "patterns": {
            "members": f"fdb://{base},type=pf,number={{member}},param=500011",
            "members_constraint": f"fdb://{base},number={{member,\\d+}},param=500011",
            "params": f"fdb://{base},type=cf,param={{p}}",
            "model": "fdb://"
            + base.replace("model=icon-ch2-eps", "model={model}")
            + ",type=cf,param=500011",
        },
    }
    return run_site(SCRIPT, job, tmp)


def test_glob_members(globbed):
    res = globbed["members"]
    # each candidate is the pattern with the member substituted (FR-GLOB-001)
    assert res["candidates"] == [
        apply_wildcards(res["query"], {"member": m}) for m in ("1", "2")
    ]
    assert res["wildcards"]["member"] == ["1", "2"]


def test_glob_members_skip_control(globbed):
    # type is a wildcard for list: the cf fields list number='' and are skipped
    assert globbed["members_constraint"]["wildcards"]["member"] == ["1", "2"]


def test_glob_params_are_canonical_cosmo_ids(globbed):
    # T_2M (timespan=none) and TOT_PREC (timespan=fs); timespan is not in the pattern
    assert globbed["params"]["wildcards"]["p"] == ["500011", "500041"]


def test_glob_model_is_listed_lower_case(globbed):
    assert globbed["model"]["wildcards"]["model"] == ["icon-ch2-eps"]
