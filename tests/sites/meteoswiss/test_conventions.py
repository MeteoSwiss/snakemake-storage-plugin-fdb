"""MARS keys of the OGD ICON-CH2-EPS samples decoded with COSMO definitions.

Decoding runs in a subprocess (``run_site``, clean environment) whose only site input
is the generic ``ECCODES_DEFINITION_PATH`` (``site_env``), so the definitions are
active when eccodes loads regardless of what this pytest process already loaded.
"""

import re

import pytest

pytestmark = pytest.mark.site_meteoswiss

DECODE = """
import json, sys
from snakemake_storage_plugin_fdb.grib import split_messages
job = json.loads(sys.argv[1])
out = {p: [[m.offset, m.length, m.mars, m.param_id] for m in split_messages(p)]
       for p in job["paths"]}
print(json.dumps(out))
"""

COMMON = {
    "class": "od",
    "stream": "enfo",
    "expver": "0001",
    "model": "ICON-CH2-EPS",
    "levtype": "sfc",
    "step": "6",
}

# (file name pattern, per-message expected keys beyond COMMON), spec §2.9
SAMPLES = [
    (
        "*_step6_t_2m_ctrl.grib2",
        [{"type": "cf", "param": "500011", "timespan": "none"}],
    ),
    (
        "*_step6_tot_prec_ctrl.grib2",
        [{"type": "cf", "param": "500041", "timespan": "fs"}],
    ),
    (
        "*_step6_t_2m_pert_m1-2.grib2",
        [
            {"type": "pf", "number": "1", "param": "500011", "timespan": "none"},
            {"type": "pf", "number": "2", "param": "500011", "timespan": "none"},
        ],
    ),
]


SYNTHETIC = """
import json, sys
import eccodes
from snakemake_storage_plugin_fdb.grib import mars_keys, variant
template = eccodes.codes_get_message(eccodes.codes_grib_new_from_samples("GRIB2"))
keys = json.loads(sys.argv[1])["keys"]
print(json.dumps(mars_keys(variant(template, zero_values=False, **keys))))
"""

# ICON-CH2-EPS control T_2M at 2 m on eccodes' GRIB2 sample grid, set in this order.
# The MARS concepts of eccodes-cosmo-mars live in the local section (definition 253,
# as in the OGD files), so it must be present.
SYNTHETIC_KEYS = {
    "centre": 215,
    "tablesVersion": 15,
    "localTablesVersion": 1,
    "grib2LocalSectionPresent": 1,
    "localDefinitionNumber": 253,
    "productDefinitionTemplateNumber": 1,
    "generatingProcessIdentifier": 142,
    "typeOfGeneratingProcess": 4,
    "perturbationNumber": 0,
    "numberOfForecastsInEnsemble": 21,
    "discipline": 0,
    "parameterCategory": 0,
    "parameterNumber": 0,
    "typeOfFirstFixedSurface": 103,
    "scaleFactorOfFirstFixedSurface": 0,
    "scaledValueOfFirstFixedSurface": 2,
}


@pytest.fixture(scope="module")
def decoded(tmp_path_factory, run_site, site_env, mch_sample) -> dict[str, list]:
    paths = [str(mch_sample(pattern)) for pattern, _ in SAMPLES]
    tmp = tmp_path_factory.mktemp("mch-conventions")
    return run_site(DECODE, {"paths": paths}, tmp, site_env)


@pytest.mark.parametrize("pattern, expected", SAMPLES, ids=[s[0] for s in SAMPLES])
def test_sample_mars_keys(mch_sample, decoded, pattern, expected):
    path = mch_sample(pattern)
    stamp = re.search(r"_(\d{8})(\d{4})_", path.name)
    assert stamp, path.name
    messages = decoded[str(path)]
    assert len(messages) == len(expected)
    assert sum(length for _, length, _, _ in messages) == path.stat().st_size
    for (_, _, mars, param_id), keys in zip(messages, expected, strict=True):
        want = {**COMMON, "date": stamp.group(1), "time": stamp.group(2), **keys}
        assert mars == want
        assert param_id == keys["param"]


def test_synthetic_icon(run_site, site_env, tmp_path):
    # needs only the definitions, not the samples
    mars, param_id = run_site(SYNTHETIC, {"keys": SYNTHETIC_KEYS}, tmp_path, site_env)
    expected = {
        "class": "od",
        "stream": "enfo",
        "type": "cf",
        "model": "ICON-CH2-EPS",
        "expver": "0001",
        "levtype": "sfc",
        "param": "500011",
    }
    assert {key: mars.get(key) for key in expected} == expected
    assert param_id == "500011"


def test_key_order_from_site_schema(make_provider, mch_schema):
    # the order comes from the schema named in the FDB config, not from the package
    provider = make_provider(schema=mch_schema)
    query = (
        "fdb://class=od,expver=0001,stream=enfo,model=icon-ch2-eps,date=20260915,"
        "time=1200,type=pf,levtype=sfc,levelist=0,step=6,number=1,param=500011,"
        "timespan=none"
    )
    assert provider.postprocess_query(query) == (
        "fdb://date=20260915,time=1200,stream=enfo,class=od,expver=0001,"
        "model=icon-ch2-eps,type=pf,levtype=sfc,number=1,step=6,param=500011,"
        "levelist=0,timespan=none"
    )
