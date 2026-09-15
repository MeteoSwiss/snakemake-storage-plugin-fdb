"""MARS keys of the OGD ICON-CH2-EPS samples decoded with COSMO definitions.

Decoding runs in a subprocess whose only site input is the generic
``ECCODES_DEFINITION_PATH`` (prepended as spec §4.1 describes), so the definitions
are active when eccodes loads regardless of what this pytest process already loaded.
"""

import json
import os
import re
import subprocess
import sys
from pathlib import Path

import pytest

pytestmark = pytest.mark.site_meteoswiss

DECODE = """
import json, sys
from snakemake_storage_plugin_fdb.grib import split_messages
out = {p: [[m.offset, m.length, m.mars, m.param_id] for m in split_messages(p)]
       for p in sys.argv[1:]}
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


def _sample(samples: Path, pattern: str) -> Path:
    found = sorted(samples.glob(pattern))
    if not found:
        pytest.fail(f"no sample matching {pattern} in {samples}")
    return found[-1]


@pytest.fixture(scope="module")
def decoded(mch_samples, eccodes_definitions) -> dict[str, list]:
    paths = [str(_sample(mch_samples, pattern)) for pattern, _ in SAMPLES]
    env = dict(os.environ)
    existing = env.get("ECCODES_DEFINITION_PATH")
    env["ECCODES_DEFINITION_PATH"] = (
        f"{eccodes_definitions}:{existing}" if existing else eccodes_definitions
    )
    proc = subprocess.run(
        [sys.executable, "-c", DECODE, *paths],
        env=env,
        capture_output=True,
        text=True,
        check=True,
    )
    return json.loads(proc.stdout)


@pytest.mark.parametrize("pattern, expected", SAMPLES, ids=[s[0] for s in SAMPLES])
def test_sample_mars_keys(mch_samples, decoded, pattern, expected):
    path = _sample(mch_samples, pattern)
    stamp = re.search(r"_(\d{8})(\d{4})_", path.name)
    assert stamp, path.name
    messages = decoded[str(path)]
    assert len(messages) == len(expected)
    assert sum(length for _, length, _, _ in messages) == path.stat().st_size
    for (_, _, mars, param_id), keys in zip(messages, expected, strict=True):
        want = {**COMMON, "date": stamp.group(1), "time": stamp.group(2), **keys}
        assert mars == want
        assert param_id == keys["param"]
