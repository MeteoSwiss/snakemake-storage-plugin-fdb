"""``examples/meteoswiss/fetch_ogd_samples.py`` (FR-DEV-002).

Neither test needs anything in the default run: the offline test needs the git-ignored
full-size originals in ``.local/samples-full/meteoswiss/``, the live test needs network
access and ``SMK_FDB_TEST_OGD_LIVE=1``. Both skip otherwise, also with
``SMK_FDB_TEST_REQUIRE_SITES=1``. The live test writes only into pytest's temporary
directory:

    SMK_FDB_TEST_OGD_LIVE=1 SMK_FDB_TEST_ECCODES_DEFINITIONS=... \\
        uv run pytest tests/sites/meteoswiss/test_fetch_ogd_samples.py -q -rs
"""

import os
import re
import subprocess
import sys
from pathlib import Path

import pytest

pytestmark = pytest.mark.site_meteoswiss

REPO = Path(__file__).resolve().parents[3]
SCRIPT = REPO / "examples" / "meteoswiss" / "fetch_ogd_samples.py"
FULL = REPO / ".local" / "samples-full" / "meteoswiss"
COMMITTED = REPO / "tests" / "data" / "grib" / "meteoswiss"
KEYS_LINE = re.compile(r"^(?P<path>\S+) \[(?P<i>\d+)\] (?P<size>\d+) B: (?P<keys>.*)$")

# the script's own empty-data step, applied to every full-size file in argv[1]
EMPTY_ALL = """
import sys
from pathlib import Path
import fetch_ogd_samples as fetch
for path in sorted(Path(sys.argv[1]).glob("*.grib2")):
    (Path(sys.argv[2]) / path.name).write_bytes(fetch.empty_file(path.read_bytes()))
"""


@pytest.fixture
def fetch_env(subprocess_env, site_env) -> dict[str, str]:
    """Clean environment with the definitions and the script importable."""
    return subprocess_env(
        {**site_env, "PYTHONPATH": str(SCRIPT.parent), "PYTHONDONTWRITEBYTECODE": "1"}
    )


def test_empty_data_reproduces_committed_samples(fetch_env, tmp_path):
    originals = sorted(FULL.glob("*.grib2"))
    if not originals:
        pytest.skip(f"no full-size originals in {FULL}")
    subprocess.run(
        [sys.executable, "-c", EMPTY_ALL, FULL, tmp_path],
        env=fetch_env,
        check=True,
        timeout=300,
    )
    for original in originals:
        committed = COMMITTED / original.name
        if committed.exists():
            assert (tmp_path / original.name).read_bytes() == committed.read_bytes()


def test_fetch_live(fetch_env, tmp_path):
    if os.environ.get("SMK_FDB_TEST_OGD_LIVE") != "1":
        pytest.skip("live OGD API test: set SMK_FDB_TEST_OGD_LIVE=1")

    def fetch(*args) -> subprocess.CompletedProcess:
        return subprocess.run(
            [sys.executable, SCRIPT, *map(str, args)],
            env=fetch_env,
            capture_output=True,
            text=True,
            timeout=600,
        )

    full, empty = tmp_path / "full", tmp_path / "empty"
    args = ["--out", full, "--empty-data", empty]

    first = fetch(*args)
    assert first.returncode == 0, first.stderr
    names = sorted(p.name for p in full.iterdir())
    assert len(names) == 3 and names == sorted(p.name for p in empty.iterdir())
    # the printed MARS keys per (directory, file, message index)
    listed = {}
    for line in first.stdout.splitlines():
        if match := KEYS_LINE.match(line):
            path = Path(match["path"])
            listed[(path.parent.name, path.name, match["i"])] = match["keys"]
    assert len(listed) == 8  # 4 messages, full and empty
    for (where, name, i), keys in listed.items():
        assert "model=ICON-CH2-EPS" in keys  # COSMO definitions active
        if where == "empty":
            assert listed[("full", name, i)] == keys
    for path in empty.iterdir():
        assert 175 <= path.stat().st_size <= 350

    again = fetch(*args)  # copies exist, no --force
    assert again.returncode == 1 and "pass --force" in again.stderr

    expired = fetch(
        "--out", tmp_path / "expired", "--reference-datetime", "2000-01-01T00:00:00Z"
    )
    assert expired.returncode == 1
    assert "OGD retains data for 24 h" in expired.stderr
