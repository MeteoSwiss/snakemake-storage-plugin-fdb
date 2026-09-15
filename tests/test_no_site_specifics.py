"""Site neutrality: no site names under ``src/`` or ``scripts/`` (spec §1 goal 7)."""

import re
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]
PATTERN = re.compile(r"mch|meteoswiss|cosmo|icon-ch", re.I)


@pytest.mark.parametrize("directory", ["src", "scripts"])
def test_no_site_specifics(directory):
    root = REPO / directory
    files = [p for p in root.rglob("*") if p.is_file() and "__pycache__" not in p.parts]
    assert files
    hits = []
    for path in files:
        text = path.read_text(errors="replace")
        for n, line in enumerate(text.splitlines(), 1):
            if PATTERN.search(line):
                hits.append(f"{path.relative_to(REPO)}:{n}: {line.strip()}")
    assert not hits, f"site-specific text under {directory}/:\n" + "\n".join(hits)
