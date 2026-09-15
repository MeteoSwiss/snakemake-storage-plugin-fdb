"""Site neutrality: no site names anywhere under ``src/`` (spec §1 goal 7)."""

import re
from pathlib import Path

SRC = Path(__file__).resolve().parents[1] / "src"
PATTERN = re.compile(r"mch|meteoswiss|cosmo|icon-ch", re.I)


def test_no_site_specifics_in_src():
    files = [p for p in SRC.rglob("*") if p.is_file() and "__pycache__" not in p.parts]
    assert files
    hits = []
    for path in files:
        text = path.read_text(errors="replace")
        for n, line in enumerate(text.splitlines(), 1):
            if PATTERN.search(line):
                hits.append(f"{path.relative_to(SRC)}:{n}: {line.strip()}")
    assert not hits, "site-specific text under src/:\n" + "\n".join(hits)
