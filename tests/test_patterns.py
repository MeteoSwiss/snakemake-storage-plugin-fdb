"""Every tagged Snakefile in ``docs/patterns.md``, run against a dev FDB.

A block of the document is executed when an HTML comment on the line before its fence
names a pattern::

    <!-- pattern: one-field; rerun: nothing; expect: Storing in storage -->

The directives after the name are optional: ``file: <relative path>`` makes the block an
auxiliary file of that pattern instead of its Snakefile, ``rerun: nothing`` requires a
second run to report "Nothing to be done", ``expect: <substring>`` (repeatable) requires
the substring in the run's log, ``cores: N`` runs with ``-cN`` (default 1), ``args:``
and ``env: NAME=VALUE`` add command-line arguments and environment variables, with
``{config}`` and ``{config2}`` standing for the two FDB configurations. Blocks without
such a comment are not executed.

Each pattern runs in its own directory against one module FDB (``init_dev_fdb.py
--seed --variants`` plus two more dates), so the patterns that write must use an
``expver`` of their own. The runs happen once per module, one worker per core, with
their logs under pytest's temporary directory.
"""

import os
import re
import sys
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]
DOC = REPO / "docs" / "patterns.md"
INIT_DEV_FDB = REPO / "scripts" / "init_dev_fdb.py"
SAMPLES = REPO / "tests" / "data" / "grib" / "ecmwf"
TEMPLATE = SAMPLES / "template.grib"
EXTRA_DATES = (20200102, 20200103)  # 20200101 comes from --variants
EXTRA_STEPS = (0, 6, 12)
EXTRA_PARAMS = (167, 165)

pytestmark = pytest.mark.skipif(not TEMPLATE.exists(), reason="no ECMWF samples")

COMMENT = re.compile(r"^<!--\s*pattern:\s*(?P<directives>.+?)\s*-->\s*$")
FENCE = re.compile(r"^```")


@dataclass
class Pattern:
    """A pattern of the document: its files and what its run must show."""

    name: str
    files: dict[str, str] = field(default_factory=dict)
    expect: list[str] = field(default_factory=list)
    args: str = ""
    env: dict[str, str] = field(default_factory=dict)
    cores: int = 1
    rerun_nothing: bool = False


def parse_patterns(text: str) -> dict[str, Pattern]:
    """The patterns of ``text``, in document order (see the module docstring)."""
    patterns: dict[str, Pattern] = {}
    lines = text.splitlines()
    for index, line in enumerate(lines):
        match = COMMENT.match(line)
        if not match:
            continue
        name, *rest = (d.strip() for d in match["directives"].split(";"))
        directives = []
        for item in rest:
            key, _, value = item.partition(":")
            directives.append((key.strip(), value.strip()))
        if not FENCE.match(lines[index + 1] if index + 1 < len(lines) else ""):
            raise AssertionError(f"pattern {name}: no fenced block on line {index + 2}")
        end = next(
            i for i in range(index + 2, len(lines)) if FENCE.match(lines[i])
        )  # the closing fence
        block = "\n".join(lines[index + 2 : end]) + "\n"

        pattern = patterns.setdefault(name, Pattern(name))
        path = "Snakefile"
        for key, value in directives:
            if key == "file":
                path = value
            elif key == "expect":
                pattern.expect.append(value)
            elif key == "rerun":
                assert value == "nothing", f"pattern {name}: unknown rerun {value!r}"
                pattern.rerun_nothing = True
            elif key == "cores":
                pattern.cores = int(value)
            elif key == "args":
                pattern.args = value
            elif key == "env":
                variable, _, content = value.partition("=")
                pattern.env[variable] = content
            else:
                raise AssertionError(f"pattern {name}: unknown directive {key!r}")
        assert path not in pattern.files, f"pattern {name}: {path} twice"
        pattern.files[path] = block
    return patterns


PATTERNS = parse_patterns(DOC.read_text())


def _seed_extra_dates(config: Path) -> None:
    """The fields of the multi-date patterns, beyond ``--variants``."""
    from snakemake_storage_plugin_fdb.backend import Backend
    from snakemake_storage_plugin_fdb.grib import variant

    backend = Backend(config)
    template = TEMPLATE.read_bytes()
    for date in EXTRA_DATES:
        for step in EXTRA_STEPS:
            for param in EXTRA_PARAMS:
                backend.archive(
                    variant(
                        template, stream="oper", dataDate=date, step=step, paramId=param
                    )
                )
    backend.flush()


@pytest.fixture(scope="module")
def patterns(tmp_path_factory, run_logged) -> dict:
    """The run (and the rerun, where declared) of every pattern of the document."""
    tmp = tmp_path_factory.mktemp("patterns")
    run = run_logged(tmp / "logs")
    config, config2 = tmp / "fdb" / "config.yaml", tmp / "fdb2" / "config.yaml"

    init = run(
        "init",
        [sys.executable, INIT_DEV_FDB, "--root", tmp / "fdb", "--seed", "--variants"],
        tmp,
    )
    init.ok()
    run("init2", [sys.executable, INIT_DEV_FDB, "--root", tmp / "fdb2"], tmp).ok()
    _seed_extra_dates(config)

    def execute(pattern: Pattern) -> tuple:
        work = tmp / "work" / pattern.name
        for path, content in pattern.files.items():
            (work / path).parent.mkdir(parents=True, exist_ok=True)
            (work / path).write_text(content)
        assert "Snakefile" in pattern.files, f"pattern {pattern.name}: no Snakefile"

        def substitute(text: str) -> str:
            return text.replace("{config}", str(config)).replace(
                "{config2}", str(config2)
            )

        args = substitute(pattern.args).split()
        # numpy's OpenBLAS starts one thread per core at import, which the parallel
        # runs never use; the workflows do no linear algebra.
        env = {"OPENBLAS_NUM_THREADS": "1"}
        env.update({k: substitute(v) for k, v in pattern.env.items()})
        flag = []
        if (
            "--storage-fdb-config" not in args
            and "SNAKEMAKE_STORAGE_FDB_CONFIG" not in env
        ):
            flag = ["--storage-fdb-config", str(config)]
        cmd = [
            sys.executable,
            "-m",
            "snakemake",
            "-c",
            str(pattern.cores),
            *flag,
            *args,
        ]
        first = run(pattern.name, cmd, work, env=env)
        second = None
        if first.returncode == 0 and pattern.rerun_nothing:
            second = run(f"{pattern.name}-rerun", cmd, work, env=env)
        return first, second

    with ThreadPoolExecutor(max_workers=min(8, os.cpu_count() or 1)) as pool:
        results = list(pool.map(execute, PATTERNS.values()))
    return dict(zip(PATTERNS, results, strict=True))


@pytest.mark.parametrize("name", list(PATTERNS))
def test_pattern_runs(patterns, name):
    """The pattern's workflow runs and its log says what the document says."""
    first, _ = patterns[name]
    log = first.ok()
    for expected in PATTERNS[name].expect:
        assert expected in log, f"{name}: {expected!r} not in the log:\n{log}"


@pytest.mark.parametrize(
    "name", [n for n, p in PATTERNS.items() if p.rerun_nothing], ids=str
)
def test_pattern_converges(patterns, name):
    """A second run of the pattern has nothing left to do."""
    _, second = patterns[name]
    assert second.NOTHING_TO_BE_DONE in second.ok()


def test_document_has_patterns():
    """The parser sees the document; a renamed heading must not silence the suite."""
    assert len(PATTERNS) >= 15
    assert all(p.files for p in PATTERNS.values())
