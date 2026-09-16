"""End-to-end: ``examples/forecast-evaluation/`` against a dev FDB (FR-DEV-003).

The example's jobs read and write FDB directly with plain pyfdb and earthkit-data
(§2.15), so this module needs the ``examples`` dependency group (``uv sync --group
examples``) and skips without earthkit-data or matplotlib, as it skips without the
ECMWF samples.

``scripts/init_dev_fdb.py --seed --variants`` creates a fresh ``.fdb/`` in a temporary
directory next to a copy of the example, whose ``config.yaml`` is rewritten to two
initialisation times. Snakemake runs as a subprocess with the committed profile and an
explicit ``--storage-fdb-config`` (the command line wins over the profile's path).
Logs are written to ``<tmp>/logs/``. The stages cover the documented behaviour: a full
run, a second run with nothing to do, widening ``params`` (the model reruns because its
FDB output is missing), narrowing them again (nothing reruns: input tracking by lookup,
FR-RERUN-002) and a new model checkpoint (a new ``expver``: the model and everything
downstream rerun, the truth does not).
"""

import hashlib
import importlib.util
import shutil
import sys
from pathlib import Path

import pytest
import yaml

REPO = Path(__file__).resolve().parents[1]
EXAMPLE = REPO / "examples" / "forecast-evaluation"
INIT_DEV_FDB = REPO / "scripts" / "init_dev_fdb.py"
SAMPLES = REPO / "tests" / "data" / "grib" / "ecmwf"

INIT_TIMES = ["2020-01-01T00:00", "2020-01-02T12:00"]
STEPS = [0, 6, 12]
CHECKPOINT = "unet-v3-2026-08.ckpt"
NEW_CHECKPOINT = "unet-v4-2026-09.ckpt"


def _expver(checkpoint: str) -> str:
    """The example's expver of a checkpoint (Snakefile: 4 hex of its sha256)."""
    return hashlib.sha256(checkpoint.encode()).hexdigest()[:4]


def _missing() -> str | None:
    """What the example needs and this environment does not have."""
    if not (SAMPLES / "template.grib").exists():
        return "no ECMWF samples"
    # find_spec("earthkit.data") raises when the "earthkit" namespace is absent
    if not importlib.util.find_spec("earthkit") or not importlib.util.find_spec(
        "earthkit.data"
    ):
        return "no earthkit-data (uv sync --group examples)"
    if not importlib.util.find_spec("matplotlib"):
        return "no matplotlib (uv sync --group examples)"
    return None


pytestmark = pytest.mark.skipif(_missing() is not None, reason=_missing() or "")


def _config(params: list[int], checkpoint: str = CHECKPOINT) -> dict:
    return {
        "init_times": INIT_TIMES,
        "params": params,
        "steps": STEPS,
        "model": {"checkpoint": checkpoint},
        "truth_expver": "0002",
    }


def _job_counts(log: str) -> dict[str, int]:
    """The rules of the first ``Job stats:`` table of a run, with their job counts."""
    lines = log.splitlines()
    counts: dict[str, int] = {}
    for line in lines[lines.index("Job stats:") + 1 :]:
        fields = line.split()
        if not fields:
            break
        if len(fields) == 2 and fields[1].isdigit():
            counts[fields[0]] = int(fields[1])
    counts.pop("total", None)
    return counts


def _storage_files(example: Path) -> list[Path]:
    return [p for p in (example / ".snakemake" / "storage").rglob("*") if p.is_file()]


def _results(example: Path) -> dict[str, tuple[list[list[str]], int]]:
    """Per initialisation time: the rows of its CSV and the size of its GIF.

    Read while the run that produced them is the last one: the later stages overwrite
    both files.
    """
    results = {}
    for init_time in INIT_TIMES:
        lines = (example / "metrics" / f"{init_time}.csv").read_text().splitlines()
        assert lines[0] == "init_time,expver,param,step,rmse"
        rows = [line.split(",") for line in lines[1:]]
        gif = example / "animations" / f"{init_time}.gif"
        results[init_time] = (rows, gif.stat().st_size)
    return results


@pytest.fixture(scope="module")
def example(tmp_path_factory, run_logged) -> dict:
    """Runs, in order: init, the example, a second run, a widened ``params`` (dry run
    and run), the narrowed ``params`` again (dry run) and a new checkpoint (dry run)."""
    tmp = tmp_path_factory.mktemp("forecast-evaluation")
    run = run_logged(tmp / "logs")
    config = tmp / ".fdb" / "config.yaml"
    work = tmp / "forecast-evaluation"
    out: dict = {"tmp": tmp, "config": config, "example": work}

    def write_config(params: list[int], checkpoint: str = CHECKPOINT) -> None:
        (work / "config.yaml").write_text(yaml.safe_dump(_config(params, checkpoint)))

    def snakemake(name: str, *args: str) -> None:
        cmd = [sys.executable, "-m", "snakemake", "--profile", "profile"]
        cmd += ["--storage-fdb-config", str(config), "-c4", *args]
        out[name] = run(name, cmd, work)

    init = [
        sys.executable,
        INIT_DEV_FDB,
        "--root",
        tmp / ".fdb",
        "--seed",
        "--variants",
    ]
    out["init"] = run("init", init, tmp)
    shutil.copytree(
        EXAMPLE,
        work,
        ignore=shutil.ignore_patterns(
            ".snakemake", "__pycache__", "metrics", "animations"
        ),
    )

    write_config([167])
    snakemake("run1")
    out["storage_after_run1"] = _storage_files(work)
    out["results_after_run1"] = _results(work)
    snakemake("run2")

    write_config([167, 165])
    snakemake("widen_dry", "--dry-run")
    snakemake("widen")
    out["storage_after_widen"] = _storage_files(work)
    out["results_after_widen"] = _results(work)

    write_config([167])
    snakemake("narrow_dry", "--dry-run")

    write_config([167], NEW_CHECKPOINT)
    snakemake("checkpoint_dry", "--dry-run")
    return out


def test_example_produces_metrics_and_animations(example):
    """One CSV and one GIF per initialisation time; the CSV has a row per parameter
    and step, with the model's expver and a finite RMSE."""
    example["run1"].ok()
    for init_time, (rows, gif_size) in example["results_after_run1"].items():
        assert len(rows) == len(STEPS)  # one param x three steps
        assert [row[0] for row in rows] == [init_time] * len(rows)
        assert {row[1] for row in rows} == {_expver(CHECKPOINT)}
        assert [int(row[3]) for row in rows] == STEPS
        assert all(0.0 <= float(row[4]) < 1e6 for row in rows)
        assert gif_size > 0


def test_example_writes_no_local_file_at_all(example):
    """FR-DIRECT-001/005, NFR-PERF-005: with every FDB object declared
    ``retrieve=False``, nothing is retrieved, nothing is stored from a local file and
    ``.snakemake/storage`` stays empty."""
    log = example["run1"].ok()
    assert "Retrieving from storage" not in log
    assert "Storing in storage" not in log
    assert "Removing local copy" not in log
    assert example["storage_after_run1"] == []
    assert example["storage_after_widen"] == []
    assert not list((example["example"] / ".snakemake" / "storage").rglob("*.grib"))


def test_example_second_run_nothing_to_be_done(example):
    run2 = example["run2"]
    assert run2.NOTHING_TO_BE_DONE in run2.ok()


def test_example_widening_params_reruns_everything(example):
    """A new parameter has no fields in FDB, so the model reruns and with it the whole
    chain, for every initialisation time."""
    counts = _job_counts(example["widen_dry"].ok())
    assert counts == {
        "truth": len(INIT_TIMES),
        "run_model": len(INIT_TIMES),
        "verify": len(INIT_TIMES),
        "animate": len(INIT_TIMES),
        "all": 1,
    }
    example["widen"].ok()
    for rows, _ in example["results_after_widen"].values():
        assert len(rows) == 2 * len(STEPS)
        assert {row[2] for row in rows} == {"167", "165"}


def test_example_narrowing_params_reruns_nothing(example):
    """FR-RERUN-002: the narrowed queries name fields FDB already holds, so nothing is
    out of date although the query text changed."""
    narrow = example["narrow_dry"]
    assert narrow.NOTHING_TO_BE_DONE in narrow.ok()


def test_example_new_checkpoint_reruns_the_model_only(example):
    """A new checkpoint is a new expver: the model's outputs are missing, the truth's
    are not."""
    log = example["checkpoint_dry"].ok()
    assert f"expver={_expver(NEW_CHECKPOINT)}" in log
    counts = _job_counts(log)
    assert counts == {
        "run_model": len(INIT_TIMES),
        "verify": len(INIT_TIMES),
        "animate": len(INIT_TIMES),
        "all": 1,
    }
