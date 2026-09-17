# Forecast-evaluation example workflow

A dummy machine-learning forecast pipeline in which every field lives in FDB and no rule
ever sees a GRIB file. Five rules over the initialisation times and parameters of
`config.yaml` (the `{init_time}` and `{param}` wildcards):

- `truth` writes random analyses (`type=an`, `expver=0002`) for the requested parameters
  and steps, one job per initialisation time;
- `run_model` is the "ML model": it reads the truth at step 0 as initial condition and
  writes forecasts (`type=fc`) under an `expver` derived from the model checkpoint, one
  job per initialisation time;
- `verify` computes the RMSE of the forecast against the truth into
  `metrics/{init_time}/{param}.csv`, one job per initialisation time and parameter;
- `animate` writes one GIF per initialisation time and parameter, a frame per step, into
  `animations/{init_time}/{param}.gif`;
- `scorecard` aggregates the metrics files — local files, not FDB queries — into
  `scorecard.csv`: every row of every metrics file plus a `mean` row per parameter and
  step over the initialisation times.

## Run

From the repository root (the example needs earthkit-data and matplotlib):

```bash
uv sync --group examples
uv run python scripts/init_dev_fdb.py --seed --variants
cd examples/forecast-evaluation
uv run snakemake --profile profile -c4
```

The profile points at the development FDB `../../.fdb/config.yaml`; a real FDB is a
`--storage-fdb-config` away. `init_dev_fdb.py --variants` also archives the single field
the jobs use as the GRIB template for everything they generate, so the example needs no
file besides this directory.

What to expect: `metrics/`, `animations/` and `scorecard.csv` fill up, no GRIB file ever
appears under `.snakemake/storage/` — not a data file, not a placeholder (the empty
directory itself may be created by `--delete-all-output` or `glob_wildcards`) — and a
second run reports "Nothing to be done".

Three things to try in `config.yaml`:

- **add a parameter** (uncomment `165`): the model's FDB output for it is missing, so
  `run_model` and everything downstream rerun;
- **remove it again**: the model and the truth do not rerun — reruns follow the fields a
  query names, not its text, and FDB already holds every field the narrower queries name
  — but `scorecard` does, because its input set is a local file per parameter and that
  set changed. The metrics file and the GIF of the dropped parameter stay on disk; they
  are simply no longer part of the scorecard;
- **change `model.checkpoint`**: its hash is the model's `expver`, so `run_model`,
  `verify`, `animate` and `scorecard` rerun under a new experiment version — `truth`
  does not.

The second one is the design rule this example is shaped around: **a summary over the
declared set aggregates local per-field artefacts, not FDB inputs directly.** A rule that
computed `scorecard.csv` straight from the FDB queries would keep the dropped parameter
until something forced it to rerun (see the user guide,
[Reruns](../../docs/user-guide.md#reruns)). The workflow-wide alternative is
`--storage-fdb-input-tracking query`, at the price of rerunning the model on every query
edit.

All three assume the provenance records of the previous run, under `.snakemake/`. A fresh
clone, a deleted `.snakemake/`, or a moved working directory with
`--persistence-backend db` has none, and then a workflow whose every intermediate lives
in FDB has no local file whose absence would force a rebuild: even a genuinely missing
FDB field is reported as "Nothing to be done". Run `--forceall`, or `-R <rule>`, once
after such a move.

## Choices this example makes

- **Multi-step queries.** Each query carries the whole step list, so there is one
  forecast job per initialisation time. Adding a step therefore rebuilds every step. The
  alternative is a `{step}` wildcard in the output queries: only the new step is
  produced, at the price of several times as many jobs and FDB round trips
  ([usage patterns](../../docs/patterns.md#one-field-one-rule)).
- **Outputs declared `retrieve=False`.** Existence in FDB is all that is checked after a
  job. For a production evaluation, declare the model's outputs
  `touch(storage.fdb(...))` instead: the cost is an empty local file, the gain is that a
  model writing the wrong fields, too few fields or nothing at all fails at once with a
  message naming the missing fields
  ([the checked variant](../../docs/user-guide.md#the-checked-variant-touch)).
- **A new checkpoint is a new `expver`.** Retry a *failed* model run under a new
  checkpoint hash rather than into the same experiment version: FDB cannot delete the
  fields a failed job already archived, and if they happen to satisfy a later query the
  producing rule is never scheduled again (force it with `-R run_model` if it comes to
  that).

## How the jobs read and write

Every FDB object a job handles itself is declared `retrieve=False`, input or output
alike. An input so declared reaches the job as the query string, and the job parses its
MARS request from it (`fdb://` stripped, `,` and `=` split). It reads the fields with
plain `pyfdb` (the GRIB template) or earthkit-data (`from_source("fdb", request)`),
which need no configuration argument: the provider exports the FDB configuration into
the job environment. Outputs are archived by the job itself with
`pyfdb.FDB().archive()` and `flush()`; because they are declared `retrieve=False` too,
Snakemake checks after the job that every field of the query is in FDB instead of
waiting for a local file, and nothing is written locally. Nothing in `Snakefile` or
`dummy.py` imports the plugin. Errors from those libraries are fdb5's own, not the
plugin's mapped messages. See the user guide,
[Direct access from run and script rules](../../docs/user-guide.md#direct-access-from-run-and-script-rules).
