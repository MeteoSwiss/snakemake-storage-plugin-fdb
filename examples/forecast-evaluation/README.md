# Forecast-evaluation example workflow

A dummy machine-learning forecast pipeline in which every field lives in FDB and no rule
ever sees a GRIB file. Five rules over the model checkpoints, initialisation times and
parameters of `config.yaml` (the `{expver}`, `{init_time}` and `{param}` wildcards):

- `truth` writes random analyses (`type=an`, `expver=0002`) for the requested parameters
  and steps, one job per initialisation time; it is shared by every experiment;
- `run_model` is the "ML model": it reads the truth at step 0 as initial condition and
  writes forecasts (`type=fc`) under an `expver` derived from the model checkpoint, one
  job per initialisation time and checkpoint;
- `verify` computes the RMSE of the forecast against the truth into
  `metrics/{expver}/{init_time}/{param}.csv`, one job per experiment, initialisation
  time and parameter;
- `animate` writes one GIF per experiment, initialisation time and parameter, a frame per
  step, into `animations/{expver}/{init_time}/{param}.gif`;
- `scorecard` aggregates the metrics files — local files, not FDB queries — into
  `scorecard.csv`: every row of every metrics file plus a `mean` row per experiment,
  parameter and step over the initialisation times.

A query cannot hold `{init_time}`: `2020-01-01T00:00` is not a MARS value, and `:` and
`-` are not allowed in a query either. The wildcards of a query are MARS's own, so the
`fdb://` strings use `date={date}` and `time={time}` and the input functions convert
(`mars_date_time`, `'2020-01-01T00:00' -> ('20200101', '0000')`); the local artefacts
keep the readable `{init_time}`. `{expver}` and `{param}` need no conversion: they are
MARS values already.

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

Things to try in `config.yaml`:

- **add a parameter** (uncomment `165`): the model's FDB output for it is missing, so
  `run_model` and everything downstream rerun;
- **remove it again**: the model and the truth do not rerun — reruns follow the fields a
  query names, not its text, and FDB already holds every field the narrower queries name
  — but `scorecard` does, because its input set is a local file per parameter and that
  set changed. The metrics file and the GIF of the dropped parameter stay on disk; they
  are simply no longer part of the scorecard;
- **add a checkpoint** (uncomment the second entry of `model.checkpoints`): its hash is a
  second `expver`, so `run_model`, `verify` and `animate` run for the new experiment
  only, `truth` is shared and does not run, the first experiment's metrics and
  animations are untouched, and `scorecard.csv` then holds both experiments — `mean` rows
  per (expver, param, step) are the comparison.

The second one is the design rule this example is shaped around: **a summary over the
declared set aggregates local per-field artefacts, not FDB inputs directly.** A rule that
computed `scorecard.csv` straight from the FDB queries would keep the dropped parameter
until something forced it to rerun (see the user guide,
[Reruns](../../docs/user-guide.md#reruns)). The workflow-wide alternative is
`--storage-fdb-input-tracking query`, at the price of rerunning the model on every query
edit.

The third one is the other rule: **every FDB key that distinguishes two runs of the same
workflow must also be a wildcard in the local artefact paths.** The `expver` tells the
two models apart in FDB; if `metrics/{init_time}/{param}.csv` and `scorecard.csv` carried
no `{expver}`, the second experiment would overwrite the first one's results and
Snakemake would never recompute them, although FDB holds both copies for good.

All of them assume the provenance records of the previous run, under `.snakemake/`. A
fresh clone, a deleted `.snakemake/`, or a moved working directory with
`--persistence-backend db` has none, and then a workflow whose every intermediate lives
in FDB has no local file whose absence would force a rebuild: even a genuinely missing
FDB field is reported as "Nothing to be done". Run `--forceall`, or `-R <rule>`, once
after such a move.

## When something goes wrong

- **A model job failed half-way.** What it archived before failing stays in FDB: FDB
  cannot delete single fields, and if those fields happen to satisfy the output query,
  the rule is never scheduled again and the next run reports "Nothing to be done". Force
  it with `-R run_model`, which reruns **every** job of the rule (a single FDB job cannot
  be targeted: a query is not a valid command-line target). Archive the retry under a
  fresh checkpoint only if the *model* changed — a crash is not a new experiment, and
  every extra `expver` is another full copy of the data in FDB.
- **An edit of `Snakefile` or `dummy.py` makes every model job rerun**
  (`Code has changed since last execution`) and re-archives fields FDB already holds; the
  old copies are masked and are reclaimed only by `fdb purge`. Run with
  `--rerun-triggers mtime` when the edit does not change the data.

## Choices this example makes

- **Multi-step queries.** Each query carries the whole step list, so there is one
  forecast job per initialisation time. Adding a step therefore rebuilds every step. The
  alternative is a `{step}` wildcard in the output queries: only the new step is
  produced, at the price of several times as many jobs and FDB round trips
  ([usage patterns](../../docs/patterns.md#one-field-one-rule)).
- **Plain libraries in the rule bodies, and `retrieve=False` on both sides.** The jobs
  read and archive with plain pyfdb and earthkit-data, as code that must also run outside
  Snakemake does; the plugin's optional `api` would give a `run:` body the same in fewer
  lines and with the plugin's checks (see
  [the user guide](../../docs/user-guide.md#a-run-body-with-the-api-module)). This
  example keeps the portable form throughout, because `dummy.py` is script-like code and
  the point of the example is that nothing a job runs depends on the plugin.
- **Existence is all that is checked after a job.** For a production evaluation, declare
  the model's outputs `touch(storage.fdb(...))` instead: the cost is an empty local file,
  the gain is that a model writing the wrong fields, too few fields or nothing at all
  fails at once with a message naming the missing fields
  ([the checked variant](../../docs/user-guide.md#the-checked-variant-touch)). It does
  not cover the previous section's failed job, though: the check only runs for jobs that
  run, and a rule whose output already looks complete is never scheduled. `api.archive`
  checks before archiving as well.
- **A new checkpoint is a new `expver`, and a new row in `model.checkpoints`.** Keeping
  the old checkpoint in the list keeps its results; removing it leaves its files and its
  FDB fields alone and only drops it from the scorecard.

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
`dummy.py` imports the plugin, which is what makes `dummy.py` runnable on its own — the
price is that errors from those libraries are fdb5's own, not the plugin's mapped
messages, and that nothing is checked before an archive. A `run:` body that wants those
checks uses `api.messages`/`api.archive` instead; see the user guide,
[Direct access from run and script rules](../../docs/user-guide.md#direct-access-from-run-and-script-rules).
