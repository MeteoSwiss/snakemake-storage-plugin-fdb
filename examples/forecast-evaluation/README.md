# Forecast-evaluation example workflow

A dummy machine-learning forecast pipeline in which every field lives in FDB and no rule
ever sees a GRIB file. Four rules, one job each per initialisation time (the
`{init_time}` wildcard of `config.yaml`):

- `truth` writes random analyses (`type=an`, `expver=0002`) for the requested parameters
  and steps;
- `run_model` is the "ML model": it reads the truth at step 0 as initial condition and
  writes forecasts (`type=fc`) under an `expver` derived from the model checkpoint;
- `verify` computes the RMSE of the forecast against the truth into
  `metrics/{init_time}.csv`;
- `animate` writes one GIF per initialisation time, a frame per step, into
  `animations/{init_time}.gif`.

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

What to expect: `metrics/` and `animations/` fill up, nothing ever appears under
`.snakemake/storage/` — not a GRIB file, not a placeholder — and a second run reports
"Nothing to be done".

Three things to try in `config.yaml`:

- **add a parameter** (uncomment `165`): the model's FDB output for it is missing, so
  `run_model` and everything downstream rerun;
- **remove it again**: nothing reruns. Reruns follow the fields a query names, not its
  text, and FDB already holds every field the narrower queries name;
- **change `model.checkpoint`**: its hash is the model's `expver`, so `run_model`,
  `verify` and `animate` rerun under a new experiment version — `truth` does not.

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
`dummy.py` imports the plugin. See the user guide,
[Direct access from run and script rules](../../docs/user-guide.md#direct-access-from-run-and-script-rules).
