# MeteoSwiss example

Everything MeteoSwiss-specific lives here, outside the plugin package: the plugin knows
nothing about COSMO definitions, `model` values or the varda schema, and gets them
through its generic settings (`eccodes_definitions`, `metkit_home`, `config`, `env`).
See also [`docs/sites/meteoswiss.md`](../../docs/sites/meteoswiss.md).

| file | purpose |
|---|---|
| `setup.sh` | clones `eccodes-cosmo-mars` (branch `varda-ext`) and installs `eccodes-cosmo-resources-python` into `.local/` |
| `make_metkit_home.py` | builds `.local/metkit-home`, a MARS language that accepts MeteoSwiss `model` values |
| `realtime-varda.schema` | FDB schema from `MeteoSwiss/evalml` (provenance and license in its header) |
| `fetch_ogd_samples.py` | re-fetches the ICON-CH2-EPS samples from the OGD STAC API |
| `Snakefile`, `profile/config.yaml`, `grib_keys.py` | the example workflow |

## Setup

From the repository root (needs `git`, `uv` and network access):

```bash
bash examples/meteoswiss/setup.sh                        # .local/eccodes-cosmo-mars, .local/eccodes-cosmo-resources
uv run python examples/meteoswiss/make_metkit_home.py    # .local/metkit-home
```

`setup.sh` is idempotent (it updates the clone and re-installs) and takes `--dest DIR`
instead of `.local`; it prints the `eccodes_definitions` value:

```
.local/eccodes-cosmo-mars/definitions:.local/eccodes-cosmo-resources/share/eccodes-cosmo-resources/definitions
```

cosmo-mars comes first: it complements cosmo-resources with the MARS concepts (`class`,
`stream`, `type`, `model`, `expver`, `timespan`) that FDB needs. The cosmo-resources
definitions are installed, not copied into this repository.

`make_metkit_home.py` adds `icon-ch1-eps,icon-ch2-eps` by default. Pass other models
with `--models` (the values eccodes-cosmo-mars `varda-ext` can produce are listed in
`docs/sites/meteoswiss.md`):

```bash
uv run python examples/meteoswiss/make_metkit_home.py \
    --models icon-ch1-eps,icon-ch2-eps,varda-single,varda-single-g
```

## Dev FDB

The MeteoSwiss dev FDB is created by the generic `scripts/init_dev_fdb.py`, with the
site environment so that eccodes decodes the samples with the COSMO definitions:

```bash
DEFS=$PWD/.local/eccodes-cosmo-mars/definitions:$PWD/.local/eccodes-cosmo-resources/share/eccodes-cosmo-resources/definitions
ECCODES_DEFINITION_PATH=$DEFS METKIT_HOME=$PWD/.local/metkit-home ECCODES_VERSION_CHECK_OFF=1 \
    uv run python scripts/init_dev_fdb.py --root .fdb-mch --schema examples/meteoswiss/realtime-varda.schema --seed .raw/meteoswiss
```

It writes `.fdb-mch/{config.yaml,schema,root/}` and archives the 4 messages of the
committed samples in `.raw/meteoswiss/`.

## Run

```bash
cd examples/meteoswiss
uv run snakemake --profile profile -c1
```

The profile configures the storage provider tagged `mch` (`TAG::VALUE`, paths relative
to this directory): the dev FDB config, the definitions, the metkit home, and
`ECCODES_VERSION_CHECK_OFF=1`, which silences the COSMO definitions' version banner.
No site environment variables are needed; override a path on the command line, e.g.
`--storage-fdb-metkit-home mch::/path/to/metkit-home`.

What to expect:

- `glob_wildcards` lists the date/time of every control T_2M field at step 6 in the FDB;
  for the committed samples that is one forecast;
- `t2m_control` retrieves the field
  (`fdb://date=...,time=...,stream=enfo,class=od,expver=0001,model=icon-ch2-eps,type=cf,levtype=sfc,step=6,param=500011`,
  the query in the schema's key order), copies it to `t2m/<date><time>.grib2` and writes
  `T_2M 6 0` (shortName, step, number) to `t2m/<date><time>.txt`;
- the local copy under `.snakemake/storage/mch/` is removed after the run, and a second
  run reports "Nothing to be done".

The Snakefile's comments explain why `t2m_control` is a `shell` rule and why the
decoder writes to its own log; `docs/sites/meteoswiss.md` has the query conventions
(lower-case `model`, COSMO paramIds, `number` only with `type=pf`, `timespan=fs` for
accumulations).

## Samples

`.raw/meteoswiss/` holds three ICON-CH2-EPS files (control T_2M, control TOT_PREC,
perturbed T_2M members 1–2) as constant-field copies of 175–350 bytes with the original
MARS keys. To refresh them (OGD keeps data for 24 h, so this picks the newest forecast):

```bash
ECCODES_DEFINITION_PATH=$DEFS ECCODES_VERSION_CHECK_OFF=1 \
    uv run python examples/meteoswiss/fetch_ogd_samples.py --empty-data --force
```

Without `--empty-data` it only writes the full-size files to
`.local/raw-full/meteoswiss/` (git-ignored); `--out DIR` and `--empty-data DIR` choose
other directories, `--reference-datetime`, `--horizon` and `--members` other fields.
The file names carry the reference time, which the tests read.

## Site tests

```bash
export SMK_FDB_TEST_MCH_SAMPLES=$PWD/.raw/meteoswiss \
    SMK_FDB_TEST_ECCODES_DEFINITIONS=$DEFS \
    SMK_FDB_TEST_METKIT_HOME=$PWD/.local/metkit-home \
    ECCODES_VERSION_CHECK_OFF=1
SMK_FDB_TEST_REQUIRE_SITES=1 uv run pytest tests/sites/meteoswiss -m site_meteoswiss -q -rs
```

`SMK_FDB_TEST_MCH_SCHEMA` defaults to `examples/meteoswiss/realtime-varda.schema`. Without
the variables the suite skips, naming what is missing; with `SMK_FDB_TEST_REQUIRE_SITES=1`
it fails instead. The live OGD test runs only with `SMK_FDB_TEST_OGD_LIVE=1` and writes
into pytest's temporary directory.
