# snakemake-storage-plugin-fdb

A [Snakemake](https://snakemake.github.io) storage plugin for ECMWF's
[Fields DataBase (FDB)](https://github.com/ecmwf/fdb).

FDB stores meteorological fields: GRIB messages indexed by MARS keys such as `class`,
`date`, `step` and `param`. With this plugin, Snakemake rules use FDB fields as inputs
and outputs. A rule names its fields with a one-line MARS-style query. The plugin
retrieves the fields into a local GRIB file for the job, and archives the GRIB files
that jobs produce.

```text
fdb://class=od,expver=0001,stream=oper,date={date},time=0000,domain=g,type=fc,levtype=sfc,step=0/6/12,param=167
```

**Status:** unreleased, pre-1.0 (version 0.1.0). Interfaces and defaults may still change.

## Features

- **Read and write:** retrieves inputs from FDB and archives outputs into it, through
  [`pyfdb`](https://github.com/ecmwf/fdb).
- **Multi-field queries:** `/` lists, `to`/`by` ranges and Snakemake wildcards. One
  query maps to one local file.
- **Snakemake semantics:** an input exists only when all its fields are in FDB.
  Modification times come from FDB index timestamps. Retrieval is atomic, and transient
  errors are retried.
- **Safe archiving:** field count, GRIB structure and duplicates are checked before
  anything is archived, and the result is verified afterwards.
- **`glob_wildcards`** from FDB listings.
- **Canonical queries:** keys are ordered as in the FDB schema, and non-canonical values
  (`param=2t` instead of `167`) are reported.
- **No deletion:** FDB cannot delete single fields, so removing an output never destroys
  data.
- **Site-neutral:** any FDB schema, eccodes definitions and MARS language, through
  generic settings. MeteoSwiss ICON data is supported and tested.

## Requirements

- Linux on x86_64 or aarch64, glibc ≥ 2.28
- Python ≥ 3.11
- Snakemake ≥ 9.27
- `pyfdb >=5.21.4.21,<5.22` and `eccodes >=2.47,<2.48`, installed as dependencies. Their
  wheels bundle the FDB, metkit, eckit and eccodes libraries.

## Installation

The plugin is not on PyPI yet. Install it into the environment that runs Snakemake,
either from a checkout or from the repository:

```bash
uv pip install /path/to/snakemake-storage-plugin-fdb
uv pip install "git+https://github.com/MeteoSwiss/snakemake-storage-plugin-fdb.git"
```

`snakemake --help` then lists the `--storage-fdb-*` options.

## Quick start

From a checkout, create a small development FDB and run the bundled example:

```bash
uv sync
uv run python scripts/init_dev_fdb.py --seed --variants    # creates and seeds .fdb/
cd examples/ecmwf
uv run snakemake --storage-fdb-config ../../.fdb/config.yaml -c1
```

The workflow retrieves three 2 m temperature fields, archives a copy under
`expver=0002` and lists the copy in `done/20200101.txt`. A second run reports "Nothing
to be done".

In your own Snakefile, wrap FDB queries in `storage.fdb(...)`:

```snakemake
storage:
    provider="fdb"


rule t2m:
    input:
        storage.fdb(
            "fdb://class=ea,expver=0001,stream=oper,date={date},time=0000,domain=g,"
            "type=an,levtype=sfc,step=0/6/12,param=167"
        ),
    output:
        "t2m/{date}.grib",
    shell:
        "cp {input} {output}"
```

```bash
snakemake --storage-fdb-config /path/to/fdb/config.yaml -c1 t2m/20200101.grib
```

With the development FDB's `.fdb/config.yaml`, this writes the three fields to
`t2m/20200101.grib`. The [user guide](docs/user-guide.md) covers profiles, outputs,
globbing and site setups.

## Documentation

- [User guide](docs/user-guide.md): configuration, queries, reading, writing, globbing,
  troubleshooting
- [Reference](docs/reference.md): settings, query grammar, local paths, methods, errors,
  environment variables
- [MeteoSwiss site guide](docs/sites/meteoswiss.md)
- Design: [requirements](docs/design/requirements.md) and
  [architecture](docs/design/architecture.md)
- [Contributing](docs/contributing.md): development setup, tests, CI, conventions
- [Changelog](CHANGELOG.md)

## Repository layout

```text
src/snakemake_storage_plugin_fdb/
    __init__.py          settings, storage provider and storage object (Snakemake interface)
    query.py             query grammar, normalisation, key order, local paths
    backend.py           pyfdb access: configuration and schema, reads, archives, error mapping
    grib.py              GRIB message splitting and MARS keys (eccodes)
    guard.py             identifier guard hook (reserved)
scripts/init_dev_fdb.py  creates and seeds a local development FDB
examples/ecmwf/          generic example workflow
examples/meteoswiss/     MeteoSwiss schema, profile, workflow and setup scripts
tests/                   generic test suite; tests/data/ holds test schemas and GRIB samples
tests/sites/meteoswiss/  MeteoSwiss site test suite
docs/                    user guide, reference, contributing, site and design documents
.github/workflows/ci.yml CI: lint, tests, site suite, pyfdb canary
```

## License

BSD-3-Clause, see [LICENSE](LICENSE). Copyright MeteoSwiss.
