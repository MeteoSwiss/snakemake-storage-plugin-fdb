# Contributing

## Development setup

The project is managed with [uv](https://docs.astral.sh/uv/). Run every tool through
`uv run`; the commands below assume the repository root as working directory.

```bash
uv sync                 # creates .venv with the locked dependencies and the dev group
uv run pytest -q        # generic test suite
uv build                # wheel and sdist in dist/
```

Development and CI run on Linux (the pyfdb wheels are `manylinux_2_28`). `uv.lock` is
committed; update it only on purpose (`uv lock`).

## Running tests

### Generic suite

```bash
uv run pytest -q                                   # everything; site tests skip without their setup
uv run pytest -q -rs                               # with skip reasons
uv run pytest tests/test_query.py -q               # one module
uv run pytest tests/test_plugin.py -q -k glob      # a subset
uv run coverage run -m pytest -q -rs && uv run coverage report --include='src/*'
```

Tests that need the ECMWF samples in `tests/data/grib/ecmwf/` skip without them. Test
FDBs are created in pytest's temporary directories. The end-to-end tests
(`tests/test_workflow.py`) run `snakemake` in subprocesses with a clean environment and
write each stage's output to a log file under pytest's temporary directory.

### Site suite

`tests/sites/meteoswiss/` (marker `site_meteoswiss`) needs the COSMO definitions, a
metkit home with the MeteoSwiss models, the varda schema and the OGD samples. Set them up
once into the git-ignored `.local/` (needs `git` and network access):

```bash
bash examples/meteoswiss/setup.sh                        # .local/eccodes-cosmo-mars, .local/eccodes-cosmo-resources
uv run python examples/meteoswiss/make_metkit_home.py    # .local/metkit-home
```

Then run the suite with the prerequisites in `SMK_FDB_TEST_*` variables:

```bash
export SMK_FDB_TEST_MCH_SAMPLES=$PWD/tests/data/grib/meteoswiss \
    SMK_FDB_TEST_ECCODES_DEFINITIONS=$PWD/.local/eccodes-cosmo-mars/definitions:$PWD/.local/eccodes-cosmo-resources/share/eccodes-cosmo-resources/definitions \
    SMK_FDB_TEST_METKIT_HOME=$PWD/.local/metkit-home
SMK_FDB_TEST_REQUIRE_SITES=1 uv run pytest tests/sites/meteoswiss -m site_meteoswiss -q -rs
```

| variable | meaning |
|---|---|
| `SMK_FDB_TEST_MCH_SAMPLES` | directory with the OGD `*.grib2` samples (`tests/data/grib/meteoswiss`) |
| `SMK_FDB_TEST_ECCODES_DEFINITIONS` | colon list of definitions directories, cosmo-mars first (`/MEMFS/...` entries allowed) |
| `SMK_FDB_TEST_METKIT_HOME` | directory with `share/metkit/language.yaml` defining the models |
| `SMK_FDB_TEST_MCH_SCHEMA` | FDB schema; default `examples/meteoswiss/realtime-varda.schema` |
| `SMK_FDB_TEST_REQUIRE_SITES` | `1`: a missing prerequisite fails instead of skipping (set in CI) |
| `SMK_FDB_TEST_OGD_LIVE` | `1`: also run the live OGD download test |

Without the variables each site test skips and names the missing prerequisite; the two
tests that need only the schema or the shipped profile still run. The site tests run FDB
access in subprocesses, because definitions and the MARS language must be set before the
libraries load.

`test_fetch_ogd_samples.py` has two tests that skip even with
`SMK_FDB_TEST_REQUIRE_SITES=1`: `test_empty_data_reproduces_committed_samples` needs the
git-ignored full-size originals in `.local/samples-full/meteoswiss/`, and
`test_fetch_live` needs `SMK_FDB_TEST_OGD_LIVE=1` and network access (it writes only
into pytest's temporary directory):

```bash
SMK_FDB_TEST_OGD_LIVE=1 uv run pytest tests/sites/meteoswiss/test_fetch_ogd_samples.py -q -rs
```

## Lint and format

```bash
uv run ruff check .
uv run ruff format --check .      # also checks Python code blocks in Markdown files
uv run ruff format .              # apply formatting
! grep -rniE 'mch|meteoswiss|cosmo|icon-ch' src/ scripts/
```

Snakefile snippets in Markdown use the `snakemake` fence, which ruff leaves alone.

CI lints with `uv run --locked --only-group dev ruff ...`, which syncs the environment to
the dev group only and removes the native stack from `.venv`. To reproduce it locally, use
a throwaway environment, e.g. `UV_PROJECT_ENVIRONMENT=<temporary dir> uv run --locked
--only-group dev ruff check .`.

## CI

`.github/workflows/ci.yml` runs on pushes to `main`, pull requests and manual dispatch.

| job | what it runs | required |
|---|---|---|
| `lint` | locked ruff format check and lint; the site-name grep over `src/` and `scripts/`; `CHANGELOG.md` has `## [Unreleased]` | yes |
| `test` | Python 3.11 and 3.12, `uv sync --locked`, `coverage run -m pytest -q -rs -m "not site_meteoswiss"`, coverage report | yes |
| `site-meteoswiss` | `setup.sh --dest .local`, `make_metkit_home.py --dest .local/metkit-home`, the site suite with `SMK_FDB_TEST_REQUIRE_SITES=1` and the committed samples | yes |
| `pyfdb-latest` | the generic suite after upgrading to `pyfdb>=5.23` and `eccodes>=2.48,<3` in the environment (never in `uv.lock`) | no (canary for lifting the pin) |

On GitHub the site job reports 2 skipped tests (the two `test_fetch_ogd_samples.py` tests
above). The coverage report has no threshold; keep `src/` coverage at 85 % or more. After
editing the workflow, check it with:

```bash
uvx --from actionlint-py actionlint .github/workflows/ci.yml
uvx check-jsonschema --builtin-schema vendor.github-workflows .github/workflows/ci.yml
```

## Design documents and consistency

[`design/requirements.md`](design/requirements.md) (what and why, with requirement IDs
and verification) and [`design/architecture.md`](design/architecture.md) (how, with
decisions and verified external facts) describe the current code. **Code, requirements
and architecture must stay consistent**: a change that alters behaviour updates both
documents, the [user guide](user-guide.md) and the [reference](reference.md) in the same
change. When code and documents disagree, resolve it, don't leave it.

- Refer to requirement IDs (`FR-STORE-004`) or document sections
  (`architecture.md §8.3`) in comments and docstrings.
- New requirements get the next free ID; removed IDs are not reused.
- New decisions get an ADR entry with context, decision, status, consequences and date;
  mark reversible ones.
- Label facts about external projects `[verified: how]` or `[assumed]`.

## Changelog

`CHANGELOG.md` follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/). Every
user-visible change adds a concise entry under `## [Unreleased]` (`Added`, `Changed`,
`Fixed`, ...) in the same commit. Internal refactors, tests and design-document edits
need none. A release renames `[Unreleased]` to the version. CI only checks that the
heading exists.

## Commits

- Commits on `main` use [Conventional Commits](https://www.conventionalcommits.org/)
  (`feat: ...`, `fix: ...`, `docs: ...`, `ci: ...`); the body may have several lines.
- No `Co-authored-by` trailers.

## Site neutrality

The package is generic FDB/MARS. Nothing site-specific goes into `src/` or `scripts/`:
no site keys or values, schemas, MARS language patches, definitions paths or aliases,
extras, or special cases. `tests/test_no_site_specifics.py` and the CI grep reject
`mch|meteoswiss|cosmo|icon-ch` there. Site material goes into `examples/<site>/`,
`docs/sites/<site>.md` and `tests/sites/<site>/`, and reaches the plugin only through its
generic settings or environment variables.

## Test data

- `tests/data/grib/ecmwf/` holds the committed ECMWF samples (`template.grib`,
  `steprange.grib`, `quantile.grib`, `synth11.grib`); `tests/data/grib/meteoswiss/`
  holds the MeteoSwiss OGD samples as empty-data GRIB (constant field, `grid_simple`,
  `bitsPerValue=0`, MARS keys unchanged, 175–350 B each).
- `tests/data/` also holds the test schemas: the plugin's own `schema`, pyfdb's
  `pyfdb-tests.schema` and ECMWF's multi-rule `ecmwf-fdb-tests.schema`.
- **Never modify the samples or the copied schemas by hand.** Derived variants are
  created in memory at test time (`grib.variant`).
- New samples must be small (use empty-data or zeroed copies) and have a known license
  and provenance.
- Dev FDBs go into the git-ignored `.fdb/` and `.fdb-mch/`; full-size OGD originals into
  the git-ignored `.local/samples-full/meteoswiss/`.

### Refreshing samples

MeteoSwiss OGD samples (the API keeps data for 24 hours, so this fetches the newest
forecast and changes the file names, which the tests read `date` and `time` from):

```bash
DEFS=$PWD/.local/eccodes-cosmo-mars/definitions:$PWD/.local/eccodes-cosmo-resources/share/eccodes-cosmo-resources/definitions
ECCODES_DEFINITION_PATH=$DEFS ECCODES_VERSION_CHECK_OFF=1 \
    uv run python examples/meteoswiss/fetch_ogd_samples.py --empty-data --force
```

Remove the old files from `tests/data/grib/meteoswiss/` afterwards, then run the site
suite. Without `--empty-data` the script only writes full-size files to
`.local/samples-full/meteoswiss/`; `--empty-data DIR` writes the copies elsewhere, which
is the way to try it without touching the committed samples.

ECMWF samples come unchanged from [`ecmwf/fdb`](https://github.com/ecmwf/fdb) at commit
`63672ea` (Apache-2.0); the source path of each file is in
[architecture §13.2](design/architecture.md#132-ecmwf-samples). There is no script
(requirements.md D-008): copy the files from that commit and compare checksums.

## Deferred work

Planned and deferred items (identifier guard, lifting the pyfdb pin, upstream pyfdb and
Snakemake issues) are listed in [`design/requirements.md`](design/requirements.md) §6.2.
