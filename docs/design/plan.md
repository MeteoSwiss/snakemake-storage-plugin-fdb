# snakemake-storage-plugin-fdb — Implementation plan (revision 4)

Companion to `spec.md`. Steps are ordered, small and independently testable; each lists
the files it touches, acceptance criteria and the commands to run. Commands assume the
repo root as cwd and `uv` on PATH (no `python` on PATH; always `uv run ...`). Set
`UV_CACHE_DIR` to a writable location if the default cache is not writable (it was not
on this node).

**Test data is in git (decided).** `.raw/` (ECMWF samples `template.grib`,
`quantile.grib`, `steprange.grib`, `synth11.grib`, `schema`) and `.raw/meteoswiss/` are
committed. The MeteoSwiss OGD samples are committed as empty-data GRIB (constant field,
`grid_simple`, `bitsPerValue=0`, 175–350 B each, MARS keys unchanged); full-size
originals live in the git-ignored `.local/raw-full/meteoswiss/`. CI uses the in-repo
samples; the fetch script only refreshes them. `compare.grib` is dropped. Tests still
skip with a reason when data is absent. (Byte sizes quoted below for OGD files refer
to the full-size originals.)

**Version pins (decided: Option A, spec §11).** `pyfdb>=5.21.4.21,<5.22`,
`eccodes>=2.47,<2.48` (bundled eccodes 2.47, same minor series as current site
definitions). Required CI jobs use the locked 5.21.4.x stack; an optional
`pyfdb-latest` canary runs 5.23 (step 11).

**Site neutrality of the package, first-class MeteoSwiss support (spec §1 goals 5 and
7).** Nothing MeteoSwiss-specific in `src/`: no keys/values, schemas, language patches,
definitions aliases or ordering special cases — but the plugin must work end-to-end
when the user supplies them via env vars/settings. Site material lives in
`examples/meteoswiss/` and `docs/sites/`; the MeteoSwiss test suite under
`tests/sites/meteoswiss/` is **required**: steps 5, 6, 7 and 9 are accepted only when
it passes with the COSMO definitions set, and CI runs it (step 11). Enforced code
location by `tests/test_no_site_specifics.py` and a CI grep (steps 0, 4, 10, 11).
The same rule applies to `scripts/` as a documentation rule (no grep enforces it):
nothing site-specific there either; site setup commands live in `examples/meteoswiss/README.md`.
Site-suite prerequisites for local runs: `examples/meteoswiss/setup.sh`,
`examples/meteoswiss/make_metkit_home.py`, `examples/meteoswiss/fetch_ogd_samples.py`,
then export `SMK_FDB_TEST_MCH_SAMPLES=.raw/meteoswiss`,
`SMK_FDB_TEST_ECCODES_DEFINITIONS=.local/eccodes-cosmo-mars/definitions:.local/eccodes-cosmo-resources/share/eccodes-cosmo-resources/definitions`,
`SMK_FDB_TEST_METKIT_HOME=.local/metkit-home` (schema defaults to
`examples/meteoswiss/realtime-varda.schema`).

Layout (mirrors the current `poetry scaffold-snakemake-storage-plugin` output: `src/`
layout, `tests/test_plugin.py`, ruff, release-please + conventional PRs):

```
pyproject.toml            hatchling build backend, uv-managed
README.md, LICENSE, docs/intro.md, docs/further.md
src/snakemake_storage_plugin_fdb/
    __init__.py           StorageProviderSettings, StorageProvider, StorageObject
    query.py              grammar, parser, normaliser, local suffix, request builders
    backend.py            pyfdb access layer (config resolution, per-thread handles, inspect/list/retrieve/archive,
                          timestamp, expansion, schema parsing, error mapping, canonical-spelling check)
    grib.py               eccodes helpers (split messages, MARS keys, paramId)
    guard.py              IdentifierGuard protocol, NoGuard, StrictGuard stub (reserved)
tests/
    conftest.py           temp FDB fixtures, derived GRIB variants, env setup, skip markers, logged subprocess runner (e2e)
    test_query.py         unit tests (no FDB)
    test_key_order.py     schema-derived / setting / generic key order (no FDB)
    test_settings.py      settings validation (no FDB)
    test_no_site_specifics.py   greps src/ for mch|meteoswiss|cosmo|icon-ch
    test_grib.py          eccodes helpers
    test_backend.py       backend against a temp FDB
    test_plugin.py        TestStorageBase subclasses (read, write), conformance + store/remove/glob tests
    test_workflow.py      end-to-end snakemake run on examples/ecmwf/
    data/schema           extended pyfdb test schema (committed, 212 B)
    sites/meteoswiss/     required site suite, configured via SMK_FDB_TEST_* env vars (conftest.py, test_read/write/glob/workflow/conventions.py)
examples/ecmwf/                                       generic ECMWF-style example: Snakefile, config.yaml, README.md
examples/meteoswiss/                                  everything MeteoSwiss, outside the package:
    README.md (incl. the MeteoSwiss dev-FDB command), realtime-varda.schema, profile/config.yaml, Snakefile,
    grib_keys.py (grib_ls stand-in for the shell rule),
    fetch_ogd_samples.py (OGD STAC API -> .local/raw-full/meteoswiss/, --empty-data also -> .raw/meteoswiss/;
    stdlib urllib + eccodes),
    setup.sh (clone eccodes-cosmo-mars, uv pip install --target eccodes-cosmo-resources-python into .local/),
    make_metkit_home.py (language.yaml recipe)
docs/sites/meteoswiss.md
scripts/init_dev_fdb.py            creates and seeds a dev FDB (generic: --root, --schema, --seed, --variants; no site flags)
scripts/fetch_ecmwf_samples.py     optional, not part of any step: re-downloads the committed .raw/ ECMWF files
                                   from ecmwf/fdb at a pinned commit (provenance in spec §2.1)
.github/workflows/ci.yml, release-please.yml, conventional-prs.yml
```

---

## Step 0 — Scaffold with uv

Files: `pyproject.toml`, `README.md`, `LICENSE` (BSD-3-Clause), `src/snakemake_storage_plugin_fdb/__init__.py`
(docstring only; provider classes arrive in step 4), `.gitignore` (add `.snakemake/`,
`*.part`, `.local/`, `.fdb/`, `.fdb-mch/` (step 10), `.venv/`, `__pycache__/`, `dist/`,
the coverage output `.coverage`, `htmlcov/` (step 8), and the personal work-tracking files
`CLAUDE.local.md`, `WORK.md`; not `.raw/`, which is committed),
`uv.lock` (committed), `[tool.ruff]`. [done: 09c3c53]

```toml
[project]
name = "snakemake-storage-plugin-fdb"
version = "0.1.0"
description = "Snakemake storage plugin for ECMWF's Fields DataBase (FDB)"
readme = "README.md"
requires-python = ">=3.11,<4.0"
license = "BSD-3-Clause"
license-files = ["LICENSE"]
authors = [{ name = "Francesco Zanetta" }]
dependencies = [
  "snakemake-interface-common>=1.23,<2",
  "snakemake-interface-storage-plugins>=4.4.1,<5",
  "tenacity>=9.1.4,<10",           # used directly for the read-path retry policy (step 5); same bound as the interface
  "pyfdb>=5.21.4.21,<5.22",        # Option A (spec §11); Option B: "pyfdb>=5.23.2.27,<6"
  "eccodes>=2.47,<2.48",           # Option B: "eccodes>=2.48,<3"
  "pyyaml>=6",
]
[dependency-groups]
dev = ["snakemake>=9.27", "pytest>=8", "ruff", "coverage"]
[build-system]
requires = ["hatchling"]
build-backend = "hatchling.build"
[tool.hatch.build.targets.wheel]
packages = ["src/snakemake_storage_plugin_fdb"]
[tool.pytest.ini_options]
testpaths = ["tests"]
markers = ["needs_raw: needs .raw/ ECMWF samples", "site_meteoswiss: MeteoSwiss site suite (env-configured; required in CI)"]
[tool.ruff]
line-length = 88
target-version = "py311"
src = ["src", "tests"]
[tool.ruff.lint]
select = ["E", "F", "W", "I", "B", "UP"]
```

Snakemake discovers the plugin by the module-name prefix `snakemake_storage_plugin_`.

Acceptance: `uv sync` succeeds on Linux; `uv run python -c "import
snakemake_storage_plugin_fdb"` works without loading `libfdb5`; `uv build` produces a
wheel; `grep -riE 'mch|meteoswiss|cosmo|icon-ch' src/` finds nothing (also a
`tests/test_no_site_specifics.py` from step 4 on).

Commands: `uv sync`, `uv run ruff check . && uv run ruff format --check .`,
`uv build`, `! grep -riE 'mch|meteoswiss|cosmo|icon-ch' src/`.

## Step 1 — Query parser and local path (`query.py`) + unit tests

Files: `src/snakemake_storage_plugin_fdb/query.py`, `tests/test_query.py`,
`tests/test_key_order.py`, `tests/data/ecmwf-fdb-tests.schema` (ecmwf/fdb test schema
at `63672ea`, key-order fixture). [done: e129782]

```python
class QueryError(ValueError)                              # every parse/order/local-path error
SCHEME = "fdb://"; SUFFIX = ".grib"; NAME_MAX = 255; HASH_CHARS = 24

@dataclass(frozen=True)
class ParsedQuery:
    pairs: tuple[tuple[str, str], ...]        # canonical key order, values verbatim
    def keys(self) -> list[str]
    def value(self, key) -> str               # raw "a/b/c"
    def items(self, key) -> list[str]         # split on "/" outside wildcard tokens
    def has_wildcards(self) -> bool
    def wildcard_keys(self) -> set[str]
    def constant_pairs(self) -> dict[str, str]
    def single_valued(self) -> dict[str, str] # keys with exactly one literal item, no wildcard
    def has_range(self, key) -> bool          # "to"/"by" present (case-insensitive)
    def to_query(self) -> str
    def to_request(self) -> dict[str, str]    # raw values, wildcard text included
    def local_suffix(self) -> str             # raises QueryError (long wildcard component)
    def oversized_components(self) -> list[tuple[str, int]]   # (key, bytes) over NAME_MAX; added in step 4 (wildcard guard)

def parse(query: str, order: KeyOrder | None = None) -> ParsedQuery   # None -> KeyOrder.generic(); raises QueryError
def normalize(query: str, order: KeyOrder | None = None) -> str       # purely syntactic (spec §3.2)
def validate(query: str) -> tuple[bool, str | None]     # order-independent
GENERIC_ORDER = ["class", "expver", "stream", "domain", "date", "time", "type", "levtype", "levelist", "step", "number", "param"]
@dataclass(frozen=True)
class KeyOrder:                                          # keys: tuple[str, ...]; unknown keys alphabetical
    @classmethod
    def from_setting(cls, csv: str); from_schema(cls, schema_text: str); generic(cls)   # QueryError on invalid/duplicate/empty
    def sort_key(self, key: str) -> tuple[int, str]
    def sorted(self, keys: Iterable[str]) -> list[str]
```

Acceptance: acceptance/rejection table (≥ 30 cases; values are generic MARS
spellings — uppercase shortnames, `10m` steps, hyphenated enum values — with no site
names in test data); `normalize` idempotent, metkit-free (a subprocess that imports
`query` and normalises a query has none of `pyfdb`/`eccodes`/`gribapi` in
`sys.modules`); order from setting / schema / generic
(`tests/test_key_order.py`: a schema text with `?`, `?default`, `-` and two rule groups
gives first-appearance order, plus `.raw/schema` when present; `#` comments to end of
line are skipped, as eckit's `StreamParser` does (spec §3.2): a comment block with
brackets and commas, a trailing `# comment` after a rule, and the full ecmwf/fdb
`tests/fdb/etc/fdb/schema` copied to `tests/data/ecmwf-fdb-tests.schema`; unknown keys
alphabetical); wildcard atoms with
constraints verbatim; `/`→`+`; `.grib` suffix; component hashing; commutation property
over a table of (query, wildcard values) using `snakemake.io.apply_wildcards`.

Commands: `uv run pytest tests/test_query.py tests/test_key_order.py -q`.

## Step 2 — GRIB helpers (`grib.py`) + tests

Files: `src/snakemake_storage_plugin_fdb/grib.py`, `tests/test_grib.py` (marked
`needs_raw` where `.raw` files are used; the synthetic parts run always),
`tests/sites/meteoswiss/{conftest.py,test_conventions.py}` (site gating for samples +
definitions, sample MARS keys; extended in steps 5–10). [done: 2660ad9]

```python
class GribError(ValueError)                     # not GRIB, non-GRIB bytes, undecodable/truncated message
@dataclass(frozen=True)
class GribMessage: offset: int; length: int; data: bytes; mars: dict[str, str]; param_id: str
def split_messages(path: str | os.PathLike[str]) -> list[GribMessage]
    # NUL padding between/after messages allowed (GRIB1 120-byte records, spec §2.1);
    # any other byte outside a message -> GribError("... trailing non-GRIB bytes at offset N" /
    # "... non-GRIB bytes at offset N"); no message -> GribError("<path> is not GRIB ...")
def mars_keys(msg: bytes) -> tuple[dict[str, str], str]   # (mars namespace, paramId); GribError if not GRIB/truncated
def variant(template: bytes, zero_values: bool = True, **keys: Any) -> bytes   # GribError on unknown key
```

`eccodes` is imported lazily inside the functions (importing `grib` loads no native
library); `ECCODES_DEFINITION_PATH` is never read or modified. The provider maps
`GribError` to `WorkflowError` (steps 6, spec §6).

Acceptance: `.raw` files → 1 message each with the keys and paramId of spec §2.1,
`data` byte-exact, trailing NUL padding accepted; two concatenated (padded) files → 2
with correct offsets; NUL padding between messages accepted; `template + b"GARBAGE"`,
bytes between/before messages, truncated messages, `b"test"`, empty file → `GribError`;
importing `grib` does not load `eccodes`; `variant()` with zeroed values ≈ 236 bytes; a
GRIB2 message built from eccodes' `GRIB2` sample with `centre=215` decodes (`mars_keys`
returns at least date/time/step/levtype/param); the 2-message
`.raw/meteoswiss/...pert_m1-2.grib2` splits into messages at offsets 0/175 without site
definitions; **site suite**: `tests/sites/meteoswiss/test_conventions.py` decodes the
OGD samples in a subprocess with `ECCODES_DEFINITION_PATH` prepended from
`SMK_FDB_TEST_ECCODES_DEFINITIONS` and gets `class=od stream=enfo expver=0001
model=ICON-CH2-EPS levtype=sfc step=6`, `type=cf`/`pf`, `number=1/2` (pf only),
`param`/paramId `500011`/`500041`, `timespan=none`/`fs`, `date`/`time` from the file name.

Commands: `uv run pytest tests/test_grib.py -q`;
`SMK_FDB_TEST_REQUIRE_SITES=1 uv run pytest tests/sites/meteoswiss -m site_meteoswiss -k conventions -q`.

## Step 3 — Backend (`backend.py`) + tests against a temp FDB

Files: `src/snakemake_storage_plugin_fdb/backend.py`, `tests/conftest.py`,
`tests/test_backend.py`, `tests/data/schema`. [done: 340ddd5]

```python
ConfigValue = Path | str | dict[str, Any] | None
@dataclass(frozen=True)
class Field: key: dict[str, str]; length: int; timestamp: int; uri_path: str | None   # length 0 / uri_path None below level 3
@dataclass(frozen=True)
class SchemaInfo: keys: tuple[str, ...]; optional: frozenset[str]; removed: frozenset[str]; defaults: dict[str, str]
def resolve_config(value: str | None) -> Path | str | None   # existing file -> Path, inline YAML/JSON mapping -> str, else WorkflowError
def resolve_schema_path(config: ConfigValue, env: Mapping[str, str] | None = None) -> Path | None   # no pyfdb (spec §5); None if not an existing file
def parse_schema(schema_text: str) -> SchemaInfo            # keys == KeyOrder.from_schema(text).keys; QueryError without rule keys
def fallback_expand(request: Mapping[str, str]) -> dict[str, list[str]]   # "/" lists, integer and YYYYMMDD to/by ranges (spec §3.4)
def count_fields(expanded: Mapping[str, list[str]]) -> int   # E = prod of distinct values per key (used by expected_count and the storage object)
def map_error(exc: BaseException, query: str, local: str | PathLike | None = None) -> WorkflowError | None   # spec §6; None -> re-raise
class Backend:                                               # methods propagate pyfdb RuntimeErrors; callers use map_error
    def __init__(self, config: ConfigValue = None, user_config: ConfigValue = None,
                 logger: logging.Logger | None = None, schema_info: SchemaInfo | None = None)
    def handle(self) -> pyfdb.FDB                            # archiving handle, threading.local; pyfdb imported here
    def reader(self) -> pyfdb.FDB                            # fresh handle per inspect/list/retrieve (stale catalogue, spec §2.4; added in step 6)
    def inspect(self, request: Mapping[str, str]) -> list[Field]
    def list(self, selection: Mapping[str, str], level: int = 3, include_masked: bool = False) -> list[Field]
    def retrieve_to(self, request, dest: str | PathLike, expected: int | None = None) -> int   # <dest>.part, fsync, os.replace
    def archive(self, data: bytes, identifier: Mapping[str, str] | None = None) -> None
    def flush(self) -> None
    def expand(self, request) -> dict[str, list[str]] | None  # internal FDBToolRequest; None if unavailable; invalid -> RuntimeError
    def expected_count(self, request) -> int                  # count_fields(expand()); fallback_expand when expand() is None
    def spelling_diffs(self, parsed: ParsedQuery, expanded: Mapping[str, list[str]] | None = None) -> list[tuple[str, str, str]]
                                                              # (key, given, canonical), canonical key order; `expanded` = expansion of
                                                              # parsed.constant_pairs() if the caller has it (avoids a second expansion)
    @staticmethod
    def timestamp_of(element: object) -> int
```

`conftest.py`: `os.environ.setdefault("ECKIT_EXCEPTION_IS_SILENT", "1")` before any
plugin import; `fdb_config(root, schema=tests/data/schema)`; session-scoped
`seeded_fdb` under `tmp_path_factory` (skips without `.raw/`): the four `.raw` files
archived natively plus zeroed `class=ea,stream=oper` variants of `template.grib` for
step 0/6/12 × param 167/165 (≈ 12 KB); function-scoped `empty_fdb` factory.
`needs_raw` is defined per test module (as in `tests/test_grib.py`). No site-suite
additions in this step (no site acceptance item); backend use under the varda schema is
exercised by steps 5–7.

Acceptance:
- `inspect` returns 4 fields for the 2×2 request; timestamps between flush start and
  end (within 1 s); aliases/`to`/`by` find 3 fields; missing combinations are omitted;
- `expand({"param": "2t/165", "step": "0/to/12/by/6", "date": "2020-01-01"})` →
  `{"param": ["167","165"], "step": ["0","6","12"], "date": ["20200101"]}`; an invalid
  request raises; fallback path unit-tested by monkeypatching the import to fail
  (`expand` → `None`, `expected_count` via `fallback_expand`, `spelling_diffs` → `[]`);
- `spelling_diffs` for `param=2t,class=EA,step=0/to/6/by/6` → `[("class","EA","ea"),("param","2t","167")]` (canonical key order; range exempt);
- `retrieve_to` writes exactly `sum(length)` bytes in request order, replaces an
  existing file, and on error or byte-count mismatch leaves no `.part` and the
  destination untouched;
- `parse_schema` on `tests/data/schema` → ordered keys, optional {domain, quantile,
  number, levelist}; on an inline schema text using `key-`, `key?default` and two rule
  groups → removed/defaults/order as expected (no site schema file in `tests/data/`);
- `resolve_schema_path(config)` finds the schema for inline YAML, a config file, a
  dict, and the `FDB_CONFIG`/`FDB5_CONFIG`/`FDB_CONFIG_FILE`/`FDB5_CONFIG_FILE`/
  `FDB_HOME` (`config.yaml`/`config.json`, `~fdb`)/`FDB_SCHEMA_FILE` fallbacks; a
  subprocess importing `backend` and calling the pure helpers loads no
  `pyfdb`/`eccodes`;
- `map_error` table from spec §6 (5.21.4.23 strings); `resolve_config` forms;
  `class=zz` → invalid request; missing schema file / root → configuration error at
  first use, not at construction; identifier archive + masking visible with
  `include_masked=True`; one handle per thread.

Commands: `uv run pytest tests/test_backend.py -q`.

## Step 4 — Settings, guard hook and provider (`__init__.py`, `guard.py`)

Files: `src/snakemake_storage_plugin_fdb/__init__.py`, `src/snakemake_storage_plugin_fdb/guard.py`,
`src/snakemake_storage_plugin_fdb/query.py` (`oversized_components`), `tests/conftest.py`
(`clean_env`, `make_provider` fixtures), `tests/test_settings.py`, `tests/test_plugin.py`
(provider part), `tests/test_no_site_specifics.py`. [done: 9185191]

```python
# guard.py
IDENTIFIER_CHECKS = ("none", "strict")
class IdentifierMismatch(ValueError): __init__(self, message_index: int, key: str, identifier_value: str, grib_value: str | None)
@runtime_checkable
class IdentifierGuard(Protocol): def check(self, message: GribMessage, identifier: Mapping[str, str], query: ParsedQuery) -> None
class NoGuard                                   # check() returns None
class StrictGuard                               # __init__ raises NotImplementedError("identifier_check=strict is reserved")
def make_guard(settings) -> IdentifierGuard     # None/"none" -> NoGuard; "strict" -> StrictGuard() (raises); else ValueError
# __init__.py
@dataclass
class StorageProviderSettings(StorageProviderSettingsBase)   # spec §4 fields, typing.Optional[str]
class StorageProvider(StorageProviderBase):
    # attributes: archive_mode, store_check, canonical_spelling, remove_policy, glob_required_keys,
    #             config, user_config, schema_path, schema_info, key_order, backend, guard
    def postprocess_query(self, query) -> str   # normalize(query, key_order); records result; invalid -> unchanged
    def is_normalised(self, query) -> bool      # recorded by postprocess_query (spec §3.3)
class StorageObject(StorageObjectRead, StorageObjectWrite, StorageObjectGlob):
    parsed: ParsedQuery                         # property, cached per query text; WorkflowError if invalid
    def local_suffix(self) -> str               # other abstract methods: NotImplementedError until steps 5-7
```

- `StorageProviderSettings` per spec §4 (incl. `identifier_check`, `canonical_spelling`,
  `eccodes_definitions` as plain paths, `metkit_home`, `key_order`, `env`). No aliases,
  no site names.
- `tests/test_no_site_specifics.py`: walks `src/` and fails on
  `re.search(r"mch|meteoswiss|cosmo|icon-ch", text, re.I)` in any file.
- `guard.py`: `IdentifierGuard` protocol, `IdentifierMismatch` exception, `NoGuard`,
  `StrictGuard` whose `__init__` raises
  `NotImplementedError("identifier_check=strict is reserved")`, and
  `make_guard(settings) -> IdentifierGuard` (returns `NoGuard()` for `none`).
- `StorageProvider.__post_init__`: settings validation (reject `identifier_check` other
  than `none` with the spec's message), env export (`ECKIT_EXCEPTION_IS_SILENT`
  setdefault, `env` overrides, `ECCODES_DEFINITION_PATH` from the plain-path list,
  `METKIT_HOME` with `language.yaml` check), schema path resolution + `KeyOrder`
  (before any lazy import), lazy import, `Backend`, `self.guard`.
- `example_queries`, `is_valid_query`, `postprocess_query`, rate-limiter methods,
  `safe_print`.
- Over-long substituted wildcard guard (spec §3.3): `postprocess_query` records the
  normalised queries; `StorageObject.__post_init__` raises `WorkflowError` if its query
  was not recorded and `local_suffix()` hashes a component. Verify the job-process
  assumption (Snakefile re-parsed → constant queries recorded) against the snakemake
  source and note the result in the spec. Result: verified, no alternative needed
  (spec §3.3).

Acceptance:
- `is_valid_query` accepts the example queries and wildcard forms, rejects `s3://x`,
  `test/x.txt`, `fdb://class=od/expver=0001`, `fdb://`, `fdb://a=`;
- `uv run snakemake --help` lists all `--storage-fdb-*` options;
- `identifier_check="strict"` → `WorkflowError` mentioning "reserved"; `NoGuard.check`
  never raises; `StrictGuard()` raises `NotImplementedError`;
- environment precedence (spec §4.1): with `ECCODES_DEFINITION_PATH=/x` pre-set and
  `eccodes_definitions="<a>:<b>"` (existing temp directories) the process ends with
  `<a>:<b>:/x`, unchanged by a second identical provider; without the setting
  the env is untouched; `METKIT_HOME` pre-set and no setting → untouched; `config`
  given → `FDB_CONFIG`/`FDB_CONFIG_FILE`/`FDB_HOME` left as they were; `env="FDB_HOME=/y"`
  overrides;
- `metkit_home` without `share/metkit/language.yaml` → error at construction;
  `eccodes_definitions` with a non-existent directory → error naming it;
  `key_order="date,time,class"` reorders a query accordingly; a provider whose config
  names `tests/data/schema` orders keys as the schema does;
- `tests/test_no_site_specifics.py` passes;
- provider with an unreachable FDB does not raise until first I/O;
- a pattern `fdb://...,param={p}` whose substituted value makes the `param` component
  exceed 255 bytes → `WorkflowError` at object construction; a constant over-long list
  query that went through `postprocess_query` is hashed without error.

Commands: `uv run pytest tests/test_settings.py tests/test_plugin.py -q -k "valid or example or settings or guard"`.

## Step 5 — Read path

Files: `__init__.py`, `backend.py` (`count_fields`, `spelling_diffs(parsed, expanded=None)`),
`pyproject.toml`/`uv.lock` (`tenacity` declared), `tests/test_plugin.py` (`needs_raw`),
`tests/conftest.py` (`clean_env` also resets the spelling-warning set),
`tests/sites/meteoswiss/{conftest.py,test_conventions.py,test_read.py}` (`mch_schema`,
`metkit_home`, `mch_sample` fixtures; read suite). [done: 9a88020]

`_fields()` (one `inspect`, not cached, spec §6), `exists()` (all combinations),
`mtime()` with `os.stat` fallback, `size()`,
`checksum() -> None`, `retrieve_object()`, `inventory()`, `get_inventory_parent() ->
None`, `cleanup()` (replacing the step-4 `NotImplementedError` stubs; `local_suffix()`
and `parsed` exist since step 4); the canonical-spelling check (spec §7.12) runs
once per object when `expand()` is first computed (`_expanded()`, cached per query
text; the same expansion is handed to `spelling_diffs`, so metkit expands once per
object); the FDB I/O helpers `_inspect`/`_retrieve_to` behind
`exists/mtime/size/retrieve_object` are wrapped with
`retry_decorator(f).retry_with(reraise=True)` (`_retry_fdb_io`: the interface's
policy, the last attempt's own exception surfaced; deterministic plugin errors are not
retried, spec §6); pyfdb errors are mapped in the `_mapping_errors(local=None)` context
manager (`map_error`, spec §6).

Acceptance:
- `TestStorageRead(TestStorageBase)` (`retrieve_only=True, delete=False, files_only=True`)
  passes `test_storage` and `test_storage_not_existing`;
- 3 of 4 fields present → `exists() is False`; `retrieve_object()` names the missing one;
  no field found → the error lists the optional schema keys the query omits; `class=zz`
  → invalid request; a wildcard query → error; a transient `inspect` failure is retried;
- `mtime()` float within 1 s of the fixture flush; monkeypatched `timestamp=0` →
  `os.stat` value;
- `inventory()` fills exists/mtime/size for `cache_key()` only (mtime/size only when it
  exists), with one `inspect`, never `checksum`;
- `test_canonical_spelling`: `param=2t` query logs exactly one warning containing
  `canonical: 167`; `canonical_spelling="error"` raises; `ignore` is silent; a `to/by`
  step range triggers nothing; local path unchanged by the check;
- **site suite (required):** `tests/sites/meteoswiss/test_read.py` — with the COSMO
  definitions, varda schema and metkit home from the env vars, `exists/mtime/size/
  retrieve_object` on the OGD samples pre-archived into a temp FDB (`cf` T_2M,
  `cf` TOT_PREC with `timespan=fs`, `pf` members 1/2) pass; `number=1/to/3` →
  `exists()` False; TOT_PREC without `timespan` → `exists()` False; without `metkit_home` the `model` request is an invalid-request
  error. FDB access runs in subprocesses (definitions and MARS language must be set
  before the libraries load; metkit reads the language once per process), configured
  once by the `eccodes_definitions`/`metkit_home` settings and once by plain
  `ECCODES_DEFINITION_PATH`/`METKIT_HOME` (`test_read_samples[settings|env]`,
  `test_read_model_requires_metkit_home`).

Commands: `uv run pytest tests/test_plugin.py -q -k "Read or exists or mtime or retrieve or inventory or spelling"`;
`SMK_FDB_TEST_REQUIRE_SITES=1 uv run pytest tests/sites/meteoswiss -m site_meteoswiss -k read -q`.

## Step 6 — Write path (with the guard hook wired, not implemented)

Files: `__init__.py`, `backend.py` (`reader()`: fresh handle per read, spec §2.4/§5),
`query.py` (`comparable`, `INT_RE` shared with `backend.py`), `tests/test_plugin.py`
(`needs_raw`), `tests/test_query.py` (`test_comparable`), `tests/test_backend.py`
(stale-read regression), `tests/conftest.py` (`clean_env` resets `_REMOVE_WARNED`;
`fdb_config_file` factory), `tests/sites/meteoswiss/{conftest.py,test_read.py,test_write.py}`
(`run_site`, `mch_fdb_config`, `mch_query_base` fixtures shared by the read and write
suites). [done: 7ac0c8f]

Implement spec §7.7 (count check, identifier and native modes, pre-check, duplicates,
**guard call site** `self.provider.guard.check(msg, identifier, parsed)` for every
message before the first `archive()`, post-check with `t_start`, `store_check`) and §7.8
(`remove_policy`). Everything checkable from the file fails before any `archive()`
("nothing was archived"); only the post-check and archive failures leave fields behind,
and their messages say so.

Acceptance:
- store a 3-message file matching `step=0/6/12,param=167` → `exists()`, `mtime() >=
  t_start`, `size()` = sum of message lengths (= file size for unpadded input; smaller
  for NUL-padded GRIB1, spec §7.2), retrieve after deleting the local copy gives the same
  message keys; both archive modes (`test_store_roundtrip`);
- the post-check takes `t_start` from `backend.fdb_time()`, FDB's own index clock, so
  a field stamped with the second before `int(time.time())` counts as fresh (spec §2.2,
  §7.7): a store passes with libc `time()` and the index timestamps both faked at a past
  second (`test_store_post_check_uses_fdb_clock`); `fdb_time()` returns libc's second,
  or `int(time.time())` without libc (`test_fdb_time_is_c_time`);
- `template.grib` (`enda`, `number=0`) stores in identifier mode under
  `tests/data/schema` **and** under `.raw/schema` (number dropped); native mode under
  `.raw/schema` raises the mapped schema error (`test_store_template`);
- strict, both modes: 2 of 3 fields → error; foreign field → error (identifier: pre-check
  before archiving; native: post-check); duplicate → error; trailing garbage → error;
  text file → "not GRIB" (`test_store_strict_rejects`);
- warn: 2 of 3 fields → warning, `exists()` False afterwards; foreign field still error
  (`test_store_warn_fewer_fields`);
- a test guard (`RecordingGuard`) injected via `provider.guard` sees every message with
  its identifier **before** any archive call; a guard raising `IdentifierMismatch` on
  message 2 leaves FDB unchanged (`inspect` empty) and surfaces as `WorkflowError`
  (`test_store_guard_*`);
- masking: `mtime()` increases, new bytes retrieved, `include_masked=True` shows 2
  (`test_store_masking_rerun`; needs fresh read handles, spec §2.4);
- a failing second `archive()` → error saying 1 of 3 calls succeeded and the fields stay
  in FDB, no retry; wildcard query rejected;
- `remove()` policies (`test_remove_policy`); 4 threads storing 4 distinct outputs
  concurrently (`test_store_threads`);
- **site suite (required):** `tests/sites/meteoswiss/test_write.py` — store the OGD
  ctrl T_2M file (native and identifier mode) under the varda schema via
  `fdb://class=od,expver=0001,stream=enfo,model=icon-ch2-eps,date=<d>,time=<t>,type=cf,levtype=sfc,step=6,param=500011`;
  listed keys have `domain=''` (removed by `domain-`, spec §2.8), `number=''`,
  `timespan=none`, `model=icon-ch2-eps`; retrieve is byte-identical to the sample file
  (`test_write_ctrl[native|identifier]`); the 2-member file via `type=pf,number=1/2`
  (`test_write_members`); strict `store_check` rejects the 2-member file stored as
  `number=1/3` (identifier: pre-check, nothing archived; native: post-check, member 1
  left in FDB; `test_write_strict_rejects_foreign_member`); `remove_policy=warn` logs
  once per query and deletes nothing (`test_write_remove_policy_warn`). Configured
  through the `eccodes_definitions`/`metkit_home` settings, in subprocesses.

Commands: `uv run pytest tests/test_plugin.py -q -k "store or remove or thread or guard"`;
`SMK_FDB_TEST_REQUIRE_SITES=1 uv run pytest tests/sites/meteoswiss -m site_meteoswiss -k write -q`.

(Step 6 was accepted with `identifier` as the default archive mode; step 6a changes
the default to `native`, and the identifier-specific tests above now set
`archive_mode=identifier` explicitly.)

## Step 6a — Default archive mode and single-value identifier check

Files: `__init__.py` (`archive_mode` default `native` and help; `_allowed_values`/
`_precheck` cover single-valued keys, keys with items of mixed kind dropped up front;
`_canonical_single_values`: identifier values from the query spelled as in the
object's expansion; `_expanded()` logs a missing metkit expansion once, for the
spelling check and identifier mode alike), `query.py` (`comparable`: one- or
two-digit `time` as hours, `date` only as `YYYYMMDD`), `tests/test_settings.py`,
`tests/test_query.py`, `tests/test_plugin.py` (`_stored_key` helper),
`tests/sites/meteoswiss/test_write.py`.
Decisions of 2026-09-15, reversible (spec §7.7). [done: 26ba6bb]

Acceptance:
- default settings → `archive_mode == "native"`; `snakemake --help` shows it;
- `synth11.grib` under `tests/data/ecmwf-fdb-tests.schema` stores and reads back with
  default settings; with `archive_mode=identifier` it fails with "cannot determine"
  (`test_store_default_native_under_multi_rule_schema`);
- identifier mode: a single-valued `step`/`param` that contradicts message 2 → error
  naming key, both values, message index and file, "nothing was archived", FDB empty
  (`test_store_identifier_single_value_mismatch`); `param=167` and `param=167.128` vs
  GRIB `167.128` store and, like `time=0`/`00`, are archived in canonical spelling (`param=167`,
  `time=0000`) and found and retrieved by the canonical query, post-check included
  (`test_store_identifier_archives_canonical_spelling`); without expansion the value
  stays verbatim with a debug log (`test_store_identifier_verbatim_without_expansion`); `quantile=1:10` for a GRIB without `quantile` labels the field
  (`test_store_identifier_key_absent_from_message_takes_query_value`);
- both modes stay covered (`test_store_roundtrip`, `test_store_strict_rejects`,
  `test_store_warn_fewer_fields`, `test_store_template`); guard and partial-archive
  tests set `archive_mode=identifier`;
- **site suite:** `test_write_identifier_param_mismatch` (ctrl T_2M stored as
  `param=500041` → error, nothing archived); all step 5/6 site tests still pass.

Commands: `uv run pytest -q`;
`SMK_FDB_TEST_REQUIRE_SITES=1 uv run pytest tests/sites/meteoswiss -m site_meteoswiss -q`;
`uv run snakemake --help | grep -A3 -- --storage-fdb-archive-mode`.

## Step 7 — Glob (`list_candidate_matches`)

Files: `__init__.py` (`list_candidate_matches`, retried `_list`), `tests/test_plugin.py`
(`needs_raw`), `tests/sites/meteoswiss/test_glob.py`. [done: 807255b]

Implement spec §7.9: required keys constant (wildcard or absent → error), one `list` of
the constant pairs, candidates from the pattern's pairs with listed values for
wildcard-bearing keys, sorted and de-duplicated.

Acceptance: pattern with `step={step}` (also `{step,\d+}`) against the fixture →
candidates for steps 0/6/12, and Snakemake's `glob_wildcards` on the storage-object
pattern (which matches them against `regex_from_filepattern(pattern)`) returns them
(`test_glob_step_candidates_match_pattern`); keys absent from the pattern collapse
(`test_glob_keys_absent_from_pattern_collapse`); elements lacking a pattern key or with
an empty value for it are skipped (`test_glob_skips_fields_without_the_wildcard_key`);
a wildcard inside a value (`date={year}0101`) with a constant list
(`test_glob_wildcard_inside_value_and_constant_list`); `glob_required_keys` enforcement
for a wildcard, an absent key and a setting value (`test_glob_required_keys_enforced`),
empty setting allows any pattern (`test_glob_required_keys_empty_allows_any_pattern`);
`class=zz` → invalid request (`test_glob_invalid_value`); a transient `list` failure is
retried (`test_glob_retries_transient_list_error`, sharing `_fail_once` with the
`exists` retry test);
**site suite (required):** `tests/sites/meteoswiss/test_glob.py` — pattern
`...,type=pf,number={member},step=6,param=500011` over the archived OGD members →
the two candidates are the normalised pattern with `member` 1 and 2 substituted
(`apply_wildcards`; `test_glob_members`); without `type` (`number={member,\d+}`) the cf
fields (`number=''`) are skipped (`test_glob_members_skip_control`); a `param={p}`
pattern yields COSMO ids (`500011`, `500041`) as canonical strings
(`test_glob_params_are_canonical_cosmo_ids`); `model={model}` yields lower-case
`icon-ch2-eps` (`test_glob_model_is_listed_lower_case`). Values are checked through
Snakemake's `glob_wildcards` in the subprocess.

Commands: `uv run pytest tests/test_plugin.py -q -k glob`;
`SMK_FDB_TEST_REQUIRE_SITES=1 uv run pytest tests/sites/meteoswiss -m site_meteoswiss -k glob -q`.

## Step 8 — `TestStorageBase` integration pass and interface conformance

Files: `tests/test_plugin.py` (`FDBStorageBase`, `TestStorageRead` gating,
`TestStorageWrite`, `test_interface_conformance`,
`test_managed_wrappers_without_rate_limiter`), `tests/conftest.py`
(`temp_fdb_config`). [done: 85fd3ff]

Spec §9.4 lists the conformance tests, and why each base test runs, is gated or has
an override. No base test is disabled; `touch = False` and `files_only = True` match
spec §1 non-goals.

Acceptance:
- `uv run pytest -q` green with `.raw/` present;
- green with skips without it (`-rs` shows one skip reason per gated test):
  `TestStorageRead::test_query_validation` and `::test_example_queries` still run;
- `TestStorageRead` and `TestStorageWrite` pass every `TestStorageBase` test;
- the plugin loads through `StoragePluginRegistry` as read-write, and `StorageObject`
  is not a `StorageObjectTouch`;
- the `managed_*` wrappers pass through the disabled rate limiter;
- `coverage report` ≥ 85 % on `src/` when data is present.

Commands: `uv run pytest tests/test_plugin.py -q -rs -k "TestStorage or conformance or managed"`;
`uv run coverage run -m pytest -q -rs && uv run coverage report --include='src/*'`
(`.coverage` and `htmlcov/` are git-ignored, step 0).

## Step 9 — Dev FDB and example workflow end-to-end

Files: `scripts/init_dev_fdb.py`, `examples/ecmwf/Snakefile`, `examples/ecmwf/config.yaml`,
`examples/ecmwf/README.md`, `tests/test_workflow.py` (skipped without `.raw/`),
`tests/conftest.py` (`subprocess_env` and `run_logged` fixtures: clean environment and
logged subprocess runner shared by both e2e tests), `tests/sites/meteoswiss/conftest.py`
(`run_site` uses `subprocess_env`; `mch_stamp` fixture), `tests/sites/meteoswiss/test_workflow.py`,
`pyproject.toml` (`addopts = ["--import-mode=importlib"]`, so both suites may have a
`test_workflow.py` without `__init__.py` files). [done: d76eea2]

Decided 2026-09-15 (reversible): the generic example lives in `examples/ecmwf/`, next to
`examples/meteoswiss/` (one examples directory instead of a separate top-level one).

`scripts/init_dev_fdb.py [--root DIR] [--schema PATH] [--seed [DIR]] [--variants [FILE]]`:
writes `<root>/schema` (copy of `--schema`, default `tests/data/schema`), `<root>/root/`,
`<root>/config.yaml` (default root `.fdb`; absolute paths, so the config works from any
working directory); defaults are relative to the repository, explicit arguments to the
working directory. `--seed` natively archives every GRIB file (content starting with
`GRIB`) directly in DIR (default `.raw`; subdirectories such as `.raw/meteoswiss/` are
not entered); a missing DIR is reported on stderr and not seeded (exit 0). `--variants`
archives the zeroed `stream=oper` variants of the message in FILE (default
`.raw/template.grib`; a missing FILE is an argument error) for steps 0/6/12 × params
167/165, the fields `examples/ecmwf/` reads. No site flags.

Decided 2026-09-15 (reversible): the variants are an explicit `--variants [FILE]` flag,
not a side effect of `--seed` on a directory holding a file named `template.grib` (a
generic script should not key its behaviour off a file name). The example's one-liner is
`init_dev_fdb.py --seed --variants`.

Decided 2026-09-15 (reversible): `init_dev_fdb.py` stays generic; the former MeteoSwiss
site flag is replaced by `--root`/`--schema`/`--seed [DIR]`. The MeteoSwiss dev FDB is a
documented command in `examples/meteoswiss/README.md`, run with the site env
(`ECCODES_DEFINITION_PATH`, `METKIT_HOME`, `ECCODES_VERSION_CHECK_OFF=1`):
`uv run python scripts/init_dev_fdb.py --root .fdb-mch --schema examples/meteoswiss/realtime-varda.schema --seed .raw/meteoswiss`.
Nothing site-specific goes in `scripts/` (documentation rule, not grep-enforced).

`examples/ecmwf/Snakefile` (ECMWF flavour; `examples/ecmwf/config.yaml` holds
`dates: ["20200101"]`):

```python
configfile: "config.yaml"

storage:
    provider="fdb"

DATES = config["dates"]
INPUT = ("fdb://class=ea,expver=0001,stream=oper,date={date},time=0000,domain=g,"
         "type=an,levtype=sfc,step=0/6/12,param=167")
OUTPUT = ("fdb://class=ea,expver=0002,stream=oper,date={date},time=0000,domain=g,"
          "type=an,levtype=sfc,step=0/6/12,param=167")

rule all:
    input: expand("done/{date}.txt", date=DATES)

rule shift_expver:
    input: storage.fdb(INPUT)
    output: storage.fdb(OUTPUT)
    run:
        import eccodes
        with open(input[0], "rb") as fi, open(output[0], "wb") as fo:
            while (h := eccodes.codes_grib_new_from_file(fi)) is not None:
                eccodes.codes_set(h, "expver", "0002"); eccodes.codes_write(h, fo); eccodes.codes_release(h)

rule done:
    input: storage.fdb(OUTPUT)
    output: "done/{date}.txt"
    run:
        # grib_ls is not shipped with the eccodes wheels: write the local file name,
        # then expver/stream/dataDate/step/paramId of each message, with eccodes
        ...
```

Both rules are `run:` rules, which the local executor spawns (spec §3.3), so the example
takes the FDB as an untagged setting.

The MeteoSwiss example (`examples/meteoswiss/Snakefile`, `profile/config.yaml`) was
sketched here and ships in step 10, which lists its final form. Notes step 9 handed to
step 10: `grib_ls` is not installed by the eccodes wheels (use eccodes from Python in a
`shell` command); keep `t2m_control` a `shell` rule, since a `run:` rule is spawned by
the local executor and loses the tagged profile settings (spec §2.7, verified end to end
in step 9).

Run: `uv run python scripts/init_dev_fdb.py --seed --variants && cd examples/ecmwf && uv run snakemake
--storage-fdb-config ../../.fdb/config.yaml -c1`.

`tests/test_workflow.py` creates a fresh `.fdb/` with `init_dev_fdb.py --root <tmp>/.fdb
--seed --variants` next to a copy of `examples/ecmwf/` in a temporary directory, runs the
command via `subprocess` (`python -m snakemake`; default `native` archive mode; neither
example sets a mode; the `run_logged` fixture of `tests/conftest.py` drops the provider
variables of spec §4.1 from the test process's environment and writes each stage's
output to `<tmp>/logs/<stage>.log`), once per module, and asserts: init seeds 10 messages
(`test_init_dev_fdb_seeds_raw_and_variants`); exit 0, `Storing in storage: <query>`, the
`done` output names the local file
`.snakemake/storage/fdb/class=ea/expver=0002/.../step=0+6+12/param=167.grib` and lists
steps 0/6/12 with `expver=0002`, the file is gone after the run
(`test_workflow_run_stores_and_cleans_local_copies`); the 3 fields are in FDB
(`test_workflow_output_fields_in_fdb`); second run "Nothing to be done"
(`test_workflow_second_run_nothing_to_be_done`); a tiny `glob_wildcards` Snakefile
returns steps 0/6/12 and, with `--keep-storage-local-copies`, keeps the 3 retrieved
files (`test_workflow_glob_wildcards_steps`); `--delete-all-output` logs the remove
warning, deletes `done/`, and leaves the fields
(`test_workflow_delete_all_output_leaves_fields`).

**Site suite (required):** `tests/sites/meteoswiss/test_workflow.py` runs
`init_dev_fdb.py --root .fdb-mch --schema <SMK_FDB_TEST_MCH_SCHEMA> --seed <SMK_FDB_TEST_MCH_SAMPLES>`
with `ECCODES_DEFINITION_PATH`/`METKIT_HOME` from the `SMK_FDB_TEST_*` variables and
`ECCODES_VERSION_CHECK_OFF=1` (the documented MeteoSwiss command; 4 messages,
`test_workflow_init_dev_fdb_site_command`). Until step 10 ships `examples/meteoswiss/`,
the test writes the Snakefile above (no `rule all`; `date`/`time` stay wildcards and the
target `t2m/<date><time>.txt` is built from the sample name, `mch_stamp` fixture; the
shell command copies the input and lists `shortName`/`step` with a `keys.py` written
next to the Snakefile instead of `grib_ls`, stderr to the rule log because the COSMO
definitions truncate a redirected stderr, spec §2.9) and a `profile/config.yaml` with
the `mch::` values of the profile above (definitions and metkit home from the env vars)
into a temporary directory, and runs `snakemake --profile profile -c1 t2m/<date><time>.txt`
**without** `ECCODES_DEFINITION_PATH`/`METKIT_HOME`, so the site reaches FDB only through the tagged
settings: exit 0, the log shows the normalised query retrieved, the output is
byte-identical to the sample, the shell job decodes `T_2M 6`, the local copy under
`.snakemake/storage/mch/` is removed (`test_workflow_retrieves_control_field`); a second
run is a no-op (`test_workflow_second_run_is_a_no_op`). Step 10 switched the test to a
copy of the shipped `examples/meteoswiss/` (see there).

Acceptance: `uv run pytest tests/test_workflow.py -q` green with data; manual run
works; `SMK_FDB_TEST_REQUIRE_SITES=1 uv run pytest tests/sites/meteoswiss -m site_meteoswiss -k workflow -q` green.

Commands: `uv run pytest tests/test_workflow.py -q`;
`SMK_FDB_TEST_REQUIRE_SITES=1 uv run pytest tests/sites/meteoswiss -m site_meteoswiss -k workflow -q`.

## Step 10 — MeteoSwiss site material (outside the package) and required site suite

Files (none under `src/`): `examples/meteoswiss/{README.md,realtime-varda.schema,profile/config.yaml,Snakefile,grib_keys.py,setup.sh,make_metkit_home.py,fetch_ogd_samples.py}`,
`tests/sites/meteoswiss/{test_conventions.py,test_read.py,test_workflow.py}` (existing
since steps 2–9, extended; `test_workflow.py` switched to the shipped example),
`tests/sites/meteoswiss/test_fetch_ogd_samples.py` (new), `docs/sites/meteoswiss.md`,
`.gitignore` (`.fdb-mch/`). [done: 601262e]

`realtime-varda.schema`: evalml's `resources/fdb/realtime-varda.schema` (branch
`enable_fdb`, last changed in `15cf43af69a1c5c90ab64be9deab76a94d371936`, blob
`c64fd7e4b23683a0e9bc3052a72e6b8bd2b016ac`, 3214 B, BSD-3-Clause) behind a `#` header
with that provenance and the license text; the body is byte-identical to the blob and to
the copy the site suite used before.

`setup.sh [--dest .local]` (idempotent): `git clone --depth 1 --branch varda-ext
https://github.com/MeteoSwiss/eccodes-cosmo-mars.git $DEST/eccodes-cosmo-mars` (evalml
uses the ssh URL; https works for the public repo; an existing clone is updated with
`git fetch --depth 1 origin varda-ext` and `git checkout --force --detach FETCH_HEAD`)
and `uv pip install --target $DEST/eccodes-cosmo-resources
"eccodes-cosmo-resources-python>=2.47.0.1,<2.48"` (run in the repository, whose
environment supplies the Python version; another eccodes series needs the constant
edited), then checks both directories and prints the `eccodes_definitions` value to use
(`$DEST/eccodes-cosmo-mars/definitions:$DEST/eccodes-cosmo-resources/share/eccodes-cosmo-resources/definitions`;
`--target` puts the wheel's data files under `<target>/share`, so
`eccodes_cosmo_resources.get_definitions_path()`, which looks in the running
environment's data directory, does not apply). The cosmo-resources definitions are
installed, never copied into the repository (`COSMO-ORG/eccodes-cosmo-resources` declares
no license; the PyPI wheel declares BSD-3-Clause). [verified: run twice with `--dest` in
a scratch directory: clone at `d04363540bb2`, second run a no-op update, both directories
identical to the definitions the site suite used before]

`make_metkit_home.py [--dest .local/metkit-home] [--models icon-ch1-eps,icon-ch2-eps]`:
copies `metkitlib/share/metkit/*` (located via `import metkitlib`) into
`<dest>/share/metkit/` and appends the models (stripped, lower-cased, known ones
skipped) to the context-free `model` enum block via YAML (the one block of
`d["_field"]["model"]["type"]` without a `context`), written back with
`yaml.safe_dump(sort_keys=False)`; a rerun starts from a fresh copy. The default covers
the committed samples and the MeteoSwiss example; other models are passed with
`--models`. The user-facing list of valid values lives in `docs/sites/meteoswiss.md`
only (README and script docstring point there). Valid model values [verified: `marsModel` concepts of
`MeteoSwiss/eccodes-cosmo-mars` via `gh api`; metkit (metkitlib 1.18.3.23) expands each
with a language generated with all of them, the stock language rejects each]: branch
`varda-ext` (`d04363540bb24b1fd432dce2b1e65ba0f27ba591`) `grib2/local.215.def` maps
`cosmo-1e`, `cosmo-2e`, `kenda-1`, `snowpolino`, `icon-ch1-eps`, `icon-ch2-eps`,
`kenda-ch1`, `icon-rea-l-ch1`, `varda-single`, `varda-ens` (upper-case in the GRIB), and
`grib2/local.98.def` maps `varda-single-g`; `main` and `varda` lack `varda-ens` and
`local.98.def`. evalml's `enable_fdb` `patch_metkit_language.py` adds only
`varda-single` and `varda-single-g`. The previously assumed extra models (`varda-single`,
`varda-single-g`, `kenda-ch1`, `icon-rea-l-ch1`) were incomplete.
Documented as a recipe in `docs/sites/meteoswiss.md`; the plugin only sees the resulting
directory through the generic `metkit_home` setting.

Decided 2026-09-15 (reversible): `--models` defaults to `icon-ch1-eps,icon-ch2-eps`
(spec §9.2).

`fetch_ogd_samples.py` (needs only network access; run with `uv run python
examples/meteoswiss/fetch_ogd_samples.py [--collection ch.meteoschweiz.ogd-forecasting-icon-ch2]
[--reference-datetime 2026-09-15T12:00:00Z] [--horizon P0DT06H00M00S] [--members 1,2]
[--out .local/raw-full/meteoswiss] [--empty-data [.raw/meteoswiss]] [--force]`).
By default it writes only full-size files to `--out`, the git-ignored `.local/raw-full/meteoswiss/`.
With `--empty-data [DIR]` it also writes the committed copies to DIR (default
`.raw/meteoswiss/`; another directory to try it without touching the samples): data
section replaced by a constant zero field (`grid_simple`, `bitsPerValue=0`), MARS keys
verified unchanged against the full-size message; existing files are overwritten only
with `--force`, checked once the reference time is known and before any download. The
reference time is the newest control T_2M item's (or `--reference-datetime`; that search
result's asset is reused, no second search for it) and is used for all three samples.
Both sets use the committed naming scheme
`<model>_<YYYYMMDDHHMM>_step<h>_<var>_<ctrl|pert_mA-B>.grib2`, with the model derived
from the collection and the step from `--horizon` (whole hours only; e.g.
`icon-ch2-eps_202609151200_step6_t_2m_pert_m1-2.grib2`). `empty_file(bytes)` is the
one function that turns a full-size file into its committed copy (the offline test
calls it).
Stdlib `urllib` + `eccodes` only, no curl/jq. Reproduces the samples of spec §2.9:

```python
STAC = "https://data.geo.admin.ch/api/stac/v1/search"


def search(collection, variable, perturbed, horizon, reference_datetime=None):
    body = {
        "collections": [collection],
        "forecast:variable": variable,
        "forecast:perturbed": perturbed,
        "forecast:horizon": horizon,
    }
    if reference_datetime:
        body["forecast:reference_datetime"] = reference_datetime
    req = urllib.request.Request(
        STAC,
        data=json.dumps(body).encode(),
        headers={"Content-Type": "application/json"},
    )
    feats = json.load(urllib.request.urlopen(req))["features"]
    # data is retained for 24 h only: without --reference-datetime take the newest feature
    feat = max(feats, key=lambda f: f["properties"]["forecast:reference_datetime"])
    (asset,) = feat["assets"].values()  # exactly one asset per item
    # pre-signed rgw.cscs.ch URL; GET only (HEAD returns an error body)
    return feat["properties"]["forecast:reference_datetime"], asset["href"]


def download(href) -> bytes:
    return urllib.request.urlopen(href).read()


def subset_members(data: bytes, members: set[int]) -> bytes:
    # split with eccodes and keep messages whose perturbationNumber is in `members`
    ...


# T_2M ctrl (1 msg), TOT_PREC ctrl (1 msg), T_2M perturb subset to members 1-2 (2 msgs)
# -> .local/raw-full/meteoswiss/icon-ch2-eps_<YYYYMMDDHHMM>_step<h>_<var>_<ctrl|pert_mA-B>.grib2
# --empty-data: also .raw/meteoswiss/<same name> (constant field, MARS keys checked; --force to overwrite)
```

Decided 2026-09-15 (reversible): the script downloads full-size files to
`.local/raw-full/meteoswiss/` by default and produces the committed `.raw/meteoswiss/`
copies only with `--empty-data` (overwrite only with `--force`), keeping the committed
file names.

The block above is a sketch; the script adds timeouts, the `FetchError` messages and the
empty-data check. After writing, the script prints `grib_ls -n mars`-equivalent key dumps
via eccodes (`class/stream/type/model/step/number/timespan/param` and the message size
per message), decoded with whatever `ECCODES_DEFINITION_PATH` the user set (the README
runs it with cosmo-mars + cosmo-resources); a message without `model` gets a note that
the COSMO definitions are not active.

Expected: 3 files, 4 messages, ≈ 2.27 MB for CH2 (`t_2m` ctrl 567 927 B, `tot_prec`
ctrl 567 951 B, perturbed members 1–2 1 135 854 B); CH1 would be ≈ 4× larger. The
full-size files are kept in the git-ignored `.local/raw-full/meteoswiss/`; the committed
`.raw/meteoswiss/` samples are their empty-data copies (175–350 B, see the header). The
full perturbed CH2 file is 11.4 MB (20 members) and is never kept.

Acceptance for the script: run twice in a row → identical file set in
`.local/raw-full/meteoswiss/` (same reference time within the 24 h window) and nothing
written to `.raw/meteoswiss/`; with `--empty-data` → same names in `.raw/meteoswiss/`,
175–350 B each, MARS keys equal to the full-size files; a second `--empty-data` run
without `--force` refuses to overwrite; run with an expired `--reference-datetime` →
clear error ("no items ...; OGD retains data for 24 h after publication"); `SMK_FDB_TEST_MCH_SAMPLES=.raw/meteoswiss uv run pytest
tests/sites/meteoswiss -rs` afterwards no longer skips for "samples". [verified
2026-09-15, live, with `--out`/`--empty-data` directories in a scratch directory and the COSMO
definitions: reference time 2026-09-15T12:00Z, full-size 567 927 / 567 951 /
1 135 854 B, constant-field copies 175 / 199 / 350 B and byte-identical to the committed
samples, same MARS keys; a rerun gives identical full-size files; `--empty-data` again
refuses; `--reference-datetime 2026-09-10T00:00:00Z` exits 1 with the message above;
`.raw/meteoswiss/` untouched]

Acceptance for `make_metkit_home.py`: the default output contains `icon-ch1-eps` and
`icon-ch2-eps` in the `model` enum and is otherwise equal to the stock `language.yaml`
(parsed YAML); the full list of valid model values is checked against
`MeteoSwiss/eccodes-cosmo-mars` and the evalml `enable_fdb` language patch, and the docs
are corrected if they differ. Result: corrected to the 11 values above (spec §9.2).

`examples/meteoswiss/Snakefile` (run from `examples/meteoswiss/` with
`uv run snakemake --profile profile -c1`; tagged provider, values from the profile):

```python
PYTHON = sys.executable  # the interpreter running Snakemake has eccodes
GRIB_KEYS = Path(workflow.basedir) / "grib_keys.py"

storage mch:
    provider="fdb"

wildcard_constraints:  # t2m/{date}{time}.txt is ambiguous without them
    date=r"\d{8}",
    time=r"\d{4}",

T2M_CONTROL = ("fdb://class=od,expver=0001,stream=enfo,model=icon-ch2-eps,date={date},"
               "time={time},type=cf,levtype=sfc,step=6,param=500011")
RUNS = glob_wildcards(storage.mch(T2M_CONTROL))  # every control T_2M field at step 6

rule all:
    input: expand("t2m/{date}{time}.txt", zip, date=RUNS.date, time=RUNS.time)

rule t2m_control:  # shell rule: runs in the main process, keeps the tagged settings
    input: storage.mch(T2M_CONTROL)
    output: grib="t2m/{date}{time}.grib2", listing="t2m/{date}{time}.txt"
    log: "logs/t2m_{date}{time}.log"  # COSMO definitions truncate a redirected stderr
    shell: "cp {input} {output.grib} && {PYTHON} {GRIB_KEYS} {input} > {output.listing} 2> {log}"
```

`grib_keys.py` prints `shortName step number` per message. `examples/meteoswiss/profile/config.yaml`
(paths relative to `examples/meteoswiss/`; list values reach the plugin unchanged, so
Snakemake does not rebase them on the profile directory):

```yaml
storage-fdb-config: ["mch::../../.fdb-mch/config.yaml"]
storage-fdb-eccodes-definitions: ["mch::../../.local/eccodes-cosmo-mars/definitions:../../.local/eccodes-cosmo-resources/share/eccodes-cosmo-resources/definitions"]
storage-fdb-metkit-home: ["mch::../../.local/metkit-home"]
storage-fdb-env: ["mch::ECCODES_VERSION_CHECK_OFF=1"]
```

`.fdb-mch/` (the README's dev-FDB root) is git-ignored.

`tests/sites/meteoswiss/` (`conftest.py`, `test_read.py`, `test_write.py`,
`test_glob.py`, `test_workflow.py`, `test_conventions.py`; marker `site_meteoswiss`;
part of the default `pytest` run — it skips only when prerequisites are missing and
`SMK_FDB_TEST_REQUIRE_SITES` is unset):
- gating (`conftest.py`): four separate `skipif` reasons from env vars only —
  `SMK_FDB_TEST_MCH_SAMPLES` (dir with the OGD files), `SMK_FDB_TEST_MCH_SCHEMA`
  (default `examples/meteoswiss/realtime-varda.schema`), `SMK_FDB_TEST_ECCODES_DEFINITIONS`
  (colon list of existing dirs), `SMK_FDB_TEST_METKIT_HOME` (must contain
  `share/metkit/language.yaml`); with `SMK_FDB_TEST_REQUIRE_SITES=1` each missing item
  is `pytest.fail` instead of skip. The plugin package is exercised only through its
  public settings (`config`, `eccodes_definitions`, `metkit_home`) or, in a second
  parametrisation, through the plain environment variables `ECCODES_DEFINITION_PATH`/
  `METKIT_HOME` with no plugin settings at all (proves goal 7's "works via generic
  means"); nothing in `src/` refers to this suite;
- fixture: temp FDB with the schema from the env var; provider settings built from the
  env vars (`site_fdb["settings"]`, reused by the `read_site` runner; `site_env` in
  `conftest.py` is the definitions-only environment for subprocesses that decode
  directly); the fixture takes `date`/`time` from the sample file names
  (`mch_query_base`; `test_conventions.py` checks them against the messages — the OGD
  reference time changes with every fetch) and builds queries with them;
- covered by tests from steps 5–7 (sizes quoted are the full-size originals'; tests
  compare against the sample file's actual size):
  - native archive and read, default `archive_mode`:
    `test_write.py::test_write_ctrl[native]` stores the ctrl T_2M file via
    `fdb://class=od,expver=0001,stream=enfo,model=icon-ch2-eps,date=<d>,time=<t>,type=cf,levtype=sfc,step=6,param=500011`
    → `exists()`, `mtime() >= t_start`, `size()` = file size (567 927 B full-size),
    retrieve round trip byte-identical, listed keys have `domain=''`, `number=''`,
    `timespan=none`, `model=icon-ch2-eps` [all verified in spec §2.9];
    `test_read.py::test_read_samples` checks the mtime against the flush interval;
  - identifier archive: `test_write_ctrl[identifier]` (single-rule varda schema);
  - members: `test_write_members` (`...,type=pf,number=1/2,step=6,param=500011` → 2
    fields, `size()` = file size, 1 135 854 B full-size) and `test_read_samples`
    (`number=1/to/3` → `exists()` False, member 3 absent);
  - accumulation needs timespan: `test_read_samples` (`param=500041` without
    `timespan=fs` → `exists()` False; with it → True);
  - model requires metkit home: `test_read_model_requires_metkit_home` (provider without
    `metkit_home` → invalid-request error mentioning `icon-ch2-eps` and the hint);
- added in this step:
  - `test_read.py::test_number_context`: `type=cf` with `number=0` → invalid-request
    error mentioning `number`;
  - `test_read.py::test_canonical_spelling_mch`: query with `param=T_2M,model=ICON-CH2-EPS`
    finds the field and logs one warning listing `500011` and `icon-ch2-eps` (the read
    script collects warnings);
  - `test_conventions.py::test_synthetic_icon` (not sample-gated, but gated on the
    definitions; like `decoded` it runs through `run_site` with `site_env`): a GRIB2
    message from eccodes' `GRIB2` sample (`grib.variant`) with, in this order,
    `centre=215, tablesVersion=15, localTablesVersion=1, grib2LocalSectionPresent=1,
    localDefinitionNumber=253, productDefinitionTemplateNumber=1,
    generatingProcessIdentifier=142, typeOfGeneratingProcess=4, perturbationNumber=0,
    numberOfForecastsInEnsemble=21, discipline=0, parameterCategory=0,
    parameterNumber=0, typeOfFirstFixedSurface=103, scaleFactorOfFirstFixedSurface=0,
    scaledValueOfFirstFixedSurface=2`; `mars_keys` gives `class=od, stream=enfo,
    type=cf, model=ICON-CH2-EPS, expver=0001, levtype=sfc, param=500011` [verified:
    without the local section (definition 253, as in the OGD files) cosmo-mars adds none
    of `class/stream/type/model/expver`];
  - `test_conventions.py::test_key_order_from_site_schema`: a provider (`make_provider`)
    whose FDB config names the schema from `SMK_FDB_TEST_MCH_SCHEMA` normalises a query
    written in ECMWF order to
    `date,time,stream,class,expver,model,type,levtype,number,step,param,levelist,timespan`
    order without any site knowledge in the package; it needs only the schema, whose
    default path exists, so it also runs without the other variables;
  - `test_workflow.py`: runs the shipped example (below);
    `test_workflow_example_profile_is_tagged` (no prerequisites) checks that every
    value of the shipped profile is `mch::`-tagged and that the two keys the e2e test
    overrides are present;
  - `test_fetch_ogd_samples.py` (gated on the definitions, then plain skips, also with
    `SMK_FDB_TEST_REQUIRE_SITES=1`; the script is imported via `PYTHONPATH`, no
    bytecode written): `test_empty_data_reproduces_committed_samples` (needs the
    git-ignored full-size originals in `.local/raw-full/meteoswiss/`; `empty_file` on
    each is byte-identical to the committed copy) and
    `test_fetch_live` (only with `SMK_FDB_TEST_OGD_LIVE=1`; writes into pytest's
    temporary directory: 3 full-size and 3 constant-field files, equal printed MARS keys,
    copies of 175–350 B, refusal without `--force`, error for an expired reference time).
- site e2e, `test_workflow.py`: copies `examples/meteoswiss/` to
  `<tmp>/examples/meteoswiss/`, runs the documented dev-FDB command in `<tmp>`
  (`init_dev_fdb.py --root .fdb-mch --schema <SMK_FDB_TEST_MCH_SCHEMA> --seed
  <SMK_FDB_TEST_MCH_SAMPLES>` with the site env; 4 messages), then in the copy
  `snakemake --profile profile -c1 --storage-fdb-eccodes-definitions mch::<definitions>
  --storage-fdb-metkit-home mch::<metkit home>` without `ECCODES_DEFINITION_PATH`/
  `METKIT_HOME`: the command-line values replace the profile's `.local/` paths, `config`
  and `env` come from the shipped profile; `rule all` globs the control field; the
  normalised query is retrieved, the `.grib2` output is byte-identical to the sample,
  `grib_keys.py` prints `T_2M 6 0`, the local copy under `.snakemake/storage/mch/` is
  removed (`test_workflow_retrieves_control_field`); the second run is a no-op.

Acceptance: `uv run pytest tests/sites/meteoswiss -m site_meteoswiss -q -rs` shows the
exact missing prerequisite when skipping locally; `SMK_FDB_TEST_REQUIRE_SITES=1 ...`
green on a node with samples + definitions (and in CI, step 11);
`grep -riE 'mch|meteoswiss|cosmo|icon-ch' src/` still empty.
[verified 2026-09-15: with `SMK_FDB_TEST_REQUIRE_SITES=1` 27 passed, 1 skipped (live
fetch) both with the earlier scratch definitions/metkit home and with the output of
`setup.sh --dest <scratch>` and `make_metkit_home.py --dest <scratch>`; without the
variables 2 passed (key order, profile), 26 skipped with one reason per missing
prerequisite, and 26 errors with `SMK_FDB_TEST_REQUIRE_SITES=1`; `SMK_FDB_TEST_OGD_LIVE=1`
live test 2 passed (before the review's simplifications; the review re-ran everything
but the live test)]

## Step 11 — CI

Files: `.github/workflows/ci.yml`, `release-please.yml`, `conventional-prs.yml`.

```yaml
name: CI
on: { push: { branches: [main] }, pull_request: }
jobs:
  lint:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: astral-sh/setup-uv@v5
        with: { enable-cache: true }
      - run: uv python install 3.12
      - run: uv sync
      - run: uv run ruff format --check . && uv run ruff check .
      - name: No site-specific code in the package
        run: '! grep -riEn "mch|meteoswiss|cosmo|icon-ch" src/'
  test:
    runs-on: ubuntu-latest
    strategy:
      matrix:
        python: ["3.11", "3.12"]
    env: { ECKIT_EXCEPTION_IS_SILENT: "1" }
    steps:
      - uses: actions/checkout@v4
      - uses: astral-sh/setup-uv@v5
        with: { enable-cache: true }
      - run: uv python install ${{ matrix.python }}
      - run: uv sync --locked --python ${{ matrix.python }}   # pinned pyfdb 5.21.4.x (spec §11 Option A)
      - run: uv run coverage run -m pytest -q -rs -m "not site_meteoswiss"
      - run: uv run coverage report -m
  pyfdb-latest:
    # Optional canary: tells us when the <5.22 pin can be lifted. Never blocks merges.
    runs-on: ubuntu-latest
    continue-on-error: true
    env: { ECKIT_EXCEPTION_IS_SILENT: "1" }
    steps:
      - uses: actions/checkout@v4
      - uses: astral-sh/setup-uv@v5
        with: { enable-cache: true }
      - run: uv python install 3.12
      - run: uv sync --locked
      - run: uv pip install --upgrade "pyfdb>=5.23" "eccodes>=2.48,<3"   # overrides the pin in the venv only
      - run: uv run --no-sync pytest -q -rs -m "not site_meteoswiss"
  site-meteoswiss:
    runs-on: ubuntu-latest
    env: { ECKIT_EXCEPTION_IS_SILENT: "1", SMK_FDB_TEST_REQUIRE_SITES: "1", ECCODES_VERSION_CHECK_OFF: "1" }   # last one: COSMO definitions' version-mismatch banner (spec §2.9)
    steps:
      - uses: actions/checkout@v4
      - uses: astral-sh/setup-uv@v5
        with: { enable-cache: true }
      - run: uv python install 3.12
      - run: uv sync
      - name: Site definitions and MARS language (CI setup step, not plugin code)
        run: |
          bash examples/meteoswiss/setup.sh --dest .local        # clones eccodes-cosmo-mars (varda-ext), installs eccodes-cosmo-resources-python
          uv run python examples/meteoswiss/make_metkit_home.py --dest .local/metkit-home
          echo "SMK_FDB_TEST_ECCODES_DEFINITIONS=$PWD/.local/eccodes-cosmo-mars/definitions:$PWD/.local/eccodes-cosmo-resources/share/eccodes-cosmo-resources/definitions" >> "$GITHUB_ENV"
          echo "SMK_FDB_TEST_METKIT_HOME=$PWD/.local/metkit-home" >> "$GITHUB_ENV"
          echo "SMK_FDB_TEST_MCH_SCHEMA=$PWD/examples/meteoswiss/realtime-varda.schema" >> "$GITHUB_ENV"
      - name: Sample data (committed empty-data samples)
        run: echo "SMK_FDB_TEST_MCH_SAMPLES=$PWD/.raw/meteoswiss" >> "$GITHUB_ENV"
      - run: uv run pytest tests/sites/meteoswiss -m site_meteoswiss -q -rs
```

**CI sample data (decided):** the committed samples under `.raw/` (ECMWF) and
`.raw/meteoswiss/` (OGD fields with emptied data section, commit c4677bd); no download
in CI. The OGD API is not usable for CI (24 h retention); `fetch_ogd_samples.py
--empty-data` is only for refreshing the samples manually.

**pyfdb versions (decided, spec §11):** required jobs run the locked 5.21.4.x stack;
the `pyfdb-latest` job is an optional, non-blocking canary on 5.23 + eccodes 2.48.

Acceptance: lint green (incl. the src grep); `test` job green on the locked stack with
the committed samples (no data-gated skips); `site-meteoswiss` job green with the
committed `.raw/meteoswiss/` samples (with `SMK_FDB_TEST_REQUIRE_SITES=1` it fails
rather than skips if the setup step breaks); `pyfdb-latest` may fail without blocking.

## Step 12 — Docs

Files: `README.md`, `docs/intro.md`, `docs/further.md`, `docs/sites/meteoswiss.md`.

Generic docs (README/intro/further): query grammar with generic MARS examples,
canonical-spelling rule and warning (incl. case of enum values), key ordering
(setting / schema / generic), settings table (all pass-throughs), semantics of
exists/mtime/remove (masking, `fdb purge`), native (default) vs identifier mode
(identifier only with schemas whose rules share one key set; single-valued query keys
checked against the GRIB; relabelling needs `grib_set`) and the reserved
`identifier_check`, wildcard rules, tagged FDBs and profiles, dev FDB setup
(`scripts/init_dev_fdb.py`, `examples/ecmwf/`), the
pyfdb/eccodes version choice, how to point the plugin at *any* site's definitions and
MARS language (`eccodes_definitions`, `metkit_home`, `env`), limitations, platform note.

`docs/sites/meteoswiss.md` (site page, links to `examples/meteoswiss/`; first version
written in step 10, reviewed and completed here): clone
`eccodes-cosmo-mars` `varda-ext` + install `eccodes-cosmo-resources-python`,
`make_metkit_home.py` (default and extra `--models`), the profile, `realtime-varda.schema`,
the MeteoSwiss dev-FDB command (from `examples/meteoswiss/README.md`), `timespan=fs` for
accumulations, COSMO paramIds, lower-case `model`, `number` only for `pf`, fetching
samples from the OGD API (full-size by default, `--empty-data` for the committed
copies) and its 24 h retention, the definitions-version warning
(harmless for patch-level differences; silence with `ECCODES_VERSION_CHECK_OFF=1`,
spec §2.9).

## Step 13 — Identifier guard (post-v1)

Files: `guard.py` (`StrictGuard`), `__init__.py` (settings accept `strict`),
`tests/test_guard.py`.

Implement spec §7.7 `StrictGuard`, on top of the built-in pre-check of step 6a (constant
query keys the message carries, light `comparable` normalisation, incomparable aliases
skipped). The guard adds:
- exact canonicalisation: derive the message's MARS keys (`grib.mars_keys`, with the
  provider's definitions active) and canonicalise both sides the way FDB does (param
  shortnames, step units such as `0m`, time/date aliases the pre-check skips, via
  `Backend.expand` of a one-field request);
- keys the message does not carry (query labels, schema defaults): schema-level
  consistency, i.e. the identifier selects exactly one schema rule and names only that
  rule's keys;
- all mismatches of a file collected and raised as `IdentifierMismatch` together.

Acceptance: `step=0m` in the GRIB vs a `step=10m` identifier is rejected (skipped by the
pre-check); `T_2M` vs `500011` is accepted (canonical equality); an identifier whose
query labels select no single schema rule is rejected; a MeteoSwiss message archived
under `realtime-varda.schema` passes with the correct identifier;
`identifier_check=strict` becomes a legal setting and the reserved-error test is
inverted.

## Step 14 — Upstream PRs to ecmwf/fdb (pyfdb)

- `src/pyfdb_bindings/bindings.cc:334-390`: add
  `.def("timestamp", [](const fdb5::ListElement& e) -> long long { return static_cast<long long>(e.timestamp()); })`
  (`ListElement::timestamp()` exists in `src/fdb5/api/helpers/ListElement.h:71`);
  `src/pyfdb/pyfdb_iterator.py`: `ListElement.timestamp() -> int` with a docstring
  ("index flush time, POSIX seconds; 0 for level < 3 and legacy indexes"); fix the
  9-digit docstring typos in `pyfdb.py`; tests in `tests/pyfdb/integration/test_list.py`.
- Optional: public `pyfdb.expand(selection) -> dict[str, list[str]]` wrapping
  `mars_request_from_map`, so the plugin need not parse `FDBToolRequest` repr.
- Plugin follow-up: prefer `element.timestamp()` / `pyfdb.expand` when present, keep the
  fallbacks.

---

## Order of work and checkpoints

| step | depends on | checkpoint command |
|---|---|---|
| 0 scaffold | – | `uv sync && uv build` |
| 1 query | 0 | `uv run pytest tests/test_query.py` |
| 2 grib | 0 | `uv run pytest tests/test_grib.py` |
| 3 backend | 1, 2 | `uv run pytest tests/test_backend.py` |
| 4 settings/guard/provider | 3 | `uv run snakemake --help \| grep storage-fdb` |
| 5 read | 4 | `uv run pytest -k "Read or spelling"` |
| 6 write (+ guard hook) | 5 | `uv run pytest -k "store or remove or guard"` |
| 6a default native, single-value check | 6 | `uv run pytest -k "store or settings"` |
| 7 glob | 5 | `uv run pytest -k glob` |
| 8 conformance | 5–7 | `uv run coverage run -m pytest -rs` |
| 9 e2e (`examples/ecmwf/`) | 8 | `uv run pytest tests/test_workflow.py` |
| 10 MeteoSwiss site suite (required) | 8 | `SMK_FDB_TEST_REQUIRE_SITES=1 uv run pytest tests/sites/meteoswiss -m site_meteoswiss -rs` |
| 11 CI | 8 | green run |
| 12 docs | 9, 10 | review |
| 13 identifier guard (post-v1) | 6 | `uv run pytest tests/test_guard.py` |
| 14 upstream | 5 | PR links |
