# snakemake-storage-plugin-fdb — Specification

Status: living design document, kept consistent with the implementation (see
`plan.md` for which steps are done).

Every statement is labelled **[verified: how]** or **[assumed]**. "Verified" means it
was checked during design and implementation (2026-09-15 onwards), either by reading the
source of the named project or by running it in a throw-away `uv` environment (initial
prototyping: Python 3.12.12, `pyfdb 5.23.2.27`,
`fdb5lib 5.23.2.27`, `eckitlib 2.2.0.27`, `metkitlib 1.20.2.27`, `eccodeslib 2.48.2.27`,
`eccodes 2.48.0`, `snakemake 9.27.0`, `snakemake-interface-storage-plugins 4.4.1`,
`qubed 0.4.11`, `eccodes-cosmo-resources-python 2.47.0.1`). Source read:
`snakemake-interface-storage-plugins` v4.4.1, `snakemake` 9.27.0, `ecmwf/fdb` at
`63672ea` (5.23.4: `src/pyfdb`, `src/pyfdb_bindings/bindings.cc`, `src/fdb5/...`), the
s3/http/fs plugins, `poetry-snakemake-plugin`, `MeteoSwiss/eccodes-cosmo-mars`
(`main` and `varda-ext`), and the `MeteoSwiss/evalml` branch diff `main...enable_fdb`
(fetched with `gh api`).

Revision 2 changes: MeteoSwiss conventions replaced by what `eccodes-cosmo-mars` and
`evalml` actually do (§2.8, §9.2); canonicalisation policy per user decision (§3.2,
§7.12); identifier guard hook and reserved setting (§4, §7.7); test data is no longer
assumed to be in git (§9; superseded since: the test data is committed, see §9).

Revision 3 changes: MeteoSwiss samples come from the MeteoSwiss Open Government Data
(OGD) STAC API, not from the `/store_new` archive (§2.9, §9.2); real ICON-CH2-EPS
fields verified end-to-end (§2.9); pyfdb/eccodes version options (§11); pyfdb
5.21.4.21 re-verified in a second scratch venv (`venv521`: `pyfdb 5.21.4.21`,
`fdb5lib 5.21.4.21`, `eccodeslib 2.47.3.21`, `metkitlib 1.18.3.21`, `eckitlib 2.1.0.21`).

Revision 4 changes (2026-09-15, reversible, plan step 6a): `archive_mode` defaults to
`native`; identifier mode checks single-valued query keys against the message and
archives query values in canonical spelling; mixed number/string pairs are not
compared (§2.4, §4, §7.7 decisions, §8, §12).

---

## 1. Goals and non-goals

### Goals

1. Read-write Snakemake storage plugin for ECMWF FDB via `pyfdb`: rule inputs are
   retrieved from FDB into one local GRIB file, rule outputs (GRIB files) are archived
   into FDB.
2. One query = one MARS-style request that may address **several fields**
   (`/` value lists, `to`/`by` ranges). It maps to exactly one local file holding all
   matching GRIB messages.
3. FDB location configurable per provider instance (settings, incl. tagged settings),
   falling back to FDB's own environment variables. Dev FDBs live under `.fdb/` (and
   `.fdb-mch/` for the MeteoSwiss example, both git-ignored) in the workspace (test FDBs
   under pytest's temporary directories) and stay small.
4. Correct Snakemake semantics for `exists`, `mtime`, `size`, retrieve, store, remove,
   inventory and `glob_wildcards`, given FDB's constraints (immutable, masked
   overwrites, no per-field deletion).
5. **MeteoSwiss data works end-to-end (first-class, tested goal):** ICON-CH1/CH2-EPS
   GRIB2 decoded with the COSMO definitions (`eccodes-cosmo-mars` +
   `eccodes-cosmo-resources`), archived under a varda-style FDB schema, and requested
   with a patched metkit language — through the read, write, glob and workflow paths.
   Verified with real ICON-CH2-EPS fields from the MeteoSwiss OGD STAC API (§2.9) and
   exercised by a required test suite and by CI (§9.2).
6. `uv`-managed project mirroring the layout of `poetry scaffold-snakemake-storage-plugin`.
7. **Site neutrality of the package (user principle; about *where the mechanism
   lives*, not about coverage).** `snakemake_storage_plugin_fdb` is generic FDB/MARS
   and never assumes or hard-codes MeteoSwiss things: no keys/values (`model`,
   `timespan`, `icon-ch*`, COSMO paramIds), no schema files, no `language.yaml`
   patches, no definitions paths or shorthands, no special cases in ordering or
   normalisation. It must work correctly when the user supplies those through generic
   means — e.g. `ECCODES_DEFINITION_PATH` (or the `eccodes_definitions` setting) pointing
   at cosmo-mars + eccodes-cosmo-resources, `METKIT_HOME`/`metkit_home` pointing at a
   patched language, an FDB config naming the varda-style schema. Site material is
   shipped as examples outside the package (`examples/meteoswiss/`,
   `docs/sites/meteoswiss.md`) and the tests for it live outside `src/`. Enforced by a
   test and a CI grep (`mch|meteoswiss|cosmo|icon-ch` must not appear under `src/`),
   which guards code location only. The rule extends to `scripts/` as a documentation
   rule, not enforced by the grep (decided 2026-09-15, reversible; §9.5).

### Non-goals (v1)

- Being usable as `--default-storage-provider`. Snakemake builds default-provider
  queries as `<prefix>/<normpath(path)>` and also uploads a source tarball through the
  default provider [verified: `snakemake/path_modifier.py:132-136`,
  `snakemake/workflow.py:403-406`]. Neither fits a MARS request. `is_valid_query`
  rejects such strings with a clear message.
- Directory objects (`files_only = True`).
- `--touch` support (`StorageObjectTouch`). See §7.10.
- Per-field deletion, wipe, purge. See §7.8.
- Non-GRIB payloads.
- Remote FDB backends (`type: remote`) are not tested; nothing in the design excludes
  them except the `os.stat` mtime fallback (§7.4).
- Implementing the identifier guard (§7.7): v1 ships the hook and the reserved setting
  only.
- Site-specific *mechanisms inside the package* (aliases for definitions directories,
  bundled schemas, bundled language patches, optional `mch` extra). These live in
  `examples/` and `docs/sites/`; MeteoSwiss *support* itself is goal 5.

---

## 2. Facts established by prototyping

All items in this section are **[verified: run in a throw-away prototyping environment]**
unless noted (prototype scripts were not kept; the behaviours that matter are covered by
the test suite).

### 2.1 The `.raw/` sample data

| file | bytes | msgs | MARS keys (eccodes `mars` namespace) |
|---|---|---|---|
| `template.grib` | 10800 | 1 | class=ea expver=0001 stream=enda date=20200101 time=0000 domain=g type=an levtype=sfc step=0 param=167.128 **number=0** (GRIB1, reduced_gg, 5248 values) |
| ~~`compare.grib`~~ (dropped, deleted from `.raw/`) | 1476 | 1 | class=rd expver=xxxx stream=oper date=20201102 time=0000 domain=g type=fc levtype=sfc step=12 param=166.128 |
| `steprange.grib` | 360 | 1 | class=od expver=0001 stream=enfo date=20260317 time=1200 domain=g type=ep levtype=sfc step=0-24 param=70.131 |
| `quantile.grib` | 480 | 1 | class=od expver=0001 stream=efhs date=20260313 time=0000 domain=g type=cd levtype=sfc step=60-132 param=228.128 **quantile=34:100** |
| `synth11.grib` | 660 | 1 | class=od expver=0001 stream=oper date=20230508 time=1200 domain=g type=fc levtype=sfc step=1 param=130.151 |

- Provenance [verified: sha256 against the `ecmwf/fdb` clone at `63672ea`, Apache-2.0]:
  `template.grib` = `tests/pyfdb/data/template.grib` (also `tests/data/`),
  `steprange.grib`/`quantile.grib` = `tests/fdb_e2e/data/`, `synth11.grib` =
  `rust/crates/fdb/tests/fixtures/synth11.grib`, `.raw/schema` = `tests/data/schema`
  (= `tests/pyfdb/data/schema`). `compare.grib` was **not** found in that clone
  (origin unknown).
- `.raw/schema` is `[ class, expver, stream, date, time, domain? [ type, levtype [ step, levelist?, param ]]]`.
- **`template.grib` and `quantile.grib` cannot be archived under `.raw/schema`**:
  FDB raises `Keywords not used: {number}` / `{quantile}`. pyfdb's own tests only
  succeed because their fixture rewrites `stream=oper`, which makes eccodes drop
  `number` from the MARS namespace. A schema with `number?` and `quantile?` in the datum
  rule archives all four remaining files (`tests/data/schema`, §9.1).
- FDB canonicalises `param`: `167.128`→`167`, `166.128`→`166`, `70.131`→`131070`,
  `130.151`→`151130`.
- **File size ≠ message length for the GRIB1 samples** [verified: eccodes 2.47.3,
  plan step 2]: `template.grib` holds a 10 732 B message, `steprange.grib` 276 B,
  `quantile.grib` 378 B; the rest of each file is `\0` padding to a 120-byte record
  (`synth11.grib` is unpadded, 660 B). The eccodes `paramId`s are `167`, `131070`,
  `228`, `151130`.

### 2.2 `ListElement` and the timestamp

- `repr(ListElement)` (level 3) is
  `{db key}{index key}{datum key},TocFieldLocation[uri=URI[scheme=file,name=<path>],offset=N,length=N,remapKey={}],length=N,timestamp=T`.
  Neither `ListElement` nor the pybind object exposes `timestamp` as an attribute
  (`dir()` = combined_key, data_handle, has_location, keys, length, offset, uri)
  [verified: run + `bindings.cc:334-390`].
- `T` is `Index::timestamp_`, a `time_t` set by `TocIndex::flush()` via `time()`, i.e.
  **the wall-clock second at which the index containing the field was flushed**
  [verified: `src/fdb5/database/Index.h:81,106,125`, `src/fdb5/toc/TocIndex.cc:150`].
  Measured: `T == int(time.time())` at flush, 10 digits. The 9-digit values in the pyfdb
  docstrings (`176253515`) are typos; the same docstring block also shows
  `1762537447`.
- Fields archived in one session and flushed together share `T`. Re-archiving a field
  later creates a new index file with a new `T`; the masked old copy keeps its old `T`
  (visible only with `include_masked=True`); unrelated fields in the same DB keep their
  `T`. So per-field timestamps behave like per-field mtimes for our purposes.
- `T == 0` for level-1/2 elements and for legacy index formats (version ≤ 2)
  [verified: `Index.cc:decodeLegacy` sets `timestamp_ = 0`]. Fallback needed (§7.4).
- `length()` equals the GRIB `totalLength`. `uri.path()` is the data file path;
  `uri.scheme()` returns `''` for local toc stores (pyfdb wraps the plain path).

### 2.3 Request semantics: `list` vs `inspect`/`retrieve`

| behaviour | `list(sel)` | `inspect(req)` / `retrieve(req)` |
|---|---|---|
| omitted key | wildcard (`{}` lists everything) | must match the field exactly: omitting `domain` or `number` on fields that have them finds nothing |
| `/` lists, `to`/`by` | expanded by metkit | expanded by metkit |
| missing combinations | – | silently omitted; nothing found → empty iterator / 0-byte handle, **no error** |
| multi-valued `class`/`stream`/`type`/`expver` | `UserError: Only one value possible for 'type'` | same |
| unknown key (`foo=bar`) | `UserError: Cannot match [foo] in [...]` | same |
| invalid enum value (`class=zz`) | `UserError: TypeEnum[name=class]: cannot expand 'zz'` | same |
| `levelist` with `levtype=sfc`; `number` with `type=cf` | `UserError: Key [...] not acceptable with context` | same |
| aliases/normalisation | `2t`≡`167`≡`167.128`, `10v`≡`166`, `10fgg15`≡`70.131`≡`131070`, `T_2M`≡`500011` (metkit's own param table, works without COSMO definitions), `2020-01-01`≡`20200101`, `time=0`≡`00`≡`0000`≡`00:00`, `expver=1`≡`0001`, `class=EA`≡`ea`, `model=ICON-CH1-EPS`≡`icon-ch1-eps`; `t2m` is ambiguous → error | same |

- `inspect` returns per-field `ListElement`s (with length and timestamp) for exactly the
  fields `retrieve` would return. It is the primitive for `exists/mtime/size`.
- `retrieve` returns messages in request order (outer key first, e.g. step then
  param, each in the order given in the request). Two identical requests give identical
  bytes; a request with reversed lists gives different bytes. `DataHandle.size()` works
  before `open()` and equals the sum of field lengths.
- Both `list` and `retrieve` go through `metkit::mars::MarsExpansion(inherit=false,
  strict=true)` in `bindings.cc:90-109`; `list` additionally wraps in `FDBToolRequest`.
- Canonical/expanded form of a request is obtainable from the **internal** API
  `pyfdb._internal.FDBToolRequest.from_internal_mars_selection(UserInputMapper.map_selection_to_internal(sel)).tool_request`,
  whose `repr` is `retrieve,\n\tclass=ea,\n\tdate=20200101,\n\ttime=0000,\n\tstep=0/6/12,\n\texpver=0001,\n\tparam=167/165`.
  Minute steps expand to mixed forms: `0/to/60/by/10m` → `0/10m/20m/.../50m/1/1h10m/.../60`.

### 2.4 Archive

- `archive(bytes)` accepts a buffer with several GRIB messages. **Trailing garbage after
  a valid message is silently ignored.** Pure non-GRIB bytes raise
  `Cannot find a metkit SplitterBuilder ...`.
- `archive(bytes, identifier=...)` stores the identifier **with no consistency check**
  against the message (a `step=7` identifier on a `step=1` message is accepted).
  Identifier values are **not** generally canonicalised [verified: pyfdb 5.21.4.23,
  plan step 6a probe, `tests/data/schema`]: `step=0m` is listed as `step=0`, but
  `param=167.128`, `param=2t` and `date=2020-01-01` are stored and listed verbatim (a
  second key for the same field), and `time=0`/`00`/`12`, `expver=2`, `class=EA`,
  `type=AN` are rejected with
  `UserError: Rule check - metadata not valid (not in canonical form) - found: time=0 - expecting 0000`.
  Identifier mode therefore archives the canonical spelling from metkit's expansion
  (§7.7).
- Four threads, each with its own `FDB` object, archiving into the same DB
  concurrently: no errors, all fields present. pyfdb docs: one instance per thread is
  safe, sharing an instance across threads is not supported [verified: docstring].
- eccodes refuses to encode `class=xx` into GRIB (`EncodingError`), so invalid class
  values cannot even reach FDB via GRIB.
- **A handle that has read a database keeps a stale catalogue** [verified: plan step 6
  probes, pyfdb 5.21.4.23]: after `inspect`/`list`/`retrieve` on an `FDB` object, fields
  archived and flushed later (by that handle or any other) stay invisible to it:
  `inspect`/`retrieve` return the old, now masked field and `list` does not show the new
  one. A handle that has not read yet sees everything. Creating `pyfdb.FDB(config)` is
  cheap (50 × create + `inspect` ≈ 0.40 s vs 0.37 s reusing one handle), so the backend
  opens a fresh handle for every read (§5).

### 2.5 Wipe and purge

- `wipe(field-level selection, doit=True)` deletes **nothing** (`CATALOGUE_SAFE`,
  `STORE_SAFE`, "explicitly untouched").
- `wipe(index-level selection)` (db keys + `type` + `levtype`) deletes the index files
  and, when no index remains, **the whole database directory** (toc, schema, lock
  files). A *partial* index selection (`type` without `levtype`) also selects the whole
  DB for deletion. `wipe` is therefore never safe as a Snakemake `remove()`.
- `purge(sel, doit=True)` removes masked duplicates and empty indexes.

### 2.6 Configuration and process environment

- `FDB(config)` accepts YAML text (`str`), `dict`, or `Path`; `user_config` likewise
  (`{"useSubToc": True}` accepted). Explicit `config` wins over `FDB_CONFIG`.
  A `str` holding a *file path* is parsed as YAML text and silently falls back to the
  default config (evalml hit this: `inference_check_fdb.py` comment) — hence the
  detection rule in §4. Env fallback: `FDB_CONFIG` (YAML text; `FDB5_CONFIG` legacy),
  `FDB_CONFIG_FILE` (`FDB5_CONFIG_FILE` legacy), otherwise
  `$FDB_HOME/etc/fdb/{config.yaml,schema}` [verified: run + `src/fdb5/config/Config.cc:88-125`].
- `ECKIT_EXCEPTION_IS_SILENT=1` suppresses eckit's stderr dump for `UserError`
  exceptions. `SeriousBug` exceptions (e.g. schema mismatch on archive) still print a
  full backtrace to stderr regardless of `ECKIT_EXCEPTION_DUMPS_BACKTRACE=0`.
- The wheels ship eccodes definitions in-memory (`/MEMFS/definitions`).
  `ECCODES_DEFINITION_PATH=<dir>[:<dir>]:/MEMFS/definitions` is honoured by both the
  Python `eccodes` module and the C++ side used by metkit/FDB (same `libeccodes`); the
  first match wins, so directory order matters.
- metkit's MARS language (`metkitlib/share/metkit/language.yaml`) can be overridden with
  `METKIT_HOME=<dir>` where `<dir>/share/metkit/language.yaml` exists; it works whether
  the variable is set before or after `import pyfdb`, as long as it is set before the
  first `FDB()`. With `model` values added to the default (context-free) enum block,
  `model=icon-ch1-eps` requests expand. **A `METKIT_HOME` without `language.yaml`
  makes the process hang (>60 s) instead of raising.** `METKIT_LANGUAGE_STRICT_MODE=0`
  does not relax enum checks. This contradicts the evalml comment "there's no
  config-based override for this" (`patch_metkit_language.py` docstring), which edits
  the installed file in place instead.
- Wheels: `pyfdb` pins `fdb5lib==5.23.2.27` which pins `eccodeslib==2.48.2.27`,
  `eckitlib==2.2.0.27`, `metkitlib==1.20.2.27`. Only `manylinux_2_28` x86_64/aarch64,
  cp311–cp314. No macOS/Windows wheels for 5.23 [verified: PyPI JSON]. The
  `pyfdb 5.21.4.21` that evalml locks pins `eccodeslib==2.47.3.21` (matching the
  `eccodes-cosmo-resources-python` 2.47 series) and does ship macOS wheels.

### 2.7 Snakemake core behaviour (verified by reading `snakemake` 9.27.0 source)

- `is_valid_query` is called on **every installed plugin** for `storage("...")` without
  provider; two positives are a hard error (`storage.py:133-152`). It is called again on
  the wildcard-substituted query (`io/__init__.py:960-965`).
- The DAG file name is `local_prefix / local_suffix()` (`storage.py:16-17`); wildcard
  regexes are built from it (`io/__init__.py:984-988`). After substitution, Snakemake
  constructs the `StorageObject` class directly with `apply_wildcards(query)`
  (`io/__init__.py:951-971`): `postprocess_query` is **not** re-applied, `__post_init__`
  is. Hence `local_suffix(apply_wildcards(q)) == apply_wildcards(local_suffix(q))` must
  hold. Wildcard constraints `{name,regex}` are injected into the query
  (`rules.py:423-430`).
- `managed_remove()` is called before a job runs on every output that
  `exists_in_storage` (`jobs.py:756-771`), on job failure cleanup, on
  `--delete-all-output`, and for temp outputs. It does not delete the local copy
  (`io/__init__.py:1251-1254`).
- After `store_object`, Snakemake immediately calls `mtime().storage()` to touch the local
  file and then `exists_in_storage()` (`dag.py:995-1028`). Local copies are removed at
  the end unless `--keep-storage-local-copies`.
- `store_object`/`retrieve_object` run synchronously inside coroutines; different jobs
  may run in different threads (local executor pool) or processes.
- Checksums are only consulted when a local copy exists, compared to the value recorded
  in `.snakemake/metadata`; `None` means "hash the local file" (`io/__init__.py:674-750`).
  Never store `None` into `cache.checksum`.
- `Mtime.storage` takes priority over local mtime and is compared as POSIX seconds
  (`snakemake_interface_storage_plugins/io.py:66-69`).
- `inventory()` is called once per file during DAG build. If a plugin marks a parent in
  `cache.exists_in_storage.has_inventory`, a missing key means "does not exist"; otherwise
  Snakemake falls back to `managed_exists()`.
- `glob_wildcards` matches `re.match(query_pattern, candidate)` against the strings
  returned by `list_candidate_matches()` (`io/__init__.py:1767-1810`); `StorageObjectGlob`
  is optional but its absence gives an `AttributeError`.
- `--touch` errors upfront if any output's plugin lacks `StorageObjectTouch`
  (`dag.py:776-787` `check_touch_compatible`, called from `workflow.py:1364`).
- Untagged local prefix: `.snakemake/storage/fdb`; **tagged: `.snakemake/storage/<tag>`**
  (`storage.py:81-83`).
- Snakemake's wildcard regex escapes `+`, `=`, `.` etc. in constant parts; a pattern
  like `.../step=0+6+12/param={param,[a-z0-9]+}.grib` matches and renders correctly.
- `Rule.update_wildcard_constraints` copies each output storage object with `copy.copy`
  and assigns a constraint-injected string to `.query` **without** calling
  `__post_init__` (`rules.py:410-422`). Anything derived from the query (parse result,
  local suffix) must follow the current `query`, not be cached once at construction.
- For a tagged provider Snakemake also registers an untagged instance with the same
  settings when none exists yet (`storage.py:112-126`), so `__post_init__` can run
  twice with identical settings in one process.
- Tagged setting values on the CLI and in profiles are written `TAG::VALUE` (double
  colon: `snakemake_interface_common/plugin_registry/plugin.py` `get_settings` splits
  on `"::"`), so untagged values may contain `:` (inline YAML, colon-separated lists).
  **Open upstream issue** [verified: settings round-trip repro, not a cluster run; still
  present on snakemake `main` as of 2026-09-15, `spawn_jobs.py:376`; no matching upstream
  issue found; an issue draft exists but is not posted yet]: spawned jobs re-emit tagged
  values as `f"{tag}:{value}"` with a single colon (`spawn_jobs.py`
  `_get_storage_provider_setting_items.fmt_value`), which the job process parses as
  the untagged value `tag:value`. [verified end to end: plan step 9, snakemake 9.27.0,
  local executor] `examples/ecmwf/Snakefile` with `storage ecm:` and
  `--storage-fdb-config ecm::../../.fdb/config.yaml -c1` fails in the spawned `run:` job
  with `FDB configuration error: 'ecm:../../.fdb/config.yaml' is neither an existing file
  nor an inline YAML mapping`. The local executor spawns only `run:` rules (and shadow
  jobs); `shell` rules run in the main process (`executors/local.py`
  `run_single_job`), where tagged settings work. The e2e tests therefore pass settings
  untagged to the `run:` example and use a `shell` rule with tagged settings for the
  site workflow (§9.5).

### 2.8 MeteoSwiss conventions (`eccodes-cosmo-mars`, `evalml` `enable_fdb`)

Sources [verified: read via `gh api`]: `MeteoSwiss/eccodes-cosmo-mars` (public,
BSD-3-Clause, branches `main`, `varda`, `varda-ext`, …; README: "MARS definitions for
operational ICON data at MeteoSwiss"), `MeteoSwiss/evalml` branch `enable_fdb`
(`resources/fdb/realtime-varda.schema`, `resources/fdb/patch_metkit_language.py`,
`workflow/rules/inference.smk`, `workflow/scripts/inference_check_fdb.py`,
`src/data_input/__init__.py`, `pyproject.toml`, `uv.lock`).

**Definitions.** `eccodes-cosmo-mars` is a *complement* to `eccodes-cosmo-resources`,
not a replacement: it contains only `definitions/grib2/local.215.def` (which
`include`s `grib2/local.78.def` from cosmo-resources and adds the concepts
`marsClass`, `marsStream`, `marsType`, `marsModel`, `marsExpver` aliased to
`mars.class/stream/type/model/expver`, driven by `generatingProcessIdentifier`,
`typeOfGeneratingProcess`, `backgroundProcess`, `productDefinitionTemplateNumber`,
`perturbationNumber`, `localNumberOfExperiment`), `grib2/localConcepts/lssw/timespanConcept.def`
(minute-based time spans `none/10m/15m/…/12h`, `fromstart`, catch-all `none`) and
`grib2/marsLevtypeConcept.def` (adds `ml` for `generalVerticalLayer`, `hl`, `dp`, …).
`eccodes-cosmo-resources` has no `grib2/local.215.def` of its own (only
`local.78.*`), so the two do not conflict. The `varda-ext` branch (the one evalml
clones) additionally maps GPI 180 (`VARDA-SINGLE`, `VARDA-ENS`, class `ai`/`rd`),
matches `stream` by PDT (`oper`/`enfo`) and adds `grib2/local.98.def` for IFS-format
output. **Activation** (evalml): `git clone --branch varda-ext --depth 1
git@github.com:meteoswiss/eccodes-cosmo-mars.git $VENV/data/share/eccodes-cosmo-mars`
(no pip package) and
`ECCODES_DEFINITION_PATH=<eccodes-cosmo-mars>/definitions:<eccodes-cosmo-resources>/definitions`
— cosmo-mars first; the comment notes it "must be set in ECCODES_DEFINITION_PATH … because
the FDB C library uses eccodes internally to resolve MARS namespace keys".

**Resulting MARS namespace for ICON-CH1-EPS GRIB2 (centre `lssw`, GPI 141)** [verified
on local constant-field templates and earthkit's `test_icon.grib`, with cosmo-mars
`varda-ext` + cosmo-resources 2.47.0.1 + eccodes 2.48.2]:
`class=od, stream=enfo, type=cf (perturbationNumber 0) | pf, model=ICON-CH1-EPS,
expver=0001, date, time, step ('0m' for minute-unit forecastTime 0, '10m', '3', '6'),
levtype (sfc | pl | ml for generalVerticalLayer), levelist, number (pf only),
param=<COSMO paramId: T_2M→500011, PS→500000, TOT_PREC→500041, PMSL→500002, T→500014,
P→500001>, timespan (none | fs | 10m …)`.

**Schema** (`realtime-varda.schema`, evalml):
```
[ date, time, stream, class, expver, model, type, domain-
    [ levtype, number?
        [ step, param, levelist?, timespan?none ]]]
```
(`domain-` removes `domain`; `timespan?none` defaults a missing timespan to `none`.)
Native `fdb.archive(bytes)` of all ICON templates and variants (cf, pf member 1, 6 h and
10-min steps, pl, ml) succeeds under this schema; FDB lists `model=icon-ch1-eps`
(lower-cased), `number=''` for cf, `domain=''`, `step=0|10m|3|6`. The ECMWF GRIB1
`template.grib` fails under it (`Could not find [model]`).

**Requests** [verified]: `inspect`/`retrieve` find the fields with
`{date,time,stream=enfo,class=od,expver=0001,model=icon-ch1-eps,type=cf,levtype=sfc,step=0,param=T_2M|500011}`;
`param=2t` (167) does **not** match COSMO ids; `number` must be omitted for `type=cf`
(metkit context error otherwise) and given for `pf`; accumulated fields need
`timespan=fs` in the request (`timespan?none` only defaults *missing* timespans);
`timespan=none` may be given explicitly; `step=10m` and `step=0/10m` work; uppercase
`model=ICON-CH1-EPS` is accepted; `model` values require the metkit language override
(stock `language.yaml` rejects `icon-ch1-eps`; `list` does not validate, `retrieve`/
`inspect` do — evalml's `patch_metkit_language.py` edits the installed
`metkitlib/share/metkit/language.yaml` in place for `varda-single`, `varda-single-g`).
Retrieved bytes are identical to the archived message.

**FDB setup in evalml**: one toc FDB root per checkpoint (`{fdb_root_global}/{env_id}`
or a local per-checkpoint root), config generated as a YAML file and passed via
`FDB5_CONFIG_FILE` on the archiving side (anemoi-inference inside a uenv
`fdb/5.19:v2`, view `realtime`, native libs via `LD_LIBRARY_PATH`), and as a
`pyfdb.FDB(dict)` on the read side (pyfdb wheels bundle their own libs). Presence check:
`fdb.list({"date","time","model"}, level=1)`; completeness check: `fdb.list(..., level=3)`
grouped into "blocks" (combined key minus step/param/levelist/timespan/levtype), all
steps present and consistent param sets; reads: `list` per date/time/steps, one
`retrieve` per block with single-valued `class/stream/type/expver/model/levtype`,
bytes concatenated and written to a temp file. Packaging: `uv pip install pyfdb`
(locked 5.21.4.21), `eccodes>=2.44,<2.48`, `eccodes-cosmo-resources-python==2.44.0.1`.

### 2.9 MeteoSwiss OGD samples: real ICON-CH2-EPS fields end-to-end

[verified: prototype with full-size OGD downloads and a temporary FDB; pyfdb 5.23.2.27,
`ECCODES_DEFINITION_PATH=<eccodes-cosmo-mars varda-ext>/definitions:<eccodes-cosmo-resources 2.47.0.1>/definitions:/MEMFS/definitions`,
`METKIT_HOME=<metkit home>` whose `language.yaml` adds `icon-ch1-eps`/`icon-ch2-eps`;
re-verified on pyfdb 5.21.4.23 by the site test suite.]

- **API.** `POST https://data.geo.admin.ch/api/stac/v1/search` with JSON
  `{"collections":["ch.meteoschweiz.ogd-forecasting-icon-ch2"], "forecast:reference_datetime":"2026-09-15T12:00:00Z", "forecast:variable":"T_2M", "forecast:perturbed":false, "forecast:horizon":"P0DT06H00M00S"}`.
  The download link is `features[0].assets.<only asset>.href`, a pre-signed
  `rgw.cscs.ch` URL; asset names look like `icon-ch2-eps-202609151200-6-t_2m-ctrl.grib2`
  / `...-perturb.grib2`. `HEAD` on the pre-signed URL returns a 225-byte error, so use
  `GET`. The CH1 collection is `ch.meteoschweiz.ogd-forecasting-icon-ch1`. Horizontal/
  vertical constants are collection assets (`GET /collections/{id}/assets`), not needed
  here. **Data is only available for 24 h after publication**, so a fetch must pick a
  recent reference time (query without `reference_datetime` and take the latest, or
  pass it as an argument). Docs:
  https://opendatadocs.meteoswiss.ch/e-forecast-data/e2-e3-numerical-weather-forecasting-model?download-options=restapi#download-options
- **Sizes.** One native-grid surface field: CH1 ≈ 2.3 MB, CH2 ≈ 568 KB. The perturbed
  file holds all members (CH2: 20 messages, 11.4 MB).
- **Samples saved** in `.raw/meteoswiss/` (CH2 only; committed as empty-data copies of
  175–350 B, §9; the full-size originals, ≈ 2.27 MB with the sizes below, live in the
  git-ignored `.local/raw-full/meteoswiss/`):
  `icon-ch2-eps_202609151200_step6_t_2m_ctrl.grib2` (1 msg, 567 927 B),
  `icon-ch2-eps_202609151200_step6_tot_prec_ctrl.grib2` (1 msg, 567 951 B),
  `icon-ch2-eps_202609151200_step6_t_2m_pert_m1-2.grib2` (2 msgs, members 1 and 2,
  extracted with eccodes by `perturbationNumber`).
- **MARS keys** (with the definitions above): control `class=od stream=enfo type=cf
  model=ICON-CH2-EPS expver=0001 step=6 levtype=sfc param=500011 timespan=none`;
  TOT_PREC `param=500041 timespan=fs`; perturbed `type=pf number=1..20`. This settles
  two earlier assumptions: **member 000 is `type=cf`** and **a 6 h step is spelled
  `step=6`**.
- **FDB round-trip** (native archive under `realtime-varda.schema`): all 4 fields
  archived; `list` shows `model` canonicalised to lower-case `icon-ch2-eps`, `domain=''`
  and `number=''` for cf; timestamp is a 10-digit epoch. `inspect`/`retrieve` with
  `model=ICON-CH2-EPS` (upper-case accepted): cf T_2M → 1 field (567 927 B);
  `type=pf,number=1/2` → 2 fields (1 135 854 B); cf TOT_PREC with `timespan=fs` → 1
  field. Temp FDB size 2.6 MB.
- **Version warning.** Every decoded message prints
  `WARNING: definitions.edzw version 2.47.0 is NOT compatible with ecCodes library version 2.48.2!`
  (pyfdb 5.23 → eccodeslib 2.48.2); the round-trip still worked. See §11.
- **pyfdb 5.21.4.21 re-verified** (second venv, `.raw/` ECMWF data, `tests/data/schema`):
  same public API (`FDB`, `ListElement`, `DataHandle`, `URI`, …; no
  `pyfdb.__version__` — use `importlib.metadata.version("pyfdb")`), timestamp only in
  `repr` (`,length=N,timestamp=T` with a 10-digit `T` at the flush second), no
  `timestamp` attribute, `inspect` with `to/by` + aliases (`2020-01-01`, `time=0`,
  `type=AN`, `2t`) finds 3 fields and silently drops a missing member, `retrieve.size()`
  equals the sum of lengths, internal `FDBToolRequest` expansion works
  (`T_2M/2t` → `500011/167`), `class=zz` → `UserError`. The bundled eccodes is 2.47.3,
  which matches `eccodes-cosmo-resources-python` 2.47.0.1 (no warning expected).
  **Correction** [verified: plan step 2, `eccodes 2.47.3` Python module, cosmo-mars +
  cosmo-resources 2.47 definitions]: the warning is still printed on stderr, as
  `definitions.edzw version 2.47.0 is NOT compatible with ecCodes library version 2.47.3!`;
  decoding is unaffected (the MARS keys of §2.9 come out exactly; values identical with
  and without the check).
  **Origin and switch** [verified: definitions source + reproducer, pyfdb 5.21.4.23 and 5.23 stacks]: the check
  is not in the ecCodes library but in COSMO's `eccodes-cosmo-resources`
  `definitions/boot_extra.def` (included by ecCodes' `boot.def:129`): a hard-coded
  `constant edzwDefinitionFilesVersion = '2.47.0'` compared as an exact string with
  `library_version()`, printed via definitions `print("/dev/stderr")` (bypasses the
  ecCodes log callback, so `codes_set_debug`, `ECCODES_LOG_STREAM`, `ECCODES_DEBUG` do not
  help). The same file reads `getenv("ECCODES_VERSION_CHECK_OFF","0")` and skips the
  check when it is `1`: warning lines 1 → 0 with the variable set in the shell or in
  `os.environ` before the first decode. Upstream v2.47.0.2 and `master` keep the exact
  comparison. The plugin does nothing about it (site-specific, passes env through);
  documented in `docs/sites/meteoswiss.md`, set in the MeteoSwiss example profile and
  the `site-meteoswiss` CI job.
  **Side effect on redirected stderr** [verified: plan step 9, eccodes 2.47.3 +
  cosmo-mars + cosmo-resources 2.47.0.1]: decoding with these definitions opens
  `/dev/stderr` as a file, which truncates a stderr redirected to a regular file
  (earlier content becomes NUL bytes or is lost), also with
  `ECCODES_VERSION_CHECK_OFF=1`. A decoding job in a workflow should send its stderr to
  its own log, not to a shared log file.
- **pyfdb 5.21.4.23 (the locked version) re-verified** [verified: plan step 3, scratch
  probes + `tests/test_backend.py`; same public/internal API as 5.21.4.21]:
  - every FDB/metkit failure reaches Python as a plain `RuntimeError`. metkit errors read
    `UserError: UserError: <detail>` (prefix doubled); eckit `SeriousBug`s read
    `Serious bug: <detail>`;
  - identifier-mode archive with keys matching no rule: `Serious bug: FDB: Could not find a
    rule to archive {...}` (in addition to `Keywords not used` / `Could not find [model]`
    for native archives);
  - `FDB(config)` does not validate the config: a missing schema file fails at first
    use with `Cannot open <path>  (No such file or directory)`, a missing root with
    `Unexpected state: No writable roots available. Configured roots: [...]` (also for
    `inspect`);
  - level-1/2 `ListElement`: `has_location()` is false, `length()`/`uri` are `None`;
    level-3 `uri.path()` is the plain data file path;
  - `FDBToolRequest` repr: `retrieve,` then one `\tkey=v1/v2,` line per key in metkit's
    order (last line without comma); aliases of one field are **not** de-duplicated
    (`param=2t/167` → `167/167`), so `E` counts distinct values;
  - `retrieve` of a request that matches nothing: `size() == 0`, opening and reading it
    works and yields 0 bytes.

---

## 3. Query language

### 3.1 Grammar

```
query      = "fdb://" ws pair ( ws "," ws pair )* ws
pair       = key ws "=" ws value
key        = [A-Za-z][A-Za-z0-9_]*                 ; lower-cased on normalisation
value      = item ( "/" item )*                    ; MARS value list
item       = ( wildcard | ichar+ )+                ; non-empty
ichar      = [A-Za-z0-9] | "." | "-" | ":" | "_"    ; e.g. 0-24, 34:100, 70.131, 2t, -1, T_2M, 10m
wildcard   = "{" name [ "," constraint ] "}"       ; Snakemake WILDCARD_REGEX, atomic
ws         = [ \t\r\n]*
```

- `to` and `by` are ordinary items (`step=0/to/12/by/6`); metkit expands them.
- Wildcard tokens are matched first with
  `snakemake_interface_storage_plugins.io.WILDCARD_REGEX` and treated as opaque atoms,
  so a constraint may contain `,`, `/`, `=`, `{n}` (e.g. `{date,\d{8}}`) [verified:
  regex test]. Wildcards are allowed only inside values, never in keys.
- **Escaping: none.** `,`, `=`, `/`, `{`, `}`, `+`, `%`, whitespace are reserved and
  cannot occur in an item outside a wildcard token. Rationale: MARS values never need
  them (metkit's language rejects such values anyway, §2.3), and an escape mechanism
  would have to survive Snakemake's own path handling unchanged. A query violating this
  is invalid, with a message naming the offending character.
- Errors (all `query.QueryError`, a `ValueError`): missing scheme (the scheme is
  case-sensitive and must be the first characters, no leading whitespace), empty query,
  duplicate key (after lower-casing), empty key/value, missing `=`, empty pair
  (`,,`, leading comma), empty item (`step=0//6`, leading or trailing `/`), trailing
  comma, wildcard in key, invalid key name, reserved/invalid character (named in the
  message, e.g. `invalid character '=' in value of key 'class'`; whitespace inside a
  value is reported as `whitespace`). `{{`/`}}` (Snakemake's escaped braces) are
  rejected like any other `{`/`}` outside a wildcard token.

Examples:

```
fdb://class=od,expver=0001,stream=oper,date={date},time=00,type=fc,levtype=sfc,step=0/6/12,param=2t
fdb://class=ea,expver=0001,stream=enda,date=20200101,time=0000,domain=g,type=an,levtype=sfc,step=0,number=0,param=167
fdb://class=od,expver=0001,stream=enfo,model=icon-ch1-eps,date={date,\d{8}},time={time},type=pf,levtype=sfc,number={member,\d+},step=0/to/33/by/1,param=500011/500000
fdb://class=od, expver=0001,
     stream=oper, date=20240101, time=00, type=fc, levtype=sfc, step=0, param=2t   (whitespace allowed)
```

### 3.2 Normalisation (`postprocess_query`) — purely syntactic

Parse, then re-serialise:

1. strip all whitespace outside wildcard tokens (around keys, `=` and `,`; whitespace
   inside an item is invalid, §3.1). Wildcard tokens, including any whitespace in them
   (`{ date }`), are kept verbatim so the text still matches what Snakemake substitutes;
2. keys lower-cased;
3. keys reordered into a canonical order that depends only on key names (stable under
   wildcard substitution). The order is determined per provider instance, in this
   precedence:
   1. the `key_order` setting (comma list), if given;
   2. otherwise the key order of the FDB schema's rules, read from the `schema:` file
      named in the resolved FDB config (pure-Python read of the config YAML and the
      schema text; no `pyfdb` import; works for `config` given as path/inline text and
      for the `FDB_CONFIG`/`FDB_CONFIG_FILE`/`FDB_HOME` fallbacks, which the plugin
      resolves the same way `Config.cc` does — §2.6). Keys are taken in order of first
      appearance across all rules (text inside `[...]`), with their decorations
      ignored: `key?`, `key?default`, `key-`, `key=v1/v2`, `key: Type`. Removed keys
      (`key-`) keep their position. Declarations outside brackets are skipped, and so
      are comments: `#` to end of line, anywhere in the text, including after rule text
      on the same line [verified: `fdb5/rules/SchemaParser.cc:49,53` constructs
      `eckit::StreamParser(in, true)`; `eckit/parser/StreamParser.h:38` defaults the
      comment characters to `"#"`; `StreamParser.cc:64-79` (`peek`) skips from a comment
      character to the newline]. `--` is not a comment (`-` marks a removed key). Keys
      absent from the schema are appended alphabetically. A schema text with no rule
      keys is an error. [verified: `fdb.config()` returns the config
      dict incl. the `schema` path, and the schema is a plain text file; pyfdb exposes
      no parsed schema or key order, so the text must be parsed by the plugin]
   3. otherwise (no readable schema, e.g. remote FDB) a generic rule: the keys of
      ECMWF's default FDB schema in their usual MARS request order
      `class, expver, stream, domain, date, time, type, levtype, levelist, step, number, param`,
      then all other keys alphabetically. This list contains no site-specific keys;
      e.g. `model`, `timespan`, `quantile` sort alphabetically unless the schema or
      `key_order` places them.
   The MeteoSwiss `realtime-varda.schema` therefore yields
   `date, time, stream, class, expver, model, type, domain, levtype, number, step, param, levelist, timespan`
   without any built-in knowledge of those keys (`domain` comes from `domain-`; it
   matters only if a query names it, so queries without it are ordered
   `date, time, stream, class, expver, model, type, levtype, number, step, param, levelist, timespan`);
4. values kept **verbatim** (case, aliases, list order).

Because the order is a provider property, `postprocess_query` (a provider method) has
access to it; `local_suffix()` uses the same order via `self.provider.key_order`.
Different providers/tags may order the same query differently — their local prefixes
differ anyway (§3.3).

**Decision (user):** `postprocess_query` never touches value semantics and never imports
`pyfdb`/metkit, so DAG building is cheap, deterministic and identical on machines
without the C++ libraries. Value canonicalisation (`2t`→`167`, `T_2M`→`500011`,
`EA`→`ea`, `ICON-CH1-EPS`→`icon-ch1-eps`, `2020-01-01`→`20200101`) is checked at
runtime instead (§7.12), where metkit expansion happens anyway. The evalml branch is
consistent with this: its reads build requests from FDB's own `combined_key()` values
(paramIds, lower-case model), i.e. canonical spellings.

Documented rule for users: **use canonical spellings** (the values FDB lists back:
lower-case enums, numeric paramIds, `YYYYMMDD` dates, `HHMM` times, 4-char expver) to
avoid duplicate local paths for the same field; list order is preserved because
retrieved message order follows request order (§2.3).

`is_valid_query` uses the same parser and must not import `pyfdb` (it is called for
every query on every plugin). It rejects any other scheme (`s3://`, plain paths,
`fdb://class=od/expver=0001` from `--default-storage-prefix`, …).

### 3.3 Local path (`local_suffix`)

```
<key>=<value> [/ <key>=<value> ...] .grib           one directory per key, canonical order
```
with `/` inside values replaced by `+` (only outside wildcard tokens), e.g.

```
class=od/expver=0001/stream=oper/date={date}/time=00/type=fc/levtype=sfc/step=0+6+12/param=2t.grib
```

- `{wildcard}` text (including constraints) is copied verbatim, so Snakemake's wildcard
  matching works on the local path and the mapping commutes with `apply_wildcards` as
  long as substituted values contain none of the reserved characters. Documented rule:
  *wildcard values must be single MARS values; put lists in the query, not in wildcards.*
- Path-component limit: each `key=value` component (UTF-8 bytes, the last one including
  `.grib`) must be ≤ 255 bytes (ext4/xfs/lustre NAME_MAX). A longer constant component
  is replaced by `key=~<sha256(value)[:24]>` (`+ ".grib"` for the last), where `value`
  is the verbatim query value with its `/` separators, hex digest; `~` cannot occur in
  a value, so hashed components never collide with literal ones. A longer component
  containing a wildcard is an error (hashing would not commute), and so is a hashed
  component that is still too long (very long key). A wildcard component under the
  limit whose substituted value pushes it over the limit would be hashed after
  substitution and break commutation; this is **guarded** (decided 2026-09-15): the
  provider records, per instance and keyed by query text, the queries it returned from
  `postprocess_query` (patterns and constant queries; `StorageProvider.is_normalised`).
  A storage object whose query is not recorded (i.e. created by Snakemake from
  `apply_wildcards`, which bypasses `postprocess_query`, §2.7) and whose query has a
  component over the limit (`ParsedQuery.oversized_components()`) raises in
  `__post_init__`
  `WorkflowError("local path component for key '<key>' of <query> is <n> bytes after wildcard substitution (limit 255); wildcard values must be single MARS values (put lists in the query, not in wildcards)")`.
  Constant queries that Snakemake rebuilds through `apply_wildcards` (every input and
  output is concretised that way, e.g. input-function results, `rules.py:855-880`)
  keep their text and therefore stay recorded, so they are hashed without error.
  **Job processes [verified: snakemake 9.27.0 source, plan step 4]:** a spawned job
  (local executor for `run:` jobs, `executors/local.py:174-185,226-233`; cluster and
  remote executors, `snakemake_interface_executor_plugins/executors/real.py:144-170`)
  runs `python -m snakemake --snakefile <Snakefile> --target-jobs <rule>:<wildcards> …`
  (`real.py:82-88`, `spawn_jobs.py:259-300`). That process re-parses the Snakefile
  (`api.py:382-415`, `workflow.include`), so every `storage.<name>(...)` call passes
  through `provider.object` → `postprocess_query` again (`storage.py:211`,
  `storage_provider.py` `object`). The job is then rebuilt from the rule patterns
  (`dag.py:264-275` `new_job` → `rules.py:855-880,1006-1007` →
  `io/__init__.py:951-972`), exactly as in the main process. Jobs run in-process
  (`run_wrapper`) use the main process's provider. The assumption holds; no
  alternative design is needed.
- Tags: with tagged settings the prefix is `.snakemake/storage/<tag>/...` (§2.7), so two
  FDBs with the same query never collide locally. The query itself does not name the
  FDB; the provider instance (tag) does. This is the only sense in which "config tags
  matter"; the grammar has no FDB selector.

### 3.4 Requests derived from a query

- `request(q)`: `{key: value}` dict where a value with `/` is passed as the raw string
  (pyfdb splits on `/`, `pyfdb_type.py:65-68`), used for `inspect`/`retrieve`.
- `ParsedQuery.constant_pairs()`: the constant keys with raw values, the selection for
  `list` (glob, §7.9).
- `expand(q)` (`Backend.expand`): canonical expanded request via the internal
  `FDBToolRequest` (§2.3), parsed from its repr with a tolerant regex `^\t?(\w+)=(.*)$`
  per line (trailing `,` stripped). It returns `None` only when that internal API is
  unavailable (import fails, shape changed) or its repr cannot be parsed; the plugin
  then falls back to `backend.fallback_expand`: `/` lists plus `a/to/b[/by/c]` ranges
  of integers and of `YYYYMMDD` dates (step in days), no alias resolution. Other ranges
  (e.g. `0/to/60/by/10m`) keep their items verbatim, which over-counts (conservative for
  `exists`). An **invalid** request is not a fallback case: metkit's `RuntimeError`
  (`UserError`) propagates and is mapped to "Invalid MARS request" (§6). The expanded
  request gives the **expected field count** `E = ∏ |distinct values_k|`
  (`backend.count_fields(expanded)`; `Backend.expected_count(request)` expands and
  counts in one go) and the canonical spellings used by §7.12
  (`Backend.spelling_diffs(parsed, expanded=None)`, returned in canonical key order;
  the storage object passes the expansion it already computed, so metkit expands a
  request once per object). [assumed: cross-product cardinality; context-dependent keys such as
  `levelist` may make some combinations meaningless.]

---

## 4. Settings

`StorageProviderSettings(StorageProviderSettingsBase)`, all `Optional[...]` with
defaults and `help`, no `nargs`, no `parse_func`; CLI names `--storage-fdb-<name>`,
taggable with `TAG::VALUE` (`--storage-fdb-config prod::/etc/fdb/prod.yaml scratch::.fdb/config.yaml`, §2.7).
Fields are annotated `typing.Optional[str]`, not `str | None`: Snakemake's argument
builder unwraps only `typing.Union` (`snakemake_interface_common/_common.py`
`dataclass_field_to_argument_args`).

| field | type / default | meaning |
|---|---|---|
| `config` | `Optional[str]`, `None` | Path to an FDB config YAML **or** inline YAML/JSON text. Detection: if `Path(v).is_file()` → `Path` (made absolute at provider construction); else if the text parses as a YAML mapping → inline; else error (never pass a path string to `FDB()`, §2.6). `None` → FDB env fallback. |
| `user_config` | `Optional[str]`, `None` | Same rules; passed as `user_config` (e.g. `useSubToc: true`). |
| `archive_mode` | `Optional[str]`, `"native"` | `native` (default since 2026-09-15, §7.7 decision; `fdb.archive(bytes)`, FDB derives the keys and picks the schema rule) or `identifier` (plugin builds the key per message; constant query values are checked against the message, §7.7). Use `identifier` only with schemas whose rules share one key set, or supply keys via the query: every key mandatory in *any* rule of a multi-rule schema needs a value (§12). |
| `identifier_check` | `Optional[str]`, `"none"` | **Reserved.** Guard that verifies each message's GRIB metadata against its identifier before archiving (§7.7), beyond identifier mode's built-in check of constant query values against the message: FDB-exact canonicalisation, keys the message does not carry, schema-level consistency. v1 accepts only `none`; `strict` raises a configuration error `WorkflowError("identifier_check=strict is reserved and not implemented in this version")` at provider construction. |
| `store_check` | `Optional[str]`, `"strict"` | `strict`: the file must provide exactly the fields the query expands to; `warn`: fewer fields allowed (logged), more/foreign fields still an error. |
| `canonical_spelling` | `Optional[str]`, `"warn"` | Runtime check of query values against FDB's canonical spelling (§7.12): `warn` (one warning per query), `error`, `ignore`. |
| `remove_policy` | `Optional[str]`, `"warn"` | What `remove()` does: `warn` (no-op + one warning per query), `ignore` (silent no-op), `error` (raise). |
| `glob_required_keys` | `Optional[str]`, `"class"` | Comma list of keys that must be constant in a `glob_wildcards` pattern, i.e. present without a wildcard (lower-cased; empty → none; invalid key names → error at construction). A pattern violating it raises at glob time (§7.9). |
| `eccodes_definitions` | `Optional[str]`, `None` | Colon-separated **plain directory paths** prepended, in order, to `ECCODES_DEFINITION_PATH` (each must be an existing directory, error naming the entry otherwise; made absolute; empty entries skipped; entries starting with `/MEMFS/`, eccodes' in-memory definitions, are passed as is). No aliases or site shorthands; how to obtain site definitions is documented per site (`docs/sites/meteoswiss.md`). |
| `metkit_home` | `Optional[str]`, `None` | Exported (made absolute) as `METKIT_HOME` for a custom MARS language (e.g. extra enum values). Validated: `<dir>/share/metkit/language.yaml` must exist (otherwise FDB hangs, §2.6). Generic pass-through; the site recipe for building such a directory is documentation only. |
| `key_order` | `Optional[str]`, `None` | Comma list overriding the canonical key order (§3.2). Default: derived from the FDB schema, else the generic rule. |
| `env` | `Optional[str]`, `None` | Extra environment overrides as `NAME=VALUE[,NAME=VALUE]` exported before the FDB libraries load (e.g. `FDB_HOME=...`, `ECKIT_EXCEPTION_IS_SILENT=0`). Names match `[A-Za-z_][A-Za-z0-9_]*`; values cannot contain `,` (they may contain `=`); a missing `=` or a duplicate name is an error. Generic escape hatch so no site needs plugin code. |
| `max_requests_per_second` | inherited | unused (`use_rate_limiter() == False`). |

Choice settings (`archive_mode`, `identifier_check`, `store_check`,
`canonical_spelling`, `remove_policy`) accept exactly the listed lower-case values;
`None` means the default; anything else raises
`WorkflowError("invalid <name> '<value>' (allowed: ...)")` at provider construction
(`identifier_check=strict` gets the reserved message instead).

All settings are generic pass-throughs. Sites configure them through Snakemake
profiles, CLI flags or environment variables; the package contains no site names,
paths or values (§1 goal 7).

### 4.1 Environment handling and precedence

Rules (applied in `StorageProvider.__post_init__`, before the lazy import of
`pyfdb`/`eccodes`, i.e. before any FDB/eccodes/metkit library loads in this process):

| variable | user already set it | setting given | result |
|---|---|---|---|
| `ECCODES_DEFINITION_PATH` | respected as is | `eccodes_definitions` | `<setting dirs>:<existing value>` (prepend; existing entries never removed). If unset: `<setting dirs>` only — the eccodes wheel appends its bundled `/MEMFS/definitions` automatically [verified: `codes_definition_path()` reports `<dirs>:/MEMFS/definitions` and GRIB1/ICON messages decode with site dirs only]. |
| `METKIT_HOME` | respected as is (but validated, see below) | `metkit_home` | setting wins for this provider's process (explicit configuration beats ambient env); logged at info level. |
| `FDB_CONFIG` / `FDB5_CONFIG` / `FDB_CONFIG_FILE` / `FDB5_CONFIG_FILE` / `FDB_HOME` | respected (pyfdb's own fallback chain, §2.6) — the plugin never sets or clears them | `config` / `user_config` | passed explicitly to `FDB(config, user_config)`, which takes precedence over the env inside FDB [verified]; env untouched. |
| `ECKIT_EXCEPTION_IS_SILENT` | respected | – | `setdefault("1")` only when unset. |
| anything | respected | `env` | explicit override (`NAME=VALUE`), documented as the one place where a setting clobbers the environment on purpose. |

Application order and validation (`StorageProvider._prepare_environment`). All values
are validated and computed before anything is exported, so a provider that fails
construction leaves the environment unchanged:
1. `env` overrides;
2. `eccodes_definitions` is prepended to the value after step 1. This is skipped when
   that value already starts with the same directories, which happens for a second
   provider with the same setting, the untagged twin of a tagged provider (§2.7), or a
   spawned job inheriting the exported value;
3. `metkit_home` replaces the value after step 1;
4. the effective `METKIT_HOME` must contain `share/metkit/language.yaml`, whatever its
   source (`metkit_home`, `env` or the ambient environment). Otherwise it raises
   `WorkflowError("METKIT_HOME=<dir> (from <source>) has no share/metkit/language.yaml; FDB would hang instead of failing")`,
   because such a value hangs FDB (§2.6);
5. `ECKIT_EXCEPTION_IS_SILENT=1` is set if it is unset after step 1.

When providers in one process apply different `eccodes_definitions` or `metkit_home`
values, a warning is logged. For `METKIT_HOME` the last value is exported; for
`ECCODES_DEFINITION_PATH` the last provider's directories end up first.

Consequences: a MeteoSwiss user who exports `ECCODES_DEFINITION_PATH=<cosmo-mars>:<cosmo-resources>`
and `METKIT_HOME=...` in a profile or shell needs no plugin settings at all; a user who
sets `eccodes_definitions` on a tagged provider gets those directories in front of
whatever the environment already had. Because all providers share one process
environment, two tagged providers with different `eccodes_definitions`/`metkit_home`
are not supported in one process (documented; last one wins with a warning).

Load-order caveat: the variables must be set before the libraries load. The plugin
defers its imports, but if another module imported `eccodes`/`pyfdb` earlier in the
same process (e.g. a Snakefile `import eccodes` at top level), eccodes reads
`ECCODES_DEFINITION_PATH` when definitions are first needed rather than at import, so
the setting usually still takes effect, but this is not guaranteed across eccodes
versions; `METKIT_HOME` is read at first expansion [verified: setting it after
`import pyfdb` works]. Documented; the recommended pattern is to set env vars in the
profile so they exist before Snakemake starts.

`env_var` is not set on any field. Environment variables are exported in
`StorageProvider.__post_init__` *before* the lazy import of `pyfdb`/`eccodes`;
`ECKIT_EXCEPTION_IS_SILENT` is `setdefault`-ed to `1` there. Caveat: if another module
imported `eccodes` earlier in the same process, definition-path changes are still read
when definitions are first needed, but this is not guaranteed → documented.

Snakefile usage (ECMWF-style FDB and a MeteoSwiss FDB in one workflow):

```python
storage:
    provider="fdb",
    config=".fdb/config.yaml"

storage mch:                      # site-specific values come from the user, see docs/sites/meteoswiss.md
    provider="fdb",
    config="/scratch/mch/<user>/fdb/config.yaml",
    eccodes_definitions="/scratch/mch/<user>/eccodes-cosmo-mars/definitions:/scratch/mch/<user>/eccodes-cosmo-resources/definitions",
    metkit_home="/scratch/mch/<user>/metkit-home"

rule t2m_ecmwf:
    input:  storage.fdb("fdb://class=ea,expver=0001,stream=oper,date={date},time=0000,domain=g,type=an,levtype=sfc,step=0/6/12,param=167")
    output: storage.fdb("fdb://class=ea,expver=0002,stream=oper,date={date},time=0000,domain=g,type=an,levtype=sfc,step=0/6/12,param=167")
    run:    ...  # rewrite expver with eccodes

rule t2m_icon_ch1_control:
    input:
        storage.mch("fdb://class=od,expver=0001,stream=enfo,model=icon-ch1-eps,date={date},time={time},type=cf,levtype=sfc,step=0/to/33/by/1,param=500011")
    output:
        "plots/t2m_{date}{time}.png"
    shell: "..."

rule tp_icon_ch1_members:
    input:
        storage.mch("fdb://class=od,expver=0001,stream=enfo,model=icon-ch1-eps,date={date},time={time},type=pf,levtype=sfc,number=1/to/10,step=0/to/33/by/1,timespan=fs,param=500041")
```
[assumed: the `storage <tag>:` directive syntax; `storage.py:register_storage(provider, tag=...)` supports it.]
The same can be expressed in a Snakemake profile
(`storage-fdb-config: [mch::/scratch/.../config.yaml]`, `storage-fdb-eccodes-definitions: [mch::/...:/...]`,
`storage-fdb-metkit-home: [mch::/...]`), which is how a site ships its defaults
(`examples/meteoswiss/profile/config.yaml`).

---

## 5. Provider

- Lazy backend: `pyfdb`/`eccodes` imported at the end of `__post_init__`, after the
  environment is prepared (§4.1), not at module import (keeps `snakemake --help` and
  `is_valid_query` free of the C++ libraries). An `ImportError` becomes
  `WorkflowError("cannot import the FDB/eccodes bindings: ...")`. No FDB handle is
  created at construction.
- `__post_init__` order: choice settings, `identifier_check`, `glob_required_keys`
  → environment (§4.1) → `config`/`user_config` (`backend.resolve_config`) → schema
  path and `SchemaInfo` → `key_order` → lazy import → `Backend` → `guard`
  (`guard.make_guard`). Provider attributes: `archive_mode`, `store_check`,
  `canonical_spelling`, `remove_policy`, `glob_required_keys` (tuple), `config`,
  `user_config`, `schema_path`, `schema_info`, `key_order`, `backend`, `guard`.
- Config resolution as in §4; the resolved `config`/`user_config` objects are stored;
  **archiving handles are created lazily, one per thread** (`Backend.handle()`,
  `threading.local`), because pyfdb instances must not be shared across threads (§2.4)
  and Snakemake may call us from several threads/processes (§2.7). **Reads
  (`inspect`, `list`, `retrieve`) open a fresh handle per call** (`Backend.reader()`),
  because a handle that has read keeps a stale catalogue (§2.4): otherwise `mtime()`
  and `retrieve_object()` after a re-store, or an input read after its producer
  stored it, would see the old fields.
- Settings validation at construction: `identifier_check` must be `none` (v1),
  `archive_mode`/`store_check`/`canonical_spelling`/`remove_policy` must be known
  values, `metkit_home` must contain `share/metkit/language.yaml`, `eccodes_definitions`
  entries must be existing directories.
- `example_queries()`: generic MARS examples only, in canonical spelling (§3.2), so
  they do not trigger the spelling warning. The first two queries of §3.1 are used
  with `time=0000` and `param=167`, plus a range one
  (`...,date={date},time={time},...,step=0/to/48/by/6,param=167/165/166`).
  Site-flavoured examples live in `docs/sites/`.
- Settings validation also covers `eccodes_definitions` (existing directories),
  `key_order` (valid key names), `env` (syntax).
- `rate_limiter_key` → `"fdb"`, `default_max_requests_per_second` → `10.0`,
  `use_rate_limiter` → `False`. `safe_print` → identity. `postprocess_query` → §3.2,
  in the provider's `key_order`. It returns invalid queries unchanged (the storage
  object reports the parser error on use) and records valid results for
  `is_normalised(query)` (§3.3).
- A resolvable schema file that cannot be read or has no rule keys raises
  `WorkflowError("FDB configuration error: schema <path>: ...")` at construction.
- Schema knowledge: if the resolved config has a `schema:` path readable locally, the
  provider parses it once (pure Python, before any `pyfdb` import) to obtain the ordered
  list of schema keys, which are optional (`key?`, `key?default`) and which are removed
  (`key-`). Tolerant regex over the rule section. Implemented in `backend.py`:
  `resolve_schema_path(config, env=None) -> Path | None` mirrors fdb5
  `Config::expandConfig`/`initializeSchemaPath` [verified: `Config.cc` at `63672ea`]:
  config = explicit `config`, else `FDB_CONFIG`/`FDB5_CONFIG` (YAML text), else
  `FDB_CONFIG_FILE`/`FDB5_CONFIG_FILE` (a missing file gives the empty skeleton
  config), else `$FDB_HOME/etc/fdb/config.{yaml,json}` (FDB also tries
  `<program name>.{yaml,json}` there; the plugin does not); schema = the config's
  `schema`, else `FDB_SCHEMA_FILE`, else `~fdb/etc/fdb/schema`, with `~fdb` expanded
  from the config's `fdb_home` or `$FDB_HOME`. It returns `None` when the result is not
  an existing file or `~fdb` cannot be expanded without the library (FDB then uses its
  install directory). `parse_schema(text) -> SchemaInfo(keys, optional, removed,
  defaults)`: `keys` in first-appearance order (identical to `KeyOrder.from_schema`),
  decorations merged over all rules (a key can be optional in one rule and removed in
  another). Used for the canonical key order
  (§3.2) and by identifier-mode archiving (§7.7). If unavailable, the generic order and
  `query keys ∪ message keys` are used.

---

## 6. Storage object: common

`StorageObject(StorageObjectRead, StorageObjectWrite, StorageObjectGlob)`.

`__post_init__`: parse the (already normalised) query and apply the over-long wildcard
guard (§3.3). The parse result is cached per query text rather than precomputed:
`StorageObject.parsed` (a `ParsedQuery`) and `local_suffix()` always follow the current
`self.query`, because Snakemake rewrites `query` on copies without `__post_init__`
(§2.7). Keys, request and wildcard information come from `self.parsed`. Invalid queries
do not raise at construction (Snakemake validates separately), but `parsed`,
`local_suffix()` and every I/O method raise
`WorkflowError("invalid FDB query <query>: <parser message>")`.

`_fields()`: one `inspect(request)` call (`Backend.inspect`); returns a list of
`backend.Field(key: dict, length: int, timestamp: int, uri_path: str|None)` (`length`
0 and `uri_path` `None` for elements without location, i.e. level < 3) with `timestamp`
extracted by `re.search(r"timestamp=(\d+)\s*$", repr(el))` (§2.2). **Not cached**:
Snakemake deactivates its IOCache after building the DAG and then expects fresh
existence/mtime answers from the same objects, e.g. an input retrieved after its producer
ran [verified: snakemake 9.27.0 `workflow.py:1351`, `io/__init__.py:800-814`];
`inventory()` fills the IOCache from one call instead (§7.6). `_expanded()` (the expanded
request, §3.4) is cached per query text; its first successful computation runs
`_check_spelling()` (§7.12), and `_fields()` calls it before any FDB I/O, so invalid
requests (metkit `UserError` during expansion) and spelling errors are raised without
touching FDB. `_expected()`: `E` from the cached expansion. A query that still contains
wildcards raises `WorkflowError("FDB query <query> has unresolved wildcards")` in every
read method.

Retries: `snakemake_interface_storage_plugins`' `retry_decorator` (3 attempts,
exponential wait from 3 s), copied with tenacity's `reraise=True`
(`retry_decorator(f).retry_with(reraise=True)`, hence the direct `tenacity`
dependency, §11),
wraps only the FDB I/O calls, `StorageObject._inspect` and
`StorageObject._retrieve_to`, which back `exists/mtime/size/retrieve_object/inventory`,
and `StorageObject._list`, which backs `list_candidate_matches` (§7.9).
Errors raised by the plugin's own logic (invalid request from expansion, missing fields,
spelling error, `FileNotFoundError` from `mtime`) are deterministic and not retried. After
the last attempt, the attempt's own exception is raised instead of tenacity's
`RetryError`, so it can be mapped below.

Error mapping (`backend.map_error(exc, query, local=None) -> WorkflowError | None`;
`RuntimeError` messages from pyfdb, all [verified] strings on 5.21.4.23, §2.9). Backend
methods propagate the raw exceptions; callers raise the mapped error `from exc`, or
re-raise when `None` (the storage object does this in its `_mapping_errors(local=None)`
context manager around every backend call and around `grib.split_messages`; it catches
`RuntimeError` and `grib.GribError`). `<detail>` is the first non-empty line with leading `UserError: `
/ `Serious bug: ` prefixes removed. Rows are checked in this order:

| contains | raised as |
|---|---|
| `UserError` | `WorkflowError("Invalid MARS request <query>: <detail>")` (includes the metkit context hints, e.g. `number` not acceptable with `type=cf`); if the text contains `cannot expand`, the hint ` (if this value is valid for your FDB, point metkit_home at a MARS language that defines it)` is appended |
| `Cannot find a metkit SplitterBuilder` | `WorkflowError("<local file> is not GRIB")` (`<query>` if no local file) |
| `Keywords not used` / `Could not find [` / `Could not find a rule` | `WorkflowError("GRIB keys do not match the FDB schema for <query> (<local file>): <detail>")` |
| `Cannot open` (config/schema) / `No writable roots available` | `WorkflowError("FDB configuration error: <detail>")` |
| `grib.GribError` (from `split_messages`, before any archive) | `WorkflowError(<its message>)` — "`<path>` is not GRIB", "trailing non-GRIB bytes at offset N", "cannot read GRIB message" |
| other | re-raised (the `managed_*` wrappers turn it into `WorkflowError`) |

---

## 7. Per-method behaviour

### 7.1 `exists()`

`E > 0 and len(_fields()) == E` — **all** combinations present.

Justification: an input with a partially present multi-field request is not usable as
"the object"; reporting it missing makes Snakemake either run the producer (which
archives the complete set) or fail with a clear "missing input". "Any" semantics would
let jobs run on incomplete data and would make `store_check=strict` meaningless.
Optional schema keys are part of the request only if the user names them, and
`inspect` requires exact keys (§2.3), so a query that omits `number` for `pf` data or
`timespan=fs` for accumulated MeteoSwiss data correctly reports "missing"; the error
text of `retrieve` failures hints at optional keys. Invalid values (`class=zz`) raise
`WorkflowError` rather than returning `False`. With the pure-Python fallback expansion
(no alias resolution) `E` may over-count when a list contains aliases of the same field;
documented. evalml's own completeness check is the same idea at "block" granularity
(§2.8).

### 7.2 `size()`

`sum(f.length for f in _fields())`. Equals the retrieved byte count (§2.3).
`local_footprint()` = `size()`. It is the sum of **message** lengths, so for a stored
file with NUL record padding (GRIB1, §2.1, §7.7) `size()` is smaller than the original
local file; a retrieved copy has no padding. Snakemake uses size only for `ensure`
non-empty checks, the checksum-eligibility threshold and input-size resources, never to
compare storage against local size [verified: snakemake 9.27.0 `dag.py:659`,
`io/__init__.py:654-670`, `scheduling/job_scheduler.py:207`], so this is documented,
not corrected.

### 7.3 `checksum()`

Returns `None`. Snakemake then hashes the local copy with its default algorithm and
records that (§2.7). A metadata pseudo-checksum was rejected: it would need a hashlib
algorithm prefix, be compared against real content hashes on the local-file fallback
path and by `ensure(sha256=...)`, and cause spurious reruns. Hashing retrieved bytes
costs a full retrieve, no cheaper than Snakemake's fallback.

### 7.4 `mtime()`

`max(f.timestamp)` over `_fields()`, as `float` POSIX seconds. For any field with
`timestamp == 0` (legacy index) and a local `uri_path`, use `os.stat(uri_path).st_mtime`
instead; if no local path, use `0.0` and log a warning once. If no fields:
`FileNotFoundError`. After `store_object`, the value is the flush second (≥ the archive
start), which Snakemake copies onto the local file.

### 7.5 `retrieve_object()`

1. `fields = _fields()`; unless `E > 0 and len(fields) == E` → `WorkflowError` listing the missing
   combinations (computed from the expansion):
   `"<query>: <n> of <E> fields found in FDB; missing: step=18; ... (and <k> more)"`.
   It shows at most 10 combinations, each naming only the keys with several distinct
   values (all keys for a single-field query), in canonical key order. When no field is
   found, `. FDB matches keys exactly; optional schema keys not in the query: <keys>` is
   appended (the schema's optional keys minus the query's keys, sorted).
2. `Backend.retrieve_to(request, local_path(), expected=sum(lengths))`:
   `fdb.retrieve(request)`, stream with `readinto` in 8 MiB chunks into
   `<local_path>.part` (e.g. `param=167.grib.part`), fsync, `os.replace` to
   `local_path()`.
3. Inside `retrieve_to`: if `bytes_written != expected`, or on any error, the `.part`
   file is deleted, `local_path()` is left untouched and the error raised
   (`WorkflowError` for the byte count).
4. Message order is FDB/request order (§2.3); no re-sorting.

### 7.6 `inventory(cache)` / `get_inventory_parent()`

`get_inventory_parent()` → `None`. `inventory()` performs exactly one `inspect` and
fills `cache.exists_in_storage[key]` for `key = self.cache_key()`, and, when the object
exists, `cache.mtime[key] = Mtime(storage=...)` and `cache.size[key]`. It does nothing
if `key` is already in `cache.exists_in_storage`. Nothing is written to
`cache.checksum`; a missing object gets no mtime/size entry (Snakemake does not ask for
them). Rationale: a DB- or index-level `list` can contain
10^5–10^6 fields on an operational FDB and does not tell us how the workflow groups
fields into queries; the per-object `inspect` is the cheapest complete answer and
replaces three later calls. Blocking inside the coroutine is accepted (http plugin does
the same).

### 7.7 `store_object()`

Preconditions: no wildcards in the query (`WorkflowError("FDB query <query> has
unresolved wildcards")`); `local_path()` exists (an empty file is "not GRIB"). The
request is expanded first (`_expected()`), so an invalid request and the
canonical-spelling check (§7.12) fail before the file is read. `<query>` and `<local>`
below are the query and `local_path()`; messages are numbered from 1.

1. Split the local file into GRIB messages with eccodes (`codes_grib_new_from_file`
   loop, tracking offsets; `grib.split_messages`); bytes between/after messages that are
   not part of a message → `WorkflowError("<local>: trailing non-GRIB bytes at offset N")`
   (FDB would silently drop them, §2.4). **NUL padding is not an error** (GRIB1 files are
   padded with `\0` to 120-byte records, e.g. `.raw/template.grib`, §2.1): only non-NUL
   bytes outside messages are rejected. Zero messages → `"<local> is not GRIB (no GRIB
   message found)"`. `grib.split_messages` raises `grib.GribError` (a `ValueError`),
   mapped to `WorkflowError` (§6).
2. For each message read the `mars` namespace keys plus `paramId`. **Count check before
   archiving** (`n` messages, `E` expected fields, §3.4): `n > E`, or `n < E` with
   `store_check=strict` →
   `WorkflowError("<query>: <local> has <n> fields, the query expands to <E>; nothing was archived")`.
   `n < E` with `warn` logs `"FDB storage: <query>: <local> has <n> fields, the query expands to <E> (store_check=warn)"`.
3. `archive_mode=native` (default, decision below): one
   `fdb.archive(<the messages' bytes concatenated>)` (NUL padding dropped); `flush()`.
   FDB derives the keys and picks the matching schema rule itself, so the pre-check and
   the guard are not consulted.
   `archive_mode=identifier`: build the identifier of each message
   - pre-check first: for every constant query key, single- or multi-valued, that the
     message carries (MARS namespace; `param` = `paramId`), the message value must equal
     the query value, or be one of the listed values, after light normalisation
     (`query.comparable`: integers compared numerically, a one- or two-digit `time` as
     hours (`12` = `1200`); `param` items `N.T` as paramId `N` for table 128, else
     `T*1000+N`; `date` only as `YYYYMMDD`; other strings case-insensitively). The key is
     skipped if its value contains `to`/`by`, if its items are not all comparable (a
     `param` shortname such as `2t`, a relative date `-1`, `2020-01-01`) and of one kind
     (all integers or all strings; decided once per store in `_allowed_values`), or if
     the message value is not comparable or not of that kind (non-comparable aliases
     such as `step=0` vs the message's `0m`, `time=00:00`). Failure →
     `WorkflowError("<query>: message <i> of <local> has <k>=<message value>, but the query has <k>=<query value>; nothing was archived")`
     (single-valued) or
     `WorkflowError("<query>: message <i> of <local> has <k>=<message value>, not one of <raw query value>; nothing was archived")`
     (multi-valued). Keys the message does not carry are not checked: the query value
     labels the message, which is how data lacking a key gets labelled. Deliberately
     relabelling data whose GRIB says otherwise (a `step=0` message stored as `step=6`)
     is not supported in v1; fix the GRIB first, e.g. with `grib_set -s step=6`;
   - for each schema key `k` (or, without schema knowledge, each key in
     `query keys ∪ message keys`, in canonical key order), skipping removed keys (`k-`):
     - if the query has exactly one literal value for `k` → that value in FDB's canonical
       spelling, taken from the object's request expansion (§3.4; `param=167.128` →
       `167`, `time=0` → `0000`), checked by the pre-check when the message carries `k`.
       Without metkit expansion (fallback, §3.4, which keeps single values as written)
       the verbatim value is used; the object logs the missing expansion once per query
       text at debug level ("... identifier values from the query archived verbatim").
       FDB may then store a second spelling or reject the value (§2.4);
     - elif `k` in message keys → message value (`param` uses `paramId`);
     - elif `k` optional → omit; else
       `WorkflowError("<query>: cannot determine <k> for message <i> of <local>; nothing was archived")`.
     The schema keys are merged over all rules (§5), so with a multi-rule schema a key
     that is mandatory only in another rule also needs a value, and data matching a rule
     without that key fails with "cannot determine" (decision below, §12). Use
     `identifier` only with schemas whose rules share one key set, or supply keys via
     the query; the plugin does not implement rule matching;
   - keys in the message but not in the schema are dropped (e.g. `number` for a schema
     without `number?`); the MeteoSwiss schema's `domain-` is skipped and FDB lists it as
     `domain=''` (§2.8);
   - **Identifier guard hook** (reserved): `self.provider.guard.check(message, identifier,
     parsed)` is called here for every message, after its identifier is built and before
     any `archive()`. An `IdentifierMismatch` becomes
     `WorkflowError("<query>: identifier check failed for <local>: <mismatch>; nothing was archived")`.
     Interface:
     ```python
     class IdentifierGuard(Protocol):
         def check(self, message: GribMessage, identifier: dict[str, str], query: ParsedQuery) -> None:
             """Raise IdentifierMismatch(message_index, key, identifier_value, grib_value) if the
             identifier contradicts the message's own GRIB metadata."""
     class NoGuard:      # identifier_check=none (v1): never raises
     class StrictGuard:  # identifier_check=strict (reserved). Beyond the built-in pre-check (constant
                         # query keys the message carries, light normalisation, incomparable aliases
                         # skipped) it would: compare every identifier key eccodes can derive from the
                         # message canonicalised exactly as FDB does (metkit expansion: param shortnames,
                         # step units such as 0m, time/date aliases the pre-check skips); check keys the
                         # message does not carry (query labels, schema defaults) for schema-level
                         # consistency, i.e. the identifier selects exactly one schema rule and names
                         # only that rule's keys; collect and report all mismatches of a file together.
                         # Not implemented in v1: constructing it raises NotImplementedError, and the
                         # settings validation rejects the value earlier.
     ```
     All messages of a file are checked before the first `archive()` so a mismatch
     leaves FDB untouched.
   - `fdb.archive(message_bytes, identifier)` per message; `flush()` once at the end.
   **Duplicates** are rejected in both modes before archiving: two messages with equal
   identifiers (identifier mode) or equal `mars` namespace keys (native mode) →
   `WorkflowError("<query>: <local> holds duplicate fields (messages <i> and <j>); nothing was archived")`.
4. Post-check via one `inspect(request)` on a fresh handle (both modes): let `fresh` be
   the found fields whose time (index timestamp, `os.stat` fallback as in §7.4) is
   `>= t_start` (`t_start = int(time.time())` taken just before the first `archive()`).
   Every message must be reachable by the query: `len(fresh) < n` →
   `WorkflowError("<query>: <local> has <n> fields, the query expands to <E>; <n - len(fresh)> landed outside the query or are duplicates (they stay in FDB until the next successful store masks them)")`.
   With the count check of step 2 this is `len(fresh) == E` for `strict` and "all `n`
   fields reachable" for `warn`. Timestamps have one-second resolution: a field of the
   query archived earlier within the second of `t_start` counts as fresh, so a foreign
   message can go unnoticed when the missing field was stored in that same second.
5. On failure after at least one `archive()` call succeeded, the error reads
   `"<error> (<k> of <m> archive calls succeeded before the failure; they stay in FDB until the next successful store masks them)"`
   (`m` = messages in identifier mode, 1 in native mode). The handle is flushed on a
   best-effort basis first, so the archived fields become visible and deterministic.
   `retry_decorator` is **not** applied to `store_object` or its archive calls; the
   post-check `inspect` uses the read path's retrying `_inspect` (§6).

**Decision (user, 2026-09-15, reversible; supersedes the identifier default of revisions
1–3): `archive_mode` defaults to `native`.** The earlier default rested on two points:
identifier mode is the only one that stores `.raw/template.grib` under a schema without
`number?` (`.raw/schema`, §2.1), and MeteoSwiss GRIB was assumed not to carry every
schema key in its MARS namespace. The MeteoSwiss assumption was disproven: with the
cosmo-mars and cosmo-resources definitions, native archive of all OGD samples under
`realtime-varda.schema` works (§2.9, plan step 6 site tests). Evidence for the change
[verified: plan step 6a, schema analysis and native archive runs]:
- identifier mode treats every key that is non-optional in *any* schema rule as
  required (merged `SchemaInfo`, §5);
- under `tests/data/ecmwf-fdb-tests.schema` (ECMWF multi-rule test schema, 56 keys),
  24 keys cannot be determined for every ECMWF sample: `country`, `dbase`, `rki`,
  `rty`, `ty`, `bcmodel`, `icmodel`, `fcmonth`, `dataset`, `anoffset`, `georef`,
  `ident`, `instrument`, `stattype`, `obsgroup`, `reportype`, `reference`, `refdate`,
  `diagnostic`, `fcperiod`, `offsetdate`, `offsettime`, `leadtime`, `opttime`. So
  `synth11`/`steprange`/`template` fail with "cannot determine";
- native `pyfdb` archive of `synth11`, `steprange`, `template` and `quantile` under that
  schema succeeds: FDB picks the matching rule, and `list` shows the expected keys
  (e.g. `number=0` for enda, `quantile=34:100` for efhs);
- with single-rule schemas (`.raw/schema`, `tests/data/schema`, varda) identifier mode
  determines all keys.

Identifier mode stays for single-rule schemas and for GRIB lacking a schema key (e.g.
`template.grib` under `.raw/schema`, where native archive fails with
`Keywords not used: {number}`). Its multi-rule limitation is documented (§4, §12); rule
matching is not implemented. Native mode is also pyfdb's recommended path and what
MeteoSwiss uses in production (§2.8).

**Decision (user, 2026-09-15, reversible): in identifier mode, single-valued query keys
are checked against the message** (step 3 pre-check). Before, such a key overrode the
message value unchecked, and only multi-valued keys were pre-checked. Risk that
remains: FDB does no consistency check (§2.4). The pre-check rejects constant query
values that contradict the message, and the post-check guards against query/file
mismatch. Neither catches a wrong label for a key the message does not carry, or an
alias the light normalisation skips; that is what the reserved
`identifier_check=strict` guard is for.

**Decision (user, 2026-09-15, reversible): identifier values taken from the query are
archived in FDB's canonical spelling**, from the expansion the object computes once
(`_expanded()`), because FDB stores some spellings verbatim and rejects others (§2.4).
Message-derived values are left as eccodes reports them (`paramId`, `YYYYMMDD`, `HHMM`,
lower-case enums; FDB canonicalises `step=0m` itself); the site suite's identifier
stores list canonical keys. With the fallback expansion the verbatim value is kept
(debug log).

**Decision (user, 2026-09-15, reversible): the pre-check skips mixed number/string
comparisons** (single- and multi-valued keys), e.g. `step=0` vs the message's `0m`,
`time=00:00` vs `0000`. This is conservative: such pairs are never reported as
mismatches, and the post-check still catches fields that land outside the query.

Concurrency: each thread archives through its own handle; `flush()` flushes all pending
archives of that handle only; parallel archives into one toc DB are supported by FDB
(§2.4). The post-check reads through a fresh handle (§5).

### 7.8 `remove()`

Never deletes data. Behaviour per `remove_policy` (§4): `ignore` does nothing; `warn`
logs, once per distinct query per process (module-level set `_REMOVE_WARNED`), through
the provider's logger:
"FDB cannot delete individual fields; existing fields for <query> will be masked by the
next archive. Use \`fdb purge\` to reclaim space." (backticks included); `error` raises
`WorkflowError("remove_policy=error: <that text>")`. Justification: per-field deletion is
impossible and `wipe` at index granularity can delete unrelated fields or the whole
database (§2.5). Snakemake's rerun semantics still hold: the rerun archives the new
fields, which mask the old ones, `exists()` stays `True`, `mtime()` advances.
Consequences (documented): `--delete-all-output` and temp outputs leave data in FDB.
A future `wipe` policy would only be acceptable when `list(level=2)` proves that the
query covers every field of every index it touches; not planned for v1.

### 7.9 `list_candidate_matches()` (glob)

Snakemake calls it on the pattern object built by `storage.<name>(...)` and matches
`re.match(regex_from_filepattern(storage_object.query), candidate)` (§2.7), so every
candidate is the pattern's normalised text (§3.2) with its wildcard-bearing values
replaced.

1. Split the pattern's keys into constants (`ParsedQuery.constant_pairs()`) and
   wildcard-bearing keys (`wildcard_keys()`, including values that mix a wildcard with
   literal text such as `date={year}0101`). Every key in `glob_required_keys` must be
   constant; a required key that is a wildcard **or absent from the pattern** (both
   would list everything under it) raises
   `WorkflowError("FDB glob pattern <query> needs constant values for <keys> (glob_required_keys)")`
   before any FDB I/O (`<keys>` comma-separated, in setting order).
2. One `list(constant pairs, level=3)` on a fresh handle (§5), masked fields excluded.
   Constant values are passed verbatim (lists, `to`/`by` ranges and aliases expanded by
   metkit); keys absent from the pattern act as wildcards (§2.3). The call is
   `StorageObject._list`, retried like `_inspect` (§6); pyfdb errors are mapped (§6),
   e.g. `class=zz` → "Invalid MARS request". No canonical-spelling check runs.
3. For each element: if any wildcard-bearing key is missing from `combined_key()` or
   listed with an empty value (optional keys: absent under `tests/data/schema`, `''`
   under the varda schema, e.g. `number` of `cf` fields), skip it. Otherwise emit
   `fdb://` + the pattern's pairs in canonical key order, with the pattern's constant
   text for constant keys and `combined_key()[k]` for wildcard-bearing keys. Keys absent
   from the pattern do not appear, so fields that differ only in such keys (or only
   within a constant list) give the same candidate.
4. Return the de-duplicated candidates, sorted as strings. Snakemake's regex then
   extracts the wildcard values (a mixed value such as `date={year}0101` matches when
   the listed value fits; a candidate that does not fit a constraint is dropped by
   Snakemake). Values from FDB are canonical (`500011`, `icon-ch2-eps` lower-case,
   `time=0000`), so wildcard constraints must match that form.

### 7.10 `touch()`

Not implemented in v1 (`--touch` fails upfront for FDB outputs with Snakemake's own
message). A later version could re-archive the retrieved bytes to refresh the index
timestamp.

### 7.11 `cleanup()`

No-op. Archiving handles are per thread and flushed after every store; reads use a
fresh handle each (§5).

### 7.12 Canonical-spelling check (runtime)

Called once per storage object and query text on first `exists/mtime/size/retrieve/inventory/store`
(i.e. when `expand()` is computed anyway; with `error` it is repeated on every call because
the failed expansion is not cached). For every key whose query value contains no `to`/`by`
and no wildcard: compare the literal items with the expanded canonical items
position-wise (`2t`→`167`, `T_2M`→`500011`, `EA`→`ea`, `ICON-CH1-EPS`→`icon-ch1-eps`,
`ICON-CH2-EPS`→`icon-ch2-eps` — the case eccodes-cosmo-mars emits in the GRIB MARS
namespace is upper-case, FDB lists it lower-case, so MeteoSwiss users copying
`grib_ls -n mars` output will see this warning until they lower-case `model`;
`2020-01-01`→`20200101`, `0`→`0000` for `time`, `1`→`0001` for `expver`). Ranges are
exempt because their canonical expansion is a long list (`0/to/60/by/10m` → 61 items,
§2.3). On a difference, per `canonical_spelling`:
- `warn` (default): one warning per distinct query per process (module-level set,
  `_SPELLING_WARNED`), through the provider's logger:
  "Query <query> uses non-canonical spelling: param=T_2M (canonical: 500011), class=EA (ea).
  Use canonical spellings to avoid duplicate local paths for the same field."
- `error`: `WorkflowError` with the same text.
- `ignore`: nothing.
Local paths are never changed by this check. If expansion is unavailable (fallback
mode, §3.4), the check is skipped; the storage object's single debug log of the missing
expansion (§7.7) covers this case too.

---

## 8. Edge cases

| case | behaviour |
|---|---|
| query with `to/by` and one missing step | `exists()` False; `retrieve` error lists the missing combinations |
| `param=2t/167` (aliases) | FDB finds one field; `E` counts 1 with metkit expansion, 2 with the fallback → documented; canonical-spelling warning fires |
| optional schema key omitted (`number` for pf, `timespan=fs` for accumulations) | not found; error text hints at optional keys |
| `number` given with `type=cf` | `WorkflowError("Invalid MARS request ... Key [number] not acceptable with context ...")` |
| `date=-1` relative dates | accepted by metkit; local path contains `date=-1`; object changes meaning daily (documented) |
| wildcard value containing `/` | not supported (breaks path commutation); documented |
| two providers/tags, same query | separate local prefixes, separate FDBs |
| multi-message file with duplicates | store error |
| local output file not GRIB (e.g. `TestStorageBase`'s `test` text) | store error "not GRIB" |
| FDB unreachable / bad config | `WorkflowError("FDB configuration error ...")` at first use, not at provider construction |
| `class=zz` typo | `WorkflowError` (invalid request), not "missing"; the same for a glob pattern |
| glob pattern with `class={c}` or without `class` (default `glob_required_keys`) | `WorkflowError("FDB glob pattern <query> needs constant values for class (glob_required_keys)")` before any FDB I/O (§7.9) |
| glob `number={n}` over fields without `number` (control members, single-rule schema) | those fields are skipped; no candidate, no error (§7.9) |
| `model=icon-ch1-eps` without `metkit_home` | `WorkflowError("Invalid MARS request ... cannot expand 'icon-ch1-eps' ...")` with a hint to set `metkit_home` |
| very long value list (> 255 bytes component) | hashed component (constant) / error (wildcard) |
| `inspect` on legacy index (timestamp 0) | `os.stat` fallback |
| concurrent stores from several jobs | independent handles; FDB handles locking |
| `identifier_check=strict` configured | configuration error at provider construction (reserved) |
| `archive_mode=identifier` with a multi-rule schema (e.g. ECMWF's test schema) | `WorkflowError("<query>: cannot determine <k> for message <i> of <local>; nothing was archived")` for a key mandatory only in another rule; use `native` (default) or supply the key in the query |
| identifier mode, single-valued query key contradicts the GRIB (`step=6` query, `step=0` message) | `WorkflowError("<query>: message <i> of <local> has step=0, but the query has step=6; nothing was archived")`; relabelling requires fixing the GRIB (e.g. `grib_set`) |
| identifier mode, query key the message does not carry (`quantile=1:10` for a GRIB without `quantile`) | the query value labels the message, unchecked |
| identifier mode, non-canonical query value (`param=167.128`, `time=0`) | archived in canonical spelling (`param=167`, `time=0000`); verbatim without metkit expansion |
| identifier mode, number vs string pair (`step=0` query, `0m` message) | not compared (conservative); FDB and the post-check decide |

---

## 9. Test data and test strategy

**Test data is in git (user decision, supersedes the earlier "not in git").** `.raw/`
holds the ECMWF samples; `.raw/meteoswiss/` holds the OGD samples as empty-data GRIB
(constant field, `grid_simple`, `bitsPerValue=0`, 175–350 B each, MARS keys unchanged;
full-size originals in git-ignored `.local/raw-full/meteoswiss/`). CI provisioning is
resolved: CI uses these in-repo samples; the fetch script only refreshes them. Tests
still skip cleanly when files are absent. `compare.grib` is dropped.

**Two tiers, both required.** Generic tests (`tests/`) use only the ECMWF samples and
generic settings. Site tests (`tests/sites/meteoswiss/`, marker `site_meteoswiss`)
are a required part of verification: read, write, glob and e2e steps are only accepted
when they also pass with the COSMO definitions set (plan steps 5–7, 9, 10). They live
outside `src/`, take schema/definitions/paths from environment variables or a test
config (never from plugin code), and run in CI with the definitions and schema set up
as a CI step. Skipping when the setup is absent is a *local convenience* only; with
`SMK_FDB_TEST_REQUIRE_SITES=1` (set in CI) a missing prerequisite is a failure. The
plugin code does not know about them. `tests/test_no_site_specifics.py` fails if
`mch|meteoswiss|cosmo|icon-ch` (case-insensitive) appears anywhere under `src/`.

### 9.1 ECMWF samples (`.raw/`)

- Location: `.raw/*.grib` and `.raw/schema`, committed. Provenance in §2.1 allows a
  fetch script from `ecmwf/fdb` at a pinned commit for the four files (all Apache-2.0).
  `compare.grib` is dropped (decided).
- `.raw/schema` stays the untouched pyfdb copy. The plugin's test schema
  `tests/data/schema` (a 212-byte text file, committed) is
  ```
  param: Param; step: Step; date: Date; levelist: Double; expver: Expver; time: Time; number: Integer;
  [ class, expver, stream, date, time, domain? [ type, levtype [ step, quantile?, number?, levelist?, param ]] ]
  ```
  [verified: archives all four `.raw` files in native mode].
- Derived variants are created **in memory** at test time from `template.grib` with
  eccodes (`stream=oper`, `step`, `paramId`, `date` changes), never written to `.raw/`.
  Zeroed-value copies are ~236 bytes, so a fixture with dozens of fields is a few KB.
- Skip rule: `pytest.mark.skipif(not (REPO/".raw/template.grib").exists(), reason=...)`
  on every fixture/test needing `.raw/`.

### 9.2 MeteoSwiss ICON-CH2-EPS samples (OGD STAC API) — required site suite

Source: the MeteoSwiss Open Government Data REST API (§2.9), **not** the internal
`/store_new` archive. Everything in this subsection is [verified: §2.9] unless marked.
All MeteoSwiss material lives outside the package: `examples/meteoswiss/` (schema,
profile, Snakefile, `grib_keys.py`, `setup.sh`, `language.yaml` recipe, fetch script,
README) and `docs/sites/meteoswiss.md`; tests under `tests/sites/meteoswiss/`. The
suite uses the `.raw/meteoswiss/` OGD samples and the `examples/meteoswiss/` material
directly, so the examples are guaranteed to work (since plan step 10 the e2e test runs
a copy of `examples/meteoswiss/` with its Snakefile and `profile/config.yaml`, §9.5).

- Samples: the three CH2 files listed in §2.9 (`.raw/meteoswiss/`, committed as
  empty-data copies; the ≈ 2.27 MB full-size originals are in the git-ignored
  `.local/raw-full/meteoswiss/`). CH2 is used instead of CH1 to keep size low (CH1 surface
  field ≈ 2.3 MB vs CH2 ≈ 568 KB); the two products share definitions, schema and key
  conventions (only `model` differs: `icon-ch1-eps` / `icon-ch2-eps`).
- `examples/meteoswiss/fetch_ogd_samples.py` (plan step 10) reproduces them: Python
  with stdlib `urllib` + `eccodes` only (runs under `uv run`), searches the STAC API for the
  latest reference time (or `--reference-datetime`), `GET`s the pre-signed asset URL,
  subsets the perturbed file to members 1–2 by `perturbationNumber`, writes full-size
  files to `--out` (default the git-ignored `.local/raw-full/meteoswiss/`). With
  `--empty-data [DIR]` it also writes the committed copies to DIR (default
  `.raw/meteoswiss/`; data section replaced by a constant zero field, `grid_simple`,
  `bitsPerValue=0`; MARS keys verified unchanged; existing files overwritten only with
  `--force`, checked before any download). Both use the committed naming scheme
  `<model>_<YYYYMMDDHHMM>_step<h>_<var>_<ctrl|pert_mA-B>.grib2` (`icon-ch2-eps` for the
  CH2 collection). [verified: plan step 10, live run into a scratch directory: the
  constant-field copies are byte-identical to the committed samples, a rerun writes
  identical full-size files, `--empty-data` again without `--force` refuses, an expired
  `--reference-datetime` fails with "no items ...; OGD retains data for 24 h after
  publication"]
  Decided 2026-09-15 (reversible): full-size by default, committed copies only with
  `--empty-data`. Because OGD data is retained for only 24 h, the script always
  produces a *recent* reference time; the file names carry it, and the tests take
  `date`/`time` from the file name (`mch_query_base` fixture), which
  `test_conventions.py` checks against the MARS keys of the messages, instead of
  hard-coding them.
- The constant-field size trick (`grib_set -d 0`, §2.6 in revision 1) is no longer
  needed for fetching; it is how the committed samples were shrunk (a zeroed CH2 field
  is a few hundred bytes).
- `examples/meteoswiss/realtime-varda.schema`: evalml's schema (§2.8) behind a `#`
  comment header with its provenance (`MeteoSwiss/evalml`, branch `enable_fdb`,
  `resources/fdb/realtime-varda.schema`, last changed in commit `15cf43af69a1`, git
  blob `c64fd7e4b236`, 3214 B) and evalml's BSD-3-Clause license text; the body is
  byte-identical to that blob [verified: `gh api`, `git hash-object`; key order and
  `parse_schema` equal those of the bare file]. Not under `tests/data/` and never
  referenced by package code.
- `examples/meteoswiss/make_metkit_home.py [--dest .local/metkit-home] [--models icon-ch1-eps,icon-ch2-eps]`:
  copies `metkitlib/share/metkit` from the installed wheel into `<dest>/share/metkit` and
  appends the models to the context-free `model` enum block (YAML-based, unlike
  evalml's regex patch); output used via the generic `metkit_home` setting [verified:
  site suite with the generated directory]. The recipe is documented in
  `docs/sites/meteoswiss.md`.
  Decided 2026-09-15 (reversible): the default is the models the committed samples and
  the MeteoSwiss example need (`icon-ch1-eps,icon-ch2-eps`); other models are passed
  with `--models`. Valid model values [verified: plan step 10, `marsModel` concepts of
  `MeteoSwiss/eccodes-cosmo-mars` `varda-ext` at `d04363540bb2` via `gh api`; metkit
  expands each with the generated language, the stock language rejects each]:
  `cosmo-1e`, `cosmo-2e`, `kenda-1`, `snowpolino`, `icon-ch1-eps`, `icon-ch2-eps`,
  `kenda-ch1`, `icon-rea-l-ch1`, `varda-single`, `varda-ens` (`grib2/local.215.def`)
  and `varda-single-g` (`grib2/local.98.def`); `main` and `varda` lack `varda-ens` and
  `local.98.def`, and evalml's `enable_fdb` language patch adds only `varda-single`,
  `varda-single-g`. (The previously assumed extras `varda-single`, `varda-single-g`,
  `kenda-ch1`, `icon-rea-l-ch1` were incomplete.)
- Definitions: users clone `eccodes-cosmo-mars` (branch `varda-ext`) and install
  `eccodes-cosmo-resources-python` themselves (documented; `examples/meteoswiss/setup.sh`
  does both into `.local/`, idempotently: `.local/eccodes-cosmo-mars` and, with
  `uv pip install --target`, `.local/eccodes-cosmo-resources`, whose definitions are
  under `share/eccodes-cosmo-resources/definitions`), then pass the two directories as
  plain paths in `eccodes_definitions` (cosmo-mars first). No package extra, no alias.
  The definitions are installed, never copied into the repository
  (`COSMO-ORG/eccodes-cosmo-resources` declares no license; the PyPI wheel declares
  BSD-3-Clause).
- Test configuration (env vars read only by `tests/sites/meteoswiss/conftest.py`):
  `SMK_FDB_TEST_MCH_SAMPLES` (directory with the OGD files), `SMK_FDB_TEST_MCH_SCHEMA`
  (defaults to `examples/meteoswiss/realtime-varda.schema`), `SMK_FDB_TEST_ECCODES_DEFINITIONS`
  (colon list), `SMK_FDB_TEST_METKIT_HOME`. Each missing item is a separate skip reason.
- Queries used in tests (canonical spellings; `{date}`/`{time}` filled from the
  fetched messages):
  `fdb://class=od,expver=0001,stream=enfo,model=icon-ch2-eps,date=<date>,time=<time>,type=cf,levtype=sfc,step=6,param=500011`,
  `...,type=cf,step=6,timespan=fs,param=500041` and
  `...,type=pf,number=1/2,step=6,param=500011` [verified: §2.9 — cf for the control,
  `step=6`, `timespan=fs` required for TOT_PREC, `number` only for pf].
- Skips: the site suite requires the four env-configured items above; each is a
  separate `skipif` reason so logs say which is missing — a convenience for local runs
  only. In CI the definitions, schema and metkit home are set up by a workflow step
  (clone `eccodes-cosmo-mars`, install `eccodes-cosmo-resources-python`, run
  `examples/meteoswiss/make_metkit_home.py`, export the `SMK_FDB_TEST_*` variables) and
  `SMK_FDB_TEST_REQUIRE_SITES=1` turns any remaining skip into a failure. Sample data
  is not an open item any more: CI uses the committed `.raw/meteoswiss/` samples (plan
  step 11). A synthetic variant (`test_synthetic_icon`) runs
  **without** the samples but still needs the definitions: a GRIB2 message built from
  eccodes' `GRIB2` sample with `centre=215`, GPI 142, PDT 1, `T_2M` and a local section
  (`grib2LocalSectionPresent=1`, `localDefinitionNumber=253`, as in the OGD files)
  decodes to `class=od, stream=enfo, type=cf, model=ICON-CH2-EPS, expver=0001,
  param=500011` [verified: plan step 10; without the local section cosmo-mars adds none
  of `class/stream/type/model/expver`].
- CI: the 24 h OGD retention makes the API unsuitable for reproducible CI fixtures, so
  CI uses the committed empty-data samples (decided; plan step 11). Everything else the
  suite needs (definitions, schema, language) is provisioned in CI.

### 9.3 Unit tests (no FDB)

`tests/test_query.py`: grammar acceptance/rejection table, normalisation idempotence,
canonical order, wildcard atoms with constraints, local suffix, `/`→`+`, component
hashing, the **commutation property**
`local_suffix(apply_wildcards(q, w)) == apply_wildcards(local_suffix(q), w)`, and the
value normalisation `comparable` of §7.7 (`test_comparable`, incl. one- or two-digit
`time` as hours and non-`YYYYMMDD` dates as not comparable).
`tests/test_settings.py`: settings validation incl. `identifier_check=strict` rejection,
plain-path `eccodes_definitions`, `key_order`, `env`.
`tests/test_key_order.py`: order derived from `.raw/schema` (skipped if absent), from an arbitrary
schema text with `?`/`-`/defaults and several rules, from `key_order`, and the generic
fallback; unknown keys alphabetical.
`tests/test_no_site_specifics.py`: greps `src/` for `mch|meteoswiss|cosmo|icon-ch`.

### 9.4 Integration tests (temporary toc FDB per test session; skipped without `.raw/`)

Fixtures build an FDB in `tmp_path_factory` with `tests/data/schema` and archive the
four `.raw` files plus derived variants. `ECKIT_EXCEPTION_IS_SILENT=1` is set in
`conftest.py` before importing the plugin.

- Interface conformance (plan step 8; `snakemake-interface-storage-plugins` 4.4.1).
  Both subclasses derive from `FDBStorageBase(TestStorageBase)` (`files_only = True`,
  `touch = False`; an autouse fixture points the provider at an empty temp FDB in a
  `clean_env`), so every base test runs; none is disabled:
  - `TestStorageRead` (`retrieve_only = True`): `get_query` → a
    3-step request over pre-archived variants; `get_query_not_existing` → same
    request with `expver=0002`. `test_storage` and `test_storage_not_existing` are
    overridden only to gate them on `.raw/` and switch the provider to the seeded
    FDB; `test_query_validation` and `test_example_queries` need no data and run
    without `.raw/`. `retrieve_only`
    because `TestStorageBase._test_storage` writes the text `test` into the local
    path before calling `store_object()` [verified: `tests.py:86-90`], which can
    never be valid GRIB.
  - `TestStorageWrite` (`.raw/`-gated, defaults `retrieve_only = store_only = False`,
    `delete = True`, empty FDB per test): runs the base store sequence (store, delete
    the local copy, `exists`, `mtime`, `size`, `checksum`, `inventory`, retrieve,
    `remove` as a warning no-op). Its `_get_obj` override makes the object overwrite
    the `test` text with the query's 3 GRIB messages before the real `store_object`.
  - `test_interface_conformance`: `StoragePluginRegistry().get_plugin("fdb")` yields
    `StorageProvider`, `StorageObject`, `StorageProviderSettings` and
    `is_read_write()` (`StorageObjectRead` and `StorageObjectWrite`); no abstract
    methods left; `StorageObject` is a `StorageObjectGlob` and deliberately **not** a
    `StorageObjectTouch` (§7.10).
  - `test_managed_wrappers_without_rate_limiter`: the `managed_*` coroutines Snakemake
    calls (`store`, `exists`, `mtime`, `size`, `local_footprint`, `checksum`,
    `retrieve`, `remove`) pass through the no-op rate limiter (`use_rate_limiter()` is
    `False`) and return what the plain methods return; `print_query` equals the query
    (`safe_print` is the identity); `managed_mtime` of a missing object raises
    `FileOrDirectoryNotFoundError`. The values themselves (size, checksum `None`, the
    `remove` warning) are asserted by `test_store_roundtrip`,
    `test_exists_size_checksum_complete` and `test_remove_policy`.
  The existing unit tests cover the rest of the provider surface
  (`test_provider_settings_rate_limiter_and_safe_print`, `test_example_queries_valid`,
  `test_exists_size_checksum_complete`: `local_footprint() == size()`,
  `get_inventory_parent() is None`, `cleanup()`).
- Write path (`tests/test_plugin.py`, plan step 6): `test_store_roundtrip` (both
  archive modes, `NoGuard` installed), `test_store_template` (padded GRIB1, schema with
  and without `number`), `test_store_strict_rejects` and `test_store_warn_fewer_fields`
  (both modes), `test_store_guard_sees_every_message_before_archive`,
  `test_store_guard_mismatch_leaves_fdb_unchanged`, `test_store_masking_rerun`,
  `test_store_threads`, `test_store_partial_archive_failure_says_fields_stay`,
  `test_store_wildcard_query_rejected`, `test_remove_policy`; `tests/test_backend.py::test_reads_see_archives_after_an_earlier_read`
  (§2.4). Plan step 6a: `test_store_default_native_under_multi_rule_schema`
  (`synth11.grib` under `tests/data/ecmwf-fdb-tests.schema` stores and reads back with
  default settings; explicit `identifier` fails with "cannot determine"),
  `test_store_identifier_single_value_mismatch` (`step`/`param`, nothing archived),
  `test_store_identifier_archives_canonical_spelling` (`param=167`/`167.128`,
  `time=0`/`00`: listed as `param=167`/`time=0000`, found and retrieved by the canonical
  query), `test_store_identifier_verbatim_without_expansion`, `test_store_identifier_key_absent_from_message_takes_query_value`; the
  guard and partial-archive tests set `archive_mode=identifier` explicitly; site suite
  `tests/sites/meteoswiss/test_write.py::test_write_identifier_param_mismatch`. Glob
  (plan step 7): `test_glob_step_candidates_match_pattern` (plain and constrained
  wildcard; candidates match `regex_from_filepattern` and Snakemake's `glob_wildcards`),
  `test_glob_keys_absent_from_pattern_collapse`, `test_glob_skips_fields_without_the_wildcard_key`,
  `test_glob_wildcard_inside_value_and_constant_list`, `test_glob_required_keys_enforced`
  (wildcard, absent, setting), `test_glob_required_keys_empty_allows_any_pattern`,
  `test_glob_invalid_value`, `test_glob_retries_transient_list_error` (`_fail_once`,
  shared with `test_exists_retries_transient_inspect_error`); site suite
  `tests/sites/meteoswiss/test_glob.py`
  (`test_glob_members`: candidates are the pattern with the member substituted,
  `test_glob_members_skip_control`, `test_glob_params_are_canonical_cosmo_ids`,
  `test_glob_model_is_listed_lower_case`). Also `test_mtime_timestamp_fallback_os_stat`,
  `tests/test_backend.py::test_config_forms` and `::test_map_error_table`,
  `test_canonical_spelling_warns_once`/`_error_raises`/`_silent` (`param=2t` vs
  `167` → warning text; `error` policy raises; ranges exempt).

### 9.5 End-to-end

`tests/test_workflow.py` runs `snakemake` as a subprocess (`python -m snakemake`, untagged
`--storage-fdb-config ../../.fdb/config.yaml -c1`, default `native` archive mode) on a
temporary copy of `examples/ecmwf/` next to a fresh `.fdb/` initialised by
`scripts/init_dev_fdb.py --seed --variants` (skipped without `.raw/`). One module-scoped
run sequence through the `run_logged` fixture (`tests/conftest.py`: provider variables of
§4.1 dropped from the environment, one log file per stage under the temporary
directory; shared with the site e2e): init; the example (exit 0, `Storing in
storage: <query>`, the `done` rule's output names the local copy
`.snakemake/storage/fdb/class=ea/expver=0002/.../step=0+6+12/param=167.grib` and lists
steps 0/6/12 with `expver=0002`, the copy is gone after the run, the 3 fields are in
FDB); a second run (`Nothing to be done`); a small `glob_wildcards` workflow over
`step={step}` (steps 0/6/12, local copies kept with `--keep-storage-local-copies`);
`--delete-all-output` (the §7.8 remove warning is logged, the local output is deleted,
the fields stay). The example's two rules are `run:` rules, so the local executor spawns
them (§3.3): untagged settings reach spawned jobs.

Site e2e (`tests/sites/meteoswiss/test_workflow.py`, required): `scripts/init_dev_fdb.py
--root .fdb-mch --schema <varda schema> --seed <samples dir>` with
`ECCODES_DEFINITION_PATH`/`METKIT_HOME`/`ECCODES_VERSION_CHECK_OFF=1` (the documented
MeteoSwiss command, 4 messages) in a temporary directory, then, in a copy of
`examples/meteoswiss/` two levels below it, `snakemake --profile profile -c1` without
those variables (since plan step 10; before, the test wrote an equivalent Snakefile and
profile itself). The shipped example has a tagged `storage mch:` provider and profile
values `mch::` for `config` (`../../.fdb-mch/config.yaml`), `eccodes_definitions`,
`metkit_home` (under `../../.local/`) and `env`; the test replaces the two `.local/` paths
with `--storage-fdb-eccodes-definitions`/`--storage-fdb-metkit-home mch::<SMK_FDB_TEST_*>`
on the command line, which take precedence over the profile. `rule all` collects
`t2m/<date><time>.txt` for every control T_2M field `glob_wildcards` finds, and one
`shell` rule retrieves the field (byte-identical copy; the example's `grib_keys.py`
decodes `T_2M 6 0` through the exported definitions, stderr to its own rule log, §2.9;
local copy under `.snakemake/storage/mch/` removed); a second run is a no-op. A `shell`
rule runs in the main process under the local executor; a `run:` rule would be spawned
and lose the tagged settings (upstream issue, §2.7).

`scripts/init_dev_fdb.py` is generic: `--root DIR` (default `.fdb`), `--schema PATH`
(default `tests/data/schema`), `--seed [DIR]` (natively archive every GRIB file directly
in DIR, default `.raw`), `--variants [FILE]` (archive zeroed `stream=oper` variants of
the message in FILE, default `.raw/template.grib`, for steps 0/6/12 × params 167/165,
the inputs of `examples/ecmwf/`); defaults relative to the repository; the written
`config.yaml` uses absolute paths; no site flags. Decided 2026-09-15 (reversible): the
variants are an explicit flag, not a side effect of `--seed` keyed off the file name
`template.grib`. The MeteoSwiss dev FDB is a documented command in
`examples/meteoswiss/README.md`, run with the site env (definitions, `METKIT_HOME`,
`ECCODES_VERSION_CHECK_OFF=1`):
`uv run python scripts/init_dev_fdb.py --root .fdb-mch --schema examples/meteoswiss/realtime-varda.schema --seed .raw/meteoswiss`.

Decided 2026-09-15 (reversible): one examples directory — the generic example is
`examples/ecmwf/` (formerly a separate top-level directory), alongside `examples/meteoswiss/`.
Decided 2026-09-15 (reversible): `init_dev_fdb.py` has no site flag;
nothing site-specific goes in `scripts/` (documentation rule, not grep-enforced; goal 7).

---

## 10. qubed evaluation

Checked: `qubed 0.4.11` from PyPI (Rust extension via maturin, `abi3`, no Python
dependencies, Apache-2.0, wheels for manylinux2014 x86_64/aarch64 and macOS, Python
≥ 3.8; repo created 2024-10, "emerging" maturity badge, 14 stars, tag `v1.0` exists on
GitHub but PyPI latest is 0.4.11; the docs are written for the Rust API and do not match
the Python surface — e.g. README mentions `n_leaves`, absent in the wheel)
[verified: install + API probing + repo metadata].

- **Request-string parser**: no. `Qube.from_ascii` accepts only the multi-line tree
  form; a one-line `class=od,step=0/6/12` is rejected. `to/by` is not expanded;
  `{wildcard}` tokens would be plain text at best.
- **Expansion / expected-vs-present**: partially. `from_datacube(dict)`, `subtract`
  (`q - present` yields the missing sub-cube), `contains`, `axes()` work; but no leaf
  count, no `&`, `select()` returned an empty tree for a plausible request, equality is
  key-order-sensitive, `to_datacubes()` returns compressed cubes with a spurious `root`
  entry.
- **Glob**: no gain over building candidates from `fdb.list` elements.
- **Snakemake constraints**: no support for braces/constraints.

**Verdict: not useful for v1; take inspiration only.** The cross-product bookkeeping we
need is a few lines of Python over metkit's expansion; qubed adds a compiled dependency
with an unstable Python API. Revisit for compressed summaries of large FDB listings.

---

## 11. Dependencies and version pins

- `python >= 3.11, < 4`
- `snakemake-interface-storage-plugins >= 4.4.1, < 5`, `snakemake-interface-common >= 1.23, < 2`
- `tenacity >= 9.1.4, < 10` (the interface's own bound): the plugin calls tenacity's
  API directly to derive its retry policy from `retry_decorator` (§6)
- `eccodes` (Python bindings; must match the bundled `eccodeslib`, see below)
- **No site extras.** Site definitions (`eccodes-cosmo-resources-python`,
  `eccodes-cosmo-mars`, …) are installed by the user and passed as paths; the package
  never depends on them.
- dev: `snakemake >= 9.27`, `pytest`, `ruff`, `coverage`.
- Platform: Linux x86_64/aarch64 (pyfdb 5.23 wheels; 5.21.4.x also ships macOS
  wheels). CI on `ubuntu-latest`.

**pyfdb / eccodes version — decided: Option A** (`pyfdb>=5.21.4.21,<5.22`,
`eccodes>=2.47,<2.48`). [verified: PyPI metadata + §2.9]
`pyfdb` pins the whole native stack through `fdb5lib`:

| pyfdb | fdb5lib → eccodeslib / metkitlib / eckitlib | COSMO definitions match |
|---|---|---|
| 5.23.2.27 (latest) | 2.48.2.27 / 1.20.2.27 / 2.2.0.27 | none: newest COSMO definitions are v2.47.0.2 on GitHub (2026-09-02) and 2.47.0.1 on PyPI; every decoded message prints `definitions.edzw version 2.47.0 is NOT compatible with ecCodes library version 2.48.2!` (decoding and the FDB round-trip nevertheless worked, §2.9) |
| 5.22.0.26 | (not checked) | – |
| 5.21.4.21 (evalml's lock; 5.21.4.22/.23 also exist) | 2.47.3.21 / 1.18.3.21 / 2.1.0.21 | `eccodes-cosmo-resources-python` 2.47.0.1 → same series, but the version warning is still printed (2.47.0 vs 2.47.3, see §2.9 correction); plugin behaviour re-verified identical (§2.9) |

Option A — **pin `pyfdb>=5.21.4.21,<5.22`** (+ `eccodes>=2.47,<2.48`): the bundled
eccodes 2.47.3 is in the same minor series as the currently available site definitions
(COSMO 2.47.x; the patch-level warning still appears, §2.9), same
stack as evalml, macOS wheels; costs the newer FDB fixes and will need bumping once
site definitions for 2.48 exist; `pyfdb.__version__` absent (use `importlib.metadata`).
Option B — **stay on `pyfdb>=5.23.2.27,<6`** (+ `eccodes>=2.48,<3`): latest FDB, but
users whose site definitions target 2.47 get the per-message warning. Note: the
warning is printed on both series (exact version comparison in the COSMO definitions)
and users can silence it with `ECCODES_VERSION_CHECK_OFF=1` (§2.9), so it does not
discriminate between the options.
**Recommendation:** Option A for the first release: the plugin's own behaviour is
identical on both series (§2.9) and the only difference is the definitions
compatibility for users with site definitions. CI: required jobs use the locked
5.21.4.x stack; an optional, non-blocking `pyfdb-latest` job runs the suite on 5.23 +
eccodes 2.48 so we know when the pin can be lifted (plan step 11). Decided by the user
(2026-09-15); pins in plan step 0. This is a generic
eccodes-version consideration and mentions no site in the package.

---

## 12. Known limitations

1. Same field, different spelling → different local paths (warning by default, §7.12).
2. No deletion; reruns mask; `fdb purge` is manual.
3. Second-resolution mtimes tied to index flushes, not to data content.
4. `exists()` cardinality is a cross product; context-dependent keys may over-count.
5. Fallback expansion (without the internal `FDBToolRequest`) does not resolve aliases
   or non-integer ranges, and identifier mode then archives query values verbatim
   (§2.4, §7.7).
6. Wildcard values must be single values without `/`.
7. `SeriousBug` backtraces from eckit cannot be silenced from the environment.
8. `METKIT_HOME` misconfiguration hangs the process; the plugin validates the path but
   cannot validate the file's content.
9. The `FDBToolRequest` repr used for expansion is an internal API (pinned version
   range; upstream PR proposed, plan step 14).
10. Not usable as default storage provider; no `--touch`; files only.
11. eccodes definition overrides depend on environment variables read at library load.
12. MeteoSwiss: `model` values need a metkit language override (`metkit_home`);
    accumulations need `timespan=fs` in the query; COSMO paramIds (`500011`) must be
    used, not ECMWF ids (`2t`); `model` is listed lower-case (canonical-spelling
    warning for `ICON-CH2-EPS`); `eccodes-cosmo-mars` must be cloned (not on PyPI) and
    its `varda-ext` branch is what evalml uses; no `eccodes-cosmo-resources` release
    for eccodes 2.48 yet (§11 decision). ICON-CH1-EPS itself was not fetched (CH2 used
    for size); the conventions are the same by construction of `local.215.def`.
13. Identifier guard (`identifier_check=strict`) is reserved, not implemented. Identifier
    mode's built-in check covers only constant query keys that the message carries, with
    light normalisation (§7.7).
14. OGD samples expire after 24 h, so they cannot be re-fetched reproducibly; the
    committed samples are empty-data copies (no real field values).
15. Site neutrality means MeteoSwiss users supply definitions paths, a metkit home and
    (if the schema is not readable locally) `key_order` through env vars, profiles or
    settings; the documented profile in `examples/meteoswiss/` makes this a copy-paste
    step. Two tagged providers with different definitions/language in one process are
    not supported (§4.1).
16. The canonical key order depends on the provider's schema; the same query used
    with two providers with different schemas gets two local paths (separate prefixes
    anyway).
17. `archive_mode=identifier` with a multi-rule schema: every key mandatory in any rule
    needs a value, so data matching a rule without that key fails with "cannot
    determine". Use `identifier` only with schemas whose rules share one key set, or
    supply keys via the query; no rule matching. `native` (the default) is unaffected
    (§7.7 decision).
18. Identifier mode cannot relabel data whose GRIB contradicts a single-valued query
    key; fix the GRIB first (e.g. `grib_set`).
19. Tagged settings (`TAG::VALUE`) do not reach spawned job processes (upstream
    Snakemake issue, §2.7): `run:` rules under the local executor, and every job under
    cluster/remote executors, see the untagged value `TAG:VALUE`. Until it is fixed,
    use untagged settings for such workflows, or `shell` rules with the local executor.
