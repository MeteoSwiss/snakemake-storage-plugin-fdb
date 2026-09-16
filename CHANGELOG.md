# Changelog

All notable user-visible changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this
project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- `docs/patterns.md`, a usage-patterns guide with complete workflows for query shapes,
  rule directives, writing outputs, reruns and site setup; every example in it is
  executed by `tests/test_patterns.py`.

### Changed

- Reruns of rules with FDB inputs follow the *fields* of their queries, not the text: a
  query naming the same fields or fewer (narrowed, reordered, a range for a list,
  `param=2t` for `param=167`, split into per-field queries) reruns nothing, while a
  query widened to fields the job did not have reruns it, even when those fields are
  older than the output or have yet to be produced. In 0.2.0 the widened case ran
  nothing at all. `input_tracking=query` still gives Snakemake's own behaviour.
- Records written by 0.2.0 hold no FDB inputs, so rules with FDB inputs rerun once after
  upgrading; the plugin now records what Snakemake records.

### Fixed

- An unreadable FDB root is reported as `FDB I/O error ... FDB root <path> is not
  readable` on every FDB version: pyfdb 5.23 returns no fields for it instead of
  failing, which made the data look missing (found by the `pyfdb-latest` CI canary).

## [0.2.0] - 2026-09-16

Fixes and gaps found by a stress test of 0.1.0 (about 90 scenarios across rule
directives, query edits, the archive path and configuration).

### Added

- `input_tracking` setting (`lookup`, the default, or `query`) choosing whether FDB
  inputs take part in Snakemake's input-set rerun trigger.
- The settings `config`, `user_config`, `eccodes_definitions`, `metkit_home`,
  `key_order`, `env` and `glob_required_keys` can be given as the environment variables
  `SNAKEMAKE_STORAGE_FDB_CONFIG` and friends; a command-line flag still wins.
- A warning, once per query, when FDB holds some but not all fields of an input:
  `FDB storage: <query>: 2 of 3 fields found in FDB; missing: step=12`. Snakemake
  reports such an input as missing without ever showing the retrieve error.
- OS-level FDB failures (unreadable or read-only root, full disk) are reported as
  `FDB I/O error for <query>: <detail> (check permissions, free space and the roots in
  the FDB configuration)` instead of a raw `RuntimeError: Failed system call: opendir
  (Success)`.
- Hints for two confusing configuration errors: a tagged setting mangled by a spawned
  job, and no FDB configuration at all.

### Changed

- Reruns of rules with FDB inputs now follow the FDB lookup alone (the fields exist and
  their index timestamps): editing a query (narrowing, widening, reordering values or
  writing a range) no longer reruns a job by itself. Local inputs are unaffected; set
  `input_tracking=query` for the previous behaviour. The first run after upgrading
  reruns rules with FDB inputs once, because the recorded input set changes.
- `archive_mode=native` now validates every message before archiving anything: its MARS
  keys must agree with the constant keys of the query, and every query key FDB indexes
  must be present in the message. Previously FDB archived the messages under their own
  keys whatever the query said (a rule with a wrong `expver` masked its own input), and
  only the post-check noticed, after the data was in FDB.
- An input exists only if the fields carry every key of its query that the FDB schema
  indexes: FDB's `inspect` matches through keys the indexed fields do not have, so
  `quantile=1:10` used to find quantile-less fields and every per-quantile query
  "existed". This also applies to `mtime`, `size` and retrieval.
- The post-check error now names the messages that landed outside the query and the
  keys that put them there, e.g. `message 3 (step=18)`.
- Only failures that may be transient are retried. An invalid MARS request, a
  configuration error, a schema mismatch, data that is not GRIB and I/O errors now fail
  on the first attempt instead of after three attempts and about 10 seconds.
- Error messages keep only the first 200 characters of the pyfdb detail, cut before
  metkit's `request=` dump; the full text is logged at debug level. Hints are no longer
  buried behind a dump of the whole MARS vocabulary.
- `snakemake --help` names the default of every plugin setting.
- The generic example queries and the queries in the README and the documentation name
  `domain=g`, the key ECMWF `class=od`/`class=ea` fields are archived under.

### Removed

- The `store_check` setting. A store of fewer fields than the query expands to can never
  satisfy `exists()`, so `store_check=warn` produced a job that failed after a
  successful store, on that run and every later one. A wrong field count is always an
  error and nothing is archived.

### Fixed

- Messages about an invalid FDB query no longer contain
  `<snakemake_storage_plugin_fdb.StorageProvider object at 0x...>`.
- Documentation: removal happens only with `--delete-all-output` (and does not make the
  producing job rerun); `temp()` and the other Snakemake flags cannot be combined with
  storage; `--touch`, command-line targets, `--cleanup-metadata`, `ensure(non_empty)`,
  `expand()` and `multiext()` with FDB objects; relative configuration paths resolve
  against Snakemake's working directory and a `profiles/default/` is applied implicitly;
  message order in a retrieved file; query identity beyond spelling (value order,
  duplicates, `to`/`by`, key aliases, relative dates); orphaned and overlapping output
  queries; the memory an archive needs; reads do not need `eccodes_definitions`; the
  unreachable `key order setting is empty` error was removed from the reference.

## [0.1.0] - 2026-09-16

First release.

### Added

- `fdb` storage provider for Snakemake: `fdb://key=value,...` queries address one or
  more GRIB fields (`/` lists, `to`/`by` ranges, Snakemake wildcards), are normalised to
  a canonical key order (FDB schema, `key_order` setting or a generic MARS order) and
  map to one local `.grib` file.
- Read support: `exists`, `mtime` (FDB index timestamps), `size`, atomic retrieval and
  inventory; transient FDB errors are retried and missing fields are named.
- Write support: rule outputs are archived message by message, natively (the default)
  or with identifiers built from the query (`archive_mode`: single-valued keys are
  checked against each message, values archived in canonical spelling), with
  field-count checks before and after archiving (`store_check`). `remove()` never
  deletes fields (`remove_policy`).
- `glob_wildcards` support from FDB listings, with `glob_required_keys`.
- Canonical-spelling check of query values (`canonical_spelling`).
- Generic settings to point the plugin at any site's FDB, definitions and MARS
  language: `config`, `user_config`, `eccodes_definitions`, `metkit_home`, `env`; the
  `identifier_check` setting is reserved.
- `scripts/init_dev_fdb.py` to create and seed a development FDB, and the
  `examples/ecmwf/` workflow.
- MeteoSwiss site material outside the package: `examples/meteoswiss/` (schema,
  profile, workflow, definitions setup, metkit home and sample fetch scripts) and
  `docs/sites/meteoswiss.md`.
- Documentation: a user guide, a reference and contributing notes under `docs/`, and a
  rewritten README with a quick start.

[Unreleased]: https://github.com/MeteoSwiss/snakemake-storage-plugin-fdb/compare/v0.2.0...HEAD
[0.2.0]: https://github.com/MeteoSwiss/snakemake-storage-plugin-fdb/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/MeteoSwiss/snakemake-storage-plugin-fdb/releases/tag/v0.1.0
