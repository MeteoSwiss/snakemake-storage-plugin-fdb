# Changelog

All notable user-visible changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this
project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

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

- Only failures that may be transient are retried. An invalid MARS request, a
  configuration error, a schema mismatch, data that is not GRIB and I/O errors now fail
  on the first attempt instead of after three attempts and about 10 seconds.
- Error messages keep only the first 200 characters of the pyfdb detail, cut before
  metkit's `request=` dump; the full text is logged at debug level. Hints are no longer
  buried behind a dump of the whole MARS vocabulary.
- `snakemake --help` names the default of every plugin setting.
- The generic example queries and the queries in the README and the documentation name
  `domain=g`, the key ECMWF `class=od`/`class=ea` fields are archived under.

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

[Unreleased]: https://github.com/MeteoSwiss/snakemake-storage-plugin-fdb/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/MeteoSwiss/snakemake-storage-plugin-fdb/releases/tag/v0.1.0
