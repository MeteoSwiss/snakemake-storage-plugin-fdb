# Changelog

All notable user-visible changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this
project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Changed

- The generic example queries and the queries in the README and the documentation name
  `domain=g`, the key ECMWF `class=od`/`class=ea` fields are archived under.

### Fixed

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
