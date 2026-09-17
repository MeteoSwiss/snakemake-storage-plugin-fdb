# Changelog

All notable user-visible changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this
project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- `examples/forecast-evaluation/`: a dummy forecast-evaluation workflow (truth, model,
  verification, animation, scorecard) in which every rule
  reads and writes FDB directly with plain pyfdb and earthkit-data, and no GRIB file is
  ever written locally. It also shows the rerun behaviour: adding a parameter reruns the
  model, removing one reruns no FDB producer, a new model checkpoint is a new `expver`.
- The `examples` dependency group (earthkit-data, matplotlib) for that example;
  `tests/test_evaluation_example.py` runs it end to end and skips without the group.

### Changed

- An output a job archives itself is declared `storage.fdb(query, retrieve=False)`, like
  an input the job reads itself: Snakemake then writes nothing locally and checks after
  the job that every field of the query is in FDB. `touch(storage.fdb(query))` remains
  as the checked variant, which also proves the fields were archived during this run;
  `api.archive` outputs keep their plain declaration. The example, the patterns and the
  documentation follow.
- `examples/forecast-evaluation/`: `verify` and `animate` now run per parameter
  (`metrics/{init_time}/{param}.csv`, `animations/{init_time}/{param}.gif`) and a new
  `scorecard` rule aggregates those local files into `scorecard.csv`. A summary over the
  declared set therefore follows the declaration: narrowing `params` still reruns no FDB
  producer, but the scorecard is rebuilt and matches `config.yaml`. The example also
  validates its configuration where the Snakefile is read, so a non-numeric `param`, a
  short `truth_expver` or a malformed init time gives a sentence instead of a traceback.

### Fixed

- The documentation claimed that Snakemake has no output-side counterpart of
  `retrieve=False` and that a direct output must therefore leave a local file. It has
  one, at least since Snakemake 9.27.
- Documentation around direct access: `flush()` is advice, not a requirement whose
  omission fails the check (fdb5 flushes when the `FDB` object is destroyed); only
  `api.archive`, not a job archiving with plain pyfdb, reads `archive_mode` and
  `identifier_check`; errors raised by plain pyfdb in a job are fdb5/eckit text, not the
  plugin's mapped messages. New notes on extra fields never being reported, on a failed
  job's archives making its rule skipped later, on mixing a direct FDB output with a
  local one, on derived local outputs after a narrowing (L-33), on reruns without
  provenance records (L-34), on `--summary` and `--list-input-changes`, and
  troubleshooting rows to match. The example profile no longer suggests
  `storage-fdb-archive-mode`, which has no effect on its jobs.

## [0.3.1] - 2026-09-16

Jobs use plain pyfdb, eccodes and earthkit-data; the plugin stays in the Snakefile.

### Added

- Jobs no longer need the plugin: a `run:` or `script:` body parses its MARS request
  from the query string it holds as input, reads FDB with plain `pyfdb` or earthkit-data
  and archives with plain `pyfdb`, declaring such an output `touch(storage.fdb(query))`
  (the empty file makes the store step check instead of archive; see the user guide).
- `api.query(request)`: the query string of a MARS request given as a dict (values
  joined with `/`, wildcards allowed), the inverse of `api.request(query)`.

### Changed

- The provider exports its FDB configuration as YAML text in `FDB5_CONFIG` (a file's
  relative paths made absolute), plus `FDB_CONFIG_FILE` for a configuration file,
  instead of `FDB_CONFIG_FILE`/`FDB_CONFIG` alone: earthkit-data's `fdb` source reads
  only `FDB5_CONFIG`, so `from_source("fdb", request)` now works in a job without an
  argument, and fdb5 reads the text first, so pyfdb opens the same FDB.
- The user guide's "Direct access from run and script rules", the reference's direct
  access section and the direct-access patterns are written around plain pyfdb, eccodes
  and earthkit-data; the `api` module is documented as an optional convenience for
  Snakefiles and for pre-checked archives.

### Removed

- `api.open`, `api.retrieve` and `api.earthkit`. They encouraged plugin imports inside
  jobs and are one call of the libraries themselves: `pyfdb.FDB().retrieve(request)`,
  a retrieval into a file where a job really needs one, and
  `earthkit.data.from_source("fdb", request)`. `api.request(query)` gives the request
  for all three.

## [0.3.0] - 2026-09-16

Direct FDB access from jobs, field-based rerun decisions, a tested patterns guide and
verification with Snakemake's database provenance backend.

### Added

- Direct FDB access from `run:` and `script:` rules, without any local GRIB file: mark an
  input `storage.fdb(query, retrieve=False)` and read its fields with the new `api`
  module (`api.messages`, `api.open`, `api.request`, `api.retrieve`, `api.earthkit`), one
  message at a time. `api.archive(output, messages)` archives a rule's fields straight
  into FDB, running the same checks as a file-based store, and leaves a small archive
  marker at the output's local path that the store step recognises. See the user guide's
  "Direct access from run and script rules" and the new patterns.
- A provider with a `config` setting now exports it as `FDB_CONFIG_FILE` (a file) or
  `FDB_CONFIG` (inline YAML), so plain `pyfdb.FDB()` and earthkit's `fdb` source find the
  same FDB inside a job. Nothing is exported when the environment already names an FDB
  configuration or when providers of one process disagree.
- `docs/patterns.md`, a usage-patterns guide with complete workflows for query shapes,
  rule directives, writing outputs, reruns and site setup; every example in it is
  executed by `tests/test_patterns.py`.
- Support for both of Snakemake's provenance backends: the end-to-end tests now run with
  the file backend and with `--persistence-backend db` (SQLite), covering the metadata
  of FDB outputs, `--summary` and every rerun decision. Note that the `db` backend keys
  records by the absolute workdir path, so a copied or moved workflow directory loses
  its provenance.

### Changed

- `archive_mode`, `identifier_check` and `canonical_spelling` are now also read from
  `SNAKEMAKE_STORAGE_FDB_ARCHIVE_MODE`, `..._IDENTIFIER_CHECK` and
  `..._CANONICAL_SPELLING`, and Snakemake carries them into every job, so a direct
  archive from a `run:` or `script:` rule uses the workflow's archive mode.
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

[Unreleased]: https://github.com/MeteoSwiss/snakemake-storage-plugin-fdb/compare/v0.3.1...HEAD
[0.3.1]: https://github.com/MeteoSwiss/snakemake-storage-plugin-fdb/compare/v0.3.0...v0.3.1
[0.3.0]: https://github.com/MeteoSwiss/snakemake-storage-plugin-fdb/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/MeteoSwiss/snakemake-storage-plugin-fdb/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/MeteoSwiss/snakemake-storage-plugin-fdb/releases/tag/v0.1.0
