# snakemake-storage-plugin-fdb — Requirements

Status: living document, kept consistent with the code and with
[`architecture.md`](architecture.md) (see [`contributing.md`](../contributing.md)).
Structure loosely follows ISO/IEC/IEEE 29148. The plugin is pre-1.0 (version 0.2.0, not
on PyPI).

## 1. Introduction

### 1.1 Purpose

This document states what the Snakemake storage plugin for ECMWF's Fields DataBase
(FDB) must do, why, and how each requirement is verified. How the plugin does it is in
[`architecture.md`](architecture.md); how to use it is in the
[user guide](../user-guide.md) and the [reference](../reference.md).

### 1.2 Scope

The product is the Python package `snakemake_storage_plugin_fdb` (distribution
`snakemake-storage-plugin-fdb`), registered with Snakemake as the storage provider
`fdb`. It:

- retrieves rule inputs from FDB into one local GRIB file per query;
- archives rule outputs (GRIB files) into FDB;
- answers Snakemake's existence, modification-time, size and inventory questions;
- lists candidates for `glob_wildcards`.

Also in scope, outside the package: the development script `scripts/init_dev_fdb.py`,
the examples under `examples/`, the test suites under `tests/`, the CI workflow and the
MeteoSwiss site material (`examples/meteoswiss/`, `docs/sites/meteoswiss.md`).

Out of scope: see §6.1.

### 1.3 Stakeholders and users

| stakeholder | interest |
|---|---|
| Workflow authors (ECMWF-style and site FDBs) | read and write GRIB fields from Snakemake rules with MARS-style queries |
| Site integrators (e.g. MeteoSwiss) | point the plugin at a site FDB, eccodes definitions and MARS language without changing plugin code |
| Maintainers and contributors | a small, testable, site-neutral code base whose behaviour is fully specified |
| Snakemake core | a plugin that conforms to `snakemake-interface-storage-plugins` |
| FDB operators | no destructive operations on shared databases |

### 1.4 Context

Snakemake calls the plugin through `snakemake-interface-storage-plugins` 4.x. The plugin
talks to FDB through `pyfdb` (which bundles the FDB, metkit, eckit and eccodes native
libraries) and decodes GRIB with the `eccodes` Python bindings. FDB location, schema,
eccodes definitions and the MARS language are supplied by the user (settings, profiles
or environment variables). See [`architecture.md`](architecture.md) §3 for the context
diagram.

### 1.5 Definitions

| term | meaning |
|---|---|
| FDB | ECMWF's Fields DataBase: a key-value store of meteorological fields (GRIB messages) addressed by MARS keys |
| MARS request | a set of `key=value` pairs describing fields; values may be lists (`a/b/c`) or ranges (`a/to/b/by/c`) |
| metkit | ECMWF library that validates and expands MARS requests using a MARS *language* (`language.yaml`) |
| eccodes | ECMWF GRIB library; its *definitions* map GRIB headers to keys, including the `mars` namespace |
| schema | FDB schema file: rules listing the keys under which fields are archived; `key?` optional, `key?default` optional with default, `key-` removed |
| query | the plugin's string form of a MARS request: `fdb://key=value,...` |
| pattern | a query containing Snakemake wildcards such as `{date}` |
| field | one GRIB message stored in FDB under one key combination |
| expected field count `E` | product over keys of the number of distinct values in the expanded request |
| canonical spelling | the value spelling FDB lists back (numeric paramIds, lower-case enums, `YYYYMMDD` dates, `HHMM` times, 4-character expver) |
| canonical key order | the key order a provider uses for normalised queries and local paths |
| local copy | the file under `.snakemake/storage/<provider or tag>/` that Snakemake reads or writes for a query |
| provider, tag | one configured instance of the plugin; `storage <tag>:` creates a tagged instance with its own settings |
| storage object | the plugin's object for one query, created by the provider |
| masking | FDB keeps the newest archive of a key combination visible; older copies stay on disk until `fdb purge` |
| native / identifier archive | `archive(bytes)` (FDB derives the keys) vs `archive(bytes, identifier)` (the plugin supplies the keys) |
| site | an organisation with its own GRIB conventions, definitions, schema or MARS language (e.g. MeteoSwiss) |
| OGD | MeteoSwiss Open Government Data, source of the committed MeteoSwiss samples |

### 1.6 Conventions

- IDs (`FR-<AREA>-NNN`, `NFR-<AREA>-NNN`, `L-*`, `D-*`, and `ADR-*`/`R-*`/`TD-*` in the
  architecture) never change: they are not renumbered or reused, and gaps are fine. New
  entries get the next free number (decided 2026-09-15: least effort for developers,
  references never go stale).
- Evidence labels: **[verified: how]** means checked by reading the named source or by
  running it; **[assumed]** means not verified. Verified external facts are collected in
  [`architecture.md`](architecture.md) §13.
- Verification methods: **test** (pytest node names, relative to the repository root),
  **CI** (a job in `.github/workflows/ci.yml`), **inspection** (code or document
  review), **demonstration** (a documented command run by hand). Tests under
  `tests/sites/meteoswiss/` run only with the site prerequisites (see
  [`contributing.md`](../contributing.md#site-suite)).
- Error and log messages are quoted here in short form, with `...` for the rest. The
  exact texts are in the [reference](../reference.md#errors-and-messages).

---

## 2. Functional requirements

### 2.1 Query language

#### FR-QUERY-001 Query syntax

A query is `fdb://` followed by one or more `key=value` pairs separated by `,`. The
scheme is case-sensitive and must be the first characters (no leading whitespace). Keys
match `[A-Za-z][A-Za-z0-9_]*`. A value is one or more items separated by `/`; an item
is a non-empty sequence of letters, digits, `.`, `-`, `:`, `_` and Snakemake wildcard
tokens. Whitespace is allowed around keys, `=` and `,`. The full grammar is in the
[reference](../reference.md#query-grammar).

- Rationale: a MARS request written in one line, readable in Snakefiles, that survives
  Snakemake's path handling.
- Verification: test `tests/test_query.py::test_valid`, `::test_invalid`,
  `::test_table_size`; `tests/test_plugin.py::test_is_valid_query_accepts`.

#### FR-QUERY-002 Multi-field queries

One query may address several fields through `/` value lists and `to`/`by` ranges, and
maps to exactly one local file holding all matching GRIB messages, in request order.

- Rationale: rules usually need a set of steps, parameters or members as one input.
- Verification: test `tests/test_backend.py::test_inspect_2x2`,
  `::test_inspect_aliases_and_missing`,
  `tests/test_plugin.py::test_retrieve_request_order_and_size`.

#### FR-QUERY-003 Wildcards

Snakemake wildcard tokens (`{name}` or `{name,constraint}`, matched with the interface's
`WILDCARD_REGEX`) are atomic: a constraint may contain `,`, `/`, `=` and braces
(`{date,\d{8}}`). Wildcards are allowed only inside values, never in keys.

- Rationale: patterns must work with Snakemake's wildcard machinery unchanged.
- Verification: test `tests/test_query.py::test_normalize_keeps_wildcard_text_verbatim`,
  `::test_items_split_outside_wildcards_only`, `::test_invalid`.

#### FR-QUERY-004 No escaping

`,`, `=`, `/`, `{`, `}`, `+`, `%` and whitespace are reserved and cannot occur in an
item outside a wildcard token; there is no escape mechanism. Snakemake's escaped braces
`{{`/`}}` are rejected like any other brace.

- Rationale: MARS values never need these characters (metkit rejects them), and an escape
  mechanism would have to survive Snakemake's path handling.
- Verification: test `tests/test_query.py::test_invalid`.

#### FR-QUERY-005 Query errors

An invalid query raises `query.QueryError` (a `ValueError`) whose message names the
problem: missing scheme, empty query, duplicate key (after lower-casing), empty key or
value, missing `=`, empty pair, empty item (`/` misplaced), trailing comma, wildcard in a
key, invalid key, invalid character (named; whitespace reported as `whitespace`).

- Rationale: users must see what is wrong without reading the grammar.
- Verification: test `tests/test_query.py::test_invalid`.

#### FR-QUERY-006 Query validation without native libraries

`StorageProvider.is_valid_query` accepts exactly the queries of FR-QUERY-001 and rejects
any other scheme or form (`s3://…`, plain paths, `fdb://class=od/expver=0001` from
`--default-storage-prefix`). It must not import `pyfdb`, `eccodes` or metkit.

- Rationale: Snakemake calls it on every installed plugin for every `storage(...)` query
  (architecture.md §13.8); two positive answers are a hard error.
- Verification: test `tests/test_plugin.py::test_is_valid_query_rejects`,
  `::test_valid_query_and_postprocess_load_no_native_library`,
  `tests/test_query.py::test_no_fdb_libraries_imported`.

#### FR-QUERY-007 Syntactic normalisation

`postprocess_query` strips whitespace outside wildcard tokens (wildcard tokens are kept
verbatim), lower-cases keys and sorts them into the provider's canonical key order
(FR-QUERY-008). Values are kept verbatim (case, aliases, list order). Normalisation is
idempotent, does not import native libraries, and returns an invalid query unchanged
(the storage object reports the error on use, FR-ERR-003).

- Rationale: cheap, deterministic DAG building that is identical on machines without the
  C++ libraries (ADR-002 in architecture.md §9).
- Verification: test `tests/test_query.py::test_normalize`, `::test_normalize_idempotent`,
  `::test_normalize_with_order`,
  `tests/test_plugin.py::test_provider_settings_postprocess_invalid_unchanged`.

#### FR-QUERY-008 Canonical key order

The key order of a provider is, in precedence: (1) the `key_order` setting; (2) the key
order of the FDB schema named by the resolved FDB configuration; (3) the generic order
`class, expver, stream, domain, date, time, type, levtype, levelist, step, number, param`.
Keys not in the chosen list follow alphabetically. The order depends only on key names.

- Rationale: normalised queries and local paths must be stable under wildcard
  substitution and must not need site knowledge in the package.
- Verification: test `tests/test_key_order.py` (all tests),
  `tests/test_plugin.py::test_provider_settings_schema_key_order`,
  `::test_provider_settings_key_order_setting`,
  `::test_provider_settings_generic_order_without_schema`,
  `tests/sites/meteoswiss/test_conventions.py::test_key_order_from_site_schema`.

#### FR-QUERY-009 Schema key order

Schema keys are taken in order of first appearance across all rules (text inside
`[...]`), ignoring the decorations `?`, `?default`, `-`, `=values` and `:Type`.
Removed keys (`key-`) keep their position. Declarations outside brackets and `#`
comments (to end of line, anywhere) are skipped; `--` is not a comment. A schema with no
rule keys is an error.

- Rationale: mirrors how fdb5's schema parser reads the file (architecture.md §8.1).
- Verification: test `tests/test_key_order.py::test_from_schema_first_appearance`,
  `::test_from_schema_skips_hash_comments`, `::test_from_ecmwf_fdb_test_schema`,
  `::test_from_schema_user_keys`, `::test_from_pyfdb_schema`,
  `::test_from_schema_without_rules`, `tests/test_backend.py::test_parse_schema_test_schema`,
  `::test_parse_schema_decorations`.

#### FR-QUERY-010 Schema resolution without pyfdb

The plugin finds the schema file the way FDB would, in pure Python: config = the
`config` setting, else `FDB_CONFIG`/`FDB5_CONFIG` (YAML text), else
`FDB_CONFIG_FILE`/`FDB5_CONFIG_FILE`, else `$FDB_HOME/etc/fdb/config.{yaml,json}`;
schema = the config's `schema`, else `FDB_SCHEMA_FILE`, else `~fdb/etc/fdb/schema` with
`~fdb` expanded from the config's `fdb_home` or `$FDB_HOME`. An unresolvable or
non-existent schema means "no schema knowledge" (generic order).

- Rationale: key order is needed at DAG build time, before native libraries load.
- Verification: test `tests/test_backend.py::test_resolve_schema_path_explicit_config`,
  `::test_resolve_schema_path_env_fallbacks`, `::test_resolve_schema_path_unresolvable`,
  `tests/test_plugin.py::test_provider_settings_env_fallback_config`.

#### FR-QUERY-011 Expected field count

The expected field count `E` of a query is the product over keys of the number of
distinct values in metkit's expansion of the request (aliases resolved, ranges
expanded). If metkit's expansion is unavailable, a pure-Python fallback splits `/`
lists and expands integer and `YYYYMMDD` `to`/`by` ranges without alias resolution, so
aliases of one field (`param=2t/167`) count 1 with metkit and 2 with the fallback. An
invalid request is an error, not a fallback case.

- Rationale: `exists`, `retrieve` and `store` compare found or stored fields with `E`.
- Verification: test `tests/test_backend.py::test_expand`, `::test_expected_count`,
  `::test_expansion_fallback`, `::test_fallback_expand`.

### 2.2 Local path mapping

#### FR-PATH-001 Local suffix

The local path of a query is one directory per `key=value` pair in canonical key order,
with `/` in values replaced by `+` (outside wildcard tokens) and `.grib` appended to the
last component, e.g.
`class=od/expver=0001/stream=oper/date={date}/time=0000/type=fc/levtype=sfc/step=0+6+12/param=167.grib`.
Values are used verbatim, so a relative date (`date=-1`) gives the path `date=-1` for an
object whose fields change daily.

- Rationale: human-readable local copies that Snakemake's wildcard matching can use.
- Verification: test `tests/test_query.py::test_local_suffix_example`,
  `::test_local_suffix_keeps_wildcard_text`,
  `tests/test_plugin.py::test_storage_object_local_suffix`.

#### FR-PATH-002 Commutation with wildcard substitution

`local_suffix(apply_wildcards(q, w)) == apply_wildcards(local_suffix(q), w)` for
wildcard values that are single MARS values (no reserved characters).

- Rationale: Snakemake builds wildcard regexes from the local path of the pattern and
  constructs job objects from the substituted query without re-normalising
  (architecture.md §13.8).
- Verification: test `tests/test_query.py::test_local_suffix_commutes_with_apply_wildcards`,
  `::test_list_wildcard_value_does_not_commute`.

#### FR-PATH-003 Path component limit

A constant component longer than 255 bytes (UTF-8, the last one including `.grib`) is
replaced by `key=~<first 24 hex chars of sha256(value)>` (plus `.grib` for the last).
A long component containing a wildcard, or a hashed component that is still too long,
is an error.

- Rationale: NAME_MAX of ext4, xfs and Lustre; `~` cannot occur in a value, so hashed
  components never collide with literal ones.
- Verification: test `tests/test_query.py::test_local_suffix_hashes_long_constant`,
  `::test_local_suffix_hashes_long_last_component`,
  `::test_local_suffix_component_limit_boundary`,
  `::test_local_suffix_long_wildcard_component_is_error`.

#### FR-PATH-004 Over-long substituted wildcard guard

The provider records every query it returned from `postprocess_query`. A storage object
whose query was not recorded (built by Snakemake from `apply_wildcards`) and that has a
component over the limit raises `local path component for key '<key>' of <query> is
<n> bytes after wildcard substitution (limit 255); ...` at construction.

- Rationale: hashing after substitution would break FR-PATH-002. Constant queries
  rebuilt by Snakemake keep their text and stay recorded, including in spawned job
  processes, which re-parse the Snakefile (architecture.md §13.8).
- Verification: test `tests/test_plugin.py::test_wildcard_guard_long_substituted_value`,
  `::test_wildcard_guard_constant_long_list_is_hashed`,
  `::test_wildcard_guard_unrecorded_short_query_accepted`.

#### FR-PATH-005 Derived state follows the current query

The parsed query and local suffix of a storage object follow its current `query`
attribute, not the value at construction.

- Rationale: Snakemake copies storage objects and rewrites `query` to inject wildcard
  constraints without calling `__post_init__` (architecture.md §13.8).
- Verification: test `tests/test_plugin.py::test_storage_object_follows_query_rewrite`.

#### FR-PATH-006 Separate prefixes per provider

Local copies of an untagged provider live under `.snakemake/storage/fdb/`, those of a
tagged provider under `.snakemake/storage/<tag>/`, so the same query against two FDBs
never collides locally. The query itself does not name the FDB.

- Rationale: several FDBs in one workflow.
- Verification: test `tests/test_workflow.py::test_workflow_run_stores_and_cleans_local_copies`
  (`fdb/`), `tests/sites/meteoswiss/test_workflow.py::test_workflow_retrieves_control_field`
  (`mch/`).

### 2.3 Configuration

#### FR-CONF-001 Settings

The provider exposes the settings `config`, `user_config`, `archive_mode`,
`identifier_check`, `canonical_spelling`, `remove_policy`,
`input_tracking`, `glob_required_keys`, `eccodes_definitions`, `metkit_home`,
`key_order` and `env`, all
optional strings with the defaults of the [reference](../reference.md#settings), as
`--storage-fdb-<name>` CLI flags, profile keys and `storage` directive arguments,
taggable with `TAG::VALUE`, some also as environment variables (FR-CONF-008). A setting
given as `None` means its default. Every help text names the setting's default.

- Rationale: every site difference is expressible through generic settings
  (NFR-NEUTRAL-002); `snakemake --help` is where defaults are looked up.
- Verification: test `tests/test_settings.py::test_settings_fields`,
  `::test_settings_defaults_construct`, `::test_settings_none_means_default`,
  `::test_settings_help_names_the_default`;
  demonstration `uv run snakemake --help` lists every flag.

#### FR-CONF-002 FDB configuration forms

`config` and `user_config` accept a path to an existing file (made absolute at provider
construction) or inline YAML/JSON text that parses as a mapping. Anything else raises
`FDB configuration error: '<value>' is neither an existing file nor ...`. Without
`config`, FDB's own environment fallback applies.

- Rationale: pyfdb silently falls back to its default configuration when given a path
  string it cannot parse as YAML (architecture.md §13.7).
- Verification: test `tests/test_backend.py::test_resolve_config_forms`,
  `::test_resolve_config_errors`, `::test_config_forms`,
  `tests/test_plugin.py::test_provider_settings_config_file_made_absolute`.

#### FR-CONF-003 Choice settings

`archive_mode`, `identifier_check`, `canonical_spelling`,
`remove_policy` and `input_tracking` accept exactly their lower-case values; any other
value raises
`invalid <name> '<value>' (allowed: ...)` at provider construction.
`glob_required_keys` and `key_order` must contain valid key names.

- Rationale: fail at startup, not in the middle of a workflow.
- Verification: test `tests/test_settings.py::test_settings_invalid_choice`,
  `::test_settings_other_choices`, `::test_settings_invalid_values`.

#### FR-CONF-004 Reserved identifier check

`identifier_check` accepts `none`. `strict` is reserved and raises
`identifier_check=strict is reserved and not implemented in this version` at provider
construction. The guard hook (FR-STORE-008) exists so the check can be added later.

- Rationale: deferred work D-001 (§6.2) without a settings change.
- Verification: test `tests/test_settings.py::test_identifier_check_strict_is_reserved`,
  `::test_guard_hook`, `::test_guard_identifier_mismatch`.

#### FR-CONF-005 No FDB access at construction

Creating a provider opens no FDB handle. A missing schema file or FDB root surfaces as
`FDB configuration error: <detail>` at the first I/O.

- Rationale: `snakemake --help`, dry runs and DAG building must not need a reachable FDB.
- Verification: test `tests/test_plugin.py::test_provider_settings_unreachable_fdb_raises_at_first_use`,
  `tests/test_backend.py::test_config_errors_at_first_use`.

#### FR-CONF-006 Unreadable schema

A resolvable schema file that cannot be read or has no rule keys raises
`FDB configuration error: schema <path>: <detail>` at provider construction.

- Rationale: a broken schema would otherwise give silently wrong key orders.
- Verification: test `tests/test_plugin.py::test_provider_settings_schema_without_rules`.

#### FR-CONF-007 Missing bindings

If `pyfdb` or `eccodes` cannot be imported at provider construction, the provider raises
`cannot import the FDB/eccodes bindings: <error>`.

- Rationale: a clear message instead of an import traceback.
- Verification: inspection (`StorageProvider.__post_init__`).

#### FR-CONF-008 Settings from environment variables

`config`, `user_config`, `eccodes_definitions`, `metkit_home`, `key_order`, `env` and
`glob_required_keys` are also read from `SNAKEMAKE_STORAGE_FDB_<NAME>` (upper case), the
interface's environment-variable mechanism: the CLI flag wins, then the variable, then
the default. The variable holds one value and may be tagged (`TAG::VALUE`). The choice
settings are deliberately excluded: they express workflow policy, not site setup.

- Rationale: site setup belongs in the environment of a site's login profile or
  container, next to `ECCODES_DEFINITION_PATH` and `METKIT_HOME`, not in every command
  line (NFR-NEUTRAL-002).
- Verification: test `tests/test_settings.py::test_settings_fields`,
  `tests/test_workflow.py::test_workflow_config_from_environment_variable`.

### 2.4 Process environment

#### FR-ENV-001 eccodes definitions

`eccodes_definitions` is a colon-separated list of existing directories (made absolute;
empty entries skipped; entries starting with `/MEMFS/` passed as is), prepended in order
to `ECCODES_DEFINITION_PATH`; existing entries are never removed. If the variable
already equals or starts with the same directories, nothing is prepended. A
non-directory entry raises `eccodes_definitions: '<entry>' is not an existing directory`.

- Rationale: site definitions without plugin code; idempotent for a second provider, the
  untagged twin of a tagged provider and spawned jobs inheriting the variable.
- Verification: test `tests/test_settings.py::test_settings_eccodes_definitions_prepend`,
  `::test_settings_eccodes_definitions_unset_env`,
  `::test_settings_eccodes_definitions_relative`,
  `::test_settings_eccodes_definitions_missing_dir`.

#### FR-ENV-002 MARS language

`metkit_home` is exported, made absolute, as `METKIT_HOME` and wins over an existing
value (logged at info level). Whatever its source (`metkit_home`, `env` or the ambient
environment), an effective `METKIT_HOME` without `share/metkit/language.yaml` raises
`METKIT_HOME=<dir> (from <source>) has no share/metkit/language.yaml; ...`.

- Rationale: sites add MARS values (e.g. `model`) through a language override; a
  `METKIT_HOME` without `language.yaml` hangs the process (architecture.md §13.7).
- Verification: test `tests/test_settings.py::test_settings_metkit_home_wins`,
  `::test_settings_metkit_home_without_language`,
  `::test_settings_invalid_metkit_home_from_env`.

#### FR-ENV-003 FDB environment untouched

The plugin never sets or clears `FDB_CONFIG`, `FDB5_CONFIG`, `FDB_CONFIG_FILE`,
`FDB5_CONFIG_FILE`, `FDB_HOME` or `FDB_SCHEMA_FILE` (except through `env`). Without the
two dedicated settings, `ECCODES_DEFINITION_PATH` and `METKIT_HOME` are left as they
are.

- Rationale: explicit `config` takes precedence inside FDB; the ambient setup of other
  tools must keep working.
- Verification: test `tests/test_settings.py::test_settings_config_leaves_fdb_env_untouched`,
  `::test_settings_no_env_settings_leave_env_untouched`.

#### FR-ENV-004 Explicit environment overrides

`env` is `NAME=VALUE[,NAME=VALUE]`: names match `[A-Za-z_][A-Za-z0-9_]*`, values may
contain `=` but not `,`; a missing `=`, an invalid name or a duplicate name is an error.
The values are exported, overriding the environment, and `eccodes_definitions` and
`metkit_home` apply on top of them.

- Rationale: a generic escape hatch so no site needs plugin code.
- Verification: test `tests/test_settings.py::test_settings_env_overrides`,
  `::test_settings_env_syntax`, `::test_settings_env_setting_then_dedicated_settings`.

#### FR-ENV-005 Silent eckit user errors

`ECKIT_EXCEPTION_IS_SILENT=1` is set when it is unset (after `env`).

- Rationale: eckit otherwise dumps every `UserError` to stderr.
- Verification: test `tests/test_settings.py::test_settings_eckit_silent_default`.

#### FR-ENV-006 Validate, then export before loading libraries

All environment values are validated and computed before anything is exported, so a
provider that fails construction leaves the environment unchanged. They are exported
before the provider imports `pyfdb`/`eccodes`. Providers in one process that apply
different `eccodes_definitions` or `metkit_home` values log a warning; the last one wins.

- Rationale: definitions and language are read when the libraries load
  (architecture.md §8.3); all providers share one process environment.
- Verification: test `tests/test_settings.py::test_settings_different_definitions_warn`,
  `::test_settings_metkit_home_without_language`.

### 2.5 Reading

#### FR-READ-001 Existence means complete

`exists()` is true only if `E > 0` and FDB holds exactly `E` fields for the query
(one `inspect`). Keys omitted from the query are not wildcards for this check (FDB
matches keys exactly). A field counts only if its key contains every query key FDB keeps
in field keys (a schema rule key not marked `key-`; none is required if the schema is
not readable, L-15): `inspect` matches through keys the indexed fields do not have
(L-22), and such fields are not what the query names. The values of the keys a field
carries are matched by FDB itself (architecture.md §13.4). The same filter applies to
`mtime()`, `size()`, `retrieve_object()` and the store post-check.

- Rationale: a partially present input is not usable; reporting it missing makes
  Snakemake run the producer or fail with "missing input" (ADR-005). Without the key
  check ten per-quantile queries all exist and retrieve the same quantile-less fields
  (ADR-032).
- Verification: test `tests/test_plugin.py::test_exists_size_checksum_complete`,
  `::test_exists_does_not_match_through_absent_key`, `::test_no_schema_requires_no_key`,
  `::test_exists_partial_retrieve_names_missing`,
  `::test_exists_missing_optional_key_and_mtime_not_found`,
  `tests/sites/meteoswiss/test_read.py::test_read_samples`.

#### FR-READ-002 Invalid requests are errors

A request metkit rejects (e.g. `class=zz`, `number` with `type=cf`) raises
`Invalid MARS request <query>: <detail>` from every read method instead of reporting the
object missing. It is raised before any FDB I/O.

- Rationale: typos must not look like missing data.
- Verification: test `tests/test_plugin.py::test_exists_invalid_request_raises`,
  `tests/sites/meteoswiss/test_read.py::test_number_context`.

#### FR-READ-003 Unresolved wildcards

Read and store methods on a query that still contains wildcards raise
`FDB query <query> has unresolved wildcards`.

- Rationale: only concrete requests can be sent to FDB.
- Verification: test `tests/test_plugin.py::test_exists_wildcard_query_rejected`,
  `::test_store_wildcard_query_rejected`.

#### FR-READ-004 Modification time

`mtime()` is the latest index timestamp of the found fields, in POSIX seconds. A field
with timestamp 0 uses `os.stat` of its data file; without a local data file its time is
0 and a warning is logged once per object. No fields raises `FileNotFoundError`.

- Rationale: index timestamps behave like per-field modification times
  (architecture.md §13.3).
- Verification: test `tests/test_plugin.py::test_mtime_is_flush_time`,
  `::test_mtime_timestamp_fallback_os_stat`,
  `::test_exists_missing_optional_key_and_mtime_not_found`.

#### FR-READ-005 Size

`size()` and `local_footprint()` are the sum of the found fields' message lengths, which
equals the retrieved byte count. For NUL-padded GRIB1 input this is smaller than the
stored local file.

- Rationale: Snakemake uses size only for non-empty checks, the checksum threshold and
  input-size resources, never to compare storage against the local file
  (architecture.md §13.8).
- Verification: test `tests/test_plugin.py::test_exists_size_checksum_complete`,
  `::test_store_template`.

#### FR-READ-006 No storage checksum

`checksum()` returns `None`; nothing is written to Snakemake's checksum cache.

- Rationale: Snakemake then hashes the local copy; a pseudo-checksum would cause spurious
  reruns (ADR-006).
- Verification: test `tests/test_plugin.py::test_exists_size_checksum_complete`,
  `::test_inventory_fills_cache_with_one_inspect`.

#### FR-READ-007 Atomic retrieval

`retrieve_object()` requires a complete object (FR-READ-001), streams FDB's `retrieve`
into `<local path>.part`, fsyncs and renames it over the local path. If the byte count
differs from the sum of field lengths, or on any error, the part file is removed and the
local path is untouched. Messages keep FDB's request order.

- Rationale: interrupted or short retrievals must never look like valid inputs.
- Verification: test `tests/test_plugin.py::test_retrieve_request_order_and_size`,
  `tests/test_backend.py::test_retrieve_to`, `::test_retrieve_to_errors_leave_no_part`,
  `::test_retrieve_to_nothing_found`.

#### FR-READ-008 Missing-field report

The report is `<query>: <n> of <E> fields found in FDB; missing: <combinations>`,
listing at most 10 missing combinations and `(and <k> more)`. A combination names the
keys with several distinct values (all keys for a single-field query) in canonical key
order. If nothing was found and the schema is known, the report also lists the optional
schema keys the query does not name. It appears:

- as a `WorkflowError` from `retrieve_object()` on an incomplete object;
- as a warning from `exists()` and `inventory()`, once per query and process, when FDB
  holds some but not all fields (`0 < n < E`);
- as a debug message from `exists()` and `inventory()` when `n = 0`.

- Rationale: Snakemake calls `retrieve_object()` only for objects `exists()` reported
  as present, so a partial input would otherwise be a bare "missing input" naming the
  whole query. Nothing found is the normal state of an output not produced yet and must
  not warn, but its optional-schema-key hint (the usual cause: omitted `number`,
  `timespan`, `domain`) stays available with `--verbose`.
- Verification: test `tests/test_plugin.py::test_exists_partial_retrieve_names_missing`,
  `::test_exists_partial_warns_once`, `::test_exists_absent_object_does_not_warn`,
  `::test_exists_missing_optional_key_and_mtime_not_found`.

#### FR-READ-009 Inventory

`inventory()` performs one `inspect` per object and fills Snakemake's existence cache
and, if the object exists, its mtime and size caches for `cache_key()`. It does nothing
if the key is already cached. `get_inventory_parent()` returns `None`.

- Rationale: replaces three later calls; a DB-level listing can hold millions of fields
  and does not know the workflow's grouping (ADR-007).
- Verification: test `tests/test_plugin.py::test_inventory_fills_cache_with_one_inspect`.

#### FR-READ-010 Fresh answers

`exists`, `mtime`, `size` and `retrieve_object` query FDB on every call (not cached per
object) and see fields archived after an earlier read in the same process.

- Rationale: Snakemake deactivates its IO cache after DAG building and expects fresh
  answers, e.g. for an input stored by its producer; pyfdb handles that have read keep a
  stale catalogue (architecture.md §8.6).
- Verification: test `tests/test_backend.py::test_reads_see_archives_after_an_earlier_read`,
  `tests/test_plugin.py::test_store_masking_rerun`.

#### FR-READ-011 Retries of transient read failures

FDB `inspect`, `retrieve` and `list` calls are retried with the interface's retry policy
(3 attempts, exponential wait from 3 s); the last attempt's own exception is surfaced.
Only failures that may be transient are retried: a failure the error mapping
(FR-ERR-001, FR-ERR-004) classifies — invalid MARS request, not GRIB, GRIB keys against
the schema, configuration error, OS-level I/O error — is raised on the first attempt.
Errors raised by the plugin's own logic are not retried.

- Rationale: transient file-system and network hiccups on shared FDBs; a permanent
  error must not cost about 10 s per query in a dry run (ADR-033).
- Verification: test `tests/test_plugin.py::test_exists_retries_transient_inspect_error`,
  `::test_exists_retries_only_transient_errors`,
  `::test_glob_retries_transient_list_error`,
  `tests/test_backend.py::test_map_error_table`, `::test_is_transient_unmapped`.

### 2.6 Writing

#### FR-STORE-001 GRIB structure

`store_object()` splits the local file into GRIB messages. NUL bytes between and after
messages (GRIB1 record padding) are allowed and not archived. Any other byte outside a
message, a truncated message or a file without messages fails before archiving.

- Rationale: FDB silently ignores trailing garbage after a valid message
  (architecture.md §13.5).
- Verification: test `tests/test_plugin.py::test_store_strict_rejects`,
  `::test_store_template`, `tests/test_grib.py::test_trailing_garbage`,
  `::test_split_errors`, `::test_nul_padding_between_and_after_messages`.

#### FR-STORE-002 Field count before archiving

With `n` messages and `E` expected fields, `n != E` raises `<query>: <local> has
<n> fields, the query expands to <E>; nothing was archived`. There is no lenient mode:
a store providing fewer fields than its query promises can never satisfy `exists()`
(FR-READ-001), so the job fails afterwards anyway (ADR-032).

- Rationale: an output must provide what its query promises.
- Verification: test `tests/test_plugin.py::test_store_strict_rejects`,
  `tests/sites/meteoswiss/test_write.py::test_write_strict_rejects_foreign_member`.

#### FR-STORE-003 Native archive mode (default)

With `archive_mode=native` the messages are archived in one `archive(bytes)` call; FDB
derives the keys and picks the schema rule. Before that call every message's MARS keys
(`param` from `paramId`) are checked against the query (ADR-032):

- the values must pass the pre-check (FR-STORE-005);
- every query key FDB keeps in the field key (a schema rule key not marked `key-`; none
  if the schema is not readable, L-15) must be present in the message, else `<query>:
  message <i> of <local> lacks <key>, which native archiving takes from the message; use
  archive_mode=identifier to label it, or drop the key from the query; nothing was
  archived` (L-23).

The guard (FR-STORE-008) is not consulted; the post-check (FR-STORE-009) verifies the
result.

- Rationale: works with multi-rule schemas, is pyfdb's recommended path and what
  MeteoSwiss uses in production (ADR-009). Unchecked, FDB archives every message under
  its own keys whatever the query says, which masks unrelated data (ADR-032).
- Verification: test `tests/test_plugin.py::test_store_roundtrip`,
  `::test_store_default_native_under_multi_rule_schema`,
  `::test_store_precheck_rejects_wrong_key`,
  `::test_store_native_rejects_key_absent_from_message`,
  `::test_no_schema_requires_no_key`,
  `tests/sites/meteoswiss/test_write.py::test_write_ctrl`.

#### FR-STORE-004 Identifier archive mode

With `archive_mode=identifier` the plugin builds each message's identifier. For each
schema key (merged over all rules; without a schema, query keys ∪ message keys),
skipping removed keys, the value is: the single literal query value (FR-STORE-006), else
the message's MARS value (`param` from `paramId`), else nothing for an optional key,
else the store fails with `<query>: cannot determine <key> for message <i> of <local>;
nothing was archived`. Message keys absent from the schema are dropped. A query key the
message does not carry labels the message unchecked (e.g. `quantile=1:10`).

- Rationale: stores GRIB that lacks a schema key (e.g. `template.grib` under
  `tests/data/pyfdb-tests.schema`); limited to schemas whose rules share one key set
  (§5, L-17).
- Verification: test `tests/test_plugin.py::test_store_template`,
  `::test_store_default_native_under_multi_rule_schema`,
  `::test_store_identifier_key_absent_from_message_takes_query_value`,
  `tests/sites/meteoswiss/test_write.py::test_write_ctrl`.

#### FR-STORE-005 Message pre-check

In both archive modes, for every constant query key (single- or multi-valued) that the
message carries, the message value must equal the query value or one of the listed
values after light normalisation: integers numerically, one- or two-digit `time` as
hours, `param` `N.T` as paramId, `date` only as `YYYYMMDD`, other strings
case-insensitively. Skipped are keys with `to`/`by`, keys whose items are not all
comparable and of one kind, and message values that are not comparable or of the other
kind (e.g. query `step=0` vs message `0m`, left to FDB and the post-check). A mismatch
raises `<query>: message <i> of <local> has <k>=<v>, but the query has <k>=<q>; nothing
was archived` (or `..., not one of <values>; ...` for a list).

- Rationale: FDB performs no consistency check between identifier and message, and in
  native mode it archives the message under its own keys (architecture.md §13.5,
  ADR-032); relabelling GRIB is not supported (ADR-010, ADR-012).
- Verification: test `tests/test_plugin.py::test_store_identifier_single_value_mismatch`,
  `::test_store_precheck_rejects_wrong_key`, `::test_store_strict_rejects`,
  `tests/test_query.py::test_comparable`,
  `tests/sites/meteoswiss/test_write.py::test_write_identifier_param_mismatch`.

#### FR-STORE-006 Canonical identifier values

Single-valued query values used in identifiers are archived in FDB's canonical spelling
from the object's metkit expansion (`param=167.128` → `167`, `time=0` → `0000`).
Without metkit expansion the value is used verbatim and a debug message is logged once
per object and query text.

- Rationale: FDB stores some spellings verbatim as a second key and rejects others
  (architecture.md §13.5, ADR-011).
- Verification: test `tests/test_plugin.py::test_store_identifier_archives_canonical_spelling`,
  `::test_store_identifier_verbatim_without_expansion`.

#### FR-STORE-007 Duplicates

Two messages with equal identifiers (identifier mode) or equal MARS namespace keys
(native mode) raise `<query>: <local> holds duplicate fields (messages <i> and <j>);
nothing was archived`.

- Rationale: one message would silently mask the other.
- Verification: test `tests/test_plugin.py::test_store_strict_rejects`.

#### FR-STORE-008 Identifier guard hook

In identifier mode, `provider.guard.check(message, identifier, parsed_query)` is called
for every message after its identifier is built and before the first `archive()`. An
`IdentifierMismatch` raises `<query>: identifier check failed for <local>: <mismatch>;
nothing was archived`, and FDB is unchanged. v1 installs `NoGuard`.

- Rationale: the reserved strict check (D-001) plugs in here.
- Verification: test `tests/test_plugin.py::test_store_guard_sees_every_message_before_archive`,
  `::test_store_guard_mismatch_leaves_fdb_unchanged`.

#### FR-STORE-009 Post-check

After archiving and flushing, one `inspect` of the query counts the fields whose time
is at or after `t_start`, taken from FDB's index clock just before the first
`archive()`. Fewer than `n` raises `<query>: <local> has <n> fields, the query expands
to <E>; <k> landed outside the query or are duplicates: message <i> (<keys>), ... (they
stay in FDB ...)`. Named are the messages whose key, on the query keys FDB indexes, no
fresh field has, at most three, each with the keys of its own MARS keys that contradict
the query plus the keys the query gives several values.

- Rationale: the pre-checks (FR-STORE-003, FR-STORE-005) cannot compare every key
  (`to`/`by` ranges, values that are not comparable), so a message may still land
  outside the query and the error must say which one and why.
  Taking `t_start` from `int(time.time())` would reject fresh fields
  stamped with the previous second (architecture.md §8.7, ADR-015).
- Verification: test `tests/test_plugin.py::test_store_post_check_uses_fdb_clock`,
  `::test_fdb_time_is_c_time`, `::test_store_post_check_names_offending_messages`.

#### FR-STORE-010 Partial archive failures

`store_object()` is never retried. If an `archive()` call fails after an earlier one
succeeded, the handle is flushed on a best-effort basis and the error says how many
archive calls succeeded and that their fields stay in FDB until the next successful
store masks them.

- Rationale: archives cannot be undone; users must know what is left behind.
- Verification: test `tests/test_plugin.py::test_store_partial_archive_failure_says_fields_stay`.

#### FR-STORE-011 Reruns mask

Storing the same query again archives new fields that mask the old ones: `exists()`
stays true, `mtime()` increases and retrieval returns the new bytes.

- Rationale: Snakemake's rerun semantics without deletion.
- Verification: test `tests/test_plugin.py::test_store_masking_rerun`.

#### FR-STORE-012 Concurrent stores

Stores from several threads in one process archive through one FDB handle per thread
and succeed concurrently.

- Rationale: Snakemake runs jobs in a thread pool; pyfdb handles must not be shared
  across threads (architecture.md §13.5).
- Verification: test `tests/test_plugin.py::test_store_threads`,
  `tests/test_backend.py::test_handle_per_thread`.

### 2.7 Removal

#### FR-REMOVE-001 Never delete

`remove()` never deletes data and never calls FDB `wipe` or `purge`.
`remove_policy=warn` (default) logs `FDB cannot delete individual fields; ...` once per
query per process; `ignore` does nothing; `error` raises `remove_policy=error: <that
text>`.

- Rationale: FDB has no per-field deletion and `wipe` can delete unrelated fields or the
  whole database (architecture.md §13.6, ADR-008). Consequence: `--delete-all-output`
  leaves the fields in FDB and, because the output still exists, does not make the
  producing job rerun. In Snakemake 9.27 it is the only trigger that reaches `remove()`:
  reruns and failed-job cleanup do not call it, and temporary outputs cannot exist
  (`temp()` and storage flags are mutually exclusive).
- Verification: test `tests/test_plugin.py::test_remove_policy`,
  `tests/sites/meteoswiss/test_write.py::test_write_remove_policy_warn`,
  `tests/test_workflow.py::test_workflow_delete_all_output_leaves_fields`.

### 2.8 Glob

#### FR-GLOB-001 Candidates

`list_candidate_matches()` performs one FDB `list` of the pattern's constant pairs
(keys the pattern omits act as wildcards) and returns, sorted and de-duplicated, the
pattern's normalised text with each wildcard-bearing value replaced by the listed
field's value. Keys absent from the pattern do not appear, so fields differing only
there give one candidate. A value mixing a wildcard with literal text
(`date={year}0101`) is replaced as a whole and matched by Snakemake's regex.

- Rationale: Snakemake matches candidates with the regex of the pattern
  (architecture.md §13.8).
- Verification: test `tests/test_plugin.py::test_glob_step_candidates_match_pattern`,
  `::test_glob_keys_absent_from_pattern_collapse`,
  `::test_glob_wildcard_inside_value_and_constant_list`,
  `tests/sites/meteoswiss/test_glob.py::test_glob_members`,
  `tests/test_workflow.py::test_workflow_glob_wildcards_steps`.

#### FR-GLOB-002 Fields without the wildcard key

Listed fields that lack a wildcard-bearing key or list it with an empty value are
skipped without error.

- Rationale: optional keys are absent or empty (`number` of control members).
- Verification: test `tests/test_plugin.py::test_glob_skips_fields_without_the_wildcard_key`,
  `tests/sites/meteoswiss/test_glob.py::test_glob_members_skip_control`.

#### FR-GLOB-003 Required constant keys

Every key in `glob_required_keys` (default `class`) must be constant in the pattern; a
required key that is a wildcard or absent raises `FDB glob pattern <query> needs
constant values for <keys> (glob_required_keys)` before any FDB I/O. An empty setting
allows any pattern.

- Rationale: prevents listing an entire FDB by accident.
- Verification: test `tests/test_plugin.py::test_glob_required_keys_enforced`,
  `::test_glob_required_keys_empty_allows_any_pattern`.

#### FR-GLOB-004 Glob errors and values

pyfdb errors are mapped (FR-ERR-001), e.g. `class=zz` gives "Invalid MARS request". No
canonical-spelling check runs. Values come from FDB and are therefore canonical, so
wildcard constraints must match that form.

- Rationale: consistent errors; FDB lists canonical spellings.
- Verification: test `tests/test_plugin.py::test_glob_invalid_value`,
  `tests/sites/meteoswiss/test_glob.py::test_glob_params_are_canonical_cosmo_ids`,
  `::test_glob_model_is_listed_lower_case`.

### 2.9 Canonical spelling

#### FR-SPELL-001 Runtime spelling check

On the first expansion of a storage object's query (first `exists`, `mtime`, `size`,
`retrieve`, `inventory` or `store`), literal values of keys without `to`/`by` or
wildcards are compared item by item with metkit's expansion. On a difference,
`canonical_spelling=warn` (default) logs `Query <query> uses non-canonical spelling:
<key>=<given> (canonical: <canonical>), ...` once per query per process; `error` raises
that text on every call; `ignore` does nothing. Local paths never change. Without
metkit expansion the check is skipped.

- Rationale: the same field written two ways gives two local paths (ADR-002).
- Verification: test `tests/test_plugin.py::test_canonical_spelling_warns_once`,
  `::test_canonical_spelling_error_raises`, `::test_canonical_spelling_silent`,
  `tests/test_backend.py::test_spelling_diffs`, `::test_expansion_fallback`,
  `tests/sites/meteoswiss/test_read.py::test_canonical_spelling_mch`.

### 2.10 Errors

#### FR-ERR-001 Error mapping

Known pyfdb failures (plain `RuntimeError`s) and GRIB errors are raised as Snakemake
`WorkflowError`s naming the query and, where relevant, the local file: invalid requests,
non-GRIB data, GRIB keys that do not match the schema, configuration errors and
OS-level I/O errors (FR-ERR-004). The mapping is in architecture.md §8.4. Other
exceptions propagate.

The pyfdb detail in the message is the first non-empty line without its
`UserError: `/`Serious bug: ` prefixes and without the ` (Success)` suffix eckit appends
when `errno` is 0, cut before metkit's `request=` dump or the first `;` and truncated to
200 characters with `…`; the full text is logged at debug level.

- Rationale: actionable messages instead of native backtraces; a metkit dump of the
  whole MARS vocabulary buries the plugin's sentence and any hint after it.
- Verification: test `tests/test_backend.py::test_map_error_table`,
  `::test_map_error_without_local_and_unknown`, `::test_map_error_detail_is_shortened`,
  `::test_archive_native_errors_map`,
  `::test_invalid_request`.

#### FR-ERR-002 Language hint

If a metkit `UserError` contains `cannot expand`, the "Invalid MARS request" message ends
with a hint to point `metkit_home` at a MARS language that defines the value.

- Rationale: unknown enum values (e.g. site `model` values) need a language override.
- Verification: test `tests/sites/meteoswiss/test_read.py::test_read_model_requires_metkit_home`.

#### FR-ERR-003 Invalid query on use

A storage object with an invalid query does not raise at construction; `parsed`,
`local_suffix()` and every I/O method raise `invalid FDB query <query>: <parser message>`.

- Rationale: Snakemake validates queries separately and may construct objects early.
- Verification: test `tests/test_plugin.py::test_storage_object_invalid_query_raises_on_use`.

#### FR-ERR-004 OS-level I/O failures

A pyfdb `RuntimeError` containing `Failed system call`, `Failed to mkdir`,
`Permission denied`, `No space left on device` or `Read-only file system` is raised as
`FDB I/O error for <query>: <detail> (check permissions, free space and the roots in the
FDB configuration)`.

When a lookup returns fewer fields than the query expands to, the plugin also checks
that every root of a local FDB configuration (`spaces[].roots[].path`) that exists is
readable, and raises `FDB I/O error for <query>: FDB root <path> is not readable (...)`
otherwise: FDB 5.23 answers a lookup under an unreadable root with no fields instead of
failing (architecture.md §13.7).

- Rationale: an unreadable or read-only FDB root escaped as a raw
  `RuntimeError: Failed system call: opendir (Success)`, whose "Success" is actively
  misleading; the class also tells the retry policy that the failure is permanent
  (FR-READ-011). Without the root check, an unreadable root under FDB 5.23 would look
  like missing data and a workflow could recompute and re-archive it.
- Verification: test `tests/test_backend.py::test_map_error_table`,
  `::test_local_roots`, `tests/test_plugin.py::test_exists_unreadable_root_is_an_io_error`,
  `::test_exists_unreadable_root_without_fdb_error`.

#### FR-ERR-005 Configuration hints

Configuration errors carry a hint where the cause is known:

- a `config`/`user_config` value that is neither an existing file nor a YAML mapping and
  has the shape `TAG:<existing file>` ends with
  `(looks like a tagged setting mangled by a spawned job, see the user guide on tagged
  settings)` (L-19);
- a configuration error naming the schema bundled with the pyfdb wheel
  (`.../fdb5lib/etc/fdb/schema`) ends with `(no FDB configuration was given: set
  --storage-fdb-config or FDB_CONFIG_FILE)`.

- Rationale: both failures name a path the user never wrote and give no clue what to do.
- Verification: test `tests/test_backend.py::test_resolve_config_tagged_setting_hint`,
  `::test_map_error_table`,
  `tests/test_workflow.py::test_workflow_without_any_configuration_hints`.

### 2.11 Reruns

#### FR-RERUN-001 Reruns of FDB inputs follow the lookup

Whether a rule with FDB inputs reruns follows from the FDB lookup alone: whether the
fields of its queries are there, and how their index timestamps compare with the outputs
(besides Snakemake's `params`, `code` and `software-env` triggers, which describe the
rule, not the input). Editing the query text — narrowing `param=167/165` to `param=167`,
widening it, reordering values, writing `0/to/12/by/6` for `0/6/12` — never triggers a
rerun by itself, and neither does adding or removing an FDB input. Inputs that are not
this plugin's keep Snakemake's input-set trigger.

- Rationale: a query names fields, it is not the state of the input; that state is in FDB
  (FR-READ-001, FR-READ-004). A query edit that selects the same or older fields must not
  invalidate outputs, and a widened query must rerun only if it names a field newer than
  the output. Snakemake's input-set trigger records the query text of storage inputs and
  has no per-rule or per-file opt-out (architecture.md §13.8), so the plugin removes its
  own inputs from that record (ADR-031, L-21).
- Verification: test `tests/test_rerun.py::test_rerun_narrowed_query_is_up_to_date`,
  `::test_rerun_widened_query_with_newer_field`,
  `::test_rerun_local_input_set_still_triggers`,
  `::test_install_hides_only_opted_out_inputs`.

#### FR-RERUN-002 `input_tracking` setting

`input_tracking` decides how FDB inputs take part in Snakemake's input-set rerun trigger:
`lookup` (default) hides them from it (FR-RERUN-001); `query` restores Snakemake's
default behaviour, in which a changed set of input queries reruns the job. The setting is
read when the provider is constructed; the first provider with `lookup` installs the
interim patch for the process, further providers reuse it, and a failed installation is
a warning, never an error (L-21). The setting applies per provider: the patch skips an
input only if its storage object's `tracks_input_changes` is false, which is the name and
meaning of the proposed upstream hook (D-011).

- Rationale: an escape hatch if the interim patch misbehaves, and a name for the
  behaviour in logs and documentation.
- Verification: test
  `tests/test_rerun.py::test_rerun_input_tracking_query_restores_the_trigger`,
  `::test_provider_setting_query_does_not_patch`, `::test_install_is_idempotent`,
  `::test_fallback_when_attribute_is_missing`, `::test_fallback_on_unexpected_signature`,
  `tests/test_settings.py::test_settings_fields`, `::test_settings_invalid_choice`.

### 2.12 Snakemake integration

#### FR-IFACE-001 Plugin surface

The plugin registers as `fdb`, is read-write (`StorageObjectRead`,
`StorageObjectWrite`), supports glob (`StorageObjectGlob`), is not a
`StorageObjectTouch`, handles files only and passes every `TestStorageBase` test of
`snakemake-interface-storage-plugins` 4.4.1.

- Rationale: conformance with the interface Snakemake expects.
- Verification: test `tests/test_plugin.py::test_interface_conformance`,
  `tests/test_plugin.py::TestStorageRead`, `tests/test_plugin.py::TestStorageWrite`.

#### FR-IFACE-002 Provider behaviour

`use_rate_limiter()` is false (`rate_limiter_key` `"fdb"`,
`default_max_requests_per_second` 10.0); `safe_print` is the identity; the `managed_*`
wrappers return what the plain methods return; `str(provider)` is `fdb`.

- Rationale: FDB access is local or cluster-internal; queries contain no secrets.
  Snakemake formats the provider object into user-facing text, so the default `repr`
  leaked `<snakemake_storage_plugin_fdb.StorageProvider object at 0x...>` into the
  catalogue URL of an invalid-query message (D-013).
- Verification: test `tests/test_plugin.py::test_provider_settings_rate_limiter_and_safe_print`,
  `::test_managed_wrappers_without_rate_limiter`, `::test_provider_str_is_the_plugin_name`.

#### FR-IFACE-003 Example queries

`example_queries()` returns generic MARS examples in canonical spelling that are valid
queries and trigger no spelling warning.

- Rationale: shown by Snakemake's plugin documentation; must not mention a site.
- Verification: test `tests/test_plugin.py::test_example_queries_valid`,
  `tests/test_plugin.py::TestStorageRead` (`test_example_queries`).

#### FR-IFACE-004 End-to-end workflow

`examples/ecmwf/` runs with `snakemake --storage-fdb-config <dev FDB config> -c1`:
it retrieves, stores (`Storing in storage: <query>`), removes local copies after the
run, reports "Nothing to be done" on a second run, supports `glob_wildcards` and leaves
fields on `--delete-all-output`.

- Rationale: proves the integration with real Snakemake runs, including spawned jobs.
- Verification: test `tests/test_workflow.py` (all tests); demonstration: the quick start
  in [`README.md`](../../README.md).

### 2.13 Site support

#### FR-SITE-001 MeteoSwiss end to end

ICON-CH1/CH2-EPS GRIB2 decoded with the COSMO definitions (`eccodes-cosmo-mars` branch
`varda-ext` + `eccodes-cosmo-resources`), archived under a varda-style schema and
requested with a MARS language that knows the `model` values, works through the read,
write, glob and workflow paths, configured only through generic settings or environment
variables.

- Rationale: first-class, tested goal; MeteoSwiss is the first site user.
- Verification: test `tests/sites/meteoswiss/` (all tests; CI job `site-meteoswiss`).

#### FR-SITE-002 Site setup through plain environment variables

The same site works with `ECCODES_DEFINITION_PATH` and `METKIT_HOME` set in the
environment and no site-related plugin settings.

- Rationale: sites that already export these variables need no plugin configuration.
- Verification: test `tests/sites/meteoswiss/test_read.py::test_read_samples` (`env`
  parametrisation).

### 2.14 Development tooling

#### FR-DEV-001 Development FDB

`scripts/init_dev_fdb.py [--root DIR] [--schema PATH] [--seed [DIR]] [--variants [FILE]]`
writes `<root>/schema`, `<root>/root/` and `<root>/config.yaml` (absolute paths), seeds
every GRIB file directly in DIR natively, and archives zeroed `stream=oper` variants of
FILE for steps 0/6/12 × params 167/165. Defaults: `--root .fdb`,
`--schema tests/data/schema`, `--seed tests/data/grib/ecmwf`,
`--variants tests/data/grib/ecmwf/template.grib`, relative to the repository; explicit
arguments are relative to the working directory. A missing seed directory is reported
and skipped; a missing variants file or schema is an argument error. It has no site
flags.

- Rationale: a small local FDB for examples, manual testing and site setups.
- Verification: test
  `tests/test_workflow.py::test_init_dev_fdb_seeds_samples_and_variants`,
  `tests/sites/meteoswiss/test_workflow.py::test_workflow_init_dev_fdb_site_command`.

#### FR-DEV-002 Site material outside the package

`examples/meteoswiss/` provides the schema, a tagged profile, a workflow, a definitions
setup script (`setup.sh`), a metkit home builder (`make_metkit_home.py`) and a sample
fetcher (`fetch_ogd_samples.py`); `docs/sites/meteoswiss.md` documents them.

- Rationale: site support without site mechanisms in the package (NFR-NEUTRAL-002).
- Verification: test `tests/sites/meteoswiss/test_workflow.py::test_workflow_example_profile_is_tagged`,
  `tests/sites/meteoswiss/test_fetch_ogd_samples.py::test_empty_data_reproduces_committed_samples`,
  `::test_fetch_live`.

---

## 3. Non-functional requirements

### 3.1 Site neutrality

#### NFR-NEUTRAL-001 No site names in package or scripts

`mch`, `meteoswiss`, `cosmo` and `icon-ch` (case-insensitive) appear nowhere under
`src/` or `scripts/`.

- Rationale: the package is generic FDB/MARS; the grep guards where site mechanisms
  live, not whether sites are supported (ADR-017).
- Verification: test `tests/test_no_site_specifics.py::test_no_site_specifics`; CI job
  `lint` (`grep`).

#### NFR-NEUTRAL-002 No site mechanisms in the package

The package contains no site keys or values, schema files, `language.yaml` patches,
definitions paths or aliases, site extras, or site special cases in ordering or
normalisation. Everything site-specific reaches the plugin through generic settings,
profiles or environment variables.

- Rationale: user principle; site material is shipped as examples and docs.
- Verification: inspection; test `tests/sites/meteoswiss/` exercises the package only
  through public settings and environment variables.

### 3.2 Performance

#### NFR-PERF-001 Lightweight import and DAG building

Importing the package, `is_valid_query` and `postprocess_query` load no FDB, metkit or
eccodes native library. `pyfdb` and `eccodes` are imported at provider construction,
FDB handles at first I/O.

- Rationale: `snakemake --help`, validation of every query on every plugin and DAG
  building stay fast and work without a configured FDB.
- Verification: test `tests/test_plugin.py::test_valid_query_and_postprocess_load_no_native_library`,
  `tests/test_query.py::test_no_fdb_libraries_imported`,
  `tests/test_grib.py::test_import_does_not_load_eccodes`,
  `tests/test_backend.py::test_import_and_helpers_do_not_load_pyfdb`.

#### NFR-PERF-002 Bounded FDB calls

Each `exists`, `mtime` or `size` call performs one `inspect`, `inventory` one per object,
glob one `list`, store one `inspect` after archiving; metkit expands a query once per
storage object. No database- or index-level listing is used for existence checks.

- Rationale: operational FDBs hold 10^5–10^6 fields per database.
- Verification: test `tests/test_plugin.py::test_inventory_fills_cache_with_one_inspect`;
  inspection.

#### NFR-PERF-003 Streaming retrieval

Retrieval streams in 8 MiB chunks; memory use does not grow with the retrieved size.
The store path has no such guarantee: it reads the output file whole and holds it
several times over (measured about four times the file, 205 MB → ~900 MB resident), so
archiving jobs need a `resources: mem_mb` to match.

- Rationale: multi-GB inputs. Fresh read handles, needed for FR-READ-010, cost about as
  much as reused ones (architecture.md §13.5).
- Verification: inspection (`backend.CHUNK`).

#### NFR-PERF-004 Small development data

Development and test FDBs stay small: the committed ECMWF samples total about 12 KB,
the MeteoSwiss samples are 175–350 B each, test variants have zeroed values (~236 B).
Test FDBs live in pytest's temporary directories.

- Rationale: fast tests and a small repository.
- Verification: inspection; test `tests/test_grib.py::test_variant_zeroed`.

### 3.3 Compatibility and platforms

#### NFR-COMPAT-001 Platforms and Python

Linux (x86_64, aarch64; glibc ≥ 2.28 for the `manylinux_2_28` wheels), Python ≥ 3.11
and < 4. CI tests Python 3.11 and 3.12 on `ubuntu-latest`. macOS may work with the
pinned pyfdb 5.21.4.x wheels but is not tested; Windows is unsupported.

- Rationale: pyfdb ships wheels for these targets only (architecture.md §13.1).
- Verification: CI job `test`.

#### NFR-COMPAT-002 Dependency pins

Runtime: `snakemake-interface-common>=1.23,<2`,
`snakemake-interface-storage-plugins>=4.4.1,<5`, `tenacity>=9.1.4,<10`,
`pyfdb>=5.21.4.21,<5.22`, `eccodes>=2.47,<2.48`, `pyyaml>=6`. Development:
`snakemake>=9.27`, `pytest>=8`, `ruff`, `coverage`. No site extras.

- Rationale: the pyfdb 5.21 stack bundles eccodes 2.47, the series of the currently
  available COSMO definitions (ADR-018); the lock (`uv.lock`) pins pyfdb 5.21.4.23 and
  snakemake 9.27.0.
- Verification: CI jobs `test` (locked stack) and `pyfdb-latest` (non-blocking canary on
  pyfdb 5.23 + eccodes 2.48).

#### NFR-COMPAT-003 FDB backends

Local toc FDBs are supported and tested. Remote FDBs (`type: remote`) are not tested;
nothing in the design excludes them except the `os.stat` mtime fallback [assumed].

- Rationale: available test infrastructure.
- Verification: inspection.

### 3.4 Reliability

#### NFR-REL-001 No silent partial state

A failed retrieval never leaves a partial local file (FR-READ-007). Everything
checkable from a local file to store (GRIB structure, field count, identifiers,
duplicates, guard) is checked before the first `archive()`, and errors raised then say
"nothing was archived". Only post-check and archive failures leave fields behind, and
their messages say so.

- Verification: test `tests/test_backend.py::test_retrieve_to_errors_leave_no_part`,
  `tests/test_plugin.py::test_store_strict_rejects`,
  `::test_store_guard_mismatch_leaves_fdb_unchanged`,
  `::test_store_partial_archive_failure_says_fields_stay`.

#### NFR-REL-002 Thread safety

pyfdb handles are never shared across threads (FR-STORE-012); module-level warning and
environment registries are lock-protected.

- Verification: test `tests/test_plugin.py::test_store_threads`; inspection.

### 3.5 Security and licensing

#### NFR-SEC-001 Trusted configuration, no destructive operations

Settings and profiles are trusted input: `env` exports arbitrary variables and `config`
may name any file. The plugin handles no credentials; error and log messages may echo
setting values and paths. The plugin never calls FDB `wipe` or `purge` (FR-REMOVE-001).

- Verification: inspection.

#### NFR-LIC-001 Licensing and provenance

The project is BSD-3-Clause (copyright MeteoSwiss). Third-party content keeps its
provenance: the ECMWF samples in `tests/data/grib/ecmwf/`,
`tests/data/pyfdb-tests.schema` and `tests/data/ecmwf-fdb-tests.schema` come from
`ecmwf/fdb` (Apache-2.0); `examples/meteoswiss/realtime-varda.schema` carries its
provenance header and evalml's BSD-3-Clause license; COSMO definitions are installed by
the user, never copied into the repository.

- Verification: inspection.

### 3.6 Maintainability

#### NFR-MAINT-001 Consistency, changelog and automated checks

Code, this document and `architecture.md` are kept consistent: a change that alters
behaviour updates them in the same change. Every user-visible change adds an entry under
`## [Unreleased]` in `CHANGELOG.md`. Formatting, lint, the generic suite and the
MeteoSwiss site suite run in CI and are required. Details are in
[`contributing.md`](../contributing.md).

- Verification: CI jobs `lint` (including the changelog heading), `test` and
  `site-meteoswiss`; inspection (review).

---

## 4. Constraints and assumptions

### 4.1 Constraints

- FDB is append-only: fields are masked by later archives, never deleted individually;
  `wipe` works at index granularity only (architecture.md §13.6).
- `inspect`/`retrieve` match keys exactly; omitted keys are not wildcards
  (architecture.md §13.4).
- pyfdb handles must not be shared across threads; a handle that has read keeps a stale
  catalogue (architecture.md §13.5).
- The FDB index timestamp is exposed only in the `ListElement` repr and has one-second
  resolution (architecture.md §13.3).
- Snakemake does not re-apply `postprocess_query` after wildcard substitution, rewrites
  `query` on copies, and re-parses the Snakefile in spawned jobs (architecture.md §13.8).
- eccodes definitions and the MARS language are read from environment variables when
  the libraries load; one process has one environment.
- Snakemake's argument builder unwraps only `typing.Optional` (a `typing.Union`), not
  `X | None`, so settings are annotated `Optional[str]`.

### 4.2 Assumptions

- The expected field count is the cross product of distinct expanded values per key
  [assumed]; context-dependent keys (e.g. `levelist`) may make some combinations
  meaningless (L-4).
- Remote FDB backends behave like local ones apart from the `os.stat` fallback
  [assumed].
- If another module imports `eccodes` earlier in the same process, definition-path
  changes usually still take effect because eccodes reads them when definitions are
  first needed, but this is not guaranteed across eccodes versions [assumed].
  `METKIT_HOME` is read at first expansion [verified: set after `import pyfdb`].
- The ICON-CH1-EPS conventions equal the verified ICON-CH2-EPS ones (same
  `local.215.def`) [assumed: CH1 fields were not fetched].

---

## 5. Known limitations

| ID | limitation |
|---|---|
| L-1 | The same field spelled differently gives different local paths (warning by default, FR-SPELL-001). The check covers values only: the order of a value list, a repeated value and `to`/`by` versus a list spelling are part of a storage object's identity and give a second local path without a warning. |
| L-2 | No deletion; reruns mask; `fdb purge` is manual. |
| L-3 | Modification times have one-second resolution and reflect index flushes, not data content. In the post-check a field of the query archived earlier within the same second as `t_start` counts as fresh. |
| L-4 | `E` is a cross product; context-dependent keys may over-count. |
| L-5 | The fallback expansion (no internal `FDBToolRequest`) resolves no aliases or non-integer ranges; identifier mode then archives query values verbatim. |
| L-6 | Wildcard values must be single values without `/`. |
| L-7 | eckit `SeriousBug` backtraces (e.g. schema mismatch on archive) cannot be silenced from the environment. |
| L-8 | A `METKIT_HOME` without `language.yaml` hangs FDB; the plugin checks the file exists but cannot validate its content. |
| L-9 | Request expansion parses the repr of pyfdb's internal `FDBToolRequest` (pinned version range). |
| L-10 | Not usable as `--default-storage-provider`; no `--touch`; files only. |
| L-11 | eccodes definition overrides depend on environment variables read at library load. |
| L-12 | MeteoSwiss: `model` values need a MARS language override; accumulations need `timespan=fs`; COSMO paramIds (`500011`) must be used, not ECMWF ones (`2t`); `model` is listed lower-case (spelling warning for `ICON-CH2-EPS`); `eccodes-cosmo-mars` must be cloned (not on PyPI); no `eccodes-cosmo-resources` release for eccodes 2.48 yet. |
| L-13 | `identifier_check=strict` is reserved; the built-in pre-check (both modes) covers only constant query keys the message carries, with light normalisation. |
| L-14 | OGD data expires after 24 h, so samples cannot be re-fetched reproducibly; the committed MeteoSwiss samples have no real field values. |
| L-15 | Site users supply definitions paths, a metkit home and (if the schema is not readable locally) `key_order`. Without a readable schema the plugin cannot tell which query keys FDB indexes, so it requires none of a message (FR-STORE-003) or a field (FR-READ-001). Two providers with different definitions or languages in one process are not supported (last one wins, with a warning). |
| L-16 | The canonical key order depends on the provider's schema; one query used with two schemas gets two local paths (under separate prefixes anyway). |
| L-17 | `archive_mode=identifier` with a multi-rule schema needs a value for every key mandatory in any rule; no rule matching. |
| L-18 | Identifier mode cannot relabel GRIB that contradicts a single-valued query key; fix the GRIB first (e.g. `grib_set`). |
| L-19 | Tagged settings (`TAG::VALUE`) do not reach spawned job processes (upstream Snakemake issue, architecture.md §11): `run:` rules under the local executor and every job under cluster or remote executors see the untagged value `TAG:VALUE`. Use untagged settings for such workflows, or `shell` rules with the local executor. |
| L-20 | eccodes-cosmo-resources prints a definitions version warning per decoded message unless `ECCODES_VERSION_CHECK_OFF=1`, and decoding with the COSMO definitions truncates a stderr redirected to a shared file. |
| L-21 | Input tracking by lookup (FR-RERUN-001) patches the private `snakemake.persistence.PersistenceBase._input`, verified for snakemake 9.27 (architecture.md ADR-031, R-14). Where the attribute is missing or its signature differs, the plugin warns and falls back to query tracking, so query edits trigger reruns again. One residual effect is not covered by any check: a query edited to name different fields that are all older than the output does not rerun, and the output keeps what the old query produced (extra fields after narrowing, missing fields after widening, the old fields after a swap). |
| L-22 | `inspect`/`retrieve` match through query keys the indexed fields do not have (`quantile=1:10` finds quantile-less fields), unlike `list` (architecture.md §13.4). Mitigated by the key check of FR-READ-001; a retrieval whose `inspect` returns matching and non-matching fields together fails on the byte count instead (FR-READ-007). |
| L-23 | Native mode cannot label a key the message does not carry: naming such a key in the query is an error (FR-STORE-003). Use `archive_mode=identifier`, set the key in the GRIB, or drop it from the query. |
| L-24 | A single unreadable database directory under an FDB root looks like missing data: FDB skips it and `inspect` returns fewer fields, with no error to map (eckit's own message is silenced by `ECKIT_EXCEPTION_IS_SILENT=1`). The partial-input warning (FR-READ-008) is the only signal; an unreadable root as a whole is an I/O error on every FDB version, because the plugin checks the configured roots itself (FR-ERR-004). |
| L-25 | Snakemake refuses `--touch` for the whole workflow, not only for FDB outputs, as soon as one output is an FDB query; FDB queries cannot be command-line targets or `--cleanup-metadata` arguments, because Snakemake path-normalises `fdb://` to `fdb:/` (target rule names or a local sentinel file instead). |
| L-26 | `ensure(non_empty=True)` on an FDB output always fails ("Detected unexpected empty output files"): Snakemake checks the storage object's size, which is 0 before the store, not the local file (D-014). |
| L-27 | MARS **key** aliases (`levtyp`, `parameter`) are accepted as unknown keys: they sort to the end of the key order and give their own local path, so two spellings of one request are retrieved twice. |
| L-28 | Relative dates (`date=-1`) expand at run time, but the local path keeps the text, so a copy kept with `--keep-storage-local-copies` goes stale. |

---

## 6. Out of scope and deferred work

### 6.1 Out of scope (v1)

- Use as `--default-storage-provider`: Snakemake builds default-provider queries as
  `<prefix>/<normpath(path)>` and uploads a source tarball through that provider, which
  fits no MARS request [verified: `snakemake/path_modifier.py:132-136`,
  `snakemake/workflow.py:403-406`]; `is_valid_query` rejects such strings.
- Directory objects.
- `--touch` (`StorageObjectTouch`): Snakemake fails upfront for FDB outputs.
- Per-field deletion, `wipe`, `purge`.
- Non-GRIB payloads.
- Relabelling GRIB whose metadata contradicts the query.
- Site-specific mechanisms inside the package (definitions aliases, bundled schemas or
  language patches, site extras).
- Snakemake plugin catalogue pages (`docs/intro.md`, `docs/further.md`): not included
  (decided 2026-09-15); the plugin's documentation lives in `README.md` and `docs/`.

### 6.2 Deferred work

| ID | item | notes |
|---|---|---|
| D-001 | Identifier guard (`identifier_check=strict`), post-v1 | `StrictGuard` in `guard.py`, `strict` accepted by the settings, tests in a new `tests/test_guard.py`. On top of the built-in pre-check it adds: exact canonicalisation of both sides the way FDB does (message MARS keys via `grib.mars_keys` with the provider's definitions; param shortnames, step units such as `0m`, time/date aliases, via `Backend.expand` of a one-field request); schema-level consistency for keys the message does not carry (the identifier selects exactly one schema rule and names only its keys); all mismatches of a file collected into one `IdentifierMismatch`. Acceptance: `step=0m` GRIB vs `step=10m` identifier rejected; `T_2M` vs `500011` accepted; labels selecting no single rule rejected; a MeteoSwiss message under `realtime-varda.schema` passes with the correct identifier; the reserved-error test is inverted. |
| D-002 | Lift the pyfdb pin to 5.23 (`pyfdb>=5.23.2.27,<6`, `eccodes>=2.48,<3`) | When COSMO definitions for eccodes 2.48 exist and the `pyfdb-latest` CI canary is green; bump `setup.sh`'s `eccodes-cosmo-resources-python` range with it. |
| D-003 | Upstream pyfdb pull requests to `ecmwf/fdb` | Bind `ListElement::timestamp()` (`src/pyfdb_bindings/bindings.cc`, `src/fdb5/api/helpers/ListElement.h:71`) and expose `ListElement.timestamp() -> int` in `src/pyfdb/pyfdb_iterator.py` ("index flush time, POSIX seconds; 0 for level < 3 and legacy indexes"), with tests in `tests/pyfdb/integration/test_list.py`; fix the 9-digit timestamp typos in the `pyfdb.py` docstrings; optionally a public `pyfdb.expand(selection) -> dict[str, list[str]]` wrapping `mars_request_from_map`. Plugin follow-up: prefer them when present, keep the fallbacks. |
| D-004 | Report the Snakemake tagged-settings bug upstream | An issue draft exists but is not posted (architecture.md §11, R-1). |
| D-005 | `--touch` support | Could re-archive the retrieved bytes to refresh the index timestamp. |
| D-006 | A `wipe` remove policy | Only acceptable if `list(level=2)` proves the query covers every field of every index it touches. |
| D-007 | Test remote FDB backends | See NFR-COMPAT-003. |
| D-008 | Script to re-download the ECMWF samples | `scripts/fetch_ecmwf_samples.py` from `ecmwf/fdb` at the pinned commit (provenance in architecture.md §13.2); never written, the manual steps are in `contributing.md`. |
| D-009 | qubed | Not useful for v1; revisit for compressed summaries of large FDB listings (architecture.md §13.12). |
| D-010 | Withdrawn | Snakemake plugin catalogue pages will not be included (decided 2026-09-15); see §6.1. |
| D-011 | Upstream hook instead of the interim patch | Snakemake decides input changes from the recorded query text of storage inputs; propose that a storage object can opt out (e.g. `tracks_input_changes`, consulted in `PersistenceBase._input`, defaulting to `True` in `StorageObjectRead` of `snakemake-interface-storage-plugins`). Issue and PR drafts, with a patch against `main`, are prepared but not posted. Remove `rerun.py`'s patch once a released Snakemake offers the hook (ADR-031, L-21, architecture.md R-14). |
| D-012 | Report the `inspect` vs `list` discrepancy upstream | `inspect`/`retrieve` match through query keys absent from the indexed fields while `list` does not (L-22, architecture.md §13.4); same behaviour on pyfdb 5.21.4.23 and 5.23.2. Open an issue on `ecmwf/fdb` asking whether this is intended; drop the plugin-side key check (FR-READ-001) if it is ever fixed. |
| D-013 | Report the `{provider}` formatting of invalid-query messages upstream | `snakemake/storage.py:205-209` (snakemake 9.27.0) formats the provider object into the catalogue URL of an invalid query, so a plugin without `__str__` produces `.../plugins/storage/<...StorageProvider object at 0x...>.html`. The plugin works around it with `StorageProvider.__str__` (FR-IFACE-002); upstream should use the plugin name. |
| D-014 | Report two Snakemake behaviours upstream | Command-line targets and `--cleanup-metadata` arguments are path-normalised, so `fdb://` becomes `fdb:/` and storage URIs cannot be named on the command line (L-25); `ensure(non_empty=True)` checks a storage output's `size()` before the store instead of the local file, which no storage plugin can satisfy (L-26). |
