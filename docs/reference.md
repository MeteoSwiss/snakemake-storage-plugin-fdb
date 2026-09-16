# Reference

Exact behaviour of the `fdb` storage provider. For explanations and examples see the
[user guide](user-guide.md); for requirement IDs and rationale see
[`design/requirements.md`](design/requirements.md).

## Settings

Every setting is available as a Snakemake CLI flag, a profile key (the flag without
`--`) and a keyword argument of the `storage` directive. Values may be tagged as
`TAG::VALUE` for a tagged provider (`storage <tag>:`), e.g.
`--storage-fdb-config prod::/etc/fdb/prod.yaml scratch::.fdb/config.yaml`. All plugin
settings are optional strings (`typing.Optional[str]`); an unset setting takes its
default. Settings are validated when the provider is constructed. The settings marked
"env" below are also read from `SNAKEMAKE_STORAGE_FDB_<NAME>` (see
[Environment variables](#environment-variables)); the CLI flag wins over the variable,
the variable over the default.

| name | CLI flag | default | allowed values | description |
|---|---|---|---|---|
| `config` | `--storage-fdb-config` (env) | unset: FDB's own environment (`FDB_CONFIG`, `FDB_CONFIG_FILE`, `FDB_HOME`) | path to an existing file (made absolute), or inline YAML/JSON text that parses as a mapping | FDB configuration, passed to `pyfdb.FDB(config, ...)`. |
| `user_config` | `--storage-fdb-user-config` (env) | unset | as `config` | FDB user configuration (e.g. `useSubToc: true`). |
| `archive_mode` | `--storage-fdb-archive-mode` | `native` | `native`, `identifier` | How outputs are archived: `native` (FDB derives the keys from the GRIB) or `identifier` (the plugin builds each message's FDB key; use it only with schemas whose rules share one key set, or supply the other keys in the query). |
| `identifier_check` | `--storage-fdb-identifier-check` | `none` | `none` (`strict` is reserved and rejected) | Check of identifiers against GRIB metadata before archiving. |
| `canonical_spelling` | `--storage-fdb-canonical-spelling` | `warn` | `warn`, `error`, `ignore` | What to do when query values are spelled differently from FDB (e.g. `param=2t` vs `167`). |
| `remove_policy` | `--storage-fdb-remove-policy` | `warn` | `warn`, `ignore`, `error` | FDB cannot delete fields; what removing an output does: no-op with a warning, silent no-op, or error. |
| `input_tracking` | `--storage-fdb-input-tracking` | `lookup` | `lookup`, `query` | What makes a rule with FDB inputs rerun: `lookup` (only the FDB lookup, so editing a query does not trigger a rerun by itself; see [Methods](#methods)) or `query` (Snakemake's default: the recorded set of input queries too). |
| `glob_required_keys` | `--storage-fdb-glob-required-keys` (env) | `class` | comma list of key names (lower-cased); empty disables the check | Keys that must be constant in `glob_wildcards` patterns. |
| `eccodes_definitions` | `--storage-fdb-eccodes-definitions` (env) | unset | colon list of existing directories (made absolute; empty entries skipped; `/MEMFS/...` entries passed as is) | Prepended in order to `ECCODES_DEFINITION_PATH`. |
| `metkit_home` | `--storage-fdb-metkit-home` (env) | unset | directory containing `share/metkit/language.yaml` (made absolute) | Exported as `METKIT_HOME` for a custom MARS language. |
| `key_order` | `--storage-fdb-key-order` (env) | unset: the FDB schema's rule order, else the generic MARS order | comma list of distinct key names (an empty value is ignored, like unset) | Canonical key order of queries and local paths. |
| `env` | `--storage-fdb-env` (env) | unset | `NAME=VALUE[,NAME=VALUE]`; names `[A-Za-z_][A-Za-z0-9_]*`; values may contain `=`, not `,`; no duplicate names | Environment overrides exported before the FDB libraries load (e.g. `FDB_HOME=/path`). |
| `max_requests_per_second` | `--storage-fdb-max-requests-per-second` | unset | float | Inherited from the interface; unused, the rate limiter is disabled. |

## Environment variables

| variable | plugin behaviour |
|---|---|
| `SNAKEMAKE_STORAGE_FDB_CONFIG`, `..._USER_CONFIG`, `..._ECCODES_DEFINITIONS`, `..._METKIT_HOME`, `..._KEY_ORDER`, `..._ENV`, `..._GLOB_REQUIRED_KEYS` | Read by Snakemake's argument parser as the value of the matching setting; the CLI flag wins, then the variable, then the default. One value per variable, optionally tagged `TAG::VALUE`. |
| `ECCODES_DEFINITION_PATH` | Read and prepended to with `eccodes_definitions` (skipped if it already starts with those directories). Read by eccodes, metkit and FDB. |
| `METKIT_HOME` | Set from `metkit_home` (overriding a different existing value is logged at info level). Its effective value, from any source, must contain `share/metkit/language.yaml`. Read by metkit. |
| `ECKIT_EXCEPTION_IS_SILENT` | Set to `1` if unset. `scripts/init_dev_fdb.py` and `tests/conftest.py` default it too. |
| `FDB_CONFIG`, `FDB5_CONFIG` | Read (YAML text) to find the schema when `config` is unset; never set. |
| `FDB_CONFIG_FILE`, `FDB5_CONFIG_FILE` | Read (config path) likewise; never set. |
| `FDB_HOME` | Read for `$FDB_HOME/etc/fdb/config.{yaml,json}` and `~fdb` expansion; never set. |
| `FDB_SCHEMA_FILE` | Read when the config has no `schema`; never set. |
| any name in `env` | Set to the given value. |
| `ECCODES_VERSION_CHECK_OFF` | Not used by the plugin. Read by the eccodes-cosmo-resources definitions; `1` silences their version warning. |
| `SMK_FDB_TEST_*` | Read only by the test suites; see [contributing](contributing.md#site-suite). |

Order within a provider: `env` first, then `eccodes_definitions`, then `metkit_home`,
then the `METKIT_HOME` check, then `ECKIT_EXCEPTION_IS_SILENT`. Everything is validated
before anything is exported, and exported before `pyfdb` and `eccodes` are imported
([architecture §8.3](design/architecture.md#83-environment-precedence-and-lazy-imports)).

## Query grammar

```text
query      = "fdb://" ws pair ( ws "," ws pair )* ws
pair       = key ws "=" ws value
key        = [A-Za-z][A-Za-z0-9_]*                  ; lower-cased by normalisation
value      = item ( "/" item )*                     ; MARS value list
item       = ( wildcard | ichar+ )+                 ; non-empty
ichar      = [A-Za-z0-9] | "." | "-" | ":" | "_"
wildcard   = "{" name [ "," constraint ] "}"        ; Snakemake wildcard, atomic
ws         = whitespace*
```

- The scheme `fdb://` is case-sensitive and must be the first characters.
- `to` and `by` are ordinary items (`step=0/to/48/by/6`); metkit expands them.
- Wildcards are matched first with `snakemake_interface_storage_plugins.io.WILDCARD_REGEX`
  and are opaque, so a constraint may contain `,`, `/`, `=` and braces (`{date,\d{8}}`).
  Wildcards are allowed only in values.
- No escaping: `,`, `=`, `/`, `{`, `}`, `+`, `%` and whitespace cannot occur in an item
  outside a wildcard. `{{` and `}}` are rejected.

Examples:

```text
fdb://class=od,expver=0001,stream=oper,date={date},time=0000,domain=g,type=fc,levtype=sfc,step=0/6/12,param=167
fdb://class=ea,expver=0001,stream=enda,date=20200101,time=0000,domain=g,type=an,levtype=sfc,step=0,number=0,param=167
fdb://class=od,expver=0001,stream=oper,date={date},time={time},domain=g,type=fc,levtype=sfc,step=0/to/48/by/6,param=167/165/166
fdb://class=od, expver=0001, stream=oper, date={date,\d{8}}, time=0000, type=fc, levtype=sfc, step=0, param=167
```

### Normalisation

`postprocess_query` rewrites a valid query as `fdb://` + `key=value` pairs joined by `,`:
whitespace outside wildcards removed, keys lower-cased and sorted into the provider's key
order, values verbatim. The key order is the `key_order` setting, else the order of first
appearance of rule keys in the FDB schema (decorations `?`, `?default`, `-`, `=values`,
`:Type` and `#` comments ignored), else
`class, expver, stream, domain, date, time, type, levtype, levelist, step, number, param`;
other keys follow alphabetically. Invalid queries are returned unchanged.

### Query errors

Invalid queries raise `QueryError`; Snakemake shows the reason from `is_valid_query`, and
storage objects raise `invalid FDB query <query>: <reason>` on use.

| example input | reason |
|---|---|
| `class=od`, ` fdb://class=od` | `query must start with 'fdb://'` |
| `fdb://` | `empty query` |
| `fdb://a` | `missing '=' in 'a'` |
| `fdb://a=` | `empty value for key 'a'` |
| `fdb://=1` | `empty key in '=1'` |
| `fdb://,a=1`, `fdb://a=1,,b=2` | `empty key=value pair` |
| `fdb://a=1,` | `trailing comma` |
| `fdb://a=0//6`, `fdb://a=/6` | `empty item ('/' misplaced) in value of key 'a'` |
| `fdb://a=1,A=2` | `duplicate key 'a'` |
| `fdb://{x}=1` | `wildcards are not allowed in keys: '{x}=1'` |
| `fdb://1a=1` | `invalid key '1a'` |
| `fdb://a-b=1` | `invalid key 'a-b' (invalid character '-')` |
| `fdb://a=o+d` | `invalid character '+' in value of key 'a'` |
| `fdb://a={{x}}` | `invalid character '{' in value of key 'a'` |
| `fdb://a=o d` | `invalid character whitespace in value of key 'a'` |
| `fdb://a=1=2` | `invalid character '=' in 'a=1=2'` |
| `fdb://a=1/b=2` | `invalid character '=' in 'a=1/b=2'` |

Local path errors, raised by `local_suffix` as `invalid FDB query <query>: <reason>`:

| cause | reason |
|---|---|
| a component over 255 bytes contains a wildcard | `local path component for key '<key>' exceeds 255 bytes and contains a wildcard (cannot be hashed)` |
| the hashed component is still over 255 bytes | `key name too long for a path component: '<key>'` |

## Local path mapping

The local copy of a query is `.snakemake/storage/<fdb or tag>/<suffix>`, where the suffix
is one `key=value` directory per pair in canonical key order, `/` in values replaced by
`+` outside wildcards, and `.grib` appended to the last component:

```text
fdb://class=od,expver=0001,stream=oper,date={date},time=0000,domain=g,type=fc,levtype=sfc,step=0/6/12,param=167
class=od/expver=0001/stream=oper/date={date}/time=0000/domain=g/type=fc/levtype=sfc/step=0+6+12/param=167.grib
```

- Wildcard text, including constraints, is copied verbatim, so the mapping commutes with
  wildcard substitution as long as wildcard values are single MARS values.
- A component longer than 255 bytes (UTF-8, the last one including `.grib`) is replaced by
  `key=~<first 24 hex digits of sha256(value)>` (`.grib` appended for the last), where
  `value` is the verbatim query value. A long component containing a wildcard, or a hashed
  component that is still too long, is an error.
- A job query whose substituted wildcard values push a component over the limit raises
  `local path component for key '<key>' of <query> is <n> bytes after wildcard substitution (limit 255); wildcard values must be single MARS values (put lists in the query, not in wildcards)`.

## Storage methods

| method | behaviour |
|---|---|
| `is_valid_query` | Parses the query (no key order, no native libraries); valid or invalid with the reason above. |
| `postprocess_query` | Normalisation above; records the result for the wildcard guard. |
| `example_queries` | Three generic examples (single date pattern with `step=0/6/12`, one ensemble analysis field, a `to/by` range over three parameters). |
| `local_suffix` | Local path mapping above. |
| `exists` | One `inspect`; true iff the expanded request's field count `E > 0` and exactly `E` fields are found. A field counts only if its key holds every query key FDB indexes (schema rule keys not marked `key-`; none without a readable schema): `inspect` matches through keys the fields do not have. Warns once per query if some but not all fields are found. |
| `mtime` | One `inspect`; latest index timestamp of the found fields (POSIX seconds; `os.stat` of the data file for timestamp 0, else 0 with a warning); `FileNotFoundError("no fields in FDB for <query>")` if none. |
| `size`, `local_footprint` | One `inspect`; sum of the found fields' message lengths. |
| `checksum` | `None` (Snakemake hashes the local copy). |
| `inventory` | One `inspect`; fills existence, and for existing objects mtime and size, for `cache_key()`; no-op if already cached. Warns like `exists`. |
| `get_inventory_parent` | `None`. |
| `retrieve_object` | Requires `exists`; streams `retrieve` into `<local>.part` in 8 MiB chunks, fsyncs, checks the byte count, renames over the local path; removes the part file on any error. |
| `store_object` | Expands the query, splits the local file into GRIB messages, requires exactly `E` of them, pre-checks every message's MARS keys against the query (both modes), builds identifiers (`identifier` mode) or requires every indexed query key to be present in the message (`native` mode), rejects duplicates, archives and flushes, then post-checks with one `inspect` that every message is reachable with a timestamp from this store, naming the offending messages if not. Never retried. |
| `remove` | Never deletes; applies `remove_policy`. Snakemake 9.27 calls it only for `--delete-all-output`: not before a rerun, not on failed-job cleanup (its "Removing output files of failed job" line removes nothing from FDB), and never for `temp()`, which cannot be combined with storage. |
| `list_candidate_matches` | Checks `glob_required_keys`; one `list` of the pattern's constant pairs; returns the sorted unique pattern texts with wildcard-bearing values replaced by listed values, skipping fields that lack them. |
| `cleanup` | No-op. |
| `rate_limiter_key`, `default_max_requests_per_second`, `use_rate_limiter` | `"fdb"`, `10.0`, `False`. |
| `safe_print` | Identity. |
| `tracks_input_changes` | `False`, or `True` with `input_tracking=query`: whether the object's query takes part in Snakemake's input-set rerun trigger. |

`exists`, `mtime`, `size`, `inventory`, `retrieve_object` and `store_object` expand the
request with metkit first (once per object and query), which runs the canonical-spelling
check and raises invalid requests before any FDB I/O. Queries with unresolved wildcards
raise `FDB query <query> has unresolved wildcards`. The FDB `inspect`, `retrieve` and
`list` calls use a fresh FDB handle each and are retried (3 attempts, exponential wait
from 3 s) unless the failure is one of the mapped permanent ones below (invalid MARS
request, not GRIB, GRIB keys against the schema, configuration error, I/O error), which
is raised on the first attempt.

With `input_tracking=lookup` (the default), constructing the first provider of a process
also patches Snakemake's private `snakemake.persistence.PersistenceBase._input`, so that
inputs whose `tracks_input_changes` is `False` are left out of the input set Snakemake
records for the `input` rerun trigger
([architecture ADR-031](design/architecture.md#adr-031-interim-patch-of-persistencebase_input-for-fdb-inputs)).
Nothing else in Snakemake is modified. If the attribute is missing or has an unexpected
signature, the plugin logs the warning below and leaves the trigger alone.

## Errors and messages

All errors are Snakemake `WorkflowError`s unless noted. `<detail>` is the first line of the
pyfdb error without `UserError: `/`Serious bug: ` prefixes and without a trailing
` (Success)`, cut before metkit's `request=` dump or the first `;` and truncated to 200
characters with `…`; the full text is logged at debug level.

### Provider construction

| message | cause |
|---|---|
| `invalid <setting> '<value>' (allowed: <values>)` | unknown value of a choice setting |
| `identifier_check=strict is reserved and not implemented in this version` | `identifier_check=strict` |
| `invalid key name(s) in glob_required_keys: <keys>` | invalid key names |
| `invalid key_order: <reason>` | `empty key name in key order '<csv>'`, `invalid key name in key order: '<key>'`, `duplicate key in key order: '<key>'` |
| `invalid env setting '<value>': expected NAME=VALUE[,NAME=VALUE], got '<item>'` | missing `=` or invalid name |
| `invalid env setting '<value>': duplicate <NAME>` | name given twice |
| `eccodes_definitions: '<entry>' is not an existing directory` | bad definitions entry |
| `METKIT_HOME=<dir> (from <metkit_home \| the env setting \| the environment>) has no share/metkit/language.yaml; FDB would hang instead of failing` | invalid metkit home |
| `FDB configuration error: '<value>' is neither an existing file nor an inline YAML mapping` | `config`/`user_config` not a file and not a mapping |
| `... (looks like a tagged setting mangled by a spawned job, see the user guide on tagged settings)` | appended when the value has the shape `TAG:<existing file>` (L-19) |
| `FDB configuration error: '<value>' is neither an existing file nor valid YAML: <error>` | unparsable YAML |
| `FDB configuration error: schema <path>: <detail>` | schema unreadable or without rule keys |
| `cannot import the FDB/eccodes bindings: <error>` | `pyfdb` or `eccodes` not importable |

### Reading and globbing

| message | cause |
|---|---|
| `invalid FDB query <query>: <reason>` | invalid query used |
| `FDB query <query> has unresolved wildcards` | wildcards left in a job query |
| `Invalid MARS request <query>: <detail>` | metkit rejects the request (unknown key or value, context error, several values for `class`/`stream`/`type`/`expver`) |
| `... (if this value is valid for your FDB, point metkit_home at a MARS language that defines it)` | appended when `<detail>` contains `cannot expand` |
| `Query <query> uses non-canonical spelling: <key>=<given> (canonical: <value>), <key>=<given> (<value>). Use canonical spellings to avoid duplicate local paths for the same field.` | `canonical_spelling=error` (a warning with `warn`) |
| `<query>: <n> of <E> fields found in FDB; missing: <combinations> (and <k> more)` | retrieving an incomplete object; at most 10 combinations. Also logged, as a warning (`0 < n < E`) or at debug level (`n = 0`), by `exists` and `inventory` |
| `. FDB matches keys exactly; optional schema keys not in the query: <keys>` | appended when nothing was found (so only in the retrieve error and the debug log) |
| `retrieved <n> bytes for <local>, expected <m>` | retrieval byte count mismatch |
| `FDB configuration error: <detail>` | `Cannot open ...` (e.g. missing schema), `No writable roots available ...` |
| `... (no FDB configuration was given: set --storage-fdb-config or FDB_CONFIG_FILE)` | appended when `<detail>` names the schema bundled with the pyfdb wheel (`.../fdb5lib/etc/fdb/schema`) |
| `FDB I/O error for <query>: <detail> (check permissions, free space and the roots in the FDB configuration)` | `Failed system call ...`, `Failed to mkdir ...`, `Permission denied`, `No space left on device`, `Read-only file system` |
| `FDB glob pattern <query> needs constant values for <keys> (glob_required_keys)` | required key is a wildcard or absent |
| `FileNotFoundError: no fields in FDB for <query>` | `mtime` of an absent object |

### Storing

| message | cause |
|---|---|
| `<local> is not GRIB (no GRIB message found)` | empty or non-GRIB file |
| `<local>: trailing non-GRIB bytes at offset <n>` / `<local>: non-GRIB bytes at offset <n>` | non-NUL bytes outside messages |
| `<local>: cannot read GRIB message[ after offset <n>]: <error>` | truncated or undecodable message |
| `<local or query> is not GRIB` | FDB found no GRIB in the data |
| `<query>: <local> has <n> fields, the query expands to <E>; nothing was archived` | more or fewer fields than the query expands to |
| `<query>: cannot determine <key> for message <i> of <local>; nothing was archived` | identifier mode, mandatory schema key without a value |
| `<query>: message <i> of <local> has <key>=<value>, but the query has <key>=<value>; nothing was archived` | pre-check (both modes), single value |
| `<query>: message <i> of <local> has <key>=<value>, not one of <values>; nothing was archived` | pre-check (both modes), list |
| `<query>: message <i> of <local> lacks <key>, which native archiving takes from the message; use archive_mode=identifier to label it, or drop the key from the query; nothing was archived` | native mode, query key absent from the message |
| `<query>: identifier check failed for <local>: <mismatch>; nothing was archived` | the identifier guard rejected a message |
| `<query>: <local> holds duplicate fields (messages <i> and <j>); nothing was archived` | duplicate keys |
| `GRIB keys do not match the FDB schema for <query> (<local>): <detail>` | `Keywords not used`, `Could not find [...]`, `Could not find a rule` |
| `<query>: <local> has <n> fields, the query expands to <E>; <k> landed outside the query or are duplicates: message <i> (<keys>)[, ...][ and <k> more] (they stay in FDB until the next successful store masks them)` | post-check; the named messages are those no fresh field matches (at most three) |
| `<error> (<k> of <m> archive calls succeeded before the failure; they stay in FDB until the next successful store masks them)` | archive failure after a successful call |
| `remove_policy=error: FDB cannot delete individual fields; ...` | `remove()` with `remove_policy=error` |

### Log messages

| level | message |
|---|---|
| warning | `Query <query> uses non-canonical spelling: ...` (once per query per process) |
| warning | ``FDB cannot delete individual fields; existing fields for <query> will be masked by the next archive. Use `fdb purge` to reclaim space.`` (once per query per process) |
| warning | `FDB storage: <query>: <n> of <E> fields found in FDB; missing: ...` (once per query per process, when FDB holds some but not all fields) |
| warning | `FDB storage: a field of <query> has no index timestamp and no local data file; its mtime is taken as 0` |
| warning | `FDB storage: providers in one process use different <VARIABLE> settings (<a> vs <b>); the last one wins` |
| warning | `FDB storage: input tracking by lookup is unavailable with snakemake <version> (<reason>); falling back to query tracking. Set input_tracking=query to silence this warning.` (per provider with `input_tracking=lookup`) |
| info | `FDB storage: metkit_home overrides METKIT_HOME=<old> with <new>` |
| debug | `FDB storage: <query>: 0 of <E> fields found in FDB ...` (nothing found; includes the optional-schema-key hint) |
| debug | `FDB storage: full error text: <full pyfdb message>` (for every error mapped to a `WorkflowError`) |
| debug | `FDB storage: <query>: <k> of <n> inspected fields lack a query key and are not counted (FR-READ-001)` |
| debug | `FDB storage: <query>: no metkit expansion; query values are used as written (spelling check skipped, identifier values from the query archived verbatim)` |

## `scripts/init_dev_fdb.py`

```text
uv run python scripts/init_dev_fdb.py [--root DIR] [--schema PATH] [--seed [DIR]] [--variants [FILE]]
```

| option | default | effect |
|---|---|---|
| `--root DIR` | `.fdb` in the repository | writes `DIR/schema`, `DIR/root/` and `DIR/config.yaml` (local toc FDB, absolute paths) |
| `--schema PATH` | `tests/data/schema` | schema to copy; missing file is an argument error |
| `--seed [DIR]` | not seeded; `tests/data/grib/ecmwf` if given without DIR | natively archives every file directly in DIR whose content starts with `GRIB` (no subdirectories); a missing DIR is reported on stderr and skipped |
| `--variants [FILE]` | none; `tests/data/grib/ecmwf/template.grib` if given without FILE | archives zeroed `stream=oper` variants of the message in FILE for steps 0/6/12 × params 167/165 (the inputs of `examples/ecmwf/`); missing FILE is an argument error |

Defaults are relative to the repository, explicit arguments to the working directory.
Data needing other eccodes definitions or a MARS language is seeded with
`ECCODES_DEFINITION_PATH`/`METKIT_HOME` set in the environment.
