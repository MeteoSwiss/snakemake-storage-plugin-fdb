# User guide

How to use the Snakemake storage plugin for ECMWF's Fields DataBase (FDB) in workflows.
Exact settings, messages and method behaviour are in the [reference](reference.md).

- [Concepts](#concepts)
- [Installation](#installation)
- [Configuration](#configuration)
- [Writing queries](#writing-queries)
- [Reading inputs](#reading-inputs)
- [Writing outputs](#writing-outputs)
- [Globbing](#globbing)
- [Canonical spelling](#canonical-spelling)
- [Removing outputs](#removing-outputs)
- [Snakemake flags and features](#snakemake-flags-and-features)
- [Site definitions and MARS language](#site-definitions-and-mars-language)
- [A local development FDB](#a-local-development-fdb)
- [Troubleshooting](#troubleshooting)
- [Limitations](#limitations)

## Concepts

- A **query** is a MARS request in one line: `fdb://key=value,key=value,...`. It may
  address several fields with `/` lists and `to`/`by` ranges.
- Each query maps to **one local GRIB file** under `.snakemake/storage/`. As an input,
  Snakemake retrieves all fields of the query into it; as an output, your rule writes the
  fields into it and the plugin archives them into FDB.
- A query **exists** only if FDB holds every field the query expands to.
- FDB never deletes single fields. Archiving the same fields again **masks** the old ones.
- The plugin is **site-neutral**: an FDB with its own schema, eccodes definitions or MARS
  language is configured through generic settings.

## Installation

Requirements and installation commands are in the
[README](../README.md#requirements). Install the plugin into the environment that runs
Snakemake; `snakemake --help` then lists the `--storage-fdb-*` options.

## Configuration

### Choosing the FDB

The `config` setting is either a path to an FDB configuration file or inline YAML:

```yaml
type: local
engine: toc
schema: /path/to/fdb/schema
spaces:
  - handler: Default
    roots:
      - path: /path/to/fdb/root
```

Without `config`, FDB's own environment applies (`FDB_CONFIG`, `FDB_CONFIG_FILE`,
`FDB_HOME`). The plugin reads the schema named by the configuration to order query keys
(see [Writing queries](#writing-queries)). A relative path resolves against
Snakemake's working directory, which is the directory the command was typed in unless
`-d`/`--directory` moves it elsewhere; with `-d` the path must be relative to that
directory, not to the shell's. An absolute path always works.

### Where settings go

On the command line:

```bash
snakemake --storage-fdb-config /path/to/fdb/config.yaml -c1
```

In a profile (`profile/config.yaml`, run with `snakemake --profile profile`):

```yaml
storage-fdb-config: /path/to/fdb/config.yaml
storage-fdb-canonical-spelling: error
```

Or in the environment, for the settings that describe the site rather than the workflow
(`config`, `user_config`, `eccodes_definitions`, `metkit_home`, `key_order`, `env`,
`glob_required_keys`):

```bash
export SNAKEMAKE_STORAGE_FDB_CONFIG=/path/to/fdb/config.yaml
snakemake -c1
```

A command-line flag overrides the variable. The variable holds one value, which may be
tagged (`SNAKEMAKE_STORAGE_FDB_CONFIG='prod::/etc/fdb/prod.yaml'`). The full list is in
the [reference](reference.md#environment-variables).

Or in the Snakefile:

```snakemake
storage:
    provider="fdb",
    config="/path/to/fdb/config.yaml",
```

The Snakefile then refers to the provider as `storage.fdb("fdb://...")`, or
`storage("fdb://...")` without naming it.

Snakemake also applies a `profiles/default/` directory next to the Snakefile without
being asked (its workflow profile), so a stray `profiles/default/config.yaml` can
supply the FDB settings of a run that seems to have none.

### Several FDBs: tagged providers

A `storage <tag>:` directive creates a separate provider with its own settings, local
directory (`.snakemake/storage/<tag>/`) and FDB. On the command line and in profiles,
values for it are written `TAG::VALUE`:

```snakemake
storage prod:
    provider="fdb"


storage scratch:
    provider="fdb"
```

```bash
snakemake --storage-fdb-config prod::/etc/fdb/prod.yaml scratch::.fdb/config.yaml -c1
```

```yaml
storage-fdb-config: ["prod::/etc/fdb/prod.yaml", "scratch::.fdb/config.yaml"]
```

Rules then use `storage.prod(...)` and `storage.scratch(...)`. All providers in one
process share one environment, so they cannot use different `eccodes_definitions` or
`metkit_home` values (the last one wins, with a warning).

### Tagged settings and spawned jobs

In Snakemake 9.27, tagged setting values do **not** reach job processes that Snakemake
spawns: a spawned job sees `TAG:VALUE` (one colon) as an untagged value and fails, e.g.
with `FDB configuration error: 'ecm:../../.fdb/config.yaml' is neither an existing file
nor an inline YAML mapping (looks like a tagged setting mangled by a spawned job, see
the user guide on tagged settings)`. This is an upstream Snakemake bug. Affected are `run:`
rules with the local executor, and every job with cluster or remote executors. Until it
is fixed:

- pass settings **untagged** for workflows with `run:` rules or cluster execution (as
  `examples/ecmwf/` does); or
- use `shell:` rules with the local executor, which run in the main process (as
  `examples/meteoswiss/` does); or
- put the settings in the Snakefile's `storage` directive, which every job re-reads.

## Writing queries

```text
fdb://class=od,expver=0001,stream=oper,date={date},time=0000,domain=g,type=fc,levtype=sfc,step=0/6/12,param=167
```

- Keys are MARS keys; values are MARS values. Lists use `/` (`param=167/165`), ranges
  `to` and `by` (`step=0/to/48/by/6`).
- Whitespace around keys, `=` and `,` is allowed, so long queries can be split:
  `"fdb://class=od, expver=0001, " "stream=oper, ..."`.
- Snakemake wildcards may appear in values (`date={date}`, `date={date,\d{8}}`,
  `date={year}0101`), never in keys. **A wildcard value must be a single MARS value**:
  put lists in the query, not in wildcards.
- There is no escaping: `,`, `=`, `{`, `}`, `+`, `%` and spaces cannot occur in a value,
  and `/` only separates list items.
- **Name every key the fields are archived under**, including optional schema keys the
  fields carry (`domain=g`, `number=...`, `timespan=...`). FDB matches keys exactly when
  reading: a query without `number` does not find ensemble members.

The plugin normalises queries: it removes whitespace, lower-cases keys and sorts them
into a canonical order: the `key_order` setting if given, else the order of keys in the
FDB schema, else a generic MARS order
(`class, expver, stream, domain, date, time, type, levtype, levelist, step, number, param`).
Values are never changed. Normalised queries appear in Snakemake's log and in local
paths:

```text
.snakemake/storage/fdb/class=od/expver=0001/stream=oper/date=20240101/time=0000/domain=g/type=fc/levtype=sfc/step=0+6+12/param=167.grib
```

### Several fields in one rule

A query with `/` lists gives one local file. To give a rule one file per value instead,
expand the query text and wrap the results, not the other way round:

```snakemake
QUERY = (
    "fdb://class=ea,expver=0001,stream=oper,date={date},time=0000,domain=g,"
    "type=an,levtype=sfc,step={step},param={param}"
)


rule merge:
    input:
        storage.fdb(expand(QUERY, step=[0, 6, 12], param=[167, 165], allow_missing=True)),
    output:
        "merged/{date}.grib",
    shell:
        "cat {input} > {output}"
```

`allow_missing=True` keeps `{date}` for Snakemake to fill in per job; a list
comprehension over formatted query strings works as well.

`expand(storage.fdb(QUERY), ...)` does **not** work: Snakemake requires flags outside
`expand`, and the error names the internal local path
(`Flags ({'storage_object': ...}) in file pattern '.snakemake/storage/fdb/.../param={param}.grib' given to expand() are invalid`)
instead of the query. `multiext(storage.fdb(QUERY), ...)` drops the storage flag
silently; the job then asks for local files nobody produces.

## Reading inputs

```snakemake
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

- The input exists only when **all** fields of the query are in FDB (3 here). If some
  are missing, Snakemake treats the input as missing and reports the whole query as a
  missing input. The plugin then logs, once per query, which fields it did find:

  ```text
  WARNING FDB storage: fdb://...,step=0/6/12,param=167: 2 of 3 fields found in FDB; missing: step=12
  ```

- If **nothing** matches, that is the normal state of data not produced yet, so there is
  no warning. Run with `--verbose` to get the same report at debug level; when the
  schema is known it also lists the optional schema keys the query does not name
  (`domain`, `number`, `timespan`), which is the usual cause. Otherwise check with
  `fdb list` using the query's constant keys.
- The local file holds the messages in request order, never sorted: each key's values in
  the order the query lists them, keys nested in canonical key order with the last key
  varying fastest. `step=12/0/6,param=165/167` gives `12/165, 12/167, 0/165, 0/167,
  6/165, 6/167`.
- The input's modification time is the time FDB last flushed the index holding the
  fields (one-second resolution), so rules rerun when inputs are re-archived.
- An invalid request (unknown key or value, `number` with `type=cf`) is an error, not a
  missing input.
- Retrieval is atomic: a failed transfer never leaves a partial local file. Errors that
  may be transient are retried (3 attempts); errors the plugin recognises as permanent
  (an invalid MARS request, a broken configuration, a permission or disk problem) fail
  at once.

## Writing outputs

A rule output is a query; the rule writes a GRIB file with exactly those fields, and the
plugin archives it:

```snakemake
rule shift_expver:
    input:
        storage.fdb(
            "fdb://class=ea,expver=0001,stream=oper,date={date},time=0000,domain=g,"
            "type=an,levtype=sfc,step=0/6/12,param=167"
        ),
    output:
        storage.fdb(
            "fdb://class=ea,expver=0002,stream=oper,date={date},time=0000,domain=g,"
            "type=an,levtype=sfc,step=0/6/12,param=167"
        ),
    run:
        import eccodes

        with open(input[0], "rb") as fi, open(output[0], "wb") as fo:
            while (h := eccodes.codes_grib_new_from_file(fi)) is not None:
                eccodes.codes_set(h, "expver", "0002")
                eccodes.codes_write(h, fo)
                eccodes.codes_release(h)
```

Before archiving, the plugin checks the file:

- it must be GRIB; NUL padding between messages is fine, any other extra bytes are an
  error;
- it must hold exactly as many fields as the query expands to, no more and no less;
- every message's MARS keys must agree with the constant keys of the query: a `step=6`
  query rejects a `step=0` message, a `step=0/6/12` query rejects `step=18`. The plugin
  never relabels data, so fix the rule or the GRIB (e.g. with `grib_set`);
- in `native` mode every query key must be present in the message (see below);
- it must not hold the same field twice.

All of these fail with "nothing was archived": FDB is untouched.

After archiving, the plugin checks that FDB now returns every message for the query.
This is the only step that can leave data behind: if a message still landed outside the
query (the checks above skip `to`/`by` ranges and values they cannot compare), the error
names it and the keys that put it there, for example for a `step=0/to/12/by/6` query

```text
... 1 landed outside the query or are duplicates: message 3 (step=18)
(they stay in FDB until the next successful store masks them)
```

Those fields stay in FDB until a later successful store masks them.

Editing an output query leaves the fields the rule archived under the old query in FDB:
nothing masks them (their keys differ), `fdb purge` does not reclaim them and nothing in
the run mentions them, so every edit of an output query leaks a full copy of the data.
Prefer a wildcard or a config value over rewriting the query of a rule that has already
run.

Two rules whose output queries overlap (`step=0/6/12` and `step=0` of the same
experiment) are accepted without a word: Snakemake compares query strings, FDB identity
is per field, so the DAG has no edge between them and the jobs mask parts of each
other's output. Keep to **one field, one rule**; per-step and whole-forecast rules over
the same fields are the usual accident.

When a job has several FDB outputs and a later one fails its checks, the earlier ones
are already archived and stay in FDB, even though Snakemake reports the outputs as
removed. A retry re-archives them, masking the first copy.

Archiving reads the whole output file into memory and keeps it several times over: a
205 MB file with 20000 messages peaked at about 900 MB resident, roughly four times the
file. Give archiving jobs a `resources: mem_mb` to match (retrieval, by contrast,
streams).

### Archive modes

- `archive_mode=native` (default): FDB derives the keys from each message and picks the
  matching schema rule. Since every key comes from the message, every key of the query
  must be in the message; a query naming a key the GRIB lacks is an error:

  ```text
  message 1 of <local> lacks quantile, which native archiving takes from the message;
  use archive_mode=identifier to label it, or drop the key from the query
  ```

  (A key the schema drops, such as `domain` under a schema that writes `domain-`, or a
  key the schema does not know at all, is not required; nor is any key when the schema
  is not readable locally.) Use this mode unless you have a reason not to.
- `archive_mode=identifier`: the plugin builds each message's FDB key from the schema,
  the query and the message. A query key the message does not carry labels it, so this
  is the mode for `quantile=1:10` on GRIB without a quantile. Query values are archived
  in canonical spelling. Use this mode only with schemas whose rules share one key set,
  such as GRIB that lacks a key the schema requires; with multi-rule schemas it fails
  with `cannot determine <key>`.

`identifier_check` is reserved for a stricter identifier check; only `none` is accepted.

### Reruns

When Snakemake reruns a job, the new fields mask the old ones in FDB: the output still
exists, its modification time advances and reads return the new data. Masked fields use
disk space until an FDB administrator runs `fdb purge`.

## Globbing

`glob_wildcards` works on FDB patterns:

```snakemake
STEPS = glob_wildcards(
    storage.fdb(
        "fdb://class=ea,expver=0001,stream=oper,date=20200101,time=0000,domain=g,"
        "type=an,levtype=sfc,step={step},param=167"
    )
).step
```

- The plugin lists the fields matching the pattern's constant keys and reports the values
  FDB lists for the wildcard keys. Keys the pattern omits are not restricted.
- Fields that lack a wildcard key, or list it empty (e.g. `number` of a control member),
  are skipped.
- Listed values are canonical (`167`, `0000`, lower-case enums), so wildcard constraints
  must match that form.
- Keys in `glob_required_keys` (default `class`) must have constant values in the
  pattern, to prevent listing a whole FDB by accident. Set it to an empty value to allow
  any pattern.

## Canonical spelling

MARS accepts several spellings of one value: `param=2t`, `167` and `167.128`;
`date=2020-01-01` and `20200101`; `time=0`, `00` and `0000`; `class=EA` and `ea`. FDB
finds the same fields for all of them, but each spelling gives a different local path,
so Snakemake treats them as different files.

**Use the spelling FDB lists back**: numeric paramIds, lower-case enum values, `YYYYMMDD`
dates, `HHMM` times, four-character `expver`. The plugin checks this at run time: by
default it logs, once per query,

```text
Query fdb://...,param=2t uses non-canonical spelling: param=2t (canonical: 167). Use canonical spellings to avoid duplicate local paths for the same field.
```

Set `canonical_spelling=error` to make it an error, or `ignore` to silence it. Values with
`to`/`by` ranges and wildcards are not checked.

Spelling is not the only way two queries can name one field set, and the check covers
values only. The order of a value list (`step=12/6/0` vs `0/6/12`), a repeated value,
`step=0/to/12/by/6` versus `step=0/6/12`, and MARS **key** aliases (`levtyp=` for
`levtype=`, `parameter=` for `param=`) each give a second storage object with its own
local path, its own retrieval and no warning; aliased keys are unknown to the schema and
sort to the end of the key order. Relative dates such as `date=-1` work — metkit expands
them at run time, and the spelling warning names the date they expanded to — but the
local path keeps the text `-1`, so a local copy kept with
`--keep-storage-local-copies` is silently stale the next day. Write queries the way FDB
lists them back, and write them the same way everywhere.

## Removing outputs

FDB cannot delete individual fields, so the plugin never deletes anything. In Snakemake
9.27, `--delete-all-output` is the only thing that asks the plugin to remove an FDB
output; `remove_policy=warn` (default) then logs once per query:

```text
FDB cannot delete individual fields; existing fields for <query> will be masked by the next archive. Use `fdb purge` to reclaim space.
```

`remove_policy=ignore` stays silent; `remove_policy=error` makes removal an error.

Neither rerunning a job nor a failed job removes anything: no `remove()` call, no
warning, and Snakemake's own `Removing output files of failed job ... (in storage)` line
removes nothing from FDB. `temp()` outputs cannot occur — `temp(storage.fdb(...))` is a
`SyntaxError` ("Storage and temporary flags are mutually exclusive"), as are
`protected()` and `directory()`, and `pipe()` gives "Pipes may not be in storage".

`--delete-all-output` therefore deletes local outputs and leaves the FDB fields in
place, and because the FDB output still exists the producing job is **not** rerun
afterwards. Use `--forceall`/`--forcerun` for a rebuild; the new fields mask the old
ones.

## Snakemake flags and features

Most of Snakemake works unchanged with FDB objects; these are the exceptions.

- **`--touch`** is refused for the *whole* workflow if a single output is an FDB query:
  `Touching output files is impossible. The workflow uses remote storage but the storage
  plugin does not support the touch operation.` Local outputs of that workflow cannot be
  touched either.
- **FDB queries cannot be command-line targets**, because Snakemake sends targets
  through path normalisation and `fdb://...` becomes `fdb:/...`
  (`MissingRuleException: No rule to produce fdb:/class=...`). Drive such a workflow by
  rule name, or let the last rule write a small local sentinel file. For the same reason
  `--cleanup-metadata "fdb://..."` reports that the metadata was not present, and an FDB
  output's metadata cannot be cleaned up.
- **`ensure(non_empty=True)` on an FDB output always fails** with `Detected unexpected
  empty output files`: Snakemake asks the storage object for its size, which is 0 before
  the store, instead of measuring the local file the rule just wrote.
- **`--not-retrieve-storage`** hands the job the local path of the input without
  retrieving it, so the job fails on a file that does not exist.
- `touch()`, `ancient()` and `report()` work on FDB objects; `temp()`, `protected()`,
  `directory()` and `pipe()` are rejected (see
  [Removing outputs](#removing-outputs)), and `multiext()` silently drops the storage
  flag (see [Several fields in one rule](#several-fields-in-one-rule)).

## Site definitions and MARS language

Sites with their own GRIB conventions set up the plugin with generic settings:

- `eccodes_definitions`: colon-separated definitions directories, prepended to
  `ECCODES_DEFINITION_PATH` in the given order (the first match wins);
- `metkit_home`: a directory with `share/metkit/language.yaml`, a MARS language that
  accepts extra values (e.g. site `model` values); exported as `METKIT_HOME`;
- `key_order`: the key order, if the FDB schema is not readable locally;
- `env`: other environment variables, `NAME=VALUE[,NAME=VALUE]`.

Exporting `ECCODES_DEFINITION_PATH` and `METKIT_HOME` in the shell or profile works just
as well; the plugin then needs no site settings. Reading fields needs the MARS language
but not the definitions; archiving and decoding GRIB keys need both. Every process that
touches FDB sets this up for itself, so with a `conda:` or `container:` directive the
environment or image must contain `pyfdb`, `eccodes` and the site definitions, and with
a cluster or remote executor every node must see the definitions, the metkit home and
the FDB configuration at the configured paths.

Two caveats:

- **Set definitions before eccodes loads.** The plugin exports its settings before it
  imports the FDB and eccodes libraries. If the Snakefile itself imports `eccodes` at top
  level, the libraries may load first and the setting may not take effect. Prefer
  exporting the variables before Snakemake starts.
- **Definitions that print warnings.** Site definitions may write to stderr for every
  decoded message. The COSMO definitions print a harmless version warning (silence it
  with `ECCODES_VERSION_CHECK_OFF=1` in the shell or the `env` setting) and open
  `/dev/stderr` as a file, which truncates a stderr redirected to a shared log. Give
  decoding jobs their own log file.

MeteoSwiss users: see [`sites/meteoswiss.md`](sites/meteoswiss.md) for the definitions,
MARS language, schema, profile and query conventions.

## A local development FDB

The [README quick start](../README.md#quick-start) creates a development FDB in a
checkout and runs the example workflow against it. `scripts/init_dev_fdb.py` creates
`.fdb/` (configuration, schema `tests/data/schema`, database root), archives the ECMWF
sample files from `tests/data/grib/ecmwf/` (`--seed`) and small variants of
`tests/data/grib/ecmwf/template.grib` (`--variants`). `--root`, `--schema`, `--seed DIR`
and `--variants FILE` choose other locations; see the
[reference](reference.md#scriptsinit_dev_fdbpy). The configuration uses absolute paths,
so `--storage-fdb-config /path/to/checkout/.fdb/config.yaml` works from any directory.
[`examples/ecmwf/README.md`](../examples/ecmwf/README.md) explains the example workflow.

## Troubleshooting

| symptom | cause and fix |
|---|---|
| `Invalid MARS request ...: TypeEnum[name=...]: cannot expand '<value>' (if this value is valid ...)` | A typo, or a value your MARS language does not define (e.g. a site `model`). Fix the value or set `metkit_home`. |
| `Invalid MARS request ...: Key [number] not acceptable with context ...` | The key is not valid with the other values (e.g. `number` with `type=cf`). Remove it. |
| `...: 0 of N fields found in FDB ... optional schema keys not in the query: ...` | The fields carry keys the query does not name. Add them (`domain=g`, `number=...`, `timespan=fs`). Only visible with `--verbose`, or when a rule retrieves the query. |
| `...: n of N fields found in FDB; missing: ...` | Some fields are not archived; the message names them. Snakemake still reports the whole query as a missing input. |
| A query you know is complete is reported partial, or missing | A database directory under the FDB root may be unreadable: FDB skips it silently. Check the permissions of the `root/<class>:<expver>:...` directories. |
| `FDB I/O error for ...: ... (check permissions, free space and the roots ...)` | The FDB root is unreadable, read-only or full. Check the roots in the configuration, the permissions and the free space. |
| `FDB configuration error: Cannot open .../fdb5lib/etc/fdb/schema ... (no FDB configuration was given ...)` | No FDB configuration reached the plugin. Set `--storage-fdb-config`, `SNAKEMAKE_STORAGE_FDB_CONFIG` or `FDB_CONFIG_FILE`. |
| `FDB configuration error: '...' is neither an existing file nor an inline YAML mapping` | Wrong path. Relative paths resolve against Snakemake's working directory (`-d`), not the directory the command was typed in. If the message ends with "looks like a tagged setting mangled by a spawned job", see [Tagged settings and spawned jobs](#tagged-settings-and-spawned-jobs). |
| `FDB configuration error: Cannot open ...` / `No writable roots available ...` | The schema file or database root in the FDB configuration does not exist. |
| `GRIB keys do not match the FDB schema for ...: Keywords not used: {number}` | The GRIB carries a key the schema does not accept. Use a schema with that key (e.g. `number?`), or `archive_mode=identifier`. |
| `...: cannot determine <key> for message 1 ...` | Identifier mode with a multi-rule schema. Use `archive_mode=native` or add the key to the query. |
| `...: message 1 of ... has step=0, but the query has step=6 ...` | The output GRIB does not match its query. Fix the rule or the GRIB. |
| `... landed outside the query or are duplicates: message 3 (step=18) ...` | Archived messages have keys outside the output query; the message names them. Fix the rule; the stray fields are masked by the next successful store. |
| `... has 2 fields, the query expands to 3; nothing was archived` | The output file has the wrong number of fields. Fix the rule: a partial store could never satisfy the query anyway. |
| `... message 1 of ... lacks quantile, which native archiving takes from the message ...` | The query names a key the GRIB does not carry. Set it in the GRIB, drop it from the query, or use `archive_mode=identifier`. |
| An input with a key the fields lack is reported missing | Correct: `quantile=1:10` on fields without a quantile does not exist, even though FDB's `inspect` matches through the key. Drop the key or archive the fields with it. |
| `Query ... uses non-canonical spelling: ...` | Use the canonical value shown (see [Canonical spelling](#canonical-spelling)). |
| `FDB glob pattern ... needs constant values for class (glob_required_keys)` | Give `class` a constant value, or change `glob_required_keys`. |
| `METKIT_HOME=... has no share/metkit/language.yaml ...` | Point `metkit_home` or `METKIT_HOME` at a complete metkit home (FDB would hang otherwise). |
| `WARNING: definitions.edzw version ... is NOT compatible ...` | Harmless; set `ECCODES_VERSION_CHECK_OFF=1`. |
| A log file is truncated or full of NUL bytes | A job decoded GRIB with the COSMO definitions while its stderr went to that file; use a per-job log. |
| Site definitions seem to be ignored | `eccodes` was imported before the provider was set up; export `ECCODES_DEFINITION_PATH` before starting Snakemake. |
| `--delete-all-output` leaves data in FDB | By design, and the producing job is not rerun afterwards (see [Removing outputs](#removing-outputs)). Use `--forceall`. |
| `Touching output files is impossible ...` | `--touch` is not supported and the check covers the whole workflow (see [Snakemake flags and features](#snakemake-flags-and-features)). |
| `MissingRuleException: No rule to produce fdb:/...` | An FDB query was used as a command-line target; Snakemake normalised it. Target the rule by name or a local file. |
| `Flags ({'storage_object': ...}) ... given to expand() are invalid` | `expand()` was applied outside `storage.fdb(...)`; swap them (see [Several fields in one rule](#several-fields-in-one-rule)). |
| `Detected unexpected empty output files ...` for an FDB output | `ensure(non_empty=True)` cannot work on FDB outputs; drop it. |
| "Nothing to be done" although the FDB inputs are gone | Snakemake only re-evaluates inputs of jobs it already plans to run, so a workflow whose inputs were wiped (retention, `fdb wipe`) while its outputs exist reports success. Force the rerun, or check the inputs yourself. |

## Limitations

- Not usable as `--default-storage-provider`; files only; no `--touch` (refused for the
  whole workflow); FDB queries cannot be command-line targets.
- No deletion; reruns mask old fields.
- Modification times have one-second resolution.
- Remote FDB backends are untested.
- Tagged settings are lost in spawned jobs (Snakemake 9.27).
- An unreadable database directory inside an FDB root looks like missing data.

The full list is in [`design/requirements.md`](design/requirements.md) §5.
