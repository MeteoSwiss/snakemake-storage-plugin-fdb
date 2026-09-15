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
(see [Writing queries](#writing-queries)). A relative path resolves against the
directory Snakemake runs in.

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

Or in the Snakefile:

```snakemake
storage:
    provider="fdb",
    config="/path/to/fdb/config.yaml",
```

The Snakefile then refers to the provider as `storage.fdb("fdb://...")`, or
`storage("fdb://...")` without naming it.

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
nor an inline YAML mapping`. This is an upstream Snakemake bug. Affected are `run:`
rules with the local executor, and every job with cluster or remote executors. Until it
is fixed:

- pass settings **untagged** for workflows with `run:` rules or cluster execution (as
  `examples/ecmwf/` does); or
- use `shell:` rules with the local executor, which run in the main process (as
  `examples/meteoswiss/` does); or
- put the settings in the Snakefile's `storage` directive, which every job re-reads.

## Writing queries

```text
fdb://class=od,expver=0001,stream=oper,date={date},time=0000,type=fc,levtype=sfc,step=0/6/12,param=167
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
.snakemake/storage/fdb/class=od/expver=0001/stream=oper/date=20240101/time=0000/type=fc/levtype=sfc/step=0+6+12/param=167.grib
```

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

- The input exists only when **all** fields of the query are in FDB (3 here). If some are
  missing, Snakemake treats the input as missing; retrieving it fails with a message
  such as `...: 2 of 3 fields found in FDB; missing: step=12`. If nothing matches, the
  message lists optional schema keys the query does not name.
- The local file holds the messages in request order (e.g. step 0, 6, 12).
- The input's modification time is the time FDB last flushed the index holding the
  fields (one-second resolution), so rules rerun when inputs are re-archived.
- An invalid request (unknown key or value, `number` with `type=cf`) is an error, not a
  missing input.
- Retrieval is atomic: a failed transfer never leaves a partial local file. Transient
  FDB errors are retried.

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
- with `store_check=strict` (default) it must hold exactly as many fields as the query
  expands to; `store_check=warn` allows fewer and logs a warning;
- it must not hold the same field twice.

Everything that fails here says "nothing was archived". After archiving, the plugin
checks that FDB now returns every message for the query. If not, some messages carry
keys outside the query; they stay in FDB until a later successful store masks them, and
the error says so.

### Archive modes

- `archive_mode=native` (default): FDB derives the keys from each message and picks the
  matching schema rule. Use it unless you have a reason not to.
- `archive_mode=identifier`: the plugin builds each message's FDB key from the schema,
  the query and the message. Constant query values are checked against the message
  first (a `step=6` query rejects a `step=0` message; the plugin never relabels data, so
  fix the GRIB instead, e.g. with `grib_set`). A query key the message does not carry
  labels it (e.g. `quantile=1:10`). Query values are archived in canonical spelling. Use
  this mode only with schemas whose rules share one key set, such as GRIB that lacks a
  key the schema requires; with multi-rule schemas it fails with `cannot determine <key>`.

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

## Removing outputs

FDB cannot delete individual fields, so the plugin never deletes anything. When Snakemake
removes an FDB output (before rerunning a job, after a failed job, with
`--delete-all-output`, for `temp()` outputs), `remove_policy=warn` (default) logs once
per query:

```text
FDB cannot delete individual fields; existing fields for <query> will be masked by the next archive. Use `fdb purge` to reclaim space.
```

`remove_policy=ignore` stays silent; `remove_policy=error` makes removal an error.
`--delete-all-output` therefore deletes local outputs but leaves FDB fields in place.
`--touch` is not supported for FDB outputs.

## Site definitions and MARS language

Sites with their own GRIB conventions set up the plugin with generic settings:

- `eccodes_definitions`: colon-separated definitions directories, prepended to
  `ECCODES_DEFINITION_PATH` in the given order (the first match wins);
- `metkit_home`: a directory with `share/metkit/language.yaml`, a MARS language that
  accepts extra values (e.g. site `model` values); exported as `METKIT_HOME`;
- `key_order`: the key order, if the FDB schema is not readable locally;
- `env`: other environment variables, `NAME=VALUE[,NAME=VALUE]`.

Exporting `ECCODES_DEFINITION_PATH` and `METKIT_HOME` in the shell or profile works just
as well; the plugin then needs no site settings.

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
| `...: 0 of N fields found in FDB ... optional schema keys not in the query: ...` | The fields carry keys the query does not name. Add them (`domain=g`, `number=...`, `timespan=fs`). |
| `...: n of N fields found in FDB; missing: ...` | Some fields are not archived; the message names them. |
| `FDB configuration error: '...' is neither an existing file nor an inline YAML mapping` | Wrong path (relative to the working directory). If the value looks like `tag:path`, see [Tagged settings and spawned jobs](#tagged-settings-and-spawned-jobs). |
| `FDB configuration error: Cannot open ...` / `No writable roots available ...` | The schema file or database root in the FDB configuration does not exist. |
| `GRIB keys do not match the FDB schema for ...: Keywords not used: {number}` | The GRIB carries a key the schema does not accept. Use a schema with that key (e.g. `number?`), or `archive_mode=identifier`. |
| `...: cannot determine <key> for message 1 ...` | Identifier mode with a multi-rule schema. Use `archive_mode=native` or add the key to the query. |
| `...: message 1 of ... has step=0, but the query has step=6 ...` | The output GRIB does not match its query. Fix the rule or the GRIB. |
| `... landed outside the query or are duplicates ...` | Archived messages have keys outside the output query. Fix the rule; the stray fields are masked by the next successful store. |
| `... has 2 fields, the query expands to 3; nothing was archived` | The output file is incomplete. Fix the rule, or set `store_check=warn`. |
| `Query ... uses non-canonical spelling: ...` | Use the canonical value shown (see [Canonical spelling](#canonical-spelling)). |
| `FDB glob pattern ... needs constant values for class (glob_required_keys)` | Give `class` a constant value, or change `glob_required_keys`. |
| `METKIT_HOME=... has no share/metkit/language.yaml ...` | Point `metkit_home` or `METKIT_HOME` at a complete metkit home (FDB would hang otherwise). |
| `WARNING: definitions.edzw version ... is NOT compatible ...` | Harmless; set `ECCODES_VERSION_CHECK_OFF=1`. |
| A log file is truncated or full of NUL bytes | A job decoded GRIB with the COSMO definitions while its stderr went to that file; use a per-job log. |
| Site definitions seem to be ignored | `eccodes` was imported before the provider was set up; export `ECCODES_DEFINITION_PATH` before starting Snakemake. |
| `--delete-all-output` leaves data in FDB | By design (see [Removing outputs](#removing-outputs)). |
| `--touch` fails for FDB outputs | Not supported. |

## Limitations

- Not usable as `--default-storage-provider`; files only; no `--touch`.
- No deletion; reruns mask old fields.
- Modification times have one-second resolution.
- Remote FDB backends are untested.
- Tagged settings are lost in spawned jobs (Snakemake 9.27).

The full list is in [`design/requirements.md`](design/requirements.md) §5.
