# User guide

How to use the Snakemake storage plugin for ECMWF's Fields DataBase (FDB) in workflows.
Exact settings, messages and method behaviour are in the [reference](reference.md);
complete, runnable workflows for the usual cases are in the
[usage patterns](patterns.md).

- [Concepts](#concepts)
- [Installation](#installation)
- [Configuration](#configuration)
- [Writing queries](#writing-queries)
- [Reading inputs](#reading-inputs)
- [Direct access from run and script rules](#direct-access-from-run-and-script-rules)
- [Writing outputs](#writing-outputs)
- [Reruns](#reruns)
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
  fields into it and the plugin archives them into FDB. `run:` and `script:` rules can
  skip that file and work
  [directly on FDB](#direct-access-from-run-and-script-rules).
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

Run it with a target (`snakemake ... t2m/20200101.grib`) or list the targets in a
`rule all` at the top of the Snakefile. Snakemake builds the first rule when no target
is given: a first rule with wildcards fails with `Target rules may not contain
wildcards`, and a first rule that is not the collecting rule builds only itself.

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

A `run:` or `script:` rule does not need that local copy at all: see
[direct access](#direct-access-from-run-and-script-rules).

## Direct access from run and script rules

A rule whose body is Python can read the fields straight from FDB and archive straight
into FDB, with no GRIB file on the local filesystem. That is the point of keeping data
in FDB: nothing is staged under `.snakemake/storage`, and jobs work on messages in
memory instead of writing and re-reading a file.

Jobs do this with **plain `pyfdb`, `eccodes` or `earthkit-data`**: a rule body or a
script never imports the plugin. The plugin's place is the Snakefile, where it declares
what a rule reads and writes, and the provider, which puts its FDB configuration into
the job environment so that an unconfigured `pyfdb.FDB()` or
`earthkit.data.from_source("fdb", ...)` opens the workflow's FDB.
[`examples/forecast-evaluation/`](../examples/forecast-evaluation/README.md) is a
runnable workflow whose every rule works this way.

Two consequences of using the plain libraries:

- **Which settings reach such a job.** `config` and `user_config` do, through the
  environment the provider exports (`FDB5_CONFIG`, `FDB_CONFIG_FILE`), so an
  unconfigured `pyfdb.FDB()` opens the workflow's FDB. `canonical_spelling`, `key_order`
  and `glob_required_keys` apply to the queries, which the plugin handles in the
  Snakefile process, so they are enforced whatever the job does. `archive_mode` and
  `identifier_check` describe the plugin's own store step, which never runs for a
  `retrieve=False` output: `--storage-fdb-archive-mode identifier` has no effect at all
  on a job archiving with plain `pyfdb`. Only [`api.archive`](#the-optional-api-module)
  reads them in a job.
- **Which errors such a job raises.** They are fdb5's and eckit's own text, not the
  plugin's mapped messages: an unwritable FDB root gives `Failed system call: mkdir
  /…/root/ea:0002:… (Success)` where the plugin would say `FDB I/O error for fdb://…:
  … (check permissions, free space and the roots in the FDB configuration)`. The
  `(Success)` suffix is eckit's `errno` reporting and means nothing. Use
  `api.messages`/`api.archive` where the mapped errors are wanted.

### Declaring the fields

The rule is the same on both sides: **an FDB object the job reads or writes itself is
declared `retrieve=False`**, input or output alike. For an output that means "expect no
local file, look the fields up in FDB after the job" (see
[Archiving in the job](#archiving-in-the-job)).

For an input, Snakemake hands the job the **query string** instead of a local path,
wildcards filled in. The job derives its MARS request from that string in two lines — strip the scheme, split on `,` and `=` — and hands the dict to
pyfdb or earthkit-data as it is. Values with `/` lists or `to`/`by` ranges stay strings;
both libraries take them as MARS lists:

```python
query = input[0].removeprefix("fdb://")  # "class=ea,...,step=0/6/12,param=167"
request = dict(item.split("=", 1) for item in query.split(","))
```

That keeps one description of the fields, the query in the Snakefile, for the DAG and
for the job. Do not pass the request through `params:` instead: anything in `params`
takes part in Snakemake's params rerun trigger, so editing it reruns the job even where
the field-based decision of [Reruns](#reruns) would not. (`api.request(query)` does the
same parse plus metkit's expansion, for the cases that need canonical values.)

### Reading in the job

```snakemake
storage:
    provider="fdb"


rule steps:
    input:
        storage.fdb(
            "fdb://class=ea,expver=0001,stream=oper,date=20200101,time=0000,domain=g,"
            "type=an,levtype=sfc,step=0/6/12,param=167",
            retrieve=False,
        ),
    output:
        "steps.txt",
    run:
        import eccodes
        import pyfdb

        query = input[0].removeprefix("fdb://")
        request = dict(item.split("=", 1) for item in query.split(","))
        with pyfdb.FDB().retrieve(request) as source:
            data = source.read()  # every field of the request, one buffer
        with open(output[0], "w") as f:
            for message in eccodes.MemoryReader(data):
                print(message.get("step"), file=f)
```

`pyfdb.FDB()` needs no configuration: the provider exports its configuration as YAML
text in `FDB5_CONFIG` (and, for a configuration file, its path in `FDB_CONFIG_FILE`
too) before the FDB libraries load, and Snakemake carries the plugin's own settings into
every job as `SNAKEMAKE_STORAGE_FDB_*`. A configuration the environment already carries
is never overwritten, and providers of one process with different configurations (tagged
providers) export nothing — such a workflow must configure its jobs itself. FDB has no
environment variable for a `user_config`, so a plain `pyfdb.FDB()` does not see one.

With earthkit-data installed the same read is one call, and no argument either: its
`fdb` source reads `FDB5_CONFIG`.

```python
from earthkit.data import from_source

fields = from_source("fdb", request).to_fieldlist()
values = fields[0].to_numpy()
```

`eccodes.MemoryReader(data)` walks the messages of the retrieved buffer;
`eccodes.StreamReader` cannot take the pyfdb handle directly (its `read(n)` pads short
reads). The whole retrieval is in memory this way; `api.messages(query)` (below) streams
one message at a time where that matters. Whatever FDB holds is returned: the job's read
is not checked against the query. The plugin has already checked that all fields exist
before the job started — an FDB input exists only when every field of its query is in
FDB — so a job that reads exactly its declared request gets exactly those fields.

### Archiving in the job

Archive with `pyfdb.FDB().archive(...)`, flush before the job ends, and declare the
output `retrieve=False` too:

```snakemake
QUERY = (
    "fdb://class=ea,expver={expver},stream=oper,date=20200101,time=0000,domain=g,"
    "type=an,levtype=sfc,step=0/6/12,param=167"
)


rule shift_expver:
    input:
        storage.fdb(QUERY.format(expver="0001"), retrieve=False),
    output:
        storage.fdb(QUERY.format(expver="0002"), retrieve=False),
    run:
        import eccodes
        import pyfdb

        query = input[0].removeprefix("fdb://")
        request = dict(item.split("=", 1) for item in query.split(","))
        fdb = pyfdb.FDB()
        with fdb.retrieve(request) as source:
            data = source.read()
        for message in eccodes.MemoryReader(data):
            message.set("expver", "0002")
            fdb.archive(message.get_buffer())
        fdb.flush()
```

A `retrieve=False` output needs no local file at all: after the job Snakemake asks the
plugin whether every field of the query is in FDB, instead of waiting for a file at the
output's path (`--latency-wait` bounds that wait as usual). Nothing is written under
`.snakemake/storage`, nothing is removed afterwards, and the plugin's store step never
runs for such an output. The DAG is unaffected: the query links this rule to the rules
that read it ("Input files updated by another job"), a missing field is reported as
`Missing output files: fdb://... (in storage)`, and a second run reports "Nothing to be
done".

`flush()` before the job ends is still the rule to follow: what is not flushed is not in
FDB, for the check and for the jobs downstream. fdb5 also flushes when the `FDB` object
is destroyed, so a job process that exits normally is usually safe in practice — but do
not rely on it: a job that reads back what it archived, or an executor that keeps the
process alive, needs the explicit call.

A rule may have a direct FDB output and a local file output at the same time. That is
allowed, and the two are checked independently: the fields can be in FDB without the
local file (nothing reruns the job, because nothing needs that file), and asking for the
local file rebuilds it and re-archives the fields along the way.

If such an output is declared *without* `retrieve=False` and without `touch()`,
Snakemake waits for a local GRIB file no one writes and the job fails with `(missing
locally, parent dir contents: )` after the latency wait. The message names no FDB cause
at all: for an `fdb://` output it means the declaration is wrong, not that the latency is
too short.

Existence is all that is checked. A job that exits 0 having archived nothing — or the
wrong fields — passes whenever FDB happens to hold the query's fields already, for
instance from an earlier run of the same workflow. Where a run must prove that *it*
wrote the fields, use the checked variant.

#### The checked variant: `touch()`

`touch(storage.fdb(query))` keeps the plugin's store step in the loop. Snakemake then
requires a local file at the output's path after the job and `touch()` creates an empty
one; **an empty file is the convention**: it tells the store step that the job archived
the fields itself. The store step archives nothing and checks that every field of the
query is in FDB with an index timestamp from this run — the run's reference time is
taken when the provider is built, at the start of the workflow. A job that archived the
wrong or too few fields, or nothing at all, fails there:

```text
fdb://class=ea,expver=0002,...,step=0/6/12,param=167: .snakemake/storage/... is empty,
so the job is taken to have archived the fields itself; 2 of 3 found in FDB with
timestamps from this run; missing or older: step=12
```

Four things to know about this check:

- Nothing is checked **before** the job's archives, unlike a file-based output or
  `api.archive`. Fields a mistaken job archived are in FDB; the message says which of
  the query's fields are missing, and the next successful run masks what was written.
- **Extra fields are never reported**, in either direct variant: a job that archives a
  `step=18` field under a `step=0/6/12` query exits 0 and the stray field stays in FDB
  for good. A file-based store would refuse the file (`landed outside the query or are
  duplicates`); the check here only asks whether the query's own fields are there.
- The check only runs for jobs that **run**. It cannot detect fields an earlier, failed
  run left behind: if those happen to satisfy the query, the DAG never schedules the
  producer in the first place and no check of any kind takes place.
- The check is by timestamp, not by identity: a field another job of the same run
  archived under the same query would satisfy it. `flush()` before the job ends, or the
  fields may not be visible to the store step yet.

`--keep-storage-local-copies` keeps the empty files like any other local copy; otherwise
Snakemake removes them at the end of the run. That is the price of the check: with
`retrieve=False` nothing is created locally in the first place.

### The optional `api` module

`api` is a convenience for the Snakefile and for jobs that want the plugin's own checks;
nothing in it is required. It is imported as
`from snakemake_storage_plugin_fdb import api`, and every call taking a query takes
`config=` and `user_config=` where the environment does not name the FDB.

| call | gives |
|---|---|
| `api.query(request)` | the query string of a MARS request (dict), with `/`-joined values; for a Snakefile that keeps its requests as dicts (wildcards allowed as values) |
| `api.request(query)` | the expanded MARS request of a query in canonical spelling, ready for `pyfdb` or earthkit-data |
| `api.messages(query)` | the GRIB messages of a query, one at a time, with a retrieval's guarantees |
| `api.archive(output, messages)` | a pre-checked archive that leaves an archive marker |
| `api.query_of(path)`, `api.read_marker(path)`, `api.Marker` | the local path mapping and the marker format |

`api.messages` is the read with the plugin's guarantee: all fields of the query must be
in FDB, otherwise it raises the same missing-field error as a retrieval, and a stream
that ends early is an error too. It holds one message at a time.

`api.archive(output, messages)` is the archive with the plugin's checks: field count,
every message's keys against the query and duplicates are checked **before** the first
archive, so a failure archives nothing, and the post-check runs in the job, naming the
offending messages. It writes an **archive marker** at the output's local path instead of
an empty file, so such an output keeps the plain declaration — `storage.fdb(...)`,
neither `touch()` nor `retrieve=False`: the job needs that local path to write the marker
at, and the store step has to run to read it:

```text
# snakemake-storage-plugin-fdb archived
query: fdb://class=ea,expver=0002,...,step=0/6/12,param=167
fields: 3
time: 1789581415
```

The store step recognises the marker, archives nothing and post-checks with the marker's
field count and timestamp; a marker for another query, or with a field count other than
the query's, is an error. **`api.archive` archives as the workflow does** — `archive_mode`, `identifier_check`,
`canonical_spelling` and `key_order` reach it through the `SNAKEMAKE_STORAGE_FDB_*`
variables Snakemake carries into every job — so a workflow run with
`--storage-fdb-archive-mode identifier` labels these archives in identifier mode too;
`api.archive(..., archive_mode=...)` overrides it for one call. A job archiving with
plain `pyfdb` reads none of those settings (see
[Direct access](#direct-access-from-run-and-script-rules)).

### When a local file is still needed

A `shell:` rule that hands the fields to an external program needs a file: leave input
and output plain (no `retrieve=False` on either), and the plugin retrieves and archives
as usual. Both styles mix freely in one rule — one retrieved input and one direct input
is fine.

## Writing outputs

A rule output is a query; the rule writes a GRIB file with exactly those fields, and the
plugin archives it (a `run:` or `script:` rule can skip the file and archive the fields
itself, see [direct access](#direct-access-from-run-and-script-rules)):

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

## Reruns

Whether a rule with FDB inputs reruns is decided by the **fields**, not by the text of
its queries: by whether the fields are there, whether any of them is newer than the
output, and whether the query now names fields the job did not have last time. Rewriting
a query that names the same fields — or fewer — does not rerun anything.

| change | rerun? |
|---|---|
| query narrowed (`param=167/165` → `param=167`) | no: the remaining fields are unchanged |
| values reordered, or `0/6/12` written as `0/to/12/by/6` | no |
| `param=2t` written for `param=167` | no: the same field |
| one query split into one query per parameter, or merged back | no |
| query widened (`param=167` → `param=167/165`) | yes: a field the job did not have, even if it is older than the output or has yet to be produced |
| a field of the query re-archived in FDB | yes: the index timestamp advances |
| an FDB input added to the rule | yes, unless its fields are among those already recorded |
| an FDB input removed from the rule | no |
| the output re-archived by someone else | no: only consumers of that output rerun |
| a local input of the same rule changed or added | yes, as always in Snakemake |

The `params`, `code` and `software-env` triggers are untouched: they describe the rule,
not its inputs.

One consequence is not flagged: after narrowing, the output keeps the extra fields the
wider query produced, because nothing reruns. Force the rule (`snakemake -R <rule>` or
`--force <target>`) if the output must match the new query. The same holds for **derived
local outputs** computed straight from FDB inputs — a metrics table, a plot, a report:
after narrowing they still describe the wider set, and nothing says so. The remedy is a
shape, not a setting: let the local artefacts mirror the declared granularity (one file
per field or per parameter) and let the summary aggregate those *local* files with
`expand()`. Its input set then shrinks with the declaration and Snakemake's own
input-set trigger reruns it, while the FDB producers still do not rerun.
[`examples/forecast-evaluation/`](../examples/forecast-evaluation/README.md) is built
this way. `--storage-fdb-input-tracking query` is the workflow-wide alternative, at the
price of rerunning every job whose query text changed.

Rerun decisions need Snakemake's **provenance records**. In a workflow whose
intermediates all live in FDB there is no local file whose absence would force a
rebuild, so without those records — a fresh clone, a deleted `.snakemake/`, a working
directory moved under the `db` backend — a genuinely missing FDB field is reported as
"Nothing to be done" rather than rebuilt, because no consumer is out of date. (Snakemake
behaves the same way for local intermediates; it is just more visible here.) Run
`--forceall` or `-R <rule>` once after such a move.

`--storage-fdb-input-tracking query` restores Snakemake's default behaviour, in which
any change to the text of the input queries reruns the job. The plugin needs a private
Snakemake API for the default (`lookup`); when that is unavailable it logs
`input tracking by lookup is unavailable with snakemake <version> ...` and behaves like
`query`. After upgrading from 0.2.0, rules with FDB inputs rerun once, because 0.2.0
recorded no FDB input at all; later versions record what Snakemake records.

### Provenance backends

Rerun decisions read Snakemake's provenance metadata, and both of its backends are
supported and tested: the default file backend and `--persistence-backend db`, which
keeps the records in a database (SQLite under `.snakemake/metadata.db` unless
`--persistence-backend-db-url` names another SQLAlchemy URL). FDB queries are recorded
and read back the same way on both, so the table above holds either way; only the
default SQLite URL is tested. Two properties of the `db` backend are worth knowing:
Snakemake calls it experimental, and it keys records by the absolute path of the
workdir, so a workflow directory that is copied or moved elsewhere no longer finds its
records and reports "Nothing to be done" where the file backend would rerun. For
parallel runs — a cluster workflow whose jobs share one workdir on a network filesystem
— follow Snakemake's own advice and point `--persistence-backend-db-url` at a real
database server instead of a SQLite file on that filesystem.

When a job does rerun and writes an FDB output, the new fields mask the old ones: the
output still exists, its modification time advances and reads return the new data.
Masked fields use disk space until an FDB administrator runs `fdb purge`.

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
  rule name, let the last rule write a small local sentinel file, or list the queries as
  inputs of a first `rule all`, which works. For the same reason
  `--cleanup-metadata "fdb://..."` reports that the metadata was not present, and an FDB
  output's metadata cannot be cleaned up.
- **`ensure(non_empty=True)` on an FDB output always fails** with `Detected unexpected
  empty output files`: Snakemake asks the storage object for its size, which is 0 before
  the store, instead of measuring the local file the rule just wrote.
- **`--not-retrieve-storage`** hands the job the local path of the input without
  retrieving it, so the job fails on a file that does not exist. The per-object flag
  `storage.fdb(query, retrieve=False)` is a different thing: on an input it hands the job
  the query itself, and on an output it means "no local file, check the fields in FDB
  after the job" — which is what
  [direct access](#direct-access-from-run-and-script-rules) builds on.
- **`--summary`** describes FDB objects properly: the `fdb://` query in the output
  column and the FDB index timestamp as the date.
- **`--list-input-changes`** prints FDB inputs as their local paths
  (`.snakemake/storage/fdb/class=ea/.../param=167.grib`), not as queries, so its output
  is not usable as a query and does not match what `--summary` shows for the same
  object. The same rendering appears in a failed job's `Error in rule` block, including
  for `retrieve=False` inputs that have no local file at all; the `CalledProcessError`
  line above it shows the command that actually ran, with the query in it.
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
[`examples/ecmwf/README.md`](../examples/ecmwf/README.md) and
[`examples/forecast-evaluation/README.md`](../examples/forecast-evaluation/README.md)
explain the two example workflows.

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
| `Job ... completed successfully, but some output files are missing ... consider to increase the wait time with --latency-wait: fdb://... (in storage) (missing locally, parent dir contents: )` | A job archived its output itself but the output is declared neither `retrieve=False` nor `touch()`, so Snakemake waits for a local GRIB file that no one writes. The latency is not the problem, and the message names no FDB cause: for an `fdb://` output, `(missing locally, parent dir contents: )` means the declaration is wrong. Declare the output `storage.fdb(query, retrieve=False)` (see [Archiving in the job](#archiving-in-the-job)). |
| `RuntimeError ... Failed system call: mkdir /.../root/... (Success)` raised inside a job | An error from plain `pyfdb` in the job, not from the plugin, so it is fdb5/eckit's own text and the `(Success)` suffix means nothing. Usually the FDB root: permissions or free space. `api.messages`/`api.archive` give the plugin's mapped messages instead (see [Direct access](#direct-access-from-run-and-script-rules)). |
| A rule that failed once is silently skipped later | The archives of a failed direct-output job stay in FDB — FDB has no delete. If those fields satisfy another query, the producing rule is never scheduled again, and `touch()` cannot help because the job does not run. Force it (`-R <rule>`, `--forceall`), or archive under a fresh `expver`. |
| A failed job's `Error in rule` block names a `.snakemake/storage/...` path for an input that has no local file | Snakemake's own formatting of a `retrieve=False` input; the path never existed. The `CalledProcessError` line above shows the command that ran, with the query in it. |
| `Detected unexpected empty output files ...` for an FDB output | `ensure(non_empty=True)` cannot work on FDB outputs; drop it. |
| "Nothing to be done" although the FDB inputs are gone | Snakemake only re-evaluates inputs of jobs it already plans to run, so a workflow whose inputs were wiped (retention, `fdb wipe`) while its outputs exist reports success. Force the rerun, or check the inputs yourself. |
| A rule reruns after a query edit although the fields are unchanged | Input tracking by lookup is off; see the warning in the log and [Reruns](#reruns). |

## Limitations

- Not usable as `--default-storage-provider`; files only; no `--touch` (refused for the
  whole workflow); FDB queries cannot be command-line targets.
- No deletion; reruns mask old fields.
- Modification times have one-second resolution.
- A narrowed query does not rerun, so the output — and any local file derived from it —
  keeps the wider result (see [Reruns](#reruns)).
- Rerun decisions need the provenance records: without them a missing FDB field is
  reported as "Nothing to be done" (see [Reruns](#reruns)).
- Remote FDB backends are untested.
- Tagged settings are lost in spawned jobs (Snakemake 9.27).
- An unreadable database directory inside an FDB root looks like missing data.
- Direct access is for rule bodies that speak pyfdb (Python, or a program of your own);
  a `shell:` rule calling a program that only reads files still needs a retrieved file.

The full list is in [`design/requirements.md`](design/requirements.md) §5.
