# Usage patterns

Complete workflows for what a pipeline does with FDB: naming fields, running jobs on
them, writing fields back, and doing that again tomorrow. The
[user guide](user-guide.md) explains the concepts and the
[reference](reference.md) lists settings, grammar and messages; this document shows
them at work.

- [Query shapes](#query-shapes)
  - [One field, one file](#one-field-one-file): the smallest workflow
  - [Several fields in one file](#several-fields-in-one-file): lists and ranges, and
    the order of the messages
  - [One file per field](#one-file-per-field): `storage.fdb(expand(...))`, and when
    to prefer it
  - [Wildcards and config](#wildcards-and-config): dates from `config.yaml`
  - [Discovering what is in FDB](#discovering-what-is-in-fdb): `glob_wildcards`
  - [Queries from an input function](#queries-from-an-input-function)
- [Execution directives](#execution-directives): the same input in `shell`, `run` and
  `script` rules, and [direct access](#direct-access-no-local-files) without local files
- [Writing outputs](#writing-outputs)
  - [One rule, one query](#one-rule-one-query)
  - [One field, one rule](#one-field-one-rule): per-step jobs instead of overlapping
    queries
  - [Several FDB outputs in one rule](#several-fdb-outputs-in-one-rule)
  - [A chain through FDB](#a-chain-through-fdb): a rule reads what an earlier rule
    archived
  - [A checkpoint that writes to FDB](#a-checkpoint-that-writes-to-fdb): glob after
    archiving
  - [A key the GRIB does not carry](#a-key-the-grib-does-not-carry):
    `archive_mode=identifier`
- [Reruns in practice](#reruns-in-practice)
- [Site setup](#site-setup): profile, environment variables, two FDBs

Every Snakefile below is a complete workflow, and `tests/test_patterns.py` runs each of
them against a development FDB, so what the text claims is what the code does. Each one
runs with

```bash
snakemake --storage-fdb-config /path/to/fdb/config.yaml -c1
```

from the directory holding the Snakefile. Snakemake builds the first rule of the file
when no target is given: a first rule with wildcards fails with `Target rules may not
contain wildcards`, and a first rule that is not the collecting rule builds only itself.
Where a workflow has a `rule all`, it therefore comes first.

The examples read ERA5-style surface fields:

```text
class=ea, expver=0001, stream=oper, date=20200101|20200102|20200103, time=0000,
domain=g, type=an, levtype=sfc, step=0|6|12, param=167|165
```

`scripts/init_dev_fdb.py --seed --variants` (see the
[user guide](user-guide.md#a-local-development-fdb)) archives those fields for
`date=20200101`; the patterns test archives the other two dates itself. Against the
quick-start FDB, keep `20200101` and drop the other dates from the examples that list
them. Each example that writes uses an `expver` of its own, which is also the habit to
keep in a real FDB.

## Query shapes

### One field, one file

The smallest workflow: one field, one local GRIB file, one rule.

<!-- pattern: one-field; rerun: nothing -->
```snakemake
storage:
    provider="fdb"


rule t2m:
    input:
        storage.fdb(
            "fdb://class=ea,expver=0001,stream=oper,date=20200101,time=0000,domain=g,"
            "type=an,levtype=sfc,step=0,param=167"
        ),
    output:
        "t2m.grib",
    shell:
        "cp {input} {output}"
```

The rule gets a path under `.snakemake/storage/fdb/` holding one GRIB message. The file
is removed after the run unless `--keep-storage-local-copies` is given. A second run has
nothing to do; the job reruns when the field is archived again, or when the query is
edited to name a field the job did not have, not when the same field is written
differently (see [reruns](user-guide.md#reruns)).

### Several fields in one file

A query with `/` lists or a `to`/`by` range names several fields and still maps to one
file. Use this when the tool reads a multi-field GRIB anyway.

<!-- pattern: several-fields; expect: 12/165 12/167 0/165 0/167 6/165 6/167 -->
```snakemake
storage:
    provider="fdb"


rule keys:
    input:
        storage.fdb(
            "fdb://class=ea,expver=0001,stream=oper,date=20200101,time=0000,domain=g,"
            "type=an,levtype=sfc,step=12/0/6,param=165/167"
        ),
    output:
        "keys.txt",
    shell:
        "python keys.py {input} | tee {output}"
```

<!-- pattern: several-fields; file: keys.py -->
```python
"""Print ``step/param`` of every message of the GRIB file named on the command line."""

import sys

import eccodes

pairs = []
with open(sys.argv[1], "rb") as f:
    while (handle := eccodes.codes_grib_new_from_file(f)) is not None:
        step, param = (eccodes.codes_get_string(handle, k) for k in ("step", "paramId"))
        pairs.append(f"{step}/{param}")
        eccodes.codes_release(handle)
print(*pairs)
```

The six messages come in request order, never sorted: the values in the order the query
lists them, the last key varying fastest, so `step=12/0/6,param=165/167` prints

```text
12/165 12/167 0/165 0/167 6/165 6/167
```

`step=0/to/12/by/6` names the same fields as `step=0/6/12`, but it is a different
storage object with its own local path, so do not switch spellings in a workflow that
has already run (see [canonical spelling](user-guide.md#canonical-spelling)).

### One file per field

To hand each field to the rule as its own file, expand the query text and wrap the
results: `storage.fdb(expand(QUERY, ..., allow_missing=True))`. `expand()` outside
`storage.fdb(...)` is an error, and `multiext()` drops the storage flag silently (see
[several fields in one rule](user-guide.md#several-fields-in-one-rule)).

<!-- pattern: file-per-field -->
```snakemake
storage:
    provider="fdb"


QUERY = (
    "fdb://class=ea,expver=0001,stream=oper,date={date},time=0000,domain=g,"
    "type=an,levtype=sfc,step={step},param={param}"
)


rule all:
    input:
        "merged/20200101.grib",


rule merge:
    input:
        storage.fdb(expand(QUERY, step=[0, 6, 12], param=[167, 165], allow_missing=True)),
    output:
        "merged/{date}.grib",
    shell:
        "cat {input} > {output}"
```

`allow_missing=True` keeps `{date}` for Snakemake to fill in per job. Six queries means
six storage objects, six local files and six retrievals. Prefer this shape when

- the tool works on one field at a time;
- the fields together would not fit in the job's memory;
- a missing field should be easy to see: Snakemake's `MissingInputException` then names
  the query of the missing field, while a multi-field query is reported missing as a
  whole, with the plugin's `n of N fields found` warning naming the fields (see
  [reading inputs](user-guide.md#reading-inputs)). Either way the run stops.

One multi-field query is better when the fields belong together (one plot, one
interpolation, one statistic), because it is one FDB read instead of many. Reruns do
not differ: the job reruns when any of its fields is newer than its output, in either
shape.

### Wildcards and config

A wildcard is filled per job and must be a single MARS value in the spelling FDB uses
(`20200101`, not `2020-01-01`); lists belong in the query. Dates usually come from a
config file, so a run can be repeated for other days without editing the Snakefile.

<!-- pattern: wildcards-config -->
```snakemake
configfile: "config.yaml"


storage:
    provider="fdb"


QUERY = (
    "fdb://class=ea,expver=0001,stream=oper,date={date},time=0000,domain=g,"
    "type=an,levtype=sfc,step=0/6/12,param=167"
)


rule all:
    input:
        expand("t2m/{date}.grib", date=config["dates"]),


rule t2m:
    input:
        storage.fdb(QUERY),
    output:
        "t2m/{date}.grib",
    wildcard_constraints:
        date=r"\d{8}",
    shell:
        "cp {input} {output}"
```

<!-- pattern: wildcards-config; file: config.yaml -->
```yaml
dates:
  - 20200101
  - 20200102
  - 20200103
```

Adding a date to `config.yaml` adds one job and leaves the other dates alone. The
constraint keeps the wildcard from matching other path shapes; whatever it matches goes
into the query verbatim.

### Discovering what is in FDB

`glob_wildcards` lists FDB instead of a directory, so a workflow can process whatever is
there. The values it returns go straight back into the query.

<!-- pattern: glob-dates; expect: DATES: 20200101 20200102 20200103 -->
```snakemake
storage:
    provider="fdb"


QUERY = (
    "fdb://class=ea,expver=0001,stream=oper,date={date},time=0000,domain=g,"
    "type=an,levtype=sfc,step=0,param=167"
)
DATES = sorted(glob_wildcards(storage.fdb(QUERY)).date)


onstart:
    print("DATES:", *DATES)


rule all:
    input:
        expand("size/{date}.txt", date=DATES),


rule size:
    input:
        storage.fdb(QUERY),
    output:
        "size/{date}.txt",
    shell:
        "wc -c < {input} > {output}"
```

The pattern's constant keys restrict the listing, and `class` must be constant unless
`glob_required_keys` says otherwise. The values come back in FDB's canonical spelling
(`20200101`, `0000`, `167`), so wildcard constraints must match that form (see
[globbing](user-guide.md#globbing)). Globbing happens while the Snakefile is read, so
the DAG describes FDB as it was at that moment; to build on fields that the same run
archives, glob after a [checkpoint](#a-checkpoint-that-writes-to-fdb).

### Queries from an input function

An input function builds the queries from the wildcards, the config or anything else
Python can compute. Each returned string must be wrapped in `storage.fdb(...)`.

<!-- pattern: input-function -->
```snakemake
configfile: "config.yaml"


storage:
    provider="fdb"


QUERY = (
    "fdb://class=ea,expver=0001,stream=oper,date={date},time=0000,domain=g,"
    "type=an,levtype=sfc,step={step},param=167"
)


def steps_of(wildcards):
    """The steps this date needs, one query each."""
    steps = config["steps"][wildcards.date]
    return [storage.fdb(QUERY.format(date=wildcards.date, step=step)) for step in steps]


rule all:
    input:
        expand("daily/{date}.grib", date=config["steps"]),


rule daily:
    input:
        steps_of,
    output:
        "daily/{date}.grib",
    shell:
        "cat {input} > {output}"
```

<!-- pattern: input-function; file: config.yaml -->
```yaml
steps:
  "20200101": [0, 6, 12]
  "20200102": [0, 6]
```

The dates are quoted in the YAML on purpose: unquoted, `20200101` is an integer, and the
lookup by wildcard value fails. `--config expver=0003` has the same problem, since YAML
parses the value to `3`; the canonical-spelling warning catches that one.

Named inputs work the same way with `unpack()` of a dict.

## Execution directives

The same FDB input reaches every kind of rule body as a local file path — unless the rule
asks for the query instead and reads FDB itself, which is the
[last pattern of this section](#direct-access-no-local-files).

### `shell`

The local paths contain `=` and `+` but no spaces or shell metacharacters (see
[local path mapping](reference.md#local-path-mapping)), so `{input}` needs no quoting.
Several inputs expand to several arguments, and named inputs are addressed one by one.
Calling a program with `{input}` and `{output}`, as
[`keys.py`](#several-fields-in-one-file) above does, keeps the workflow independent of
Snakemake's Python.

<!-- pattern: shell-directive; expect: step=0/param=167.grib 236; expect: step=6/param=167.grib 236 -->
```snakemake
storage:
    provider="fdb"


QUERY = (
    "fdb://class=ea,expver=0001,stream=oper,date=20200101,time=0000,domain=g,"
    "type=an,levtype=sfc,step={step},param=167"
)


rule all:
    input:
        "pair.grib",
        "sizes.txt",


rule pair:
    input:
        first=storage.fdb(QUERY.format(step=0)),
        last=storage.fdb(QUERY.format(step=12)),
    output:
        "pair.grib",
    shell:
        "cat {input.first} {input.last} > {output}"


rule each:
    input:
        storage.fdb(QUERY.format(step=0)),
        storage.fdb(QUERY.format(step=6)),
    output:
        "sizes.txt",
    shell:
        "for f in {input}; do echo $f $(wc -c < $f); done | tee {output}"
```

### `run`

A `run:` body executes in a process Snakemake spawns, which is convenient for a few
lines of Python and has one caveat: **tagged** setting values do not survive the spawn
(Snakemake 9.27, see
[tagged settings and spawned jobs](user-guide.md#tagged-settings-and-spawned-jobs)).
Pass the settings untagged, or put them in the `storage` directive, when a workflow has
`run:` rules.

<!-- pattern: run-directive -->
```snakemake
storage:
    provider="fdb"


rule steps:
    input:
        storage.fdb(
            "fdb://class=ea,expver=0001,stream=oper,date=20200101,time=0000,domain=g,"
            "type=an,levtype=sfc,step=0/6/12,param=167"
        ),
    output:
        "steps.txt",
    run:
        import eccodes

        with open(input[0], "rb") as fi, open(output[0], "w") as fo:
            while (handle := eccodes.codes_grib_new_from_file(fi)) is not None:
                print(eccodes.codes_get_string(handle, "step"), file=fo)
                eccodes.codes_release(handle)
```

### `script`

A `script:` rule keeps the code in a file of its own, testable outside Snakemake, and is
not affected by the `run:` caveat above. The script sees the retrieved local path in
`snakemake.input`. A `notebook:` rule receives its input the same way (not exercised by
the patterns test). Both can also read FDB directly instead
([below](#direct-access-no-local-files)).

<!-- pattern: script-directive -->
```snakemake
storage:
    provider="fdb"


rule mean:
    input:
        storage.fdb(
            "fdb://class=ea,expver=0001,stream=oper,date=20200101,time=0000,domain=g,"
            "type=an,levtype=sfc,step=0/6/12,param=167"
        ),
    output:
        "mean.txt",
    script:
        "scripts/mean.py"
```

<!-- pattern: script-directive; file: scripts/mean.py -->
```python
"""Write the mean of every message of the retrieved GRIB file, one per line."""

import eccodes

with open(snakemake.input[0], "rb") as fi, open(snakemake.output[0], "w") as fo:
    while (handle := eccodes.codes_grib_new_from_file(fi)) is not None:
        values = eccodes.codes_get_values(handle)
        print(f"{values.mean():.3f}", file=fo)
        eccodes.codes_release(handle)
```

### Direct access: plain libraries, no local files

A `run:` or `script:` body can read the fields straight from FDB and archive straight
into FDB with **plain pyfdb, eccodes or earthkit-data**; no rule imports the plugin
([user guide](user-guide.md#direct-access-from-run-and-script-rules)). Flag the input
`retrieve=False` — the job then gets the query string instead of a path — and derive
the MARS request from that string in the job (strip `fdb://`, split on `,` and `=`; `/`
lists stay strings, which pyfdb and earthkit-data take as MARS lists). The provider puts
its FDB configuration in the job environment, so `pyfdb.FDB()` and
`from_source("fdb", request)` need no arguments. Nothing GRIB-shaped is written under
`.snakemake/storage`, which the rules below print (`GRIB FILES: 0`).

<!-- pattern: run-plain; expect: STEPS: 0 6 12; expect: GRIB FILES: 0 -->
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
        import sys
        from pathlib import Path

        import eccodes
        import pyfdb

        query = input[0].removeprefix("fdb://")  # input[0] is the query text
        request = dict(item.split("=", 1) for item in query.split(","))
        with pyfdb.FDB().retrieve(request) as source:
            data = source.read()  # every field of the request, one buffer
        steps = [str(message.get("step")) for message in eccodes.MemoryReader(data)]
        Path(output[0]).write_text(" ".join(steps) + "\n")

        storage = Path(".snakemake/storage")
        grib = [p for p in storage.rglob("*.grib") if p.open("rb").read(4) == b"GRIB"]
        print("STEPS:", *steps, file=sys.stderr)
        print("GRIB FILES:", len(grib), file=sys.stderr)
```

A `script:` rule gets the query in `snakemake.input[0]`. This one reads with
earthkit-data where it is installed and with pyfdb otherwise; both are configured by the
environment the provider prepared.

<!-- pattern: script-plain; expect: MEANS: 3; expect: GRIB FILES: 0 -->
```snakemake
storage:
    provider="fdb"


rule mean:
    input:
        storage.fdb(
            "fdb://class=ea,expver=0001,stream=oper,date=20200101,time=0000,domain=g,"
            "type=an,levtype=sfc,step=0/6/12,param=167",
            retrieve=False,
        ),
    output:
        "mean.txt",
    script:
        "scripts/plain_mean.py"
```

<!-- pattern: script-plain; file: scripts/plain_mean.py -->
```python
"""The mean of every field of the job's input query, read straight from FDB."""

import sys
from pathlib import Path

query = snakemake.input[0].removeprefix("fdb://")
request = dict(item.split("=", 1) for item in query.split(","))

try:
    from earthkit.data import from_source
except ImportError:
    import eccodes
    import pyfdb

    with pyfdb.FDB().retrieve(request) as source:
        data = source.read()
    means = [m.get_array("values").mean() for m in eccodes.MemoryReader(data)]
else:
    means = [f.to_numpy().mean() for f in from_source("fdb", request).to_fieldlist()]

Path(snakemake.output[0]).write_text("".join(f"{m:.3f}\n" for m in means))
storage = Path(".snakemake/storage")
grib = [p for p in storage.rglob("*.grib") if p.open("rb").read(4) == b"GRIB"]
print("MEANS:", len(means), file=sys.stderr)
print("GRIB FILES:", len(grib), file=sys.stderr)
```

Outputs work the same way: the job archives with `pyfdb.FDB().archive(...)` and the
output is declared `touch(storage.fdb(...))`. The empty file Snakemake leaves at the
output's local path tells the store step that the job archived the fields itself; the
store archives nothing and checks that every field of the query is in FDB with a
timestamp from this run. This is the [first write pattern](#one-rule-one-query) without
a GRIB file anywhere.

<!-- pattern: script-plain-archive; rerun: nothing; expect: Storing in storage: fdb://class=ea,expver=0023; expect: GRIB FILES: 0 -->
```snakemake
storage:
    provider="fdb"


QUERY = (
    "fdb://class=ea,expver={expver},stream=oper,date=20200101,time=0000,domain=g,"
    "type=an,levtype=sfc,step=0/6/12,param=167"
)


rule shift_expver:
    input:
        storage.fdb(QUERY.format(expver="0001"), retrieve=False),
    params:
        expver="0023",  # a plain value: fine in params, unlike the request
    output:
        touch(storage.fdb(QUERY.format(expver="0023"))),
    script:
        "scripts/plain_shift.py"
```

<!-- pattern: script-plain-archive; file: scripts/plain_shift.py -->
```python
"""Relabel every field of the job's input query and archive it, with plain pyfdb."""

import sys
from pathlib import Path

import eccodes
import pyfdb

query = snakemake.input[0].removeprefix("fdb://")
request = dict(item.split("=", 1) for item in query.split(","))

fdb = pyfdb.FDB()  # configured by the environment the provider prepared
with fdb.retrieve(request) as source:
    data = source.read()
archived = 0
for message in eccodes.MemoryReader(data):
    message.set("expver", snakemake.params.expver)
    fdb.archive(message.get_buffer())
    archived += 1
fdb.flush()  # before the job ends: the store step looks the fields up

storage = Path(".snakemake/storage")
grib = [p for p in storage.rglob("*.grib") if p.open("rb").read(4) == b"GRIB"]
print("ARCHIVED:", archived, file=sys.stderr)
print("GRIB FILES:", len(grib), file=sys.stderr)
```

Nothing is checked before such an archive, so a job that writes the wrong fields puts
them in FDB and the store step's post-check reports them afterwards. Where that matters,
the plugin's optional `api.archive` runs the checks of a file-based store first (field
count, every message's keys against the query, duplicates), archives nothing if one
fails and leaves an archive marker instead of an empty file:

<!-- pattern: run-api-archive; expect: Storing in storage: fdb://class=ea,expver=0022; expect: GRIB FILES: 0 -->
```snakemake
storage:
    provider="fdb"


QUERY = (
    "fdb://class=ea,expver={expver},stream=oper,date=20200101,time=0000,domain=g,"
    "type=an,levtype=sfc,step=0/6/12,param=167"
)


rule shift_expver:
    input:
        storage.fdb(QUERY.format(expver="0001"), retrieve=False),
    output:
        storage.fdb(QUERY.format(expver="0022")),
    run:
        import sys
        from pathlib import Path

        import eccodes

        from snakemake_storage_plugin_fdb import api

        def shifted():
            """One message at a time: nothing is held but the message in hand."""
            for message in api.messages(input[0]):
                handle = eccodes.codes_new_from_message(message)
                eccodes.codes_set(handle, "expver", "0022")
                yield eccodes.codes_get_message(handle)
                eccodes.codes_release(handle)

        marker = api.archive(output[0], shifted())  # checks, archives, writes the marker

        storage = Path(".snakemake/storage")
        grib = [p for p in storage.rglob("*.grib") if p.open("rb").read(4) == b"GRIB"]
        print("GRIB FILES:", len(grib), "->", marker.fields, "fields", file=sys.stderr)
```

The only file under `.snakemake/storage` is the empty file, or the marker, of the output,
and Snakemake removes it with the other local copies at the end of the run. Where a job
needs a real file after all — a `shell:` rule calling an external program — leave the
input retrieved and the output a file, as in the sections above.

## Writing outputs

An FDB output is a query too: the rule writes a GRIB file with exactly the fields the
query names, and the plugin archives it. The checks the plugin runs before and after
archiving are in the [user guide](user-guide.md#writing-outputs); this section is about
how to lay the rules out. Every pattern here converges: a second run reports "Nothing to
be done", which the patterns test checks.

### One rule, one query

<!-- pattern: write-one; rerun: nothing; expect: Storing in storage -->
```snakemake
storage:
    provider="fdb"


QUERY = (
    "fdb://class=ea,expver={expver},stream=oper,date=20200101,time=0000,domain=g,"
    "type=an,levtype=sfc,step=0/6/12,param=167"
)


rule shift_expver:
    input:
        storage.fdb(QUERY.format(expver="0001")),
    output:
        storage.fdb(QUERY.format(expver="0011")),
    run:
        import eccodes

        with open(input[0], "rb") as fi, open(output[0], "wb") as fo:
            while (handle := eccodes.codes_grib_new_from_file(fi)) is not None:
                eccodes.codes_set(handle, "expver", "0011")
                eccodes.codes_write(handle, fo)
                eccodes.codes_release(handle)
```

The rule must set every key its output query claims: the plugin never relabels data, and
a message whose keys disagree with the query is rejected before anything is archived.
A forced run archives the fields again, which masks the first copy.

An FDB query cannot be a command-line target (see
[Snakemake flags and features](user-guide.md#snakemake-flags-and-features)), so drive
such a workflow by rule name (`snakemake shift_expver`), give the last rule a small
local file, or list the queries in a first `rule all`, as the next pattern does.

### One field, one rule

Snakemake compares output queries as strings, while FDB identity is per field. Two rules
whose queries overlap, such as a per-step rule and a whole-forecast rule over the same
`expver`, are accepted without a word and mask parts of each other's output. Split the
work so that each field is produced by exactly one job:

<!-- pattern: write-per-step; cores: 4; rerun: nothing -->
```snakemake
storage:
    provider="fdb"


QUERY = (
    "fdb://class=ea,expver={expver},stream=oper,date=20200101,time=0000,domain=g,"
    "type=an,levtype=sfc,step={step},param=167"
)
STEPS = [0, 6, 12]


rule all:
    input:
        [storage.fdb(QUERY.format(expver="0012", step=step)) for step in STEPS],


rule one_step:
    input:
        storage.fdb(QUERY.format(expver="0001", step="{step}")),
    output:
        storage.fdb(QUERY.format(expver="0012", step="{step}")),
    run:
        import eccodes

        with open(input[0], "rb") as fi, open(output[0], "wb") as fo:
            while (handle := eccodes.codes_grib_new_from_file(fi)) is not None:
                eccodes.codes_set(handle, "expver", "0012")
                eccodes.codes_write(handle, fo)
                eccodes.codes_release(handle)
```

The `step` wildcard makes three jobs, each writing one field; a rule that wants the
whole forecast reads `step=0/6/12` of `expver=0012` as an *input*. With `-c4` the jobs
archive concurrently into the same database, which FDB supports.

### Several FDB outputs in one rule

A rule may name several FDB outputs. They are archived one after another; what happens
when a later one fails its checks is in the
[user guide](user-guide.md#writing-outputs).

<!-- pattern: write-two-outputs; rerun: nothing; expect: Storing in storage -->
```snakemake
storage:
    provider="fdb"


QUERY = (
    "fdb://class=ea,expver={expver},stream=oper,date=20200101,time=0000,domain=g,"
    "type=an,levtype=sfc,step=0/6/12,param={param}"
)


rule split_params:
    input:
        storage.fdb(QUERY.format(expver="0001", param="167/165")),
    output:
        t2m=storage.fdb(QUERY.format(expver="0013", param="167")),
        d2m=storage.fdb(QUERY.format(expver="0013", param="165")),
    run:
        import eccodes

        handles = {"167": [], "165": []}
        with open(input[0], "rb") as fi:
            while (handle := eccodes.codes_grib_new_from_file(fi)) is not None:
                eccodes.codes_set(handle, "expver", "0013")
                handles[eccodes.codes_get_string(handle, "paramId")].append(handle)
        for param, path in (("167", output.t2m), ("165", output.d2m)):
            with open(path, "wb") as fo:
                for handle in handles[param]:
                    eccodes.codes_write(handle, fo)
                    eccodes.codes_release(handle)
```

### A chain through FDB

A rule can read what an earlier rule archived in the same run: the fields are visible as
soon as the archiving job has finished.

<!-- pattern: write-chain; rerun: nothing; expect: Storing in storage: fdb://class=ea,expver=0015 -->
```snakemake
storage:
    provider="fdb"


QUERY = (
    "fdb://class=ea,expver={expver},stream=oper,date=20200101,time=0000,domain=g,"
    "type=an,levtype=sfc,step=0/6/12,param=167"
)


rule all:
    input:
        "chain.txt",


rule a:
    input:
        storage.fdb(QUERY.format(expver="0001")),
    output:
        storage.fdb(QUERY.format(expver="0014")),
    run:
        import eccodes

        with open(input[0], "rb") as fi, open(output[0], "wb") as fo:
            while (handle := eccodes.codes_grib_new_from_file(fi)) is not None:
                eccodes.codes_set(handle, "expver", "0014")
                eccodes.codes_write(handle, fo)
                eccodes.codes_release(handle)


rule b:
    input:
        storage.fdb(QUERY.format(expver="0014")),
    output:
        storage.fdb(QUERY.format(expver="0015")),
    run:
        import eccodes

        with open(input[0], "rb") as fi, open(output[0], "wb") as fo:
            while (handle := eccodes.codes_grib_new_from_file(fi)) is not None:
                eccodes.codes_set(handle, "expver", "0015")
                eccodes.codes_write(handle, fo)
                eccodes.codes_release(handle)


rule c:
    input:
        storage.fdb(QUERY.format(expver="0015")),
    output:
        "chain.txt",
    shell:
        "echo 0014 0015 > {output}"
```

### A checkpoint that writes to FDB

A checkpoint whose output is an FDB query lets the DAG depend on what was archived.
Ask FDB with `glob_wildcards` after the checkpoint, not before.

<!-- pattern: write-checkpoint; rerun: nothing -->
```snakemake
storage:
    provider="fdb"


QUERY = (
    "fdb://class=ea,expver={expver},stream=oper,date=20200101,time=0000,domain=g,"
    "type=an,levtype=sfc,step={step},param=167"
)
PRODUCED = QUERY.format(expver="0016", step="{step}")


rule all:
    input:
        "steps.txt",


checkpoint produce:
    input:
        storage.fdb(QUERY.format(expver="0001", step="0/6/12")),
    output:
        storage.fdb(QUERY.format(expver="0016", step="0/6/12")),
    run:
        import eccodes

        with open(input[0], "rb") as fi, open(output[0], "wb") as fo:
            while (handle := eccodes.codes_grib_new_from_file(fi)) is not None:
                eccodes.codes_set(handle, "expver", "0016")
                eccodes.codes_write(handle, fo)
                eccodes.codes_release(handle)


def produced_steps(wildcards):
    checkpoints.produce.get()
    steps = sorted(glob_wildcards(storage.fdb(PRODUCED)).step, key=int)
    return [storage.fdb(PRODUCED.format(step=step)) for step in steps]


rule collect:
    input:
        produced_steps,
    output:
        "steps.txt",
    shell:
        "ls {input} > {output}"
```

`rule all` comes first on purpose: a `checkpoint` as the first rule is the default
target, and the run then stops after it, reporting success.

### A key the GRIB does not carry

A query key that the messages do not carry is a case for `archive_mode=identifier`: it
builds each message's FDB key from the schema, the query and the message, so the query
labels the fields. The default `native` mode takes every key from the message and
rejects the output with `message 1 of <local> lacks quantile, ...` (see
[archive modes](user-guide.md#archive-modes)).

<!-- pattern: write-identifier; rerun: nothing; expect: Storing in storage -->
```snakemake
storage:
    provider="fdb",
    archive_mode="identifier",


QUERY = (
    "fdb://class=ea,expver=0001,stream=oper,date=20200101,time=0000,domain=g,"
    "type=an,levtype=sfc,step=0/6/12,param=167"
)
OUTPUT = (
    "fdb://class=ea,expver=0017,stream=oper,date=20200101,time=0000,domain=g,"
    "type=an,levtype=sfc,step=0/6/12,quantile=1:10,param=167"
)


rule quantile:
    input:
        storage.fdb(QUERY),
    output:
        storage.fdb(OUTPUT),
    run:
        import eccodes

        with open(input[0], "rb") as fi, open(output[0], "wb") as fo:
            while (handle := eccodes.codes_grib_new_from_file(fi)) is not None:
                eccodes.codes_set(handle, "expver", "0017")
                eccodes.codes_write(handle, fo)
                eccodes.codes_release(handle)
```

Identifier mode needs a schema whose rules share one key set; with a multi-rule schema
it fails with `cannot determine <key>`. Where the messages do carry the key, set it in
the GRIB and stay with `native`.

## Reruns in practice

Whether a rule with FDB inputs reruns is decided by the fields its queries name, not by
their text; the table of changes and their effect is in the
[user guide](user-guide.md#reruns), and `tests/test_rerun.py` verifies it. What the
patterns add to that table:

| pattern | consequence |
|---|---|
| [wildcards and config](#wildcards-and-config) | a new date in `config.yaml` adds jobs and leaves the others alone; removing one leaves its outputs behind |
| [wildcards and config](#wildcards-and-config) | a parameter added to a list that a query interpolates reruns the rules that read it, and their producers, although their outputs exist |
| [discovering what is in FDB](#discovering-what-is-in-fdb) | new fields appear in the DAG on the next run, because globbing happens while the Snakefile is read |
| [writing outputs](#writing-outputs) | a rerun archives the fields again and masks the previous copy; the output keeps existing, so `--delete-all-output` does not make the producer rerun (use `--forceall`, see [removing outputs](user-guide.md#removing-outputs)) |
| [one field, one rule](#one-field-one-rule) | per-step jobs rerun per step; a whole-forecast rule reruns as one job |
| [a chain through FDB](#a-chain-through-fdb) | re-archiving the first query reruns the whole chain, one job per link |

Two edits deserve care, because nothing flags them: a **narrowed input** query reruns
nothing, so the output keeps the extra fields the wider query produced (see
[reruns](user-guide.md#reruns)), and an edited **output** query leaves the fields
archived under the old query in FDB forever (see
[writing outputs](user-guide.md#writing-outputs)). Prefer a wildcard or a config value
over rewriting the query of a rule that has already run.

`--storage-fdb-input-tracking query` restores Snakemake's default behaviour, in which
any change to the text of the input queries reruns the job. It is a per-provider
setting, so with tagged providers it is written
`--storage-fdb-input-tracking prod::query`.

## Site setup

The settings that describe the site, such as the FDB configuration, eccodes definitions
and the MARS language, belong next to the machine, not in the Snakefile. Every route
below gives the same result; see [where settings go](user-guide.md#where-settings-go)
for the full list.

### A profile

A profile keeps the flags out of the command line. `snakemake --profile profile` then
runs the workflow, and the same profile serves every workflow at the site.

```yaml
# profile/config.yaml
storage-fdb-config: /etc/fdb/prod.yaml
storage-fdb-eccodes-definitions: /opt/site/definitions
storage-fdb-metkit-home: /opt/site/metkit
storage-fdb-canonical-spelling: error
cores: 4
```

### The environment

A module file or a container image can set the variables, and no workflow mentions the
paths:

```bash
export SNAKEMAKE_STORAGE_FDB_CONFIG=/etc/fdb/prod.yaml
export SNAKEMAKE_STORAGE_FDB_ECCODES_DEFINITIONS=/opt/site/definitions
snakemake -c1
```

The Snakefile then only names the provider:

<!-- pattern: env-config; env: SNAKEMAKE_STORAGE_FDB_CONFIG={config} -->
```snakemake
storage:
    provider="fdb"


rule t2m:
    input:
        storage.fdb(
            "fdb://class=ea,expver=0001,stream=oper,date=20200101,time=0000,domain=g,"
            "type=an,levtype=sfc,step=0/6/12,param=167"
        ),
    output:
        "t2m.grib",
    shell:
        "cp {input} {output}"
```

A command-line flag overrides the variable, and the variable holds one value, which may
be tagged.

### Two FDBs: read from one, write to the other

A `storage <tag>:` directive creates a second provider with its own settings and its own
local directory (see [tagged providers](user-guide.md#several-fdbs-tagged-providers)).
This is the shape for reading operational data and writing into a scratch or project
FDB.

<!-- pattern: two-fdbs; args: --storage-fdb-config src::{config} dst::{config2}; rerun: nothing; expect: Storing in storage -->
```snakemake
storage src:
    provider="fdb"


storage dst:
    provider="fdb"


QUERY = (
    "fdb://class=ea,expver={expver},stream=oper,date=20200101,time=0000,domain=g,"
    "type=an,levtype=sfc,step=0/6/12,param=167"
)


rule copy:
    input:
        storage.src(QUERY.format(expver="0001")),
    output:
        storage.dst(QUERY.format(expver="0021")),
    shell:
        "python fix_expver.py {input} {output}"
```

<!-- pattern: two-fdbs; file: fix_expver.py -->
```python
"""Copy a GRIB file, setting ``expver=0021`` in every message."""

import sys

import eccodes

with open(sys.argv[1], "rb") as fi, open(sys.argv[2], "wb") as fo:
    while (handle := eccodes.codes_grib_new_from_file(fi)) is not None:
        eccodes.codes_set(handle, "expver", "0021")
        eccodes.codes_write(handle, fo)
        eccodes.codes_release(handle)
```

```bash
snakemake --storage-fdb-config src::/etc/fdb/prod.yaml dst::/scratch/fdb/config.yaml -c1
```

The rule is a `shell:` rule on purpose: tagged setting values do not reach jobs that
Snakemake spawns, which includes every `run:` rule and every job of a cluster or remote
executor (Snakemake 9.27, see
[tagged settings and spawned jobs](user-guide.md#tagged-settings-and-spawned-jobs)).
Putting the settings into the two `storage` directives lifts that restriction, at the
price of naming site paths in the Snakefile.

MeteoSwiss users: the definitions, MARS language, schema and query conventions are in
the [site guide](sites/meteoswiss.md).
