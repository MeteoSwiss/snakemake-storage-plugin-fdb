# ECMWF-style example workflow

A two-rule workflow against a local development FDB:

- `shift_expver` retrieves 2 m temperature (`param=167`) at steps 0, 6 and 12 of
  `class=ea,expver=0001,stream=oper` for each date in `config.yaml` (one local GRIB
  file with 3 fields), rewrites `expver` to `0002` with eccodes and archives the result
  into FDB as the rule output.
- `done` retrieves the `expver=0002` fields again and writes the local file name and
  the keys of each message to `done/<date>.txt`.

## Run

From the repository root:

```bash
uv run python scripts/init_dev_fdb.py --seed --variants
cd examples/ecmwf
uv run snakemake --storage-fdb-config ../../.fdb/config.yaml -c1
```

`init_dev_fdb.py` writes `.fdb/{config.yaml,schema,root/}` (schema `tests/data/schema`);
`--seed` archives the ECMWF samples in `tests/data/grib/ecmwf/` and `--variants` the
`stream=oper` variants of `template.grib` that this workflow reads (steps 0/6/12, params
167/165). See `scripts/init_dev_fdb.py --help` for `--root`, `--schema`, `--seed DIR`
and `--variants FILE`.

What to expect:

- local copies of the FDB queries appear under
  `.snakemake/storage/fdb/class=ea/expver=.../step=0+6+12/param=167.grib` while the jobs
  run and are removed afterwards (`--keep-storage-local-copies` keeps them);
- a second run reports "Nothing to be done";
- `snakemake --delete-all-output ...` deletes `done/` but only warns for the FDB
  output: FDB cannot delete individual fields; a later run masks them with new ones
  (`fdb purge` reclaims the space). The next run therefore rebuilds `done/` only:
  `shift_expver` is skipped because its FDB output still exists. Use `--forceall` to
  re-archive it.

The rules use `run:`, so Snakemake runs them in spawned job processes. Pass the FDB
untagged, as above: tagged settings (`TAG::VALUE`) do not reach spawned jobs in
Snakemake 9.27 (see
[Tagged settings and spawned jobs](../../docs/user-guide.md#tagged-settings-and-spawned-jobs)).
