# snakemake-storage-plugin-fdb

A [Snakemake](https://snakemake.github.io) storage plugin for ECMWF's Fields DataBase
(FDB), built on [`pyfdb`](https://github.com/ecmwf/fdb). Rule inputs are retrieved from
FDB as GRIB files and rule outputs are archived into FDB, addressed by MARS-style
queries such as

```
fdb://class=od,expver=0001,stream=oper,date={date},time=0000,type=fc,levtype=sfc,step=0/6/12,param=167
```

Status: under development, not usable yet.

## Development

The project is managed with [uv](https://docs.astral.sh/uv/):

```bash
uv sync
uv run ruff check . && uv run ruff format --check .
uv build
```
