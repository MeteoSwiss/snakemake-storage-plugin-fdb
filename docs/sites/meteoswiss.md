# MeteoSwiss data with the FDB storage plugin

The plugin is site-neutral: MeteoSwiss GRIB works through its generic settings. This
page collects what a MeteoSwiss user sets up; the runnable material is in
[`examples/meteoswiss/`](../../examples/meteoswiss/README.md).

## Definitions

ICON GRIB2 from MeteoSwiss (centre `lssw`) needs two definition sets, cosmo-mars first:

- [`MeteoSwiss/eccodes-cosmo-mars`](https://github.com/MeteoSwiss/eccodes-cosmo-mars),
  branch `varda-ext` (not on PyPI; clone it): the MARS concepts `class`, `stream`,
  `type`, `model`, `expver` and `timespan` that FDB archives by;
- `eccodes-cosmo-resources-python` from PyPI, the COSMO definitions it complements
  (the wheel declares BSD-3-Clause; the `COSMO-ORG/eccodes-cosmo-resources` repository
  declares no license; nothing from either is copied into this repository).

`examples/meteoswiss/setup.sh` does both into `.local/` and prints the value for the
`eccodes_definitions` setting (or for `ECCODES_DEFINITION_PATH`):

```text
.local/eccodes-cosmo-mars/definitions:.local/eccodes-cosmo-resources/share/eccodes-cosmo-resources/definitions
```

## MARS language (`metkit_home`)

metkit rejects `model` values it does not know, so `retrieve` requests (and the plugin's
`exists`/`retrieve`) fail with "Invalid MARS request ... cannot expand" until the
language lists them. `examples/meteoswiss/make_metkit_home.py` copies the metkit files
of the installed `metkitlib` and appends models to the context-free `model` enum of
`language.yaml`:

```bash
uv run python examples/meteoswiss/make_metkit_home.py                                   # icon-ch1-eps,icon-ch2-eps
uv run python examples/meteoswiss/make_metkit_home.py --models icon-ch1-eps,varda-single # others
```

Point `metkit_home` (or `METKIT_HOME`) at the result, `.local/metkit-home`. The models
eccodes-cosmo-mars `varda-ext` can produce (the one list; README and script point
here): `cosmo-1e`, `cosmo-2e`, `kenda-1`, `snowpolino`, `icon-ch1-eps`,
`icon-ch2-eps`, `kenda-ch1`, `icon-rea-l-ch1`, `varda-single`, `varda-ens`
(`grib2/local.215.def`) and `varda-single-g` (`grib2/local.98.def`).

## Schema and profile

`examples/meteoswiss/realtime-varda.schema` is evalml's FDB schema:

```text
[ date, time, stream, class, expver, model, type, domain-
    [ levtype, number?
        [ step, param, levelist?, timespan?none ]]]
```

The plugin orders query keys by it (`date, time, stream, class, ...`). A Snakemake
profile passes everything to a tagged provider, as in
`examples/meteoswiss/profile/config.yaml`:

```yaml
storage-fdb-config: ["mch::../../.fdb-mch/config.yaml"]
storage-fdb-eccodes-definitions: ["mch::<cosmo-mars>/definitions:<cosmo-resources>/definitions"]
storage-fdb-metkit-home: ["mch::<metkit home>"]
storage-fdb-env: ["mch::ECCODES_VERSION_CHECK_OFF=1"]
```

Tagged settings do not reach spawned job processes in Snakemake 9.27 (`run:` rules,
cluster jobs); use `shell` rules with the local executor or untagged settings (see
[Tagged settings and spawned jobs](../user-guide.md#tagged-settings-and-spawned-jobs)).

The example README has the dev-FDB command (`scripts/init_dev_fdb.py --root .fdb-mch
...` with the site environment) and the run instructions.

## Query conventions

```text
fdb://class=od,expver=0001,stream=enfo,model=icon-ch2-eps,date=20260915,time=1200,type=cf,levtype=sfc,step=6,param=500011
fdb://...,type=cf,step=6,timespan=fs,param=500041          accumulations need timespan=fs
fdb://...,type=pf,number=1/2,step=6,param=500011           number only with type=pf
```

- COSMO paramIds (`500011` T_2M, `500041` TOT_PREC); `param=2t` finds nothing.
- `model` is listed lower-case by FDB; `ICON-CH2-EPS` works but logs the
  canonical-spelling warning.
- The control member is `type=cf` without `number`; `number=0` is an invalid request.

## Samples from the OGD API

`examples/meteoswiss/fetch_ogd_samples.py` fetches ICON-CH2-EPS fields from the
[MeteoSwiss Open Government Data STAC API](https://opendatadocs.meteoswiss.ch/e-forecast-data/e2-e3-numerical-weather-forecasting-model).
Full-size files go to `.local/raw-full/meteoswiss/` (`--out DIR`); `--empty-data [DIR]`
also writes the constant-field copies committed in `.raw/meteoswiss/` (overwrite with
`--force`). OGD
keeps data for 24 h after publication, so it fetches the newest forecast unless
`--reference-datetime` is given.

## Version warning

With these definitions every decoded message prints
`WARNING: definitions.edzw version 2.47.0 is NOT compatible with ecCodes library version 2.47.3!`.
The check is an exact version comparison in cosmo-resources' `boot_extra.def`; decoding
is unaffected for patch-level differences. Set `ECCODES_VERSION_CHECK_OFF=1` (the
`env` setting or the shell) to silence it.
