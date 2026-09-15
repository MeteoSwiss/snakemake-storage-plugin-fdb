"""Fetch ICON-EPS sample fields from the MeteoSwiss Open Government Data STAC API.

    uv run python examples/meteoswiss/fetch_ogd_samples.py
        [--collection ch.meteoschweiz.ogd-forecasting-icon-ch2]
        [--reference-datetime 2026-09-15T12:00:00Z] [--horizon P0DT06H00M00S]
        [--members 1,2] [--out DIR] [--empty-data [DIR] [--force]]

Downloads the control T_2M and TOT_PREC fields and the perturbed T_2M file (reduced to
``--members`` by ``perturbationNumber``) of one forecast and writes them full-size to
``--out`` (default ``.local/raw-full/meteoswiss``, git-ignored). With ``--empty-data``
it also writes copies whose data section is a constant field (``grid_simple``,
``bitsPerValue=0``, a few hundred bytes) to DIR (default ``.raw/meteoswiss``, the
committed samples), after checking that their MARS keys equal the full-size messages';
existing copies are only overwritten with ``--force``. File names:
``<model>_<YYYYMMDDHHMM>_step<h>_<variable>_<ctrl|pert_mA-B>.grib2``.

OGD keeps data for 24 h after publication. Without ``--reference-datetime`` the newest
forecast is used. Run with the COSMO definitions in ``ECCODES_DEFINITION_PATH`` (see
README.md) so that the MARS keys printed at the end are the ones FDB sees.
Needs network access; stdlib ``urllib`` and ``eccodes`` only.
"""

import argparse
import contextlib
import json
import re
import sys
import tempfile
import urllib.request
from datetime import datetime
from pathlib import Path

import eccodes

REPO = Path(__file__).resolve().parents[2]
STAC = "https://data.geo.admin.ch/api/stac/v1/search"
DEFAULT_COLLECTION = "ch.meteoschweiz.ogd-forecasting-icon-ch2"
# (variable, perturbed) of the samples, in the order they are fetched
SAMPLES = (("T_2M", False), ("TOT_PREC", False), ("T_2M", True))
SHOWN_KEYS = ("class", "stream", "type", "model", "step", "number", "timespan", "param")
TIMEOUT = 300


class FetchError(Exception):
    pass


def search(collection, variable, perturbed, horizon, reference_datetime=None):
    """``(reference datetime, asset href)`` of the newest matching item."""
    body = {
        "collections": [collection],
        "forecast:variable": variable,
        "forecast:perturbed": perturbed,
        "forecast:horizon": horizon,
    }
    if reference_datetime:
        body["forecast:reference_datetime"] = reference_datetime
    req = urllib.request.Request(
        STAC,
        data=json.dumps(body).encode(),
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=TIMEOUT) as resp:
        feats = json.load(resp)["features"]
    if not feats:
        raise FetchError(
            f"no items for {collection} {variable} perturbed={perturbed} "
            f"horizon={horizon} reference_datetime={reference_datetime or 'any'}; "
            "OGD retains data for 24 h after publication"
        )
    feat = max(feats, key=lambda f: f["properties"]["forecast:reference_datetime"])
    (asset,) = feat["assets"].values()  # exactly one asset per item
    return feat["properties"]["forecast:reference_datetime"], asset["href"]


def download(href: str) -> bytes:
    # pre-signed URL: GET only (HEAD returns an error body)
    with urllib.request.urlopen(href, timeout=TIMEOUT) as resp:
        return resp.read()


def messages(data: bytes) -> list[bytes]:
    """The GRIB messages in ``data``."""
    out = []
    with tempfile.TemporaryFile() as f:
        f.write(data)
        f.seek(0)
        while (h := eccodes.codes_grib_new_from_file(f)) is not None:
            out.append(eccodes.codes_get_message(h))
            eccodes.codes_release(h)
    return out


@contextlib.contextmanager
def handle(message: bytes):
    h = eccodes.codes_new_from_message(message)
    try:
        yield h
    finally:
        eccodes.codes_release(h)


def mars_keys(message: bytes) -> dict[str, str]:
    with handle(message) as h:
        keys = {}
        it = eccodes.codes_keys_iterator_new(h, "mars")
        while eccodes.codes_keys_iterator_next(it):
            name = eccodes.codes_keys_iterator_get_name(it)
            keys[name] = eccodes.codes_get_string(h, name)
        eccodes.codes_keys_iterator_delete(it)
        return keys


def perturbation_number(message: bytes) -> int:
    with handle(message) as h:
        return eccodes.codes_get(h, "perturbationNumber")


def empty_data(message: bytes) -> bytes:
    """``message`` with a constant zero field (``grid_simple``, ``bitsPerValue=0``)
    and the same MARS keys."""
    with handle(message) as h:
        eccodes.codes_set_string(h, "packingType", "grid_simple")
        eccodes.codes_set_values(h, [0.0] * eccodes.codes_get_size(h, "values"))
        empty = eccodes.codes_get_message(h)
    if mars_keys(empty) != mars_keys(message):
        raise FetchError("emptying the data section changed the MARS keys")
    return empty


def empty_file(data: bytes) -> bytes:
    """A GRIB file's content with every message emptied (the committed samples)."""
    return b"".join(empty_data(m) for m in messages(data))


def model_name(collection: str) -> str:
    """``ch.meteoschweiz.ogd-forecasting-icon-ch2`` -> ``icon-ch2-eps``."""
    match = re.fullmatch(r".*ogd-forecasting-(icon-ch\d)", collection)
    if not match:
        raise FetchError(f"cannot derive the model name from collection {collection}")
    return f"{match.group(1)}-eps"


def step_label(horizon: str) -> str:
    """``P0DT06H00M00S`` -> ``6`` (whole hours)."""
    match = re.fullmatch(r"P(\d+)DT(\d+)H00M00S", horizon)
    if not match:
        raise FetchError(f"unsupported horizon {horizon} (expected PnDTnnH00M00S)")
    days, hours = (int(g) for g in match.groups())
    return str(days * 24 + hours)


def fetch(args) -> list[Path]:
    """Download and write the samples; the files written."""
    model, step = model_name(args.collection), step_label(args.horizon)
    reference, hrefs = args.reference_datetime, {}
    if reference is None:  # newest forecast of the first sample
        reference, hrefs[SAMPLES[0]] = search(
            args.collection, *SAMPLES[0], args.horizon
        )
    stamp = datetime.strptime(reference, "%Y-%m-%dT%H:%M:%SZ").strftime("%Y%m%d%H%M")
    members = f"pert_m{args.members[0]}-{args.members[-1]}"
    names = {
        (variable, perturbed): f"{model}_{stamp}_step{step}_{variable.lower()}_"
        f"{members if perturbed else 'ctrl'}.grib2"
        for variable, perturbed in SAMPLES
    }
    if args.empty_data and not args.force:
        existing = [n for n in names.values() if (args.empty_data / n).exists()]
        if existing:
            raise FetchError(
                f"{args.empty_data} already has {', '.join(existing)}; "
                "pass --force to overwrite"
            )

    written = []
    args.out.mkdir(parents=True, exist_ok=True)
    for (variable, perturbed), name in names.items():
        href = hrefs.get((variable, perturbed))
        if href is None:
            _, href = search(
                args.collection, variable, perturbed, args.horizon, reference
            )
        print(f"GET {variable} perturbed={perturbed} reference={reference}")
        msgs = messages(download(href))
        if perturbed:
            by_member = {perturbation_number(m): m for m in msgs}
            missing = sorted(set(args.members) - set(by_member))
            if missing:
                raise FetchError(f"{name}: members {missing} not in the download")
            msgs = [by_member[n] for n in args.members]
        elif len(msgs) != 1:
            raise FetchError(f"{name}: expected 1 message, got {len(msgs)}")
        full = args.out / name
        full.write_bytes(b"".join(msgs))
        written.append(full)
        if args.empty_data:
            args.empty_data.mkdir(parents=True, exist_ok=True)
            empty = args.empty_data / name
            empty.write_bytes(b"".join(empty_data(m) for m in msgs))
            written.append(empty)
    return written


def show(path: Path) -> None:
    """Print the MARS keys of every message, like ``grib_ls -n mars``."""
    for i, message in enumerate(messages(path.read_bytes()), 1):
        keys = mars_keys(message)
        text = " ".join(f"{k}={keys[k]}" for k in SHOWN_KEYS if k in keys)
        print(f"{path} [{i}] {len(message)} B: {text}")
        if "model" not in keys:
            print("  (no model key: COSMO definitions not in ECCODES_DEFINITION_PATH)")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--collection", default=DEFAULT_COLLECTION)
    parser.add_argument(
        "--reference-datetime",
        help="forecast reference time, e.g. 2026-09-15T12:00:00Z (default: newest)",
    )
    parser.add_argument("--horizon", default="P0DT06H00M00S")
    parser.add_argument(
        "--members",
        default="1,2",
        type=lambda s: sorted({int(m) for m in s.split(",")}),
        help="perturbed members to keep (default 1,2)",
    )
    parser.add_argument(
        "--out",
        type=Path,
        default=REPO / ".local" / "raw-full" / "meteoswiss",
        help="directory for the full-size files (default .local/raw-full/meteoswiss)",
    )
    parser.add_argument(
        "--empty-data",
        type=Path,
        nargs="?",
        const=REPO / ".raw" / "meteoswiss",
        default=None,
        metavar="DIR",
        help="also write constant-field copies to DIR (default .raw/meteoswiss)",
    )
    parser.add_argument(
        "--force", action="store_true", help="overwrite existing constant-field copies"
    )
    args = parser.parse_args(argv)
    try:
        written = fetch(args)
    except FetchError as e:
        print(f"error: {e}", file=sys.stderr)
        return 1
    for path in written:
        show(path)
    return 0


if __name__ == "__main__":
    sys.exit(main())
