"""Print ``shortName step number`` of every GRIB message in a file (stands in for
``grib_ls``, which the eccodes wheels do not ship)."""

import sys

import eccodes

KEYS = ("shortName", "step", "number")

with open(sys.argv[1], "rb") as f:
    while (h := eccodes.codes_grib_new_from_file(f)) is not None:
        print(*(eccodes.codes_get_string(h, key) for key in KEYS))
        eccodes.codes_release(h)
