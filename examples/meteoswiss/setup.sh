#!/usr/bin/env bash
# MeteoSwiss GRIB definitions for the FDB plugin (see README.md).
#
#   bash examples/meteoswiss/setup.sh [--dest DIR]      (default DIR: .local)
#
# Idempotent. Into DIR it
#   - clones (or updates) eccodes-cosmo-mars, branch varda-ext, to DIR/eccodes-cosmo-mars
#   - installs eccodes-cosmo-resources-python with `uv pip install --target` into
#     DIR/eccodes-cosmo-resources (definitions under share/eccodes-cosmo-resources/)
# and prints the eccodes_definitions value (cosmo-mars first). Needs git and uv; run it
# from anywhere inside the repository checkout.
set -euo pipefail

REPO=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
DEST=$REPO/.local
MARS_URL=https://github.com/MeteoSwiss/eccodes-cosmo-mars.git
MARS_BRANCH=varda-ext
# Same minor series as the eccodes bundled with the locked pyfdb (spec §11).
RESOURCES="eccodes-cosmo-resources-python>=2.47.0.1,<2.48"

USAGE="usage: bash examples/meteoswiss/setup.sh [--dest DIR]   (default DIR: .local)"
while [ $# -gt 0 ]; do
    case "$1" in
    --dest)
        [ $# -ge 2 ] || { echo "--dest needs a directory" >&2; exit 2; }
        DEST=$2
        shift 2
        ;;
    -h | --help)
        echo "$USAGE"
        exit 0
        ;;
    *)
        echo "unknown argument: $1" >&2
        echo "$USAGE" >&2
        exit 2
        ;;
    esac
done
mkdir -p "$DEST"
DEST=$(cd "$DEST" && pwd)

MARS=$DEST/eccodes-cosmo-mars
if [ -d "$MARS/.git" ]; then
    git -C "$MARS" fetch --quiet --depth 1 origin "$MARS_BRANCH"
    git -C "$MARS" checkout --quiet --force --detach FETCH_HEAD
else
    git clone --quiet --depth 1 --branch "$MARS_BRANCH" "$MARS_URL" "$MARS"
fi
echo "eccodes-cosmo-mars $MARS_BRANCH at $(git -C "$MARS" rev-parse HEAD)"

RESOURCES_DIR=$DEST/eccodes-cosmo-resources
# --target installs the wheel's data files under $RESOURCES_DIR/share; the project
# environment (cwd = repository) only provides the Python version.
(cd "$REPO" && uv pip install --quiet --target "$RESOURCES_DIR" "$RESOURCES")

MARS_DEFS=$MARS/definitions
RESOURCES_DEFS=$RESOURCES_DIR/share/eccodes-cosmo-resources/definitions
for dir in "$MARS_DEFS" "$RESOURCES_DEFS"; do
    [ -d "$dir" ] || { echo "missing definitions directory $dir" >&2; exit 1; }
done
echo "eccodes_definitions: $MARS_DEFS:$RESOURCES_DEFS"
