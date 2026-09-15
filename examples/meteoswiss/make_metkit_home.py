"""Build a metkit home whose MARS language accepts MeteoSwiss ``model`` values.

    uv run python examples/meteoswiss/make_metkit_home.py [--dest DIR] [--models LIST]

Copies ``share/metkit/`` of the installed ``metkitlib`` wheel into
``<dest>/share/metkit/`` (default ``.local/metkit-home``) and appends the models to the
context-free ``model`` enum of ``language.yaml``: ``_field.model.type``, the block
without a ``context``. The plugin sees the result only through the generic
``metkit_home`` setting (or ``METKIT_HOME``). Re-running starts from a fresh copy, so
the output depends only on the installed metkitlib and ``--models``.

``--models`` defaults to ``icon-ch1-eps,icon-ch2-eps`` (the committed samples and the
example); values are lower-cased, as FDB lists them. The values eccodes-cosmo-mars
can produce are listed in ``docs/sites/meteoswiss.md``.
"""

import argparse
import shutil
import sys
from pathlib import Path

import yaml

try:
    import metkitlib
except ImportError:
    sys.exit("metkitlib is not installed in this environment (it comes with pyfdb)")

REPO = Path(__file__).resolve().parents[2]
METKIT_SHARE = Path(metkitlib.__file__).parent / "share" / "metkit"
DEFAULT_MODELS = "icon-ch1-eps,icon-ch2-eps"


def add_models(language: dict, models: list[str]) -> list[str]:
    """Append ``models`` to the context-free ``model`` enum; the models added."""
    blocks = [b for b in language["_field"]["model"]["type"] if "context" not in b]
    if len(blocks) != 1:
        raise ValueError(f"expected one context-free model block, found {len(blocks)}")
    values = blocks[0]["values"]
    known = {name for entry in values for name in entry}
    added = [m for m in models if m not in known]
    values.extend([m] for m in added)
    return added


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--dest",
        type=Path,
        default=REPO / ".local" / "metkit-home",
        help="metkit home to create (default .local/metkit-home)",
    )
    parser.add_argument(
        "--models",
        default=DEFAULT_MODELS,
        help=f"comma list of model values to add (default {DEFAULT_MODELS})",
    )
    args = parser.parse_args(argv)
    models = [m.strip().lower() for m in args.models.split(",") if m.strip()]
    if not models:
        parser.error(f"invalid --models {args.models!r}")

    dest = args.dest.absolute()
    target = dest / "share" / "metkit"
    shutil.copytree(METKIT_SHARE, target, dirs_exist_ok=True)
    path = target / "language.yaml"
    language = yaml.safe_load(path.read_text())
    added = add_models(language, models)
    path.write_text(yaml.safe_dump(language, sort_keys=False))
    print(f"added models {added or '(none, already known)'} to {path}")
    print(f"metkit_home: {dest}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
