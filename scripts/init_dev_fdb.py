"""Create (and optionally seed) a local development FDB.

    uv run python scripts/init_dev_fdb.py [--root DIR] [--schema PATH] [--seed [DIR]]
                                          [--variants [FILE]]

Writes ``<root>/schema`` (a copy of ``--schema``), the database root ``<root>/root/``
and ``<root>/config.yaml``, a local toc FDB config with absolute paths, so it can be
passed from any working directory (``--storage-fdb-config <root>/config.yaml``).
Defaults are relative to the repository: ``--root .fdb``,
``--schema tests/data/schema``.

``--seed [DIR]`` (default ``tests/data/grib/ecmwf``) archives every GRIB file directly
in DIR (files starting with ``GRIB``, no subdirectories) natively; FDB derives the
keys. A missing DIR is reported and skipped. ``--variants [FILE]`` (default
``tests/data/grib/ecmwf/template.grib``) archives zeroed ``stream=oper`` variants of
the GRIB message in FILE for steps 0/6/12 and params 167/165: the fields
``examples/ecmwf/`` reads. Data needing other eccodes definitions or MARS language is
seeded with ``ECCODES_DEFINITION_PATH``/``METKIT_HOME`` set in the environment.
"""

import argparse
import os
import shutil
import sys
from pathlib import Path

import yaml

os.environ.setdefault("ECKIT_EXCEPTION_IS_SILENT", "1")

REPO = Path(__file__).resolve().parents[1]
VARIANT_STEPS = (0, 6, 12)
VARIANT_PARAMS = (167, 165)


def write_config(root: Path, schema: Path) -> Path:
    """``<root>/schema``, ``<root>/root/`` and ``<root>/config.yaml``; the config."""
    root.mkdir(parents=True, exist_ok=True)
    (root / "root").mkdir(exist_ok=True)
    shutil.copyfile(schema, root / "schema")
    config = root / "config.yaml"
    config.write_text(
        yaml.safe_dump(
            {
                "type": "local",
                "engine": "toc",
                "schema": str(root / "schema"),
                "spaces": [
                    {"handler": "Default", "roots": [{"path": str(root / "root")}]}
                ],
            },
            sort_keys=False,
        )
    )
    return config


def seed(backend, source: Path) -> int:
    """Archive the GRIB files directly in ``source``; the number of messages."""
    from snakemake_storage_plugin_fdb.grib import split_messages

    count = 0
    for path in sorted(p for p in source.iterdir() if p.is_file()):
        data = path.read_bytes()
        if not data.startswith(b"GRIB"):
            continue
        messages = len(split_messages(path))  # validates the file, counts messages
        backend.archive(data)
        print(f"archived {path} ({messages} messages)")
        count += messages
    return count


def variants(backend, template: Path) -> int:
    """Archive the ``stream=oper`` variants of ``template``; the number of messages."""
    from snakemake_storage_plugin_fdb.grib import variant

    data = template.read_bytes()
    for step in VARIANT_STEPS:
        for param in VARIANT_PARAMS:
            backend.archive(variant(data, stream="oper", step=step, paramId=param))
    count = len(VARIANT_STEPS) * len(VARIANT_PARAMS)
    print(f"archived {count} stream=oper variants of {template}")
    return count


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--root", type=Path, default=REPO / ".fdb", help="FDB directory (default .fdb)"
    )
    parser.add_argument(
        "--schema",
        type=Path,
        default=REPO / "tests" / "data" / "schema",
        help="FDB schema to copy (default tests/data/schema)",
    )
    parser.add_argument(
        "--seed",
        type=Path,
        nargs="?",
        const=REPO / "tests" / "data" / "grib" / "ecmwf",
        default=None,
        metavar="DIR",
        help="archive every GRIB file in DIR (default tests/data/grib/ecmwf)",
    )
    parser.add_argument(
        "--variants",
        type=Path,
        nargs="?",
        const=REPO / "tests" / "data" / "grib" / "ecmwf" / "template.grib",
        default=None,
        metavar="FILE",
        help="archive stream=oper variants of FILE "
        "(default tests/data/grib/ecmwf/template.grib), "
        "steps 0/6/12 x params 167/165: the inputs of examples/ecmwf/",
    )
    args = parser.parse_args(argv)
    if not args.schema.is_file():
        parser.error(f"schema {args.schema} not found")
    if args.variants is not None and not args.variants.is_file():
        parser.error(f"variants template {args.variants} not found")
    root = args.root.absolute()
    config = write_config(root, args.schema)
    print(f"FDB config: {config}")
    if args.seed is None and args.variants is None:
        return 0

    from snakemake_storage_plugin_fdb.backend import Backend

    backend = Backend(config)
    count = 0
    if args.seed is not None:
        if args.seed.is_dir():
            count += seed(backend, args.seed)
        else:
            print(f"seed directory {args.seed} not found; not seeded", file=sys.stderr)
    if args.variants is not None:
        count += variants(backend, args.variants)
    backend.flush()
    print(f"{count} messages archived into {root / 'root'}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
