"""Ask FDB what it holds, without a workflow (FR-DEV-004).

    python -m snakemake_storage_plugin_fdb inspect [--config PATH] "fdb://..."
    python -m snakemake_storage_plugin_fdb list    [--config PATH] "fdb://..."

``inspect`` answers the question a workflow answers implicitly: are all the fields of
this query in FDB, which are missing, and when was each one archived. It exits 1 when
the query is incomplete, so a script can test it. ``list`` takes a query that may leave
keys out and reports the distinct values FDB holds under it, one line per key.

Both use the plugin's own settings: ``--config``/``--user-config``, else the
``SNAKEMAKE_STORAGE_FDB_*`` variables, else FDB's own environment (FR-CONF-008,
FR-DIRECT-003).
"""

from __future__ import annotations

import argparse
import sys
from datetime import UTC, datetime

from snakemake_interface_common.exceptions import WorkflowError

from . import api
from .query import SCHEME


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m snakemake_storage_plugin_fdb",
        description="Inspect and list FDB fields with the plugin's own lookup.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)
    for name, help_text in (
        ("inspect", "count the fields of a query in FDB and name the missing ones"),
        ("list", "distinct values FDB holds under a (possibly partial) query"),
    ):
        sub = subparsers.add_parser(name, help=help_text)
        sub.add_argument("query", help=f"an FDB query ({SCHEME}class=...)")
        sub.add_argument("--config", help="FDB configuration: path or inline YAML")
        sub.add_argument("--user-config", help="FDB user configuration")
    return parser


def _timestamp(value: int) -> str:
    if not value:
        return "no timestamp"
    return datetime.fromtimestamp(value, UTC).strftime("%Y-%m-%d %H:%M:%SZ")


def _inspect(args: argparse.Namespace) -> int:
    lookup = api.exists(args.query, config=args.config, user_config=args.user_config)
    print(f"{lookup.found} of {lookup.expected} fields in FDB for {lookup.query}")
    for field in sorted(lookup.fields, key=lambda f: sorted(f.keys.items())):
        keys = ",".join(f"{k}={v}" for k, v in sorted(field.keys.items()))
        print(f"  {_timestamp(field.timestamp)}  {field.length} bytes  {keys}")
    for combination in lookup.missing:
        print(f"  missing: {combination}")
    return 0 if lookup.complete else 1


def _list(args: argparse.Namespace) -> int:
    obj = api._object(args.query, args.config, args.user_config)
    with obj._mapping_errors():  # FDB failures in the plugin's words (FR-ERR-001)
        fields = obj.provider.backend.list(obj.parsed.constant_pairs())
    if not fields:
        print(f"no fields in FDB under {obj.query}")
        return 1
    values: dict[str, list[str]] = {}
    for field in fields:
        for key, value in field.key.items():
            seen = values.setdefault(key, [])
            if value not in seen:
                seen.append(value)
    print(f"{len(fields)} fields in FDB under {obj.query}")
    for key in obj.provider.key_order.sorted(values):
        print(f"  {key}: {'/'.join(sorted(values[key]))}")
    return 0


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        return _inspect(args) if args.command == "inspect" else _list(args)
    except WorkflowError as e:
        print(f"error: {e}", file=sys.stderr)
        return 2


if __name__ == "__main__":  # pragma: no cover - exercised as a subprocess
    sys.exit(main())
