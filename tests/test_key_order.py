"""Canonical key order from setting, schema text and the generic fallback (no FDB)."""

from pathlib import Path

import pytest

from snakemake_storage_plugin_fdb.query import (
    GENERIC_ORDER,
    KeyOrder,
    QueryError,
    normalize,
    parse,
)

REPO = Path(__file__).resolve().parents[1]
DATA = Path(__file__).resolve().parent / "data"

SCHEMA_TWO_RULES = """
# a comment mentioning [ bogus, keys ]
param:  Param;
step:   Step;

[ class, expver, stream=oper/dcda, date, time, domain?
    [ type, levtype # trailing comment [ with, brackets ]
        [ step, levelist?, param ]]]

[ class, expver, stream=enfo, date: Date, time, domain-
    [ type=pf/cf, levtype
        [ step, number?0, quantile?, param ]]]
"""

# Arbitrary user keys, placed only by the schema (no built-in knowledge of them).
SCHEMA_USER_KEYS = """
[ date, time, stream, class, expver, model, type, domain-
    [ levtype, number?
        [ step, param, levelist?, timespan?none ]]]
"""

# Header comment block as in ECMWF's FDB schemas: brackets and commas inside "#".
SCHEMA_COMMENT_HEADER = """# * Format of the rules is:

# [a1, a2, a3 ...[b1, b2, b3... [c1, c2, c3...]]]

# [a1, a2, a3 ...
#   [b1, b2, b3... [c1, c2, c3...]]
#   [B1, B2, B3... [C1, C2, C3...]]
# ]
#   [type=cl, ... [date:ClimateMonth, ...]]

param:      Param;   # typed globally
########################################################
[ class, expver, stream, date, time #expver?, model?, type?
    [ type, levtype, grid- # The minus sign is here to consume 'grid'
        [ step, param ]]]
"""


def test_generic():
    assert KeyOrder.generic().keys == tuple(GENERIC_ORDER)


def test_from_schema_skips_hash_comments():
    assert KeyOrder.from_schema(SCHEMA_COMMENT_HEADER).keys == (
        "class",
        "expver",
        "stream",
        "date",
        "time",
        "type",
        "levtype",
        "grid",
        "step",
        "param",
    )


def test_from_ecmwf_fdb_test_schema():
    """ecmwf/fdb ``tests/fdb/etc/fdb/schema`` at 63672ea (Apache-2.0), verbatim."""
    text = (DATA / "ecmwf-fdb-tests.schema").read_text()
    keys = KeyOrder.from_schema(text).keys
    assert keys[:14] == (
        "class",
        "expver",
        "stream",
        "date",
        "time",
        "model",
        "origin",
        "type",
        "levtype",
        "hdate",
        "step",
        "number",
        "levelist",
        "param",
    )
    assert len(keys) == 56
    assert keys[-3:] == ("grid", "leadtime", "opttime")
    assert not {"a1", "a2", "b1", "c1"} & set(keys)


def test_from_schema_first_appearance():
    assert KeyOrder.from_schema(SCHEMA_TWO_RULES).keys == (
        "class",
        "expver",
        "stream",
        "date",
        "time",
        "domain",
        "type",
        "levtype",
        "step",
        "levelist",
        "param",
        "number",
        "quantile",
    )


def test_from_schema_user_keys():
    order = KeyOrder.from_schema(SCHEMA_USER_KEYS)
    query = (
        "fdb://class=od,expver=0001,stream=enfo,model=m1,date=20240101,time=0000,"
        "type=pf,levtype=sfc,step=6,number=1,param=1,timespan=fs"
    )
    assert normalize(query, order) == (
        "fdb://date=20240101,time=0000,stream=enfo,class=od,expver=0001,model=m1,"
        "type=pf,levtype=sfc,number=1,step=6,param=1,timespan=fs"
    )


@pytest.mark.skipif(not (REPO / ".raw/schema").exists(), reason="no .raw/schema")
def test_from_raw_schema():
    order = KeyOrder.from_schema((REPO / ".raw/schema").read_text())
    assert order.keys == (
        "class",
        "expver",
        "stream",
        "date",
        "time",
        "domain",
        "type",
        "levtype",
        "step",
        "levelist",
        "param",
    )


def test_from_schema_without_rules():
    with pytest.raises(QueryError, match="no rule keys"):
        KeyOrder.from_schema("param: Param;\n")


def test_from_setting():
    order = KeyOrder.from_setting(" date, TIME ,class")
    assert order.keys == ("date", "time", "class")
    assert order.sorted(["param", "class", "time", "date", "alpha"]) == [
        "date",
        "time",
        "class",
        "alpha",
        "param",
    ]


@pytest.mark.parametrize("csv", ["", " , ", "date,,time", "date,1x", "date,date"])
def test_from_setting_invalid(csv):
    with pytest.raises(QueryError):
        KeyOrder.from_setting(csv)


@pytest.mark.parametrize(
    "keys, expected",
    [
        (
            ["param", "step", "class", "date"],
            ["class", "date", "step", "param"],
        ),
        # unknown keys follow the known ones, alphabetically
        (
            ["timespan", "param", "model", "quantile", "class"],
            ["class", "param", "model", "quantile", "timespan"],
        ),
        (["zeta", "alpha"], ["alpha", "zeta"]),
    ],
)
def test_generic_unknown_keys_alphabetical(keys, expected):
    assert KeyOrder.generic().sorted(keys) == expected


def test_order_does_not_depend_on_values():
    a = parse("fdb://param={p},date={d,\\d{8}},class=od")
    b = parse("fdb://param=2t,date=20200101,class=od")
    assert a.keys() == b.keys() == ["class", "date", "param"]
