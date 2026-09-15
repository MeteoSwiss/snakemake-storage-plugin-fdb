"""Query grammar, normalisation, local path and request builders (no FDB)."""

import hashlib
import re
import subprocess
import sys

import pytest
from snakemake.io import apply_wildcards, regex_from_filepattern

from snakemake_storage_plugin_fdb.query import (
    NAME_MAX,
    KeyOrder,
    ParsedQuery,
    QueryError,
    comparable,
    normalize,
    parse,
    validate,
)

BASE = "class=od,expver=0001,stream=oper,date=20240101,time=00,type=fc,levtype=sfc"

VALID = [
    f"fdb://{BASE},step=0,param=2t",
    f"fdb://{BASE},step=0/6/12,param=2t",
    f"fdb://{BASE},step=0/to/12/by/6,param=167/165",
    f"fdb://{BASE},step=0/to/60/by/10m,param=2T",
    "fdb://class=ea,expver=0001,stream=enda,date=20200101,time=0000,domain=g,"
    "type=an,levtype=sfc,step=0,number=0,param=167",
    "fdb://class=od,type=ep,step=0-24,param=70.131",
    "fdb://class=od,type=cd,step=60-132,quantile=34:100,param=228.128",
    "fdb://class=od,param=T_2M,step=10m",
    "fdb://class=od,date=2020-01-01,time=00:00",
    "fdb://class=od,date=-1",
    "fdb://class=EA,type=AN",
    "fdb://CLASS=od,Param=2t",
    "fdb://class=od,stream=some-hyphenated-enum",
    "fdb://class=od,level_type=sfc",
    "fdb://a=b",
    "fdb://class=od,date={date}",
    "fdb://class=od,date={date,\\d{8}}",
    "fdb://class=od,date={date,\\d{4,8}}",
    "fdb://class=od,param={param,[a-z0-9]+}",
    "fdb://class=od,number={n,1|2,3}",  # constraint with a comma
    "fdb://class=od,x={w,a=b}",  # constraint with "="
    "fdb://class=od,x={w,a/b}",  # constraint with "/"
    "fdb://class=od,step={s}/{t}",
    "fdb://class=od,step=0/{s}/12",
    "fdb://class=od,date=2024{mmdd}",
    "fdb://class=od,x={ spaced }",
    "fdb://class=od,x={a.b}",
    "fdb://class=od, expver=0001,\n     stream=oper, date = 20240101 ,param=2t",
    "fdb://\tclass=od\n",
    "fdb://class=od,step=0/6,step2=12",
]

INVALID = [
    ("", "must start with"),
    ("s3://bucket/key", "must start with"),
    ("test/x.txt", "must start with"),
    ("FDB://class=od", "must start with"),
    (" fdb://class=od", "must start with"),
    ("fdb://", "empty query"),
    ("fdb://   ", "empty query"),
    ("fdb://class=od/expver=0001", "invalid character '='"),
    ("fdb://a=", "empty value"),
    ("fdb://=od", "empty key"),
    ("fdb://class", "missing '='"),
    ("fdb://class=od,", "trailing comma"),
    ("fdb://,class=od", "empty key=value pair"),
    ("fdb://class=od,,type=fc", "empty key=value pair"),
    ("fdb://class=od,CLASS=rd", "duplicate key 'class'"),
    ("fdb://class=od,step=0//6", "empty item"),
    ("fdb://class=od,step=0/6/", "empty item"),
    ("fdb://class=od,step=/0", "empty item"),
    ("fdb://class=od,param=a b", "whitespace"),
    ("fdb://class=od,param=a+b", "'+'"),
    ("fdb://class=od,param=a%20", "'%'"),
    ("fdb://class=od,param=a{b", "'{'"),
    ("fdb://class=od,param=a}b", "'}'"),
    ("fdb://class=od,param={{b}}", "'{'"),
    ("fdb://class=od,param=a*", "'*'"),
    ("fdb://class=od,param=é", "'é'"),
    ("fdb://{key}=od", "wildcards are not allowed in keys"),
    ("fdb://cl{x}=od", "wildcards are not allowed in keys"),
    ("fdb://1class=od", "invalid key"),
    ("fdb://cla-ss=od", "invalid character '-'"),
    ("fdb://class=od,step=0==6", "invalid character '='"),
]


@pytest.mark.parametrize("query", VALID)
def test_valid(query):
    assert validate(query) == (True, None)


@pytest.mark.parametrize("query, message", INVALID)
def test_invalid(query, message):
    ok, reason = validate(query)
    assert not ok
    assert message in reason
    with pytest.raises(QueryError, match=re.escape(message)):
        parse(query)


def test_table_size():
    assert len(VALID) + len(INVALID) >= 30


@pytest.mark.parametrize("query", VALID)
def test_normalize_idempotent(query):
    once = normalize(query)
    assert normalize(once) == once
    assert parse(once) == parse(query)


def test_normalize():
    query = "fdb://Param = 2T, step=0/6 ,\n CLASS=EA,date={date,\\d{8}}"
    # whitespace stripped, keys lower-cased and reordered, values verbatim
    assert normalize(query) == "fdb://class=EA,date={date,\\d{8}},step=0/6,param=2T"


def test_normalize_keeps_wildcard_text_verbatim():
    q = "fdb://x={ w , [a b]{2} },class=od"
    assert normalize(q) == "fdb://class=od,x={ w , [a b]{2} }"


def test_normalize_with_order():
    order = KeyOrder.from_setting("param,step")
    assert normalize(f"fdb://{BASE},step=0,param=2t", order) == (
        "fdb://param=2t,step=0,class=od,date=20240101,expver=0001,levtype=sfc,"
        "stream=oper,time=00,type=fc"
    )


def test_parsed_query_accessors():
    q = parse("fdb://param=2t/167,step=0/to/12/by/6,date={d,\\d{4,8}},class=od")
    assert isinstance(q, ParsedQuery)
    assert q.keys() == ["class", "date", "step", "param"]
    assert q.value("step") == "0/to/12/by/6"
    assert q.items("step") == ["0", "to", "12", "by", "6"]
    assert q.items("date") == ["{d,\\d{4,8}}"]
    assert q.items("param") == ["2t", "167"]
    assert q.has_wildcards()
    assert q.wildcard_keys() == {"date"}
    assert q.constant_pairs() == {
        "class": "od",
        "step": "0/to/12/by/6",
        "param": "2t/167",
    }
    assert q.single_valued() == {"class": "od"}
    assert q.has_range("step") and not q.has_range("param")
    assert q.to_request() == {
        "class": "od",
        "date": "{d,\\d{4,8}}",
        "step": "0/to/12/by/6",
        "param": "2t/167",
    }
    assert q.to_query() == (
        "fdb://class=od,date={d,\\d{4,8}},step=0/to/12/by/6,param=2t/167"
    )
    with pytest.raises(KeyError):
        q.value("type")


def test_items_split_outside_wildcards_only():
    q = parse("fdb://x={w,a/b}/1/{v}")
    assert q.items("x") == ["{w,a/b}", "1", "{v}"]
    assert not parse("fdb://class=od").has_wildcards()


def test_local_suffix_example():
    q = parse(
        "fdb://class=od,expver=0001,stream=oper,date={date},time=00,type=fc,"
        "levtype=sfc,step=0/6/12,param=2t"
    )
    assert q.local_suffix() == (
        "class=od/expver=0001/stream=oper/date={date}/time=00/type=fc/levtype=sfc/"
        "step=0+6+12/param=2t.grib"
    )


def test_local_suffix_keeps_wildcard_text():
    q = parse("fdb://class=od,x={w,a/b}/1,y={n,\\d{2,3}}")
    assert q.local_suffix() == "class=od/x={w,a/b}+1/y={n,\\d{2,3}}.grib"


def test_local_suffix_hashes_long_constant():
    value = "/".join(str(i) for i in range(200))
    q = parse(f"fdb://class=od,step={value},param=2t")
    digest = hashlib.sha256(value.encode()).hexdigest()[:24]
    parts = q.local_suffix().split("/")
    assert parts == ["class=od", f"step=~{digest}", "param=2t.grib"]
    assert all(len(p.encode()) <= NAME_MAX for p in parts)


def test_local_suffix_hashes_long_last_component():
    value = "/".join(str(i) for i in range(200))
    q = parse(f"fdb://class=od,param={value}")
    digest = hashlib.sha256(value.encode()).hexdigest()[:24]
    assert q.local_suffix() == f"class=od/param=~{digest}.grib"


def test_local_suffix_component_limit_boundary():
    # "param=" + value + ".grib" is exactly NAME_MAX bytes: not hashed
    value = "1" * (NAME_MAX - len("param=") - len(".grib"))
    q = parse(f"fdb://param={value}")
    assert q.local_suffix() == f"param={value}.grib"
    q = parse(f"fdb://param={value}1")
    assert q.local_suffix().startswith("param=~")


def test_local_suffix_long_wildcard_component_is_error():
    value = "/".join(str(i) for i in range(200))
    q = parse(f"fdb://class=od,step={value}/{{s}}")
    with pytest.raises(QueryError, match="wildcard"):
        q.local_suffix()


QUERY_WILDCARDS = [
    ("fdb://class=od,date={date},step=0/6/12,param=2t", {"date": "20240101"}),
    (
        "fdb://param={param,[a-z0-9]+},date={date,\\d{8}},time={time},class=od",
        {"param": "2t", "date": "20240101", "time": "0000"},
    ),
    ("fdb://class=od,number={n,[1,2]},step=0/to/6", {"n": "2"}),
    ("fdb://class=od,x={w,[a-z=]+},y={v,[a/b]}/1", {"w": "ab", "v": "b"}),
    ("fdb://class=od,date=2024{mmdd},step=0/{s}/12", {"mmdd": "0101", "s": "6"}),
    ("fdb://class=od, date = { date } ,\n step=0/6", {"date": "20240101"}),
    ("fdb://param={p},step={s}/{s}", {"p": "167.128", "s": "10m"}),
    ("fdb://class=od,param=T_2M,quantile={q}", {"q": "34:100"}),
    ("fdb://class=od,date={d}", {"d": "-1"}),
]


@pytest.mark.parametrize("query, wildcards", QUERY_WILDCARDS)
@pytest.mark.parametrize(
    "order", [KeyOrder.generic(), KeyOrder.from_setting("step,date,x")]
)
def test_local_suffix_commutes_with_apply_wildcards(query, wildcards, order):
    normalized = normalize(query, order)
    pattern = parse(normalized, order).local_suffix()
    substituted = parse(apply_wildcards(normalized, wildcards), order)
    assert substituted.local_suffix() == apply_wildcards(pattern, wildcards)
    # Snakemake's DAG regex built from the pattern matches the substituted path
    m = re.match(regex_from_filepattern(pattern), substituted.local_suffix())
    assert m and {k: m.group(k) for k in wildcards} == wildcards
    # substituting into the raw query gives the same object
    assert parse(apply_wildcards(query, wildcards), order) == substituted


def test_list_wildcard_value_does_not_commute():
    """Documented limitation: wildcard values must be single MARS values."""
    q = "fdb://class=od,step={step}"
    pattern = parse(q).local_suffix()
    w = {"step": "0/6"}
    assert parse(apply_wildcards(q, w)).local_suffix() != apply_wildcards(pattern, w)


@pytest.mark.parametrize(
    "key, value, expected",
    [
        ("step", "06", 6),
        ("type", "CF", "cf"),
        ("param", "167.128", 167),
        ("param", "70.131", 131070),
        ("param", "2t", None),
        ("step", "10m", "10m"),
    ],
)
def test_comparable(key, value, expected):
    assert comparable(key, value) == expected


def test_no_fdb_libraries_imported():
    code = (
        "import sys, snakemake_storage_plugin_fdb.query as q\n"
        "q.normalize('fdb://class=od,step=0/6,param={p}')\n"
        "bad = [m for m in ('pyfdb', 'eccodes', 'gribapi') if m in sys.modules]\n"
        "assert not bad, bad\n"
    )
    subprocess.run([sys.executable, "-c", code], check=True)
