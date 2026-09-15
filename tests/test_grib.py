"""GRIB helpers: message splitting, MARS keys, in-memory variants (no FDB)."""

import subprocess
import sys
from pathlib import Path

import pytest

from snakemake_storage_plugin_fdb.grib import (
    GribError,
    GribMessage,
    mars_keys,
    split_messages,
    variant,
)

DATA = Path(__file__).resolve().parent / "data"
SAMPLES = DATA / "grib" / "ecmwf"
OGD_SAMPLES = DATA / "grib" / "meteoswiss"
PERT = OGD_SAMPLES / "icon-ch2-eps_202609151200_step6_t_2m_pert_m1-2.grib2"

_samples_present = pytest.mark.skipif(
    not (SAMPLES / "template.grib").exists(), reason="no ECMWF samples"
)


def needs_samples(func):
    return pytest.mark.needs_samples(_samples_present(func))


# architecture.md §13.2: MARS namespace keys and paramId of the ECMWF samples
ECMWF_SAMPLES = {
    "template.grib": (
        10800,
        10732,
        {
            "class": "ea",
            "expver": "0001",
            "stream": "enda",
            "date": "20200101",
            "time": "0000",
            "domain": "g",
            "type": "an",
            "levtype": "sfc",
            "step": "0",
            "param": "167.128",
            "number": "0",
        },
        "167",
    ),
    "steprange.grib": (
        360,
        276,
        {
            "class": "od",
            "expver": "0001",
            "stream": "enfo",
            "date": "20260317",
            "time": "1200",
            "domain": "g",
            "type": "ep",
            "levtype": "sfc",
            "step": "0-24",
            "param": "70.131",
        },
        "131070",
    ),
    "quantile.grib": (
        480,
        378,
        {
            "class": "od",
            "expver": "0001",
            "stream": "efhs",
            "date": "20260313",
            "time": "0000",
            "domain": "g",
            "type": "cd",
            "levtype": "sfc",
            "step": "60-132",
            "param": "228.128",
            "quantile": "34:100",
        },
        "228",
    ),
    "synth11.grib": (
        660,
        660,
        {
            "class": "od",
            "expver": "0001",
            "stream": "oper",
            "date": "20230508",
            "time": "1200",
            "domain": "g",
            "type": "fc",
            "levtype": "sfc",
            "step": "1",
            "param": "130.151",
        },
        "151130",
    ),
}


@pytest.fixture(scope="module")
def grib2() -> bytes:
    """eccodes' bundled ``GRIB2`` sample (synthetic, always available)."""
    import eccodes

    handle = eccodes.codes_grib_new_from_samples("GRIB2")
    try:
        return eccodes.codes_get_message(handle)
    finally:
        eccodes.codes_release(handle)


def _write(tmp_path: Path, data: bytes, name: str = "f.grib") -> Path:
    path = tmp_path / name
    path.write_bytes(data)
    return path


def test_import_does_not_load_eccodes():
    code = (
        "import sys, snakemake_storage_plugin_fdb.grib\n"
        "bad = [m for m in ('pyfdb', 'eccodes', 'gribapi') if m in sys.modules]\n"
        "assert not bad, bad\n"
    )
    subprocess.run([sys.executable, "-c", code], check=True)


@needs_samples
@pytest.mark.parametrize("name", sorted(ECMWF_SAMPLES))
def test_ecmwf_samples(name):
    file_size, length, mars, param_id = ECMWF_SAMPLES[name]
    raw = (SAMPLES / name).read_bytes()
    assert len(raw) == file_size
    (msg,) = split_messages(SAMPLES / name)
    assert msg == GribMessage(0, length, raw[:length], mars, param_id)
    assert msg.data.endswith(b"7777")
    assert mars_keys(msg.data) == (mars, param_id)


@needs_samples
def test_template_padding_is_nul():
    """GRIB1 samples are NUL-padded to 120-byte records after ``7777``."""
    raw = (SAMPLES / "template.grib").read_bytes()
    assert len(raw) % 120 == 0
    assert set(raw[10732:]) == {0}


@needs_samples
def test_two_padded_files_concatenated(tmp_path):
    raw = (SAMPLES / "template.grib").read_bytes()
    msgs = split_messages(_write(tmp_path, raw + raw))
    assert [(m.offset, m.length) for m in msgs] == [(0, 10732), (10800, 10732)]
    assert msgs[0].data == msgs[1].data == raw[:10732]


def test_synthetic_messages_concatenated(tmp_path, grib2):
    a = variant(grib2, step=0)
    b = variant(grib2, step=6)
    msgs = split_messages(_write(tmp_path, a + b))
    assert [(m.offset, m.length) for m in msgs] == [(0, len(a)), (len(a), len(b))]
    assert [m.data for m in msgs] == [a, b]
    assert [m.mars["step"] for m in msgs] == ["0", "6"]


def test_nul_padding_between_and_after_messages(tmp_path, grib2):
    pad = b"\0" * 7
    msgs = split_messages(_write(tmp_path, grib2 + pad + grib2 + pad))
    n = len(grib2)
    assert [(m.offset, m.length) for m in msgs] == [(0, n), (n + 7, n)]


@needs_samples
def test_trailing_garbage(tmp_path):
    raw = (SAMPLES / "template.grib").read_bytes()
    with pytest.raises(GribError, match="trailing non-GRIB bytes at offset 10800"):
        split_messages(_write(tmp_path, raw + b"GARBAGE"))


@pytest.mark.parametrize(
    "build, message",
    [
        (lambda m: m + b"GARBAGE", "trailing non-GRIB bytes"),
        (lambda m: m + b"\0\0x", "trailing non-GRIB bytes"),
        (lambda m: m + b"zz" + m, "non-GRIB bytes at offset"),
        (lambda m: b"xx" + m, "non-GRIB bytes at offset 0"),
        (lambda m: m + m[:-10], "cannot read GRIB message"),
        (lambda m: b"test", "is not GRIB"),
        (lambda m: b"", "is not GRIB"),
        (lambda m: b"\0" * 16, "is not GRIB"),
        (lambda m: b"GRIB", "cannot read GRIB message"),
    ],
    ids=["garbage", "nul-then-byte", "between", "leading", "truncated", "text",
         "empty", "nul-only", "indicator-only"],
)  # fmt: skip
def test_split_errors(tmp_path, grib2, build, message):
    with pytest.raises(GribError, match=message):
        split_messages(_write(tmp_path, build(grib2)))


def test_split_missing_file(tmp_path):
    with pytest.raises(FileNotFoundError):
        split_messages(tmp_path / "missing.grib")


@pytest.mark.skipif(not PERT.exists(), reason="no MeteoSwiss samples")
def test_multi_message_file_offsets():
    """Generic splitting of a 2-message GRIB2 file (no site definitions needed)."""
    raw = PERT.read_bytes()
    msgs = split_messages(PERT)
    assert [(m.offset, m.length) for m in msgs] == [(0, 175), (175, 175)]
    assert b"".join(m.data for m in msgs) == raw
    for m in msgs:
        assert {"date", "time", "step", "levtype", "param"} <= m.mars.keys()
        assert m.param_id.isdigit()


@pytest.mark.parametrize(
    "data, message",
    [
        (b"test", "not a GRIB message"),
        (b"", "not a GRIB message"),
        (b"GRIB", "truncated"),
    ],
)
def test_mars_keys_errors(data, message):
    with pytest.raises(GribError, match=message):
        mars_keys(data)


def test_mars_keys_truncated(grib2):
    with pytest.raises(GribError, match="truncated"):
        mars_keys(grib2[:-1])


def test_grib2_sample_with_centre_215(grib2):
    """A GRIB2 message with a local centre decodes with whatever definitions are set."""
    mars, param_id = mars_keys(variant(grib2, centre=215))
    assert {"date", "time", "step", "levtype", "param"} <= mars.keys()
    assert param_id.isdigit()


@needs_samples
def test_variant_zeroed():
    raw = (SAMPLES / "template.grib").read_bytes()
    small = variant(raw, stream="oper", step=6, date=20200102)
    assert 200 <= len(small) <= 300  # ~236 bytes (NFR-PERF-004)
    mars, param_id = mars_keys(small)
    assert mars["stream"] == "oper" and mars["step"] == "6"
    assert mars["date"] == "20200102"
    assert "number" not in mars  # eccodes drops number for stream=oper
    assert param_id == "167"


@needs_samples
def test_variant_keeps_values_and_accepts_reserved_names():
    raw = (SAMPLES / "template.grib").read_bytes()
    full = variant(raw, zero_values=False, **{"class": "od", "paramId": 165})
    assert len(full) == 10732
    mars, param_id = mars_keys(full)
    assert (mars["class"], param_id) == ("od", "165")
    assert variant(raw, zero_values=False) == raw[:10732]


def test_variant_errors(grib2):
    with pytest.raises(GribError, match="nosuchkey"):
        variant(grib2, nosuchkey=1)
    with pytest.raises(GribError, match="not a GRIB message"):
        variant(b"test")
