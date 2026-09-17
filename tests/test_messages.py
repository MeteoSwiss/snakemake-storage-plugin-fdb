"""What the plugin says: the startup line, the lookup reports, removal, the end-of-run
summary and the sharpened error messages (requirements.md §2.3, §2.5, §2.7, §2.10,
§2.12).
"""

import logging
from pathlib import Path

import pytest
import yaml
from snakemake_interface_common.exceptions import WorkflowError

from snakemake_storage_plugin_fdb import StorageProvider, summary
from snakemake_storage_plugin_fdb.backend import (
    map_error,
    missing_default_schema,
    parse_schema,
)

from .conftest import TEST_SCHEMA

DATA = Path(__file__).resolve().parent / "data"
SAMPLES = DATA / "grib" / "ecmwf"
EA = (
    "class=ea,expver=0001,stream=oper,date=20200101,time=0000,domain=g,type=an,"
    "levtype=sfc"
)

needs_samples = pytest.mark.skipif(
    not (SAMPLES / "template.grib").exists(), reason="no ECMWF samples"
)


@pytest.fixture
def seeded_provider(seeded_fdb, make_provider):
    def make(**settings) -> StorageProvider:
        return make_provider(config=yaml.safe_dump(seeded_fdb.config), **settings)

    return make


def _messages(caplog, level: int) -> list[str]:
    return [r.getMessage() for r in caplog.records if r.levelno == level]


# --- the startup line (FR-CONF-010) --------------------------------------------------


def test_startup_line_names_the_fdb(tmp_path, make_provider, caplog):
    config = tmp_path / "fdb" / "config.yaml"
    config.parent.mkdir(parents=True, exist_ok=True)
    config.write_text(
        yaml.safe_dump(
            {
                "type": "local",
                "engine": "toc",
                "schema": str(TEST_SCHEMA),
                "spaces": [
                    {"handler": "Default", "roots": [{"path": str(tmp_path / "db")}]}
                ],
            }
        )
    )
    (tmp_path / "db").mkdir()
    with caplog.at_level(logging.INFO, logger="fdb-test"):
        make_provider(config=str(config))
        make_provider(config=str(config))  # a second provider says nothing more
    lines = [m for m in _messages(caplog, logging.INFO) if "using" in m]
    assert len(lines) == 1
    assert str(config) in lines[0]
    assert f"roots: {tmp_path / 'db'}" in lines[0]
    assert f"schema: {TEST_SCHEMA}" in lines[0]
    assert "input tracking: lookup" in lines[0]


def test_startup_line_for_inline_configuration(make_provider, caplog):
    with caplog.at_level(logging.INFO, logger="fdb-test"):
        make_provider()
    assert any("using inline configuration" in m for m in _messages(caplog, 20))


# --- no configuration at all (FR-CONF-010) -------------------------------------------


def test_missing_default_schema_is_reported_before_pyfdb(make_provider, monkeypatch):
    """The bundled schema does not exist in the wheels, so a run without any
    configuration fails with the plugin's one-line error, not eckit's backtrace."""
    schema = missing_default_schema({})
    assert schema is not None and schema.endswith("fdb5lib/etc/fdb/schema")
    with pytest.raises(WorkflowError) as e:
        make_provider(config=None)
    message = str(e.value)
    assert message.startswith("FDB configuration error: Cannot open ")
    assert "no FDB configuration was given" in message


def test_missing_default_schema_skipped_when_the_environment_names_an_fdb(tmp_path):
    for name in ("FDB_CONFIG", "FDB5_CONFIG", "FDB_CONFIG_FILE", "FDB_HOME"):
        assert missing_default_schema({name: str(tmp_path)}) is None


# --- lookups (FR-READ-008, FR-ERR-008) -----------------------------------------------


@needs_samples
def test_absent_query_missing_first_level_key_is_reported(seeded_provider, caplog):
    """A query that omits a key of the schema's first rule level cannot match: said
    once per query at info level, where a plain run sees it."""
    obj = seeded_provider().object(
        f"fdb://{EA.replace(',domain=g', '')},step=0,param=167"
    )
    with caplog.at_level(logging.INFO, logger="fdb-test"):
        assert obj.exists() is False
        assert obj.exists() is False  # once per query
    lines = [m for m in _messages(caplog, logging.INFO) if "no fields in FDB" in m]
    assert len(lines) == 1
    assert "does not name domain" in lines[0]
    assert "FDB matches keys exactly" in lines[0]


@needs_samples
def test_absent_query_with_every_key_stays_quiet(seeded_provider, caplog):
    """The normal case of an output that does not exist yet: nothing above debug."""
    obj = seeded_provider().object(f"fdb://{EA},step=99,param=167")
    with caplog.at_level(logging.DEBUG, logger="fdb-test"):
        assert obj.exists() is False
    assert not _messages(caplog, logging.INFO)
    assert not _messages(caplog, logging.WARNING)
    assert any("0 of 1 fields found in FDB" in m for m in _messages(caplog, 10))


@needs_samples
def test_key_alias_warns_once(seeded_provider, caplog):
    """metkit maps ``levtyp`` to ``levtype`` silently (L-27); the plugin says so."""
    query = f"fdb://{EA.replace('levtype=sfc', 'levtyp=sfc')},step=0,param=167"
    with caplog.at_level(logging.WARNING, logger="fdb-test"):
        provider = seeded_provider()
        assert provider.object(query).exists() is True
        assert provider.object(query).exists() is True
    warnings = [m for m in _messages(caplog, logging.WARNING) if "alias" in m]
    assert len(warnings) == 1
    assert "levtyp (canonical: levtype)" in warnings[0]


# --- removal (FR-REMOVE-002) ---------------------------------------------------------


@needs_samples
def test_remove_complete_query_explains_the_consequence(seeded_provider, caplog):
    obj = seeded_provider().object(f"fdb://{EA},step=0/6/12,param=167")
    with caplog.at_level(logging.WARNING, logger="fdb-test"):
        obj.remove()
        obj.remove()  # once per query
    warnings = _messages(caplog, logging.WARNING)
    assert len(warnings) == 1
    assert "all 3 fields are in FDB" in warnings[0]
    assert "Nothing was removed" in warnings[0]
    assert "still looks complete" in warnings[0]
    assert "-R <rule>" in warnings[0]
    assert warnings[0].count(obj.query) == 1  # Snakemake prints the query itself


@needs_samples
def test_remove_partial_query_names_the_missing_fields(seeded_provider, caplog):
    obj = seeded_provider().object(f"fdb://{EA},step=0/6/12/18,param=167")
    with caplog.at_level(logging.WARNING, logger="fdb-test"):
        obj.remove()
    warnings = _messages(caplog, logging.WARNING)
    assert "3 of 4 fields are in FDB; missing: step=18" in warnings[0]


@needs_samples
def test_remove_absent_query_is_a_clause_not_a_warning(seeded_provider, caplog):
    obj = seeded_provider().object(f"fdb://{EA},step=99,param=167")
    with caplog.at_level(logging.DEBUG, logger="fdb-test"):
        obj.remove()
    assert not _messages(caplog, logging.WARNING)
    assert any("nothing to remove" in m for m in _messages(caplog, logging.INFO))


@needs_samples
def test_remove_policy_error_keeps_the_counts(seeded_provider):
    obj = seeded_provider(remove_policy="error").object(
        f"fdb://{EA},step=0/6/12,param=167"
    )
    with pytest.raises(WorkflowError, match="remove_policy=error.*all 3 fields"):
        obj.remove()


# --- the end-of-run summary (FR-IFACE-007) -------------------------------------------


def _incomplete(**kwargs) -> summary.Incomplete:
    values = dict(
        report="fdb://q: 3 of 4 fields found in FDB; missing: step=18",
        found=3,
        expected=4,
        plain=True,
        local_file=True,
        fresh=0,
    )
    values.update(kwargs)
    return summary.Incomplete(**values)


def test_summary_counts_archived_and_masked_fields():
    run = summary.RunState()
    run.record_lookup("fdb://a", 3)  # three fields were there before the run
    run.record_archive("fdb://a", 3, direct=False)
    run.record_lookup("fdb://b", 0)
    run.record_archive("fdb://b", 4, direct=True)
    run.record_archive("fdb://b", 4)  # a later lookup of the same fields
    lines = run.lines()
    assert lines[0].startswith("7 fields archived (2 queries), 3 of which masked")
    assert summary.MASK_NOTE in lines[0]


def test_summary_lists_incomplete_queries_and_omits_produced_ones():
    run = summary.RunState()
    run.record_incomplete("fdb://a", _incomplete())
    run.record_incomplete("fdb://b", _incomplete())
    run.record_archive("fdb://b", 4, direct=True)  # a job produced it after all
    lines = run.lines()
    assert "1 query was incomplete in FDB and no job produced them:" in lines
    assert [x for x in lines if x.strip().startswith("fdb://q:")]
    assert lines[-1] == "Run -R <rule> or --forceall to produce them."


def test_summary_diagnoses_a_file_based_output_the_job_archived_itself():
    run = summary.RunState()
    run.record_incomplete("fdb://a", _incomplete(local_file=False, fresh=3))
    (line,) = run.lines()
    assert line.startswith("fdb://a was declared as a file-based output")
    assert "3 of 4 of its fields are in FDB" in line
    assert "declare the output retrieve=False" in line


def test_summary_warns_about_an_archive_mode_no_output_can_use():
    run = summary.RunState()
    run.archive_modes.add("identifier")
    run.record_archive("fdb://a", 2, direct=True)
    assert any("archive_mode=identifier had no effect" in x for x in run.lines())
    run.record_archive("fdb://b", 2, direct=False)
    assert not any("had no effect" in x for x in run.lines())


def test_summary_is_empty_and_emitted_once(caplog):
    run = summary.RunState()
    logger = logging.getLogger("fdb-test")
    with caplog.at_level(logging.INFO, logger="fdb-test"):
        assert run.emit(logger) is False  # nothing to say
    assert not caplog.records
    run = summary.RunState()
    run.record_lookup("fdb://a", 0)
    run.record_archive("fdb://a", 1, direct=False)
    assert run.lines()[0].startswith("1 field archived (1 query), 0 of which")
    with caplog.at_level(logging.INFO, logger="fdb-test"):
        assert run.emit(logger) is True
        assert run.emit(logger) is False
    assert len(_messages(caplog, logging.INFO)) == 1
    assert "FDB storage: run summary:" in caplog.records[0].getMessage()


def test_summary_hook_is_the_snakemake_logger_shutdown():
    """ADR-041: the block is logged from ``LoggerManager.stop``, while the console and
    log-file handlers are still attached."""
    from snakemake.logging import LoggerManager

    summary.reset()
    assert summary.install(logging.getLogger("fdb-test")) == "logger"
    assert getattr(LoggerManager.stop, "__fdb_run_summary__", False) is True


# --- error messages (FR-ERR-007) -----------------------------------------------------


def _user_error(text: str) -> RuntimeError:
    return RuntimeError(f"UserError: {text}")


def test_map_error_names_an_unknown_key_with_the_schema_keys():
    exc = _user_error(
        "Cannot match [nonsensekey] in [style,class,type,stream,number,levtype]"
    )
    info = parse_schema(TEST_SCHEMA.read_text())
    error = map_error(exc, "fdb://class=ea", schema_keys=info.keys)
    message = str(error)
    assert "unknown MARS key 'nonsensekey'" in message
    assert "…" not in message
    assert "the FDB schema names: class, expver" in message
    error = map_error(_user_error("Cannot match [numbre] in [class,number]"), "fdb://q")
    assert "did you mean 'number'?" in str(error)
    assert "the MARS language accepts: class, number" in str(error)


def test_map_error_translates_a_key_refused_by_another_key():
    exc = _user_error(
        "TypeToByListFloat[name=levelist]: Key [levelist] not acceptable with context: "
        "Context[Include[key=levtype,vals=[{al,o2d,sfc,wave}]]]"
    )
    error = map_error(exc, "fdb://q", request={"levtype": "sfc", "levelist": "500"})
    assert "levelist is not allowed with levtype=sfc" in str(error)
    assert "Context[" not in str(error)
    error = map_error(exc, "fdb://q")  # without the request: the refused values
    assert "levelist is not allowed with these levtype values (al,o2d,sfc,wave)" in str(
        error
    )


def test_map_error_adds_the_mars_shape_hint():
    exc = _user_error("Bad value: Invalid date 2020-01-01T00:00 request=retrieve,...")
    message = str(map_error(exc, "fdb://q"))
    assert "Invalid date 2020-01-01T00:00" in message
    assert "MARS dates are YYYYMMDD, times HHMM" in message
    assert "a wildcard used in a query must expand to a MARS value" in message


def test_storage_object_repr_is_the_query(make_provider):
    obj = make_provider().object(f"fdb://{EA},step=0,param=167")
    assert repr(obj) == f"<{obj.query}>"
    assert str({"storage_object": obj}).startswith("{'storage_object': <fdb://class=ea")


def test_clean_errors_leaves_no_plugin_frames(make_provider):
    """FR-ERR-007: the error Snakemake renders carries the plugin's frames no more
    (they are dropped by the synthetic file name of the wrapper, ADR-042)."""
    import traceback

    from snakemake.exceptions import cut_traceback, format_traceback

    from snakemake_storage_plugin_fdb import frames

    obj = make_provider().object("fdb://class=ea,date={date},param=167")
    with pytest.raises(WorkflowError) as e:
        obj.exists()
    tb = traceback.extract_tb(e.value.__traceback__)
    assert [f.filename for f in tb] == [__file__, frames.FILENAME]
    # what Snakemake would print for it: the caller and one self-describing frame
    rendered = list(format_traceback(cut_traceback(e.value), {}))
    assert not any("/snakemake_storage_plugin_fdb/" in line for line in rendered)
    assert rendered[-1].strip().startswith(f'File "{frames.FILENAME}"')
    assert rendered[-1].endswith("in fdb_storage_error")
