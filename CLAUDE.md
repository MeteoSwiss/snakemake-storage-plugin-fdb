@README.md

# Notes for AI agents

Follow [docs/contributing.md](docs/contributing.md): setup, tests, lint, changelog,
commits, test data and site neutrality. In addition:

- The design documents are long. Open only the section you need, found by ID:
  [requirements](docs/design/requirements.md) has `FR-*`, `NFR-*`, limitations `L-*`
  and deferred work `D-*`; [architecture](docs/design/architecture.md) has concepts
  (§8), `ADR-*`, risks `R-*` and verified external facts (§13). In code comments, cite
  IDs or sections, never document history.
- A behaviour change updates the code, both design documents, `docs/user-guide.md`,
  `docs/reference.md` and `CHANGELOG.md` in the same change.
- A green `uv run pytest -q` does not mean the site suite ran: without its
  `SMK_FDB_TEST_*` variables it skips. Use `-rs` and report what was skipped.
- Unless asked, do not modify the samples in `tests/data/grib/`, the copied schemas in
  `tests/data/` or `uv.lock`, and do not create `.fdb*/` in the repository; tests use
  temporary directories.
- Before finishing, run `uv run ruff format --check .`, `uv run ruff check .` and
  `uv run pytest -q -rs`.
