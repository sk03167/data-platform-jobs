# Data Platform Jobs

Deployable Python jobs for an AWS Glue learning data platform. This repository
owns the data-product code, local tests, package build, and code CI. Its paired
Terraform repository owns AWS infrastructure, IAM, and Glue job configuration.

## What the current job does

`brewery_pipeline` fetches a bounded page from the public Open Brewery DB API
and produces a small Bronze → Silver → Gold Delta Lake pipeline:

```text
Open Brewery DB
      │
      ▼
Bronze: typed raw records + ingestion metadata
      │
      ├── invalid required fields → quarantine
      ▼
Silver: valid, deduplicated brewery records
      ▼
Gold: country/type brewery summary and coordinate coverage
```

The Glue entry point accepts an `environment`, `page`, `per_page`, and stable
`batch_id`. It resolves environment-specific S3 paths such as
`lead-de-dev-bronze-data` and writes Delta tables there.

## Repository layout

```text
glue/glue_job.py                 AWS Glue entry script
src/brewery_pipeline/pipeline.py Reusable Bronze/Silver/Gold pipeline logic
tests/                           Pytest suite and fixed input fixtures
pyproject.toml                   Package metadata and dependency definitions
.github/workflows/test.yml       Lint, test, package-build, and artifact CI
```

The Glue script is intentionally outside `src/`. The wheel contains only
reusable package code; Glue receives `glue_job.py` as its script and the wheel
as an additional Python module.

## Local development

Prerequisites: Python 3.11 and Java 17. Spark needs the driver and worker to
use the same Python minor version; the shared pytest fixture explicitly points
both to the repository virtual environment.

```bash
python3.11 -m venv .venv
.venv/bin/python -m pip install -e ".[dev]"

JAVA_HOME=$(/usr/libexec/java_home -v 17) \
PATH="$JAVA_HOME/bin:$PATH" \
.venv/bin/python -m pytest -q
```

Run code-quality checks and build deployable artifacts locally with:

```bash
.venv/bin/python -m ruff check .
.venv/bin/python -m build
```

`dist/` contains a source distribution and the deployable wheel. It is ignored
by Git because CI creates the authoritative artifact for each run.

## Tests and data contract

Tests use small fixed JSON fixtures rather than the live API, so results are
repeatable and do not depend on network availability or changing source data.

The current suite verifies:

- environment names resolve to the correct Bronze, Silver, and Gold S3 paths;
- valid API data becomes typed Bronze data with `batch_id`, `ingested_at`, and
  `ingested_date` metadata;
- records missing required `id`, `name`, `city`, or `country` fields are
  quarantined with a clear reason;
- replaying the same source record with the same `batch_id` does not duplicate
  the Bronze Delta table.

The `batch_id` identifies one logical source extract. A retry must reuse the
same value; for a manual run, `brewery-page-1-YYYY-MM-DD` is a clear format.

## CI and artifact flow

GitHub Actions runs on pull requests and pushes to `main`:

```text
Ruff lint → local Spark/Delta pytest suite → Python wheel build
→ temporary GitHub Actions artifact
```

The workflow uploads `glue-artifacts`, containing the wheel and Glue script,
for seven days. This validates packaging on a clean runner without AWS
credentials. A later delivery step will publish immutable, Git-SHA-addressed
artifacts to an S3 artifact bucket only after changes reach `main`.

## What this project has taught me

- A production Glue job can have one executable script while still keeping
  testable pipeline code in a normal Python package.
- `pyproject.toml` is the source of truth for packaging and dependencies;
  editable installs are for development, while `python -m build` creates the
  actual wheel.
- Pytest fixtures provide controlled setup: JSON fixtures provide known source
  data, while the session-scoped Spark fixture supplies a reusable local Spark
  session.
- Delta `MERGE` logic and a caller-supplied batch identifier are the basis of
  retry-safe ingestion; a rerun is not automatically safe unless its identity
  is designed deliberately.
- CI must prove more than unit behavior: linting, a clean-environment Spark
  test, and a package build catch different classes of failure.
- The code and Terraform repositories have separate responsibilities: code CI
  creates tested artifacts, while infrastructure CI controls where and how
  those artifacts are deployed.

## Current boundary

This repository does not create AWS resources or publish to S3 yet. The next
phase provisions the artifact bucket and restricted GitHub OIDC publishing in
Terraform, then pins a tested artifact version in the Glue job configuration.
