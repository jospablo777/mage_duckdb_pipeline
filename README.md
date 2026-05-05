# SODA to DuckDB: building a data pipeline for high-performance analytics with modern tools

This project demonstrates how to build an efficient data pipeline to move data from the **Socrata Open Data API (SODA)** to a local **DuckDB** database. The goal is to create a lightweight, high-performance data repository optimized for running analytical queries on your local machine.

The project is ideal for data analysts, analytics engineers, and data engineers who want an efficient local workflow for ingesting, transforming, storing, and analyzing large open datasets.

You can revisit this in-depth [tutorial/article](https://jospablo777.github.io/mage_duckdb_pipeline/data_pipeline_mage.html) to build and understand this system.

## Key features

- **Data ingestion**: retrieve datasets from the Socrata Open Data API (SODA) for local analytics.
- **High-performance analytics**: use DuckDB for fast analytical SQL queries on a local database.
- **Modern data processing**: use Polars for efficient transformations before loading data into DuckDB.
- **Workflow orchestration**: use Mage to organize the data pipeline into reusable blocks.
- **Reproducible Python environment**: manage dependencies with `uv`, `pyproject.toml`, and `uv.lock`.

---

## Tools and technologies

- **[Mage](https://www.mage.ai/)**: orchestrates data pipelines through modular blocks and visual workflows.
- **[Polars](https://www.pola.rs/)**: provides a high-performance DataFrame engine for data cleaning and transformation.
- **[DuckDB](https://duckdb.org/)**: enables local OLAP-style SQL analytics without requiring a separate database server.
- **[uv](https://docs.astral.sh/uv/)**: manages Python dependencies, virtual environments, and reproducible lockfiles.

---

## Project workflow

1. **Data retrieval**: fetch data from the Socrata Open Data API (SODA) using Python.
2. **Data transformation**: use Polars to clean, reshape, and enrich the raw data.
3. **Data loading**: store the transformed data in a local DuckDB database.
4. **Analytics**: run SQL queries against DuckDB to generate insights.
5. **Orchestration**: manage the pipeline execution with Mage.

---

## Project structure

```text
mage_duckdb_pipeline/
├── docs/                 # Website with the tutorial
├── communication/        # Quarto source code for the tutorial article
├── transformers/         # Mage transformer blocks
├── data_loaders/         # Mage data loader blocks
├── custom/               # Mage custom blocks
├── data_exporters/       # Mage data exporter blocks
├── pipelines/            # Mage pipelines
│   └── socrata_iowa_liquor_pipeline/
├── data/                 # Local data, including the DuckDB database file
├── metadata.yaml         # Mage metadata for blocks, pipelines, and relationships
├── pyproject.toml        # Project metadata and direct Python dependencies
├── uv.lock               # Locked dependency versions for reproducible installs
├── .python-version       # Python version used by uv, if present
├── .nojekyll
├── dont_forget.sh        # Useful terminal commands and notes
├── LICENSE               # Open source license
└── README.md
```

---

## Installation and setup

### Prerequisites

- Python 3.12
- `uv`
- macOS or Linux. The project was originally developed on Ubuntu, and it has also been tested from macOS during development.

### Install uv

On macOS or Linux:

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

You can also install it with Homebrew on macOS:

```bash
brew install uv
```

Check that `uv` is available:

```bash
uv --version
```

### Clone the repository

```bash
git clone https://github.com/jospablo777/mage_duckdb_pipeline.git
cd mage_duckdb_pipeline
```

### Install dependencies

Install the project environment from the lockfile:

```bash
uv sync
```

This creates or updates the project virtual environment under `.venv/` and installs the dependencies defined by `pyproject.toml` and pinned in `uv.lock`.

You do not need to manually create a virtual environment with `python -m venv`, and you do not need to run `pip install -r requirements.txt`.

---

## Usage

### Run the pipeline from the terminal

From the project root, run:

```bash
uv run mage run . socrata_iowa_liquor_pipeline
```

This executes the Mage pipeline using the Python environment managed by `uv`.

### Start the Mage UI

From the project root, run:

```bash
uv run mage start
```

Then open the local Mage UI in your browser. Mage will show the available pipelines, blocks, logs, and execution status.

### Optional: activate the virtual environment

Most commands can be run with `uv run`, so activation is optional. If you prefer to activate the environment manually:

```bash
source .venv/bin/activate
```

After activation, you can run commands without the `uv run` prefix:

```bash
mage start
mage run . socrata_iowa_liquor_pipeline
```

---

## Dependency management with uv

Use `uv` to add, remove, update, and inspect dependencies.

### Add a dependency

```bash
uv add package-name
```

Example:

```bash
uv add duckdb
```

### Remove a dependency

```bash
uv remove package-name
```

### Update one dependency

```bash
uv lock --upgrade-package package-name
uv sync
```

Example:

```bash
uv lock --upgrade-package polars
uv sync
```

### Recreate the environment

If the local environment becomes inconsistent, remove `.venv/` and resync from the lockfile:

```bash
rm -rf .venv
uv sync
```

### Inspect the dependency tree

```bash
uv tree
```

---

## Notes for contributors

- Commit both `pyproject.toml` and `uv.lock`.
- Do not commit `.venv/`.
- Prefer `uv add`, `uv remove`, and `uv lock --upgrade-package` instead of manually editing dependency versions without syncing.
- Run pipeline commands with `uv run` so everyone uses the same locked environment.
- If you export a `requirements.txt` file for compatibility with external tools, keep `pyproject.toml` and `uv.lock` as the source of truth.

---

## Citation

No need to cite, but it would mean a lot if you did! 😃 Feel free to use this code and project structure in your personal or work projects. Make it yours!

---

## Contact

Have questions, suggestions, or just want to connect? Feel free to reach out!

- LinkedIn: [José Pablo Barrantes](https://www.linkedin.com/in/jose-barrantes/)
- BlueSky: [doggofan77.bsky.social](https://bsky.app/profile/doggofan77.bsky.social)
