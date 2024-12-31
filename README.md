# SODA to DuckDB: building a data pipeline for high-performance analytics

This project demonstrates how to build an efficient data pipeline to move data from the **Socrata Open Data API (SODA)** to a local **DuckDB** database. The goal is to create a lightweight, high-performance data repository optimized for running analytical queries.

## Key Features

- **Data Ingestion**: rethrieve a dataset from the SODA API.
- **High-Performance Analytics**: use DuckDB, a columnar database, to enable aggressive querying for analytics.
- **Modern Tooling**: utilize open-source tools like Mage, Polars, and DuckDB to streamline the ETL process.

---

## Tools and Technologies

- **[Mage](https://www.mage.ai/)**: a modern, open-source data orchestration tool.
- **[Polars](https://www.pola.rs/)**: a high-performance DataFrame library for efficient data manipulation in Python.
- **[DuckDB](https://duckdb.org/)**: a fast, in-process SQL OLAP database engine.

---

## Project Workflow

1. **Data Retrieval**: fetch data from the Socrata Open Data API (SODA) using Python.
2. **Data Transformation**: use Polars to manipulate and transform the data before loading it into DuckDB.
3. **Data Loading**: store the resulting data into DuckDB for high-performance analytics.
4. **Analytics**: perform SQL queries on the DuckDB database to generate insights.

---

## Project structure

```
TODO
```

## Installation and Setup

### Prerequisites
- Python 3.12 or later
- virtualenv (recommended)
- Linux (I used Ubuntu 24.04)

### Clone the Repository
```bash
git clone https://github.com/jospablo777/mage_duckdb_pipeline.git
cd mage_duckdb_pipeline
```
### Install Dependencies
Set up a virtual environment and install the required Python packages:

```bash
python -m venv venv             # The first 'venv' is the command, the second is the name of the folder for the virtual environment.
source venv/bin/activate        # Activate the virtual environment.
pip install -r requirements.txt # Install dependencies from the requirements file.
```


## Usage

### Running the Pipeline

Run it directly from the terminal. To fetch some data, in the project folder run:

```bash
mage run . socrata_iowa_liquor_pipeline
```

To start the Mage UI, run in the terminal (in the project's folder):
```bash
mage start
```

## Citation

No need to cite, but it would mean a lot if you did! 😃 Feel free to use this code and project structure in your personal or work projects—make it yours!

## Contact

Let's keep in touch!

- LinkedIn: [José Pablo Barrantes](https://www.linkedin.com/in/jose-barrantes/)
- BlueSky: [doggofan77.bsky.social](https://bsky.app/profile/doggofan77.bsky.social)