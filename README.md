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

## Installation and Setup

### Prerequisites
- Python 3.12 or later
- Required libraries: `requests`, `polars`, `duckdb`, `mage-ai`, `tqdm`
- Linux (I used Ubuntu 24.04)

### Clone the Repository
```bash
git clone https://github.com/...TODO
cd ...TODO