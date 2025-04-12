# Air Quality Analysis Project 🌍

This project collects, cleans, and analyzes real-time and historical air quality data from multiple global cities. The data is stored in a PostgreSQL database and exported for Power BI dashboards.

## Features
- 🌐 Real-time air quality data from the WAQI API
- 🗃️ Historical datasets imported from CSV files
- 🧼 Deduplication and data cleaning logic
- 📦 PostgreSQL + pgAdmin backend
- 📊 Excel exports for Power BI dashboards

## Tech Stack
- Python
- PostgreSQL
- VS Code
- Power BI
- WAQI API

## Folder Structure
project/ ├── config/ # Database credentials (gitignored) ├── data/ # Historical air quality & healthcare CSVs ├── data_pipeline/ # ETL scripts (fetch, insert, clean, dedup) ├── logs/ # Daily log files ├── powerbi/ # Final exported Excel files


## Setup
1. Clone the repo
2. Run `pip install -r requirements.txt`
3. Add your `db_config.py` under `config/` with DB credentials
4. Run the pipeline: `python data_pipeline/run_daily.py`

## Disclaimer
This repo ignores sensitive files like `db_config.py`, `.env`, and local `.xlsx` exports.
