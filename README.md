# 🌍 Air Quality Analysis Project

This project analyzes global air quality data by combining real-time sensor feeds, historical pollution metrics, and public health data. It provides a modular, automated pipeline to collect, clean, store, and visualize data across multiple cities using PostgreSQL, Python, and Power BI.

---

## 🚀 Features

- 📡 Ingests real-time air quality data from the WAQI API
- 🗃 Loads and cleans historical datasets (CSV) and burden of disease data (Excel)
- 🧠 Calculates AQI scores using EPA's PM2.5 breakpoints
- 🏥 Merges air quality and health impact datasets
- 🧼 Deduplicates and standardizes data in PostgreSQL
- 📊 Ready for Power BI dashboards via clean exports
- ☁️ Hosted on Render with automation via GitHub Actions (runs every 6 hours)

---

## 📁 Folder Structure

```
air_quality_project/
├── config/                     # DB credentials & API token (gitignored)
│   └── db_config.py
│
├── data/                       # Input datasets 
│   ├── Air Quality Datasets/
│   ├── Burden Datasets/
│   └── Cleaned Burden Datasets/
│
├── data_pipeline/              # Core ETL pipeline
│   ├── cleaning/
│   │   └── remove_duplicates.py
│   ├── ingestion/
│   │   ├── fetch_waqi.py
│   │   ├── import_burden_data.py
│   │   └── import_historical_air_quality.py
│   ├── output/
│   │   └── clean_export_data.py
│   ├── transformation/
│   │   ├── merge_and_calculate_city_aqi.py
│   │   ├── merge_burden_with_aqi.py
│   │   ├── merge_public_sources.py
│   │   └── preprocess_burden_excel.py
│   ├── insert_to_db.py
│   └── run_daily.py
│
├── sql/                        # SQL schema and table setup
├── powerbi/                    # Excel exports and PBIX files
├── logs/                       # Logging outputs
├── main.py                     # Runs the full pipeline
├── .github/workflows/          # GitHub Actions pipeline
└── requirements.txt
```

---

## 🛠 Setup

1. Clone the repo and create a virtual environment:
   ```bash
   python -m venv venv
   source venv/bin/activate  # or venv\Scripts\activate on Windows
   pip install -r requirements.txt
   ```

2. Add your database credentials in `config/db_config.py`:
```python
DB_CONFIG = {
    'host': 'your-host',
    'dbname': 'your-db',
    'user': 'your-user',
    'password': 'your-password',
    'port': 5432
}
```

3. Run the pipeline:
   ```bash
   python main.py
   ```

---

## 🔁 GitHub Actions

- `run_pipeline.yml`: runs `main.py` every 6 hours
- Uses `DB_CONFIG` secret for credentials

---

## 📦 Requirements

All dependencies are listed in `requirements.txt`.

---

## 📊 Output

Final cleaned table: `final_city_burden_merged` → used in Power BI.

---

## 🔒 Notes

Sensitive files like `.env`, `db_config.py`, and raw data are excluded via `.gitignore`.
