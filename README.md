# 🌍 Air Quality Analysis Project

A robust end-to-end data pipeline that collects, processes, and analyzes real-time and historical air quality data across major cities. This system integrates health burden statistics and supports visual analytics via Power BI.

---

## 🚀 Features

- 📡 Fetches real-time air quality data via the WAQI API
- 📁 Ingests historical air quality CSVs and health burden Excel files
- 🧹 Cleans, deduplicates, and preprocesses datasets
- 🔁 Combines real-time + historical + health burden data
- 🧠 Calculates AQI and classifies into health categories
- 💾 Stores data in PostgreSQL under appropriate schemas (`real_time_data`, `historical_data`, `burden_data`, `transformations`)
- 📤 Exports cleaned results for Power BI dashboards

---

## ⚙️ Tech Stack

- **Language**: Python
- **Database**: PostgreSQL + pgAdmin
- **Tools**: VS Code, Power BI
- **API**: WAQI (World Air Quality Index)

---

## 📁 Project Structure

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
└── requirements.txt
```

---

## ▶️ Usage

### 🧰 Setup

1. Clone the repo:
   ```bash
   git clone https://github.com/your-username/air_quality_project.git
   cd air_quality_project
   ```

2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

3. Create `config/db_config.py` with your credentials:
   ```python
   DB_CONFIG = {
       'dbname': 'air_quality_db',
       'user': 'postgres',
       'password': 'your_password',
       'host': 'localhost',
       'port': 5432
   }

   API_TOKEN = 'your_waqi_api_key'

   CITIES = ['beijing', 'delhi', 'paris']
   ```

4. Prepare your PostgreSQL database with required schemas:
   - `real_time_data`
   - `historical_data`
   - `burden_data`
   - `transformations`

### ✅ Run the Pipeline

```bash
python main.py
```

This will:

1. Fetch and insert real-time data
2. Ingest historical and burden datasets
3. Clean, merge, and calculate AQI
4. Insert results into PostgreSQL for analysis

---

## 📊 Power BI

Final data is available in the `powerbi/` folder or can be loaded directly from:

- `transformations.final_city_merged`
- `transformations.burden_aqi_merged`

Use these tables for dashboarding and analytics in Power BI.

---

## ⚠️ Disclaimer

This repository **excludes** sensitive or large files:
- `config/db_config.py`
- `.env`, `.vscode/`, `*.log`, and other machine-specific files

---