import sys
import os

# Ensure root directory is in sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), 'data_pipeline')))

from data_pipeline.run_daily import run_daily
from data_pipeline.ingestion.import_historical_air_quality import import_historical_data
from data_pipeline.ingestion.import_burden_data import import_burden_data
from data_pipeline.transformation.merge_public_sources import merge_public_sources, create_schema_and_table
from data_pipeline.transformation.merge_and_calculate_city_aqi import merge_city_data
from data_pipeline.transformation.preprocess_burden_excel import preprocess_excel
from data_pipeline.transformation.merge_burden_with_aqi import merge_burden_data

def main():
    print("🚀 Starting Full Pipeline...\n")

    run_daily()
    print("✅ Finished run_daily\n")

    import_historical_data()
    print("✅ Imported historical air quality data\n")

    import_burden_data()
    print("✅ Imported burden datasets\n")

    create_schema_and_table()

    merge_public_sources()
    print("✅ Merged public sources\n")

    merge_city_data()
    print("✅ Merged and calculated AQI per city\n")

    preprocess_excel()
    print("✅ Preprocessed burden Excel files\n")

    merge_burden_data()
    print("✅ Merged AQI with burden datasets\n")

    print("\n🎉 Full pipeline execution complete!")

if __name__ == "__main__":
    main()
