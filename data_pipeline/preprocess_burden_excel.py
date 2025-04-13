import os
import pandas as pd
import re

# === SETUP ===
input_folder = "data/Burden Datasets"
output_folder = "data/Cleaned Burden Datasets"

# Make sure output folder exists
os.makedirs(output_folder, exist_ok=True)

# === CLEANING FUNCTION ===
def clean_cell(cell):
    if pd.isna(cell):
        return None
    text = str(cell)
    text = re.sub(r'\[.*?\]', '', text)  # remove bracket content
    return text.strip()

# === PROCESS ALL FILES ===
for filename in os.listdir(input_folder):
    if filename.endswith(".xlsx"):
        input_path = os.path.join(input_folder, filename)
        output_path = os.path.join(output_folder, filename)

        print(f"📄 Cleaning: {filename}")

        # Load file
        df = pd.read_excel(input_path, dtype=str)

        # Clean column names
        df.columns = [clean_cell(col) for col in df.columns]

        # Clean data values
        df_cleaned = df.applymap(clean_cell)

        # Save cleaned file
        df_cleaned.to_excel(output_path, index=False)
        print(f"✅ Saved cleaned file to: {output_path}")
