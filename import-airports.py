import sqlite3
import csv
import requests
import os

# URLs
AIRPORTS_URL = "https://ourairports.com/data/airports.csv"
RUNWAYS_URL = "https://ourairports.com/data/runways.csv"

# Filenames
AIRPORTS_FILE = "airports.csv"
RUNWAYS_FILE = "runways.csv"
DB_FILE = "airports.db"

# Download CSV if not already present
def download_csv(url, filename):
    if not os.path.exists(filename):
        print(f"Downloading {filename}...")
        response = requests.get(url)
        with open(filename, "wb") as f:
            f.write(response.content)
    else:
        print(f"{filename} already exists.")

# Create SQLite schema
def create_tables(cursor):
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS airports (
        id INTEGER PRIMARY KEY,
        ident TEXT,
        type TEXT,
        name TEXT,
        latitude_deg REAL,
        longitude_deg REAL,
        elevation_ft INTEGER,
        iso_country TEXT,
        iso_region TEXT,
        municipality TEXT,
        gps_code TEXT,
        iata_code TEXT,
        local_code TEXT
    );
    """)
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS runways (
        id INTEGER PRIMARY KEY,
        airport_ref INTEGER,
        airport_ident TEXT,
        length_ft INTEGER,
        width_ft INTEGER,
        surface TEXT,
        lighted BOOLEAN,
        closed BOOLEAN,
        le_ident TEXT,
        le_latitude_deg REAL,
        le_longitude_deg REAL,
        he_ident TEXT,
        he_latitude_deg REAL,
        he_longitude_deg REAL,
        FOREIGN KEY (airport_ref) REFERENCES airports(id)
    );
    """)

# Import CSV into SQLite
def import_csv_to_db(cursor, csv_file, table, column_map):
    with open(csv_file, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        rows = []
        for row in reader:
            data = [row.get(col, None) or None for col in column_map]
            rows.append(data)
        placeholders = ",".join(["?"] * len(column_map))
        columns = ",".join(column_map)
        cursor.executemany(f"INSERT INTO {table} ({columns}) VALUES ({placeholders})", rows)

def main():
    # Step 1: Download files
    download_csv(AIRPORTS_URL, AIRPORTS_FILE)
    download_csv(RUNWAYS_URL, RUNWAYS_FILE)

    # Step 2: Create DB
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    create_tables(cur)

    # Step 3: Import data
    print("Importing airports...")
    airport_columns = [
        'id', 'ident', 'type', 'name', 'latitude_deg', 'longitude_deg',
        'elevation_ft', 'iso_country', 'iso_region', 'municipality',
        'gps_code', 'iata_code', 'local_code'
    ]
    import_csv_to_db(cur, AIRPORTS_FILE, 'airports', airport_columns)

    print("Importing runways...")
    runway_columns = [
        'id', 'airport_ref', 'airport_ident', 'length_ft', 'width_ft',
        'surface', 'lighted', 'closed', 'le_ident', 'le_latitude_deg',
        'le_longitude_deg', 'he_ident', 'he_latitude_deg', 'he_longitude_deg'
    ]
    import_csv_to_db(cur, RUNWAYS_FILE, 'runways', runway_columns)

    conn.commit()
    conn.close()
    print(f"✅ Import completed. SQLite DB saved as '{DB_FILE}'.")

if __name__ == "__main__":
    main()
