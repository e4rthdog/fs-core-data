import sqlite3
import csv
import os

# Source data directory
SOURCE_DATA_DIR = "source-data"

# Filenames
AIRPORTS_FILE = os.path.join(SOURCE_DATA_DIR, "airports.csv")
RUNWAYS_FILE = os.path.join(SOURCE_DATA_DIR, "runways.csv")
DB_FILE = "airports.db"

# Create SQLite schema
def create_tables(cursor):
    cursor.execute("""
    CREATE TABLE airports (
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
    CREATE TABLE runways (
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

# Drop views and recreate them
def create_views(cursor):
    cursor.execute("""
    CREATE VIEW runway_headings AS
    SELECT 
        r.airport_ident AS airport_icao,
        r.le_ident AS runway,
        CASE 
            WHEN CAST(SUBSTR(r.le_ident, 1, 2) AS INTEGER) > 0 
            THEN CAST(SUBSTR(r.le_ident, 1, 2) AS INTEGER) * 10 
            ELSE NULL 
        END AS heading_degrees
    FROM 
        runways r
    WHERE 
        r.le_ident IS NOT NULL AND
        r.le_ident != '' AND
        SUBSTR(r.le_ident, 1, 2) GLOB '[0-9]*'
    
    UNION
    
    SELECT 
        r.airport_ident AS airport_icao,
        r.he_ident AS runway,
        CASE 
            WHEN CAST(SUBSTR(r.he_ident, 1, 2) AS INTEGER) > 0 
            THEN CAST(SUBSTR(r.he_ident, 1, 2) AS INTEGER) * 10 
            ELSE NULL 
        END AS heading_degrees
    FROM 
        runways r
    WHERE 
        r.he_ident IS NOT NULL AND
        r.he_ident != '' AND
        SUBSTR(r.he_ident, 1, 2) GLOB '[0-9]*'
    
    ORDER BY 
        airport_icao, runway;
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
    # Check if source files exist
    if not os.path.exists(AIRPORTS_FILE):
        print(f"Error: {AIRPORTS_FILE} not found in {SOURCE_DATA_DIR} directory")
        return
    if not os.path.exists(RUNWAYS_FILE):
        print(f"Error: {RUNWAYS_FILE} not found in {SOURCE_DATA_DIR} directory")
        return
    
    print(f"Using airport data from {AIRPORTS_FILE}")
    print(f"Using runway data from {RUNWAYS_FILE}")

    # Delete existing DB file if it exists
    if os.path.exists(DB_FILE):
        print(f"Removing existing database file: {DB_FILE}")
        os.remove(DB_FILE)

    # Create DB
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    create_tables(cur)

    # Import data
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

    # Create useful views
    print("Creating database views...")
    create_views(cur)

    conn.commit()
    conn.close()
    print(f"✅ Import completed. SQLite DB saved as '{DB_FILE}'.")

if __name__ == "__main__":
    main()
