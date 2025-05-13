# Flight Simulation Core Data

Uses data from https://ourairports.com/data/

## Overview

This repository contains tools for processing flight simulation data from OurAirports into a usable format for flight simulation applications.

## Data Sources

- Airports and runway data from OurAirports
- Data is processed into an SQLite database

## Setup

1. Create a `source-data` directory
2. Download CSV files from OurAirports and place them in the directory:
   - airports.csv
   - runways.csv

## Usage

Run the import script to generate the SQLite database:

```python
python import-airports.py
```
