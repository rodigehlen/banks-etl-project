# Banks ETL Project

This project implements an **ETL (Extract, Transform, Load) pipeline** in Python. It extracts data on the largest banks from Wikipedia and currency exchange rates from a CSV file, transforms the information into multiple currencies, and loads the results into a CSV file and a SQLite database. Each step of the process is logged for traceability.

---

## ETL Steps

### 1. Extract
- Reads bank market capitalization data from [Wikipedia](https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks) (archived snapshot).
- Reads exchange rates (USD to GBP, EUR, INR) from a [CSV file](https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMSkillsNetwork-PY0221EN-Coursera/labs/v2/exchange_rate.csv).

### 2. Transform
- Removes unnecessary columns.
- Renames columns for readability.
- Converts Market Cap (USD) into **GBP, EUR, and INR**.

### 3. Load
- Saves the transformed data into:
  - `Largest_banks_data.csv` (CSV file)
  - `Banks.db` (SQLite database)
  - `Largest_banks` (Table)

### 4. Logging
- Logs messages with timestamps into `code_log.txt`.

---

## Project Structure

```plaintext
banks-etl-project/
├── banks_project.py        # Main ETL script
├── Largest_banks_data.csv  # Output CSV (after running ETL)
├── Banks.db                # SQLite database (after running ETL)
├── code_log.txt            # Log file (after running ETL)
├── README.md               # Markdown readme
└── requirements.txt        # Python dependencies
