# ETL Data Pipeline — Job Market Analysis

A Python-based ETL (Extract, Transform, Load) pipeline that takes messy job postings data, cleans and standardises it, loads it into a SQLite database, and runs analytical SQL queries against the cleaned dataset.

Built as a portfolio project to demonstrate core data engineering skills: data ingestion, transformation, quality validation, database loading, and SQL analysis.

## What It Does

The pipeline processes 500+ raw job postings with real-world data quality issues — inconsistent formatting, missing values, duplicate records, messy salary strings, mixed delimiters in skill lists — and produces a clean, queryable dataset.

**Raw data problems handled:**
- Duplicate rows removed
- Missing/corrupt records filtered out
- Job titles standardised to title case
- Locations mapped to consistent format (e.g. "dublin" → "Dublin, Ireland")
- Salary strings parsed from formats like "50k", "EUR 60000", "30000 - 70000", "Competitive"
- Skills split from mixed delimiters (commas, pipes, semicolons)
- Experience levels mapped to clean categories (Junior, Mid, Senior, Lead)
- Employment types standardised

**Pipeline output:**
- SQLite database with clean data (`output/job_market.db`)
- Clean CSV file (`output/job_postings_clean.csv`)
- Pipeline log with full audit trail (`output/pipeline.log`)

## Project Structure

```
etl-job-market-pipeline/
├── README.md
├── requirements.txt
├── generate_sample_data.py      # Creates a realistic messy dataset
├── data/
│   └── raw/
│       └── job_postings_raw.csv  # Raw messy input data
├── src/
│   ├── __init__.py
│   ├── extract.py               # Reads raw CSV data
│   ├── transform.py             # Cleans and standardises data
│   ├── load.py                  # Loads into SQLite and CSV
│   ├── validate.py              # Data quality checks
│   └── pipeline.py              # Main orchestrator
├── sql/
│   └── analysis_queries.sql     # SQL queries for analysis
└── output/
    ├── job_market.db             # SQLite database (generated)
    ├── job_postings_clean.csv    # Clean CSV output (generated)
    └── pipeline.log              # Execution log (generated)
```

## How to Run

**Prerequisites:** Python 3.8+ and pip

```bash
# Clone the repo
git clone https://github.com/santhosh2401-lab/etl-job-market-pipeline.git
cd etl-job-market-pipeline

# Install dependencies
pip install -r requirements.txt

# Generate sample data (creates 515 messy job postings)
python generate_sample_data.py

# Run the full ETL pipeline
python src/pipeline.py

# Query the results
sqlite3 output/job_market.db < sql/analysis_queries.sql
```

## Pipeline Steps

| Step | Module | What It Does |
|------|--------|-------------|
| 1. Extract | `extract.py` | Reads raw CSV, logs row/column counts and null percentages |
| 2. Validate Raw | `validate.py` | Checks for missing columns, duplicate rates, high null percentages |
| 3. Transform | `transform.py` | Cleans titles, locations, salaries, skills, experience levels |
| 4. Validate Clean | `validate.py` | Verifies no duplicate IDs, salary ranges are reasonable, categories are correct |
| 5. Load | `load.py` | Writes to SQLite database and clean CSV, verifies row count |

## Sample Output

Running the pipeline on 515 raw rows:
- 15 duplicates removed
- 26 corrupt records filtered (missing job_id)
- 474 clean rows loaded into SQLite
- 0 data quality issues in final output

## Tech Stack

- **Python** — pandas for data manipulation
- **SQLite** — lightweight database for storing cleaned data
- **SQL** — analytical queries against the loaded data
- **Logging** — full pipeline audit trail

## Author

**Santhosh Balaji Aravamudhan**
MSc Data Analytics, University of Galway (First Class Honours, 2025)

- LinkedIn: [linkedin.com/in/santhosh-techie](https://linkedin.com/in/santhosh-techie)
- Portfolio: [santhosh2401-lab.github.io](https://santhosh2401-lab.github.io)
- GitHub: [github.com/santhosh2401-lab](https://github.com/santhosh2401-lab)
