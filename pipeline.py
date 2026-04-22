"""
pipeline.py
Main ETL pipeline - orchestrates the full Extract, Transform, Load process.
Run this to execute the entire pipeline end to end.

Usage: python src/pipeline.py
"""
import os
import sys
import logging
from datetime import datetime

# Add parent directory to path so imports work
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.extract import extract_from_csv
from src.transform import transform
from src.load import load_to_sqlite, load_to_csv
from src.validate import validate_raw, validate_clean

# Set up logging so we can see what the pipeline is doing
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(message)s",
    datefmt="%H:%M:%S",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(
            os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                         "output", "pipeline.log"),
            mode="w"
        )
    ]
)
logger = logging.getLogger(__name__)


def run_pipeline():
    """Execute the full ETL pipeline."""

    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    raw_data_path = os.path.join(base_dir, "data", "raw", "job_postings_raw.csv")
    db_path = os.path.join(base_dir, "output", "job_market.db")
    clean_csv_path = os.path.join(base_dir, "output", "job_postings_clean.csv")

    logger.info("=" * 60)
    logger.info("ETL PIPELINE STARTED")
    logger.info(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("=" * 60)

    # ── STEP 1: EXTRACT ──
    logger.info("")
    logger.info("STEP 1: EXTRACT")
    logger.info("-" * 40)
    raw_df = extract_from_csv(raw_data_path)

    # ── STEP 2: VALIDATE RAW DATA ──
    logger.info("")
    logger.info("STEP 2: VALIDATE RAW DATA")
    logger.info("-" * 40)
    raw_issues = validate_raw(raw_df)

    # ── STEP 3: TRANSFORM ──
    logger.info("")
    logger.info("STEP 3: TRANSFORM")
    logger.info("-" * 40)
    clean_df = transform(raw_df)

    # ── STEP 4: VALIDATE CLEAN DATA ──
    logger.info("")
    logger.info("STEP 4: VALIDATE CLEAN DATA")
    logger.info("-" * 40)
    clean_issues = validate_clean(clean_df)

    # ── STEP 5: LOAD ──
    logger.info("")
    logger.info("STEP 5: LOAD")
    logger.info("-" * 40)
    rows_loaded = load_to_sqlite(clean_df, db_path)
    load_to_csv(clean_df, clean_csv_path)

    # ── SUMMARY ──
    logger.info("")
    logger.info("=" * 60)
    logger.info("ETL PIPELINE COMPLETE")
    logger.info(f"Raw rows extracted:    {len(raw_df)}")
    logger.info(f"Clean rows loaded:     {rows_loaded}")
    logger.info(f"Rows removed:          {len(raw_df) - rows_loaded}")
    logger.info(f"Raw data issues:       {len(raw_issues)}")
    logger.info(f"Clean data issues:     {len(clean_issues)}")
    logger.info(f"Database:              {db_path}")
    logger.info(f"Clean CSV:             {clean_csv_path}")
    logger.info(f"Log file:              output/pipeline.log")
    logger.info("=" * 60)


if __name__ == "__main__":
    os.makedirs(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "output"), exist_ok=True)
    run_pipeline()
