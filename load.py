"""
load.py
Loads the cleaned DataFrame into a SQLite database.
This is the 'L' in ETL - storing the processed data for analysis.
"""
import sqlite3
import os
import logging

logger = logging.getLogger(__name__)


def load_to_sqlite(df, db_path, table_name="job_postings"):
    """
    Load a cleaned DataFrame into a SQLite database.
    Creates the database and table if they don't exist.
    Replaces existing data on each run to keep things simple.
    """
    os.makedirs(os.path.dirname(db_path), exist_ok=True)

    logger.info(f"Loading {len(df)} rows into {db_path} (table: {table_name})")

    conn = sqlite3.connect(db_path)

    # Write the DataFrame to SQLite
    df.to_sql(table_name, conn, if_exists="replace", index=False)

    # Verify the load
    cursor = conn.cursor()
    cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
    count = cursor.fetchone()[0]
    logger.info(f"Verified: {count} rows loaded into {table_name}")

    conn.close()

    return count


def load_to_csv(df, filepath):
    """Also save a clean CSV version for easy viewing."""
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    df.to_csv(filepath, index=False)
    logger.info(f"Clean CSV saved to {filepath}")
