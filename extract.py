"""
extract.py
Reads raw job postings data from CSV file.
This is the 'E' in ETL - pulling data from its source.
"""
import pandas as pd
import os
import logging

logger = logging.getLogger(__name__)


def extract_from_csv(filepath):
    """
    Read raw job postings from a CSV file and return a DataFrame.
    Handles common file issues like missing files and encoding problems.
    """
    if not os.path.exists(filepath):
        raise FileNotFoundError(f"Data file not found: {filepath}")

    logger.info(f"Extracting data from {filepath}")

    df = pd.read_csv(filepath, encoding="utf-8")

    row_count = len(df)
    col_count = len(df.columns)
    logger.info(f"Extracted {row_count} rows and {col_count} columns")

    # Log basic info about what we pulled in
    logger.info(f"Columns found: {list(df.columns)}")
    logger.info(f"Null counts per column:\n{df.isnull().sum()}")

    return df
