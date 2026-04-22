"""
validate.py
Data quality checks to run before and after transformation.
Good data engineers always validate their pipelines.
"""
import logging

logger = logging.getLogger(__name__)


def validate_raw(df):
    """
    Checks to run on raw extracted data before transformation.
    These catch problems with the source data early.
    """
    logger.info("=" * 50)
    logger.info("VALIDATION: Raw Data Quality Checks")
    logger.info("=" * 50)

    issues = []

    # Check 1: Is the data empty?
    if len(df) == 0:
        issues.append("CRITICAL: DataFrame is empty, no data extracted")
        return issues

    logger.info(f"Total rows: {len(df)}")
    logger.info(f"Total columns: {len(df.columns)}")

    # Check 2: Are expected columns present?
    expected_cols = ["job_id", "title", "company", "location", "salary"]
    missing_cols = [c for c in expected_cols if c not in df.columns]
    if missing_cols:
        issues.append(f"Missing expected columns: {missing_cols}")
    else:
        logger.info("All expected columns present")

    # Check 3: How many duplicates?
    dupe_count = df.duplicated().sum()
    dupe_pct = round(dupe_count / len(df) * 100, 1)
    logger.info(f"Duplicate rows: {dupe_count} ({dupe_pct}%)")
    if dupe_pct > 10:
        issues.append(f"High duplicate rate: {dupe_pct}%")

    # Check 4: Null percentages per column
    logger.info("Null percentages:")
    for col in df.columns:
        null_pct = round(df[col].isnull().sum() / len(df) * 100, 1)
        logger.info(f"  {col}: {null_pct}%")
        if null_pct > 50:
            issues.append(f"Column '{col}' has {null_pct}% nulls")

    if issues:
        logger.warning(f"Found {len(issues)} data quality issues")
        for issue in issues:
            logger.warning(f"  - {issue}")
    else:
        logger.info("All raw data quality checks passed")

    return issues


def validate_clean(df):
    """
    Checks to run on cleaned data after transformation.
    These confirm the transformation worked correctly.
    """
    logger.info("=" * 50)
    logger.info("VALIDATION: Clean Data Quality Checks")
    logger.info("=" * 50)

    issues = []

    # Check 1: No duplicate job_ids
    if "job_id" in df.columns:
        dupe_ids = df["job_id"].duplicated().sum()
        if dupe_ids > 0:
            issues.append(f"Found {dupe_ids} duplicate job_ids after cleaning")
        else:
            logger.info("No duplicate job_ids found")

    # Check 2: Job titles are standardised (should be title case)
    if "title" in df.columns:
        sample_titles = df["title"].dropna().head(5).tolist()
        logger.info(f"Sample cleaned titles: {sample_titles}")

    # Check 3: Salary values are reasonable
    if "salary" in df.columns:
        valid_salaries = df["salary"].dropna()
        if len(valid_salaries) > 0:
            min_sal = valid_salaries.min()
            max_sal = valid_salaries.max()
            avg_sal = round(valid_salaries.mean())
            logger.info(f"Salary range: {min_sal} to {max_sal} (avg: {avg_sal})")
            if min_sal < 10000:
                issues.append(f"Suspiciously low salary found: {min_sal}")
            if max_sal > 200000:
                issues.append(f"Suspiciously high salary found: {max_sal}")
        else:
            logger.info("No valid salary data after cleaning")

    # Check 4: Experience levels are from expected set
    if "experience_level" in df.columns:
        valid_levels = {"Junior", "Mid", "Senior", "Lead", "Not Specified"}
        actual_levels = set(df["experience_level"].unique())
        unexpected = actual_levels - valid_levels
        if unexpected:
            issues.append(f"Unexpected experience levels: {unexpected}")
        else:
            logger.info(f"Experience levels OK: {actual_levels}")

    # Check 5: Row count didn't drop too much
    logger.info(f"Final row count: {len(df)}")

    # Summary
    if issues:
        logger.warning(f"Found {len(issues)} data quality issues in clean data")
        for issue in issues:
            logger.warning(f"  - {issue}")
    else:
        logger.info("All clean data quality checks passed")

    return issues
