"""
transform.py
Cleans, standardises and enriches the raw job postings data.
This is the 'T' in ETL - where messy data becomes usable.
"""
import pandas as pd
import re
import logging

logger = logging.getLogger(__name__)


def clean_text_column(series):
    """Strip whitespace and normalise empty strings to None."""
    cleaned = series.astype(str).str.strip()
    cleaned = cleaned.replace(["", "nan", "None", "N/A", "Unknown", "Not Specified"], pd.NA)
    return cleaned


def standardise_title(title):
    """Standardise job titles to title case and clean up spacing."""
    if pd.isna(title):
        return pd.NA
    title = str(title).strip().title()
    title = re.sub(r"\s+", " ", title)
    return title


def standardise_location(location):
    """Standardise location names to a consistent format."""
    if pd.isna(location):
        return pd.NA

    location = str(location).strip().title()

    # Map common variations
    location_map = {
        "Dublin": "Dublin, Ireland",
        "Galway": "Galway, Ireland",
        "Cork": "Cork, Ireland",
        "London": "London, UK",
        "Remote": "Remote",
        "Hybrid - Dublin": "Dublin, Ireland (Hybrid)",
    }

    for key, value in location_map.items():
        if location.lower().startswith(key.lower()):
            return value

    return location


def parse_salary(salary_str):
    """
    Extract a numeric salary value from messy salary strings.
    Returns the midpoint for ranges, converts 'k' notation, handles EUR prefix.
    """
    if pd.isna(salary_str):
        return pd.NA

    salary_str = str(salary_str).strip().upper()

    # Skip non-numeric values
    if salary_str in ["COMPETITIVE", "DOE", "N/A", "", "0", "-1"]:
        return pd.NA

    # Handle range like "30000 - 60000"
    range_match = re.match(r"(\d+)\s*-\s*(\d+)", salary_str.replace(",", ""))
    if range_match:
        low = int(range_match.group(1))
        high = int(range_match.group(2))
        if low > 0 and high > 0 and high <= 200000:
            return (low + high) // 2
        return pd.NA

    # Handle "50k" or "50K"
    k_match = re.match(r"(\d+)\s*K", salary_str)
    if k_match:
        return int(k_match.group(1)) * 1000

    # Handle "EUR 50000" or plain numbers
    num_match = re.findall(r"\d+", salary_str.replace(",", ""))
    if num_match:
        value = int(num_match[0])
        # Filter out unrealistic values
        if 15000 <= value <= 200000:
            return value
        return pd.NA

    return pd.NA


def standardise_experience(exp):
    """Map messy experience levels to a clean set of categories."""
    if pd.isna(exp):
        return "Not Specified"

    exp = str(exp).strip().lower()

    if exp in ["junior", "entry level", "0-1 years"]:
        return "Junior"
    elif exp in ["mid", "2-3 years", "3-5 years"]:
        return "Mid"
    elif exp in ["senior", "5+ years"]:
        return "Senior"
    elif exp in ["lead", "10+ years"]:
        return "Lead"
    else:
        return "Not Specified"


def standardise_employment_type(emp_type):
    """Standardise employment type values."""
    if pd.isna(emp_type):
        return "Not Specified"

    emp_type = str(emp_type).strip().lower()

    type_map = {
        "full-time": "Full-time", "ft": "Full-time",
        "part-time": "Part-time", "pt": "Part-time",
        "contract": "Contract",
        "permanent": "Permanent",
        "freelance": "Freelance",
    }

    return type_map.get(emp_type, "Not Specified")


def parse_skills(skills_str):
    """
    Split skills string into a clean comma-separated list.
    Handles different delimiters: comma, pipe, semicolon, space.
    """
    if pd.isna(skills_str):
        return pd.NA

    skills_str = str(skills_str).strip()
    if not skills_str:
        return pd.NA

    # Try different delimiters
    if "|" in skills_str:
        skills = skills_str.split("|")
    elif ";" in skills_str:
        skills = skills_str.split(";")
    elif "," in skills_str:
        skills = skills_str.split(",")
    else:
        skills = skills_str.split(" ")

    # Clean each skill
    cleaned = [s.strip().title() for s in skills if s.strip()]
    return ", ".join(cleaned) if cleaned else pd.NA


def count_skills(skills_str):
    """Count the number of skills listed."""
    if pd.isna(skills_str):
        return 0
    return len(str(skills_str).split(", "))


def transform(df):
    """
    Main transformation function.
    Takes the raw extracted DataFrame and returns a clean, standardised version.
    """
    logger.info(f"Starting transformation on {len(df)} rows")
    original_count = len(df)

    # Make a copy so we don't modify the original
    df = df.copy()

    # Step 1: Remove exact duplicate rows
    df = df.drop_duplicates()
    dupes_removed = original_count - len(df)
    logger.info(f"Removed {dupes_removed} duplicate rows")

    # Step 2: Remove rows with no job_id (these are corrupt records)
    df["job_id"] = clean_text_column(df["job_id"])
    before = len(df)
    df = df.dropna(subset=["job_id"])
    logger.info(f"Removed {before - len(df)} rows with missing job_id")

    # Step 3: Clean and standardise each column
    logger.info("Standardising job titles...")
    df["title"] = df["title"].apply(standardise_title)

    logger.info("Cleaning company names...")
    df["company"] = clean_text_column(df["company"])

    logger.info("Standardising locations...")
    df["location"] = df["location"].apply(standardise_location)

    logger.info("Parsing salary values...")
    df["salary_cleaned"] = df["salary"].apply(parse_salary)
    df["salary_cleaned"] = pd.to_numeric(df["salary_cleaned"], errors="coerce").astype("Int64")

    logger.info("Standardising experience levels...")
    df["experience_level"] = df["experience_level"].apply(standardise_experience)

    logger.info("Standardising employment types...")
    df["employment_type"] = df["employment_type"].apply(standardise_employment_type)

    logger.info("Parsing skills...")
    df["skills_cleaned"] = df["skills_required"].apply(parse_skills)
    df["skill_count"] = df["skills_cleaned"].apply(count_skills)

    # Step 4: Select and reorder final columns
    output_columns = [
        "job_id", "title", "company", "location",
        "salary_cleaned", "experience_level", "employment_type",
        "skills_cleaned", "skill_count", "date_posted"
    ]
    df = df[output_columns]

    # Rename for clarity
    df = df.rename(columns={
        "salary_cleaned": "salary",
        "skills_cleaned": "skills"
    })

    logger.info(f"Transformation complete. Output: {len(df)} rows, {len(df.columns)} columns")
    logger.info(f"Null counts after transformation:\n{df.isnull().sum()}")

    return df
