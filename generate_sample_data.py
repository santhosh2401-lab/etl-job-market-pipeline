"""
generate_sample_data.py
Generates a realistic messy job postings CSV for the ETL pipeline to clean.
Run this once to create the raw data file.
"""
import csv
import random
import os

random.seed(42)

titles = [
    "Data Engineer", "data engineer", "DATA ENGINEER", "  Data Engineer ",
    "Junior Data Engineer", "Senior Data Engineer", "DevOps Engineer",
    "devops engineer", "Cloud Engineer", "Site Reliability Engineer",
    "Data Analyst", "ML Engineer", "Software Engineer", "IT Support Engineer",
    "Infrastructure Engineer", "Platform Engineer", "Database Administrator",
    "Systems Engineer", "Backend Engineer", "Full Stack Developer"
]

companies = [
    "Google", "Meta", "Stripe", "Workday", "Salesforce", "HubSpot",
    "Accenture", "Infosys", "TCS", "Deloitte", "PwC", "KPMG",
    "Amazon", "Microsoft", "Oracle", "SAP", "VMware", "Dell",
    "Fidelity", "JPMorgan", "Citi", "Bank of Ireland", "AIB",
    "", None, "  ", "Unknown"
]

locations = [
    "Dublin, Ireland", "dublin", "DUBLIN", "Dublin",
    "Galway, Ireland", "galway", "Cork, Ireland",
    "London, UK", "london", "Berlin, Germany",
    "Remote", "remote", "REMOTE", "Hybrid - Dublin",
    "", None, "  ", "N/A"
]

skills_pool = [
    "Python", "SQL", "AWS", "Azure", "GCP", "Docker", "Kubernetes",
    "Jenkins", "Git", "Linux", "Terraform", "Spark", "Airflow",
    "pandas", "NumPy", "Java", "Scala", "Bash", "PostgreSQL",
    "MySQL", "MongoDB", "Redis", "Kafka", "ETL", "CI/CD",
    "React", "JavaScript", "TypeScript", "Go", "Rust"
]

experience_levels = [
    "Junior", "Mid", "Senior", "Lead", "Entry Level",
    "junior", "SENIOR", "", None, "Not Specified", "2-3 years",
    "0-1 years", "3-5 years", "5+ years", "10+ years"
]

employment_types = [
    "Full-time", "full-time", "FULL-TIME", "Part-time",
    "Contract", "contract", "Permanent", "Freelance",
    "", None, "FT", "PT"
]

rows = []
for i in range(1, 501):
    salary_raw = random.choice([
        str(random.randint(25000, 120000)),
        f"EUR {random.randint(30000, 100000)}",
        f"{random.randint(30, 90)}k",
        f"{random.randint(30000, 60000)} - {random.randint(60000, 120000)}",
        "Competitive",
        "competitive",
        "DOE",
        "",
        None,
        "0",
        "-1",
        str(random.randint(200000, 500000)),
        "N/A"
    ])

    num_skills = random.randint(2, 8)
    skills = random.sample(skills_pool, min(num_skills, len(skills_pool)))
    skills_str = random.choice([
        ", ".join(skills),
        " | ".join(skills),
        ";".join(skills),
        " ".join(skills),
        "",
        None
    ])

    posted_date = random.choice([
        f"2025-{random.randint(1,12):02d}-{random.randint(1,28):02d}",
        f"{random.randint(1,28):02d}/{random.randint(1,12):02d}/2025",
        f"{random.randint(1,12):02d}-{random.randint(1,28):02d}-2025",
        "2025",
        "",
        None,
        "yesterday",
        "2 days ago"
    ])

    row = {
        "job_id": f"JOB-{i:04d}" if random.random() > 0.05 else "",
        "title": random.choice(titles),
        "company": random.choice(companies),
        "location": random.choice(locations),
        "salary": salary_raw,
        "experience_level": random.choice(experience_levels),
        "employment_type": random.choice(employment_types),
        "skills_required": skills_str,
        "date_posted": posted_date,
        "description": f"We are looking for a talented professional to join our team. Reference {i}." if random.random() > 0.1 else ""
    }
    rows.append(row)

# Add some exact duplicates
for _ in range(15):
    rows.append(random.choice(rows[:400]).copy())

random.shuffle(rows)

output_path = os.path.join(os.path.dirname(__file__), "data", "raw", "job_postings_raw.csv")
os.makedirs(os.path.dirname(output_path), exist_ok=True)

with open(output_path, "w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=rows[0].keys())
    writer.writeheader()
    writer.writerows(rows)

print(f"Generated {len(rows)} raw job postings at {output_path}")
