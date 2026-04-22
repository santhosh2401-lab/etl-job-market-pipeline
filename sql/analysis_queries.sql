-- analysis_queries.sql
-- Run these against the job_market.db SQLite database to explore the cleaned data.
-- Usage: sqlite3 output/job_market.db < sql/analysis_queries.sql

-- 1. How many total job postings do we have after cleaning?
SELECT COUNT(*) AS total_jobs FROM job_postings;

-- 2. What are the top 10 most common job titles?
SELECT title, COUNT(*) AS posting_count
FROM job_postings
WHERE title IS NOT NULL
GROUP BY title
ORDER BY posting_count DESC
LIMIT 10;

-- 3. Which companies are hiring the most?
SELECT company, COUNT(*) AS posting_count
FROM job_postings
WHERE company IS NOT NULL
GROUP BY company
ORDER BY posting_count DESC
LIMIT 10;

-- 4. What is the average salary by experience level?
SELECT experience_level,
       COUNT(*) AS jobs,
       ROUND(AVG(salary)) AS avg_salary,
       MIN(salary) AS min_salary,
       MAX(salary) AS max_salary
FROM job_postings
WHERE salary IS NOT NULL
GROUP BY experience_level
ORDER BY avg_salary DESC;

-- 5. Which locations have the most job postings?
SELECT location, COUNT(*) AS posting_count
FROM job_postings
WHERE location IS NOT NULL
GROUP BY location
ORDER BY posting_count DESC;

-- 6. What is the average salary by location?
SELECT location,
       COUNT(*) AS jobs_with_salary,
       ROUND(AVG(salary)) AS avg_salary
FROM job_postings
WHERE salary IS NOT NULL AND location IS NOT NULL
GROUP BY location
HAVING jobs_with_salary >= 3
ORDER BY avg_salary DESC;

-- 7. What are the most commonly required skills?
-- (This counts how often each skill appears across all postings)
WITH skill_split AS (
    SELECT TRIM(value) AS skill
    FROM job_postings, json_each('["' || REPLACE(skills, ', ', '","') || '"]')
    WHERE skills IS NOT NULL
)
SELECT skill, COUNT(*) AS frequency
FROM skill_split
WHERE skill != ''
GROUP BY skill
ORDER BY frequency DESC
LIMIT 15;

-- 8. Employment type breakdown
SELECT employment_type, COUNT(*) AS count,
       ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM job_postings), 1) AS percentage
FROM job_postings
GROUP BY employment_type
ORDER BY count DESC;

-- 9. Average number of skills required per posting
SELECT ROUND(AVG(skill_count), 1) AS avg_skills_per_posting
FROM job_postings
WHERE skill_count > 0;

-- 10. High-paying Data Engineer roles (salary > 70000)
SELECT job_id, title, company, location, salary, skills
FROM job_postings
WHERE title LIKE '%Data Engineer%' AND salary > 70000
ORDER BY salary DESC
LIMIT 10;
