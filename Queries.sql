-- Data Cleaning
USE world_layoffs;
SELECT * FROM dbo.layoffs;

--1. Remove Duplicates
--2. Standardize the Data
--3. Handle Null/Blank values
--4. Remove any unnecessary columns

-- Creating a staging table of the same structure as raw data, which we are going to use to perform 
-- data cleaning methods so that we do not mess with the original data
SELECT *
INTO layoffs_staging
FROM layoffs
WHERE 1 = 0;

SELECT * FROM dbo.layoffs_staging;

-- Inserting the raw data into the staging table.
INSERT layoffs_staging
SELECT * 
FROM layoffs;

-- Removing Duplicates
-- Using the ROW_NUMBER() window function to identify the duplicates.
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, industry, total_laid_off, percentage_laid_off, 'date' ORDER BY total_laid_off) AS row_num
FROM layoffs_staging

-- Creating a CTE and deleting the duplicate data.
WITH duplicate_cte AS (
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, 'location', industry, total_laid_off, percentage_laid_off, 'date', stage, country, funds_raised_millions ORDER BY total_laid_off) AS row_num
FROM layoffs_staging
)

DELETE
FROM duplicate_cte
WHERE row_num > 1

-- Standardizing the data 
-- Removing the extra spaces at both the ends on the 'company' column.
UPDATE layoffs_staging
SET company = TRIM(company)

SELECT DISTINCT industry FROM layoffs_staging
SELECT * FROM layoffs_staging WHERE industry LIKE 'Crypto%'
UPDATE layoffs_staging SET industry = 'Crypto' WHERE industry LIKE 'Crypto%'

SELECT DISTINCT location FROM layoffs_staging ORDER BY 1
SELECT * FROM layoffs_staging WHERE location = 'Chennai'

SELECT DISTINCT country FROM layoffs_staging ORDER BY 1
SELECT DISTINCT country, TRIM(TRAILING '.' FROM country) FROM layoffs_staging ORDER BY 1
--UPDATE layoffs_staging SET country = 'United States' WHERE country = 'United States.'
UPDATE layoffs_staging 
SET country = TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'United States%'

-- Handling Missing/Null values
SELECT DISTINCT industry FROM layoffs_staging;

-- We have both NULL values and value as'NULL' in the industry column.
SELECT * FROM layoffs_staging WHERE industry IS NULL;
SELECT * FROM layoffs_staging WHERE industry = 'NULL';
-- 'Airbnb', 'Juul', 'Carvana', 'Bally's Interactive' are the 4 companies that have missing industry data.

--Let's try to populate the values for missing data in industry column
SELECT * 
FROM layoffs_staging 
WHERE company = 'Airbnb';

SELECT * 
FROM layoffs_staging t1
JOIN layoffs_staging t2
	on t1.company = t2.company
	AND t1.location = t2.location
WHERE (t1.industry IS NULL OR t1.industry = 'NULL')
AND t2.industry IS NOT NULL;

UPDATE layoffs_staging
SET industry = NULL
WHERE industry = 'NULL';

SELECT * 
FROM layoffs_staging t1
JOIN layoffs_staging t2
	on t1.company = t2.company
	AND t1.location = t2.location
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL;

-- Converting the above SELECT statement to an UPDATE statement
-- UPDATE layoffs_staging t1
-- JOIN layoffs_staging t2
-- 	ON t1.company = t2.company
-- SET t1.industry = t2.industry
-- WHERE t1.industry IS NULL
-- AND t2.industry IS NOT NULL;

UPDATE t1
SET t1.industry = t2.industry
FROM layoffs_staging t1
JOIN layoffs_staging t2
    ON t1.company = t2.company
   AND t1.location = t2.location
WHERE t1.industry IS NULL
  AND t2.industry IS NOT NULL;

SELECT * FROM layoffs_staging WHERE industry IS NULL;

-- SELECT all the rows that have both total_laid_off and percentage_laid_off as NULL
SELECT * FROM layoffs_staging
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

DELETE FROM layoffs_staging
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

SELECT * FROM layoffs_staging;


-- Exploratory Data Analysis
SELECT * 
FROM layoffs_staging;

-- Maximum jobs laid off
SELECT MAX(total_laid_off) 
FROM layoffs_staging;

-- Maximum percentage of jobs laid off
SELECT MAX(percentage_laid_off) 
FROM layoffs_staging;

-- List of companies that laid off all their employees (or we can say they are completely shut)
SELECT *
FROM layoffs_staging
WHERE percentage_laid_off = 1
ORDER BY total_laid_off DESC;

SELECT *
FROM layoffs_staging
WHERE percentage_laid_off = 1
ORDER BY funds_raised_millions DESC;

-- Let's try to get the timeframe of these layoffs by identifying the min and max of dates
SELECT MIN(date), MAX(date)
FROM layoffs_staging;

-- Let's check what is the number of jobs that are let go by each company
SELECT company, SUM(total_laid_off) AS total_jobs_laid_off
FROM layoffs_staging
GROUP BY 1 ORDER BY SUM(total_laid_off) DESC;

-- Let's check what is the number of jobs that are let go by each industry
SELECT industry, SUM(total_laid_off) AS total_jobs_laid_off
FROM layoffs_staging
GROUP BY 1 ORDER BY SUM(total_laid_off) DESC;

-- Let's check what is the number of jobs that are let go by each country
SELECT country, SUM(total_laid_off) AS total_jobs_laid_off
FROM layoffs_staging
GROUP BY country ORDER BY 2 DESC;

-- Let's check what is the number of jobs that are let go by each year
SELECT YEAR(date), SUM(total_laid_off) AS total_jobs_laid_off
FROM layoffs_staging
GROUP BY YEAR(date)
ORDER BY 1 DESC;

-- Let's check what is the number of jobs that are let go by each stage
SELECT stage, SUM(total_laid_off) AS total_jobs_laid_off
FROM layoffs_staging
GROUP BY stage
ORDER BY 2 DESC;

-- Let's check the total number of jobs that are let go by each month
SELECT FORMAT([date], 'yyyy-MM') AS [MONTH], SUM(total_laid_off) AS total_jobs_laid_off
FROM layoffs_staging
WHERE FORMAT([date], 'yyyy-MM') IS NOT NULL
GROUP BY FORMAT([date], 'yyyy-MM')
ORDER BY 1;

-- Let's now get the cumulative (running) total of the total number of jobs that are let go by each month

-- WITH Rolling_Total AS (
-- SELECT FORMAT([date], 'yyyy-MM') AS yearmonth, SUM(total_laid_off) AS total_jobs_laid_off
-- FROM layoffs_staging
-- WHERE FORMAT([date], 'yyyy-MM') IS NOT NULL
-- GROUP BY FORMAT([date], 'yyyy-MM')
-- )
-- SELECT yearmonth, SUM(total_jobs_laid_off) OVER(ORDER BY yearmonth) AS cumulative_sum
-- FROM Rolling_Total

WITH Rolling_Total AS (
    SELECT 
        YEAR([date]) * 100 + MONTH([date]) AS yearmonth_key,-- Integer key for ordering
		CONCAT(YEAR([date]), '-', RIGHT('0' + CAST(MONTH([date]) AS VARCHAR), 2)) AS yearmonth,
        SUM(total_laid_off) AS total_jobs_laid_off
    FROM layoffs_staging
    WHERE [date] IS NOT NULL
    GROUP BY YEAR([date]), MONTH([date])  -- Group by year and month
)
SELECT 
    yearmonth, total_jobs_laid_off,
    SUM(total_jobs_laid_off) OVER (ORDER BY yearmonth_key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_sum
FROM Rolling_Total
ORDER BY yearmonth_key;

SELECT company, SUM(total_laid_off) AS total_jobs_laid_off
FROM layoffs_staging
GROUP BY company
ORDER BY 2 DESC;

SELECT company, YEAR(date) AS years, SUM(total_laid_off) AS total_jobs_laid_off
FROM layoffs_staging
GROUP BY company, YEAR(date)
ORDER BY 1 ASC;

SELECT company, YEAR(date) AS years, SUM(total_laid_off) AS total_jobs_laid_off
FROM layoffs_staging
GROUP BY company, YEAR(date)
ORDER BY 3 DESC;

WITH Company_Year (company, Years, total_laid_offs) AS (
SELECT company, YEAR(date), SUM(total_laid_off) AS total_jobs_laid_off
FROM layoffs_staging
GROUP BY company, YEAR(date)
), Company_Year_Rank AS
(SELECT *, 
DENSE_RANK() OVER(PARTITION BY Years ORDER BY total_laid_offs DESC) AS ranking
FROM Company_Year
WHERE Years IS NOT NULL
)
SELECT * 
FROM Company_Year_Rank
WHERE ranking <= 5;

SELECT industry, YEAR(date) AS Years, SUM(total_laid_off) as total_jobs_laid_off
FROM layoffs_staging
WHERE YEAR(date) IS NOT NULL
GROUP BY industry, YEAR(date)
ORDER BY Years ASC;