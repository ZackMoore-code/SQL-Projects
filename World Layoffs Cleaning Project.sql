-- Data Cleaning layoff data
-- The columns for this table are company, location, industry, total_laid_off,
-- percentage_laid_off, date, stage, country, and funds_raised_millions
-- With the below script I removed duplicated, standardized data,
-- Filled nulls/blanks where applicable, and removed nulls when applicable


-- 1. Remove duplicates
-- 2. Standardize data
-- 3. Null values or blank values
-- 4. Remove any columns not necessary

SELECT *
FROM layoffs;

-- Create mock copy of original data for cleaning

CREATE TABLE layoffs_staging
LIKE layoffs;

INSERT layoffs_staging
SELECT *
FROM layoffs;


-- Note: layoffs_staging2 is the final version of the cleaned data in this script.

SELECT *,
ROW_NUMBER() OVER
(PARTITION BY company, location, industry,
total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging;

-- Use a CTE to register original data as "1" and duplicates as "2" under added column "row_num"
WITH duplicate_cte AS
(
SELECT *,
ROW_NUMBER() OVER
(PARTITION BY company, industry, total_laid_off, percentage_laid_off, `date`) AS row_num
FROM layoffs_staging
)
SELECT *
FROM duplicate_cte
WHERE row_num > 1;

-- Create table where final version of data will be consolidated.

CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

SELECT *
FROM layoffs_staging2;

-- Creating row_num column to check for duplicates
INSERT INTO layoffs_staging2
SELECT *,
ROW_NUMBER() OVER
(PARTITION BY company, industry, total_laid_off, percentage_laid_off, `date`) AS row_num
FROM layoffs_staging;

-- Deleting all duplicates (row_num > 2)
DELETE
FROM layoffs_staging2
WHERE row_num > 1;

-- Standardizing

-- Trimming empty space in company name

SELECT company, TRIM(company)
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET company = TRIM(company);

SELECT DISTINCT industry
FROM layoffs_staging2
ORDER BY 1;

-- Consolidating all variances in the crypto industry into "Crypto"
SELECT *
FROM layoffs_staging2
WHERE industry LIKE 'Crypto%';

UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';

-- Manually setting industry for some companies where some observations are blank.
SELECT *
FROM layoffs_staging2
WHERE industry = '';

SELECT *
FROM layoffs_staging2
WHERE company LIKE '%uul%';

UPDATE layoffs_staging2
SET industry = 'Consumer'
WHERE company LIKE '%uul%';

SELECT *
FROM layoffs_staging2
WHERE industry IS NULL
OR industry = '';

-- Blanket sweep for this issue by setting all blanks to null and doing a self-join to fill
-- any nulls with the correct information.

SELECT t1.industry, t2.industry
FROM layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company
WHERE (t1.industry IS NULL OR t1.industry = '')
AND t2.industry IS NOT NULL;

UPDATE layoffs_staging2
SET industry = NULL
WHERE industry = '';

UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL;

-- Check 
SELECT *
FROM layoffs_staging2
WHERE industry IS NULL;

-- One null still leftover as there was only one observation for that company

SELECT DISTINCT country
FROM layoffs_staging2
ORDER BY 1;

-- Only issue found in country was a period after some submissions of "United States."

-- Remove period from "United States."

UPDATE layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'United States%';

-- Checking then updating date to correct format and data type
SELECT `date`,
STR_TO_DATE(`date`, '%m/%d/%Y')
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;

-- Checking to see if any observations would not be applicable
-- Data isn't applicable if both total and percentage of layoffs are null (no usable data)

SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

-- Deleting observations with no applicable information

DELETE
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

-- Drop column row_num as it's no longer needed
ALTER TABLE layoffs_staging2
DROP COLUMN row_num;
