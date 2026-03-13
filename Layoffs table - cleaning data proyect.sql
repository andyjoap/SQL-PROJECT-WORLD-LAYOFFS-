-- Data cleaning

#In this project, we will focus on cleaning data from a table named 'layoffs':

SELECT *
FROM layoffs ;


#STEPS:

-- 1. Remove duplicate
-- 2. Standarize the data
-- 3. Null or blank values`layoffs`
-- 4. Remove any Columns or Rows

#We create a new table with same data, to avoid working with the original table

CREATE TABLE layoffs_staging
LIKE layoffs
;

SELECT *
FROM layoffs_staging;

INSERT layoffs_staging
SELECT *
FROM layoffs
;

-- 1. Remove duplicate


SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging
# the row_number > 1 means it's a duplicate row
;

#we create a CTE 
WITH duplicate_cte AS
(
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging
)
SELECT *
FROM duplicate_cte
WHERE row_num > 1
;

#We make a copy with Create Statement, and also we add a new row-number column

CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` text,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` text,
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


INSERT INTO layoffs_staging2
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging;

#Delete all the rows wiht row number > 1, that means duplicate

DELETE
FROM layoffs_staging2
WHERE row_num > 1
;

SELECT *
FROM layoffs_staging2
WHERE row_num > 1
;

-- 2. Standarize the data

-- Standarize company column
#we can check we need to trim all company data to make it more formal
SELECT company
FROM layoffs_staging2
;

UPDATE layoffs_staging2
SET company = TRIM(company)

;
-- Standarize industry column

SELECT DISTINCT industry
FROM layoffs_staging2
ORDER BY 1
#we can see some industries, like Crypto, are repeated
;

SELECT industry
FROM layoffs_staging2
WHERE industry LIKE 'Crypto%'

;
UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%'
;
-- Standarize location column

SELECT DISTINCT location
FROM layoffs_staging2
ORDER BY 1
#everything look fine
;
-- Standarize country column

SELECT DISTINCT country
FROM layoffs_staging2
ORDER BY 1
;
#we found 2 same united states country:

UPDATE layoffs_staging2
SET country = 'United States'
WHERE country LIKE 'United States%'
;
SELECT country
FROM layoffs_staging2
;
-- Standarize date column

#Let's convert this from TEXT to DATE format
SELECT `date`,
STR_TO_DATE (`date`, '%m/%d/%Y')
FROM layoffs_staging2
;
UPDATE layoffs_staging2
SET `date`= STR_TO_DATE (`date`, '%m/%d/%Y')
;
SET SQL_SAFE_UPDATES = 0;

ALTER TABLE layoffs_staging2
modify `date` DATE;

-- 3. Null or blank values`layoffs (1)`

SELECT DISTINCT industry
FROM layoffs_staging2;
#we can see there's a blank industry and NULL industry

SELECT company, industry
FROM layoffs_staging2
WHERE (industry = '' OR industry IS NULL)
;

SELECT *
FROM layoffs_staging2
WHERE company = 'Airbnb' #we can check some of them already have their industry defined, but is NULL or blank so we need to fix it
;

UPDATE layoffs_staging2
SET industry = NULL
WHERE industry = '' #we update all the blank files in NULL so the next query works
;

;
SELECT *, t1.industry, t2.industry
FROM layoffs_staging2 AS t1
JOIN layoffs_staging2 AS t2
	ON t1.company = t2.company
WHERE t1.industry IS NULL
	AND
    t2.industry is NOT NULL #t1 null values, t2 have their industry value
    
;
#Now we complete the industries we know, because they are the same company

UPDATE layoffs_staging2 AS t1
JOIN layoffs_staging2 AS t2
	ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL
	AND
    t2.industry is NOT NULL
;

SELECT DISTINCT industry
FROM layoffs_staging2
ORDER BY 1 #there is still a null value
;

SELECT company, industry
FROM layoffs_staging2
WHERE industry IS NULL
;
#Let's check if there's another Bally company to get the kind of industry
SELECT *
FROM layoffs_staging2
WHERE company LIKE 'Bally%'
;
#as we can see, there's only one so nothing we can do about it.

-- 4. Remove any column or rows

#There's some columns with Null values, which are not very useful

;
SELECT *
FROM layoffs_staging2
WHERE percentage_laid_off IS NULL AND total_laid_off IS NULL
;
DELETE
FROM layoffs_staging2
WHERE percentage_laid_off IS NULL AND total_laid_off IS NULL
;
#Now we proceed to eliminate the row_num column we used before

ALTER TABLE layoffs_staging2
DROP COLUMN row_num
;

-- We finished cleaning all the data from layoffs column, our final result is:

SELECT *
FROM layoffs_staging2



