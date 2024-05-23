-- Data Cleaning 
-- We aim to clean the entire dataset. 

-- First we created a new schema called 'world_layoffs'
-- Next we imported the table data set in to the schema using import table wizard. (csv or json)

SELECT *
FROM layoffs;

-- Step 1: Remove Duplicates
-- Step 2: Standardize the Data
-- Step 3: Null values or blank values
-- Step 4: Remove any Columns

-- Creating a copy of the raw data, as we will make a lot of changes. 
-- This helps to prevnt errors. 
CREATE TABLE layoffs_staging
LIKE layoffs;

-- Inserting everything from our raw data set to the copy dataset.
INSERT layoffs_staging
SELECT *
FROM layoffs;

SELECT *
FROM layoffs_staging;

-- Excecuting step 1 (Remove Duplicates)
-- Using row_number() window function to set row_num for each data record. 
-- Getting a row_num greater than 1 indicates that there is a duplicate.
SELECT *, 
row_number() over(
partition by company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging;

-- Creating a CTE with the above query. 
WITH duplicate_cte AS
(
SELECT *, 
row_number() over(
partition by company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging
)
SELECT *
FROM duplicate_cte
WHERE row_num > 1;

SELECT *
FROM layoffs_staging
WHERE company = 'Casper';


-- We cannot directly delete duplicates using CTE. So we create another duplicate table 'layoffs_staging2'
-- We also add a new column 'row_num'
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
  `row_num` text
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


SELECT *
FROM layoffs_staging2;

-- Insering all records into this duplicate table. 
INSERT INTO layoffs_staging2
SELECT *, 
row_number() over(
partition by company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging;

SELECT *
FROM layoffs_staging2
WHERE row_num > 1;

-- Delete the duplicates. 
DELETE
FROM layoffs_staging2
WHERE row_num > 1;


-- Executing step 2 (Standardizing data)
-- We should select distinct and check for each column for any probable issues.

-- Looking at each DISTINCT company. 
-- Using TRIM to eliminate any spaces up front of the company name.
SELECT company, TRIM(company)
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET company = TRIM(company);

SELECT DISTINCT company
FROM layoffs_staging2
ORDER BY company;

-- Looking at each distinct industries.
SELECT DISTINCT industry
FROM layoffs_staging2
ORDER BY industry;

-- We figure out there are industries like 'Crypto', 'Crypto Currency', 'CryptoCurrency', all being considered as different industries.
-- But they all are actually the same and they should be considered as one industry.
SELECT *
FROM layoffs_staging2
WHERE industry LIKE 'Crypto%';

-- Updating them into a single industry 'Crypto'
UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';

-- Looking at each DISTINCT location.
SELECT DISTINCT location
FROM layoffs_staging2
ORDER BY location;

-- Looking at each DISTINCT countries.
SELECT DISTINCT country
FROM layoffs_staging2
ORDER BY country;

-- We figure out that 'United States' and 'United States.' are being considered as two distinct countries. 
SELECT *
FROM layoffs_staging2
WHERE country = 'United States.';

-- Updating them into one country 'United States'
-- We could have also update it by using TRIM(TRAILING '.' from country)
UPDATE layoffs_staging2
SET country = 'United States'
WHERE country = 'United States.'; 

-- Updating dates from text to date format excluding the none values.
SELECT `date`
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET `date` = str_to_date(`date`, '%m/%d/%Y')
WHERE `date` != 'None';

-- Check for none dates.
SELECT *
FROM layoffs_staging2
WHERE date = 'None';

-- This is an issue because of the json file and the none date values.
ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;


SELECT *
FROM layoffs_staging2
ORDER BY 1;

-- Executing step 3. (Dealing with none/null values or blank spaces)
-- We are treating null values as 'None' 

SELECT *
FROM layoffs_staging2
WHERE total_laid_off = 'None';

SELECT *
FROM layoffs_staging2
WHERE total_laid_off = 'None' AND percentage_laid_off = 'None';

SELECT DISTINCT industry
FROM layoffs_staging2;

-- Checking every record for industries that are 'None' or blank.
SELECT *
FROM layoffs_staging2
WHERE industry = 'None' OR industry = '';

-- By looking at the output of the above query, we look for the companies and all of their records.
-- Here we see the company 'Airbnb' has two records one as blank as industry and one 'as Travel' as industry.
SELECT *
FROM layoffs_staging2
WHERE company = 'Airbnb';

-- Creating a self-join where we can see blank industries and their appropriate value (if any)
SELECT *
FROM layoffs_staging2 AS t1
JOIN layoffs_staging2 AS t2
	ON t1.company = t2.company
    AND t1.location = t2.location
WHERE t1.industry = '' 
AND t2.industry != '';

-- Updating all blank value for the industry with their ppropriate values found.
UPDATE layoffs_staging2 AS t1
JOIN layoffs_staging2 AS t2
	ON t1.company = t2.company
    AND t1.location = t2.location
SET t1.industry = t2.industry
WHERE t1.industry = '' 
AND t2.industry != '';

-- Creating a self-join where we can see none industries and their appropriate value (if any)
-- Running this confirms us that there are no appropriate value. So nothing to update.
SELECT *
FROM layoffs_staging2 AS t1
JOIN layoffs_staging2 AS t2
	ON t1.company = t2.company
    AND t1.location = t2.location
WHERE t1.industry = 'None' 
AND t2.industry != 'None';

-- We cannot populate any other null or blank values based on the data we have. 

-- Executing step 4. (Remove any columnns)
-- We see from the below query, there are many records that do not have any value for total_laid_off and percentage_laid_off. 
-- These records could be wrong, ambiguous or irrelevant. 
-- We can delete these records if we are confident that we do not need them.
SELECT *
FROM layoffs_staging2
WHERE total_laid_off = 'None' AND percentage_laid_off = 'None';


SELECT *
FROM layoffs_staging2;

-- We do not need the row_num column that we added, anymore. We can drop it.
ALTER TABLE layoffs_staging2
DROP COLUMN row_num;



DELETE
FROM layoffs_staging2
WHERE total_laid_off = 'None' AND percentage_laid_off = 'None';

-- That completes the data cleaning job for this data set.

