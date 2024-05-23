-- Exploratory Data Analysis
-- This is an extensive data analysis where I explored the cleaned data set by filtering, monitoring and comparing different records.


SELECT *
FROM layoffs_staging2;

-- Looking at the maximum percentage_laid_off and the maximum total_laid_off.
-- I had to change the datatype from text to int, as I am using a json file. 
-- I am treating null values as 'None'
SELECT max(convert(percentage_laid_off,unsigned integer)), max(convert(total_laid_off,unsigned integer))
FROM layoffs_staging2
WHERE percentage_laid_off != 'None' AND total_laid_off != 'None';

-- Looking at the total_laid_off ordered in descending when percentage_laid_off is 1
SELECT *
FROM layoffs_staging2
WHERE percentage_laid_off = 1 AND total_laid_off != 'None'
ORDER BY convert(total_laid_off,unsigned integer) DESC;

-- Looking at the funds_raised_millions ordered in descending when percentage_laid_off is 1
SELECT *
FROM layoffs_staging2
WHERE percentage_laid_off = 1 AND total_laid_off != 'None'
ORDER BY convert(funds_raised_millions,unsigned integer) DESC;

-- Looking at companies and their sum of total_laid_off sorted in descending
SELECT company, SUM(convert(total_laid_off,unsigned integer))
FROM layoffs_staging2
GROUP BY company
ORDER BY 2 DESC;

-- Looking at the minimum and maximum dates
SELECT MIN(`date`), MAX(`date`)
FROM layoffs_staging2
WHERE `date` != 'None';

-- Looking at industries and their sum of total_laid_off sorted in descending
SELECT industry, SUM(convert(total_laid_off,unsigned integer))
FROM layoffs_staging2
GROUP BY industry
ORDER BY 2 DESC;

-- Looking at countries and their sum of total_laid_off sorted in descending
SELECT country, SUM(convert(total_laid_off,unsigned integer))
FROM layoffs_staging2
GROUP BY country
ORDER BY 2 DESC;

-- Looking at years and their sum of total_laid_off sorted in descending
SELECT YEAR (`date`), SUM(convert(total_laid_off,unsigned integer))
FROM layoffs_staging2
GROUP BY YEAR (`date`)
ORDER BY 1 DESC;

-- Looking at stage and their sum of total_laid_off sorted in descending
SELECT stage, SUM(convert(total_laid_off,unsigned integer))
FROM layoffs_staging2
GROUP BY stage
ORDER BY 2 DESC;

-- Looking at the total_laid_off based on the year and month in ascending order.
SELECT substring(`date`, 1, 7) AS `MONTH`, SUM(convert(total_laid_off,unsigned integer)) AS total_off
FROM layoffs_staging2
WHERE substring(`date`, 1, 7) != 'None'
GROUP BY `MONTH`
ORDER BY 1 ASC;

-- Using a CTE to find the rolling total to analyse and visualize data better. 
WITH Rolling_Total AS 
(
SELECT substring(`date`, 1, 7) AS `MONTH`, SUM(convert(total_laid_off,unsigned integer)) AS total_off
FROM layoffs_staging2
WHERE substring(`date`, 1, 7) != 'None'
GROUP BY `MONTH`
ORDER BY 1 ASC
)
SELECT `MONTH`, total_off, SUM(total_off) OVER(ORDER BY `MONTH`) AS Rolling_total
FROM Rolling_Total;

-- Looking at the company, date and total_laid_off sorted in descending order
SELECT company, YEAR(`date`) AS date_year, SUM(convert(total_laid_off,unsigned integer)) AS total_off
FROM layoffs_staging2
GROUP BY company, YEAR(`date`)
ORDER BY 3 DESC;


-- Using CTE to filter and rank the top 5 total_laid_off based on the particular year and the companies that laid off.
WITH Company_year AS 
(
SELECT company, YEAR(`date`) AS date_year, SUM(convert(total_laid_off,unsigned integer)) AS total_off
FROM layoffs_staging2
GROUP BY company, YEAR(`date`)
), Company_year_rank AS
(
SELECT *, dense_rank() OVER(PARTITION BY date_year ORDER BY total_off DESC) AS Ranking
FROM Company_year
WHERE date_year != ''
)
SELECT *
FROM Company_year_rank
WHERE Ranking <= 5;


