-- Exploring Data

	# In this project we explore the data from the table 'layoffs_staging2'
    # to see if there's anything interesting to analize and if there's any
    #more that we missed cleaning or needs to be fixed.

SELECT *
FROM layoffs_staging2
;
#What time period do these data cover?

SELECT MIN(`date`), MAX(`date`)
FROM layoffs_staging2
	-- 3 years, from March 2020 to March 2023

;

#Which are the countrys with most laid off?
SELECT country, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY country
ORDER BY 2 DESC
;

#Which kind of industry is the most affected?
SELECT industry, SUM(total_laid_off), AVG(percentage_laid_off)
FROM layoffs_staging2
GROUP BY industry
ORDER BY 2 DESC
;
#Where are the companies that shut down the most?

WITH counting AS
(
SELECT country, percentage_laid_off, 
		row_number() OVER(PARTITION BY company,country, percentage_laid_off) AS row_num
FROM layoffs_staging2
WHERE percentage_laid_off = 1
)
SELECT country, SUM(row_num) 'Companies that shut down'
FROM counting
GROUP BY country
;

#Months of the year that were more affected

SELECT SUM(total_laid_off) 'suma', substring(`date`,6,2) as `Month`
FROM layoffs_staging2
GROUP BY `Month`
HAVING `Month` IS NOT NULL
ORDER BY 2 

;
#Month more affected: June
WITH table_before AS
(
SELECT SUM(total_laid_off) 'suma', substring(`date`,6,2) as `Month`
FROM layoffs_staging2
GROUP BY `Month`
HAVING `Month` IS NOT NULL
)
SELECT `Month`, suma
FROM table_before
WHERE suma = (SELECT MAX(suma) FROM table_before)
;

WITH table_before2 AS
(
SELECT SUM(total_laid_off) 'suma', YEAR(`date`) AS `year`
FROM layoffs_staging2
GROUP BY `year`
HAVING `year` IS NOT NULL 	#in this CTE we can see the difference between all the years
ORDER BY 2
)
SELECT `year`, suma
FROM table_before2
WHERE suma = (SELECT MAX(suma) FROM table_before2)
	#After the COVID-19 period, the process of layoffs has been masived.
    #We observe that the highest number of layoffs occurred in 2022. 
    #However, in 2023, the numbers are very close to those of 2022, 
    #although this comparison only accounts for the first three months of the year.
;

#Let's make a Rolling total, month by month

WITH TABLE1 AS
(
SELECT SUM(total_laid_off) AS total_off, substring(`date`,1,7) AS `MONTH`
FROM layoffs_staging2
GROUP BY `MONTH`
HAVING `MONTH` IS NOT NULL
ORDER BY `MONTH` 
)
SELECT `MONTH`, total_off AS Total_off_month, SUM(total_off) OVER(ORDER BY `MONTH`) AS Rolling_total
FROM TABLE1

;

#Now we want to create a table with the top 5 companies with more layoffs of every year:

WITH Company_Year (company, years, total_laid_off) AS
(
SELECT company, YEAR(`date`), SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY company, YEAR(`date`)
),
Company_Year_Rank AS
(
SELECT *,
DENSE_RANK() OVER (partition by years ORDER BY total_laid_off DESC) AS Ranking
FROM Company_Year
WHERE years IS NOT NULL
)
SELECT *
FROM Company_Year_Rank
WHERE Ranking <= 5

