SELECT * FROM layoffs_copy2;

#SIMPLE ANALYSIS
SELECT MAX(total_laid_off), MAX(percentage_laid_off) FROM layoffs_copy2;

#time-span of the dataset
SELECT MIN(`date`), MAX(`date`) FROM layoffs_copy2;

#General analysis
SELECT company, SUM(total_laid_off) FROM layoffs_copy2 GROUP BY company ORDER BY 2 DESC LIMIT 10;
SELECT industry, SUM(total_laid_off) FROM layoffs_copy2 GROUP BY industry ORDER BY 2 DESC;
SELECT country, SUM(total_laid_off) FROM layoffs_copy2 GROUP BY country ORDER BY 2 DESC;
SELECT stage, SUM(total_laid_off) FROM layoffs_copy2 GROUP BY stage ORDER BY 2 DESC;


SELECT YEAR(`date`), SUM(total_laid_off) FROM layoffs_copy2 GROUP BY YEAR(`date`) ORDER BY 1;
SELECT YEAR(`date`) AS yr, MONTH(`date`) AS mnt, SUM(total_laid_off) FROM layoffs_copy2 GROUP BY Yr,mnt HAVING yr is NOT NULL ORDER BY 1,2;

SELECT company, AVG(total_laid_off) AS avg_layoffs FROM layoffs_copy2 GROUP BY company ORDER BY avg_layoffs DESC LIMIT 10;

#Companies with highest layoff in each year
WITH company_by_year AS
(SELECT YEAR(`date`) AS yr, company, SUM(total_laid_off) as tlo FROM layoffs_copy2 GROUP BY yr,company),
rank_year AS
(SELECT yr,company,tlo,DENSE_RANK() OVER (PARTITION BY yr ORDER BY Tlo DESC) AS rank_yr FROM company_by_year)
SELECT * FROM rank_year WHERE rank_yr=1;

#industries with highest layoff in each year
WITH industry_rank AS 
(SELECT YEAR(`date`) AS yr, industry, SUM(total_laid_off) as tlo FROM layoffs_copy2 GROUP BY yr,industry),
industry_rank_order AS
(SELECT yr, industry, tlo, DENSE_RANK() OVER(PARTITION BY yr ORDER BY tlo DESC) AS rank_cnt FROM industry_rank)
SELECT * FROM industry_rank_order WHERE rank_cnt=1;

#Countries with highest layoff in each year
WITH country_rank AS 
(SELECT YEAR(`date`) AS yr, country, SUM(total_laid_off) as tlo FROM layoffs_copy2 GROUP BY yr,country),
country_rank_order AS
(SELECT yr, country, tlo, DENSE_RANK() OVER(PARTITION BY yr ORDER BY tlo DESC) AS rank_cnt FROM country_rank)
SELECT * FROM country_rank_order WHERE rank_cnt=1;

#SHUTDOWN COMPANIES
SELECT * FROM layoffs_copy2 WHERE percentage_laid_off=1;

#Rolling cumulative layoffs by month to observe the overall trend
WITH rolling_sum_month AS 
(SELECT YEAR(`date`) AS yr, MONTH(`date`) AS mnt, SUM(total_laid_off) AS tot_sum FROM layoffs_copy2 GROUP BY Yr,mnt HAVING yr is NOT NULL ORDER BY 1,2)
SELECT yr, mnt, tot_sum, SUM(tot_sum) OVER(ORDER BY yr,mnt) AS rolling_total FROM rolling_sum_month;

#Rank companies by layoffs within each year
WITH year_company_wise_layoff(company,yr,laid_off) AS 
(SELECT company, YEAR(`date`), SUM(total_laid_off) FROM layoffs_copy2 GROUP BY company, YEAR(`date`))
SELECT company,yr,laid_off,DENSE_RANK() OVER(PARTITION BY yr ORDER BY laid_off DESC) AS rank_laid FROM year_company_wise_layoff WHERE (company is NOT NULL) AND (yr is not NULL) ; 

#Top 5 companies with the highest layoffs each year
WITH year_company_wise_layoff(company,yr,laid_off) AS 
(SELECT company, YEAR(`date`), SUM(total_laid_off) FROM layoffs_copy2 GROUP BY company, YEAR(`date`)),
selection AS
(SELECT company,yr,laid_off,DENSE_RANK() OVER(PARTITION BY yr ORDER BY laid_off DESC) AS rank_laid FROM year_company_wise_layoff WHERE (company is NOT NULL) AND (yr is not NULL))
SELECT * FROM selection where rank_laid<=5; 

#month with the highest layoffs in each year
WITH month_by_year AS
(SELECT YEAR(`date`) AS yr, MONTH(`date`) AS mnt, SUM(total_laid_off) as tlo FROM layoffs_copy2 GROUP BY yr,mnt),
rank_year AS
(SELECT yr,mnt,tlo,DENSE_RANK() OVER (PARTITION BY yr ORDER BY Tlo DESC) AS rank_yr FROM month_by_year)
SELECT * FROM rank_year WHERE rank_yr=1;

#minimum and maximum funding raised by companies in the dataset
SELECT MIN(funds_raised_millions), MAX(funds_raised_millions) FROM layoffs_copy2;

#Analyze layoffs based on funding levels by grouping companies into funding categories
SELECT 
CASE
	WHEN funds_raised_millions<100 AND funds_raised_millions>=0 THEN "low funding"
    WHEN funds_raised_millions<1000 AND funds_raised_millions>=100 THEN "moderate funding"
    WHEN funds_raised_millions<10000 AND funds_raised_millions>=1000 THEN "high funding"
    WHEN funds_raised_millions>=10000 THEN "mega funding"
END label,
SUM(total_laid_off) AS total_layoffs FROM layoffs_copy2 GROUP BY label HAVING label is NOT NULL ORDER BY total_layoffs DESC;
