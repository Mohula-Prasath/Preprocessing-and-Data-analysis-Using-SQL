SELECT * FROM layoffs;

#USUAL STEPS TO BE INVOLVED 
#Remove Duplicates
#Standardize the data
#Handle NULL values
#Remove unnecessary column in huge dataset

#Before applying let us take a copy of the original dataset
CREATE TABLE layoffs_copy LIKE layoffs;
INSERT INTO layoffs_copy SELECT * FROM layoffs;
SELECT * FROM layoffs_copy;

#STEP-1 Removing duplicates:
SELECT *, ROW_NUMBER() OVER(PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num FROM layoffs_copy ORDER BY row_num DESC;
#or
WITH duplicates_CTE AS 
(
SELECT *, ROW_NUMBER() OVER(PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num FROM layoffs_copy
)
SELECT * FROM duplicates_CTE where row_num>1;

WITH duplicates_CTE AS 
(
SELECT *, ROW_NUMBER() OVER(PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num FROM layoffs_copy
)
DELETE FROM duplicates_CTE where row_num>1;


#SINCE WE CANNOT DELETE AS IT IS A TEMPORARY COLUMN LET US CREATE NEW TABLE AND DELETE
CREATE TABLE `layoffs_copy2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

SELECT * FROM layoffs_copy2;

INSERT INTO layoffs_copy2 SELECT *, ROW_NUMBER() OVER(PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num FROM layoffs_copy;

DELETE FROM layoffs_copy2 WHERE row_num>1;
SELECT * FROM layoffs_copy2 WHERE row_num>1;

#STANDARDIZING DATA
SELECT company, TRIM(company) FROM layoffs_copy2;
UPDATE layoffs_copy2 SET company=TRIM(company);

#FOUND SIMILAR INDUSTRIES LIKE CRYPTO, CRYPTOCURRENCY, CRYPTO_CURRENCY
SELECT DISTINCT industry FROM layoffs_copy2 ORDER BY 1;
SELECT DISTINCT industry FROM layoffs_copy2 WHERE industry like "Crypto%";

UPDATE layoffs_copy2 SET industry="Crypto" WHERE industry like "Crypto%";

SELECT DISTINCT location FROM layoffs_copy2 ORDER BY 1; #no major flaws

SELECT DISTINCT country FROM layoffs_copy2 ORDER BY 1;
SELECT DISTINCT country FROM layoffs_copy2 WHERE country LIKE "United States%";
UPDATE layoffs_copy2 SET country=TRIM(TRAILING '.' FROM country) WHERE country LIKE "United States%";

#Converting into date format
SELECT `date`, STR_TO_DATE(`date`, "%m/%d/%Y") FROM layoffs_copy2;
UPDATE layoffs_copy2 SET `date`=STR_TO_DATE(`date`, "%m/%d/%Y");
SELECT `date` FROM layoffs_copy2;
ALTER TABLE layoffs_copy2 MODIFY `date` DATE;

#Handling missing values
SELECT * FROM layoffs_copy2 WHERE industry IS NULL OR industry='';
#Let us find the industry of AIRbnb first
SELECT * FROM layoffs_copy2 WHERE company="Airbnb";
UPDATE layoffs_copy2 SET industry="Travel" WHERE company="Airbnb";
#or
SELECT * FROM layoffs_copy2 as t1 INNER JOIN layoffs_copy2 as t2 ON t1.company=t2.company WHERE (t1.industry is NULL OR t1.industry='') AND (t2.industry is NOT NULL AND t2.industry!='');

UPDATE layoffs_copy2 t1 JOIN layoffs_copy2 t2 ON t1.company=t2.company
SET t1.industry=t2.industry
WHERE (t1.industry is NULL OR t1.industry='') AND (t2.industry is NOT NULL AND t2.industry!='');

SELECT * FROM layoffs_copy2 WHERE (total_laid_off is NULL or total_laid_off = '') and (percentage_laid_off is NULL or percentage_laid_off='');

#DROP unnecessary columns
SELECT * FROM layoffs_copy2;
#WE do not need row_num
ALTER TABLE layoffs_copy2 DROP column row_num;