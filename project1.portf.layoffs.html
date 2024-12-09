-- Створю базу даних 'world layoffs' та імпортую до неї набір даних 'layoffs' з інформацією про звільнення
-- Створю попередній етап обробки у вигляді ще однієї таблиці Layoffs_staging, скопіюю у неї дані із 'сирої' таблиці.
-- Так як при обробці, можуть виникати помилки, треба мати можливість робити порівняння з початковими даними або копіювати їх у подальшому для інших маніпуляцій.
-- Дивлюсь на стан даних та намічаю етапи по очистці даних для подальшого аналізу 
-- Видалю дублікати, якщо такі існують
-- Стандартизую дані
-- Попрацюю з порожніми або нульовими значеннями
-- Видалю значення, у яких не буде потреби при аналізі
CREATE TABLE layoffs_staging
LIKE layoffs;
INSERT layoffs_staging 
SELECT * FROM layoffs;

-- Нажаль у табличці немає ідентифікатора, що ускладнюю процес видалення дублів. Зроблю номери рядків
-- щоб мати можливість фільтрувати, де номер рядка 2 або більше. Це виявить дублікати.
SELECT *,
ROW_NUMBER() OVER(PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging;

-- поміщу запит в CTE для зручності та виведу дублікати на екран
WITH duplicate_cte AS
(SELECT *,
ROW_NUMBER() OVER(PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging)
SELECT * FROM duplicate_cte
WHERE row_num > 1;

-- тепер для видалення цих дублікатів, я по суті створю ще одну табличку
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
 
INSERT INTO layoffs_staging2
SELECT *,
ROW_NUMBER() OVER(PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging;

SELECT * FROM layoffs_staging2
WHERE row_num > 1;


DELETE FROM layoffs_staging2
WHERE row_num > 1;

-- Стандартизація даних(пошук проблем в данних та їх виправлення)
-- приберу помічені відступи та об'єднаю однакові по суті назви
UPDATE layoffs_staging2
SET company = TRIM(company);

UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';

UPDATE layoffs_staging2
SET country = TRIM(TRAILING '.' FROM COUNTRY)
WHERE country LIKE 'United States%';

-- зміню формат дати і тип дати з text на date
UPDATE layoffs_staging2
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;

-- нульові та порожні значення
-- заповню пропущені значення для компаній, де можна встановити до якої індустрії вони належать
SELECT * 
FROM layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company
WHERE (t1.industry IS NULL OR t1.industry = '') 
AND t2.industry IS NOT NULL;

UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE (t1.industry IS NULL OR t1.industry = '') 
AND t2.industry IS NOT NULL; 

DELETE FROM layoffs_staging2
WHERE total_laid_off IS NULL AND percentage_laid_off IS NULL;

ALTER TABLE layoffs_staging2
DROP COLUMN row_num;

-- Почну досліджувати дані
-- Дивлюсь на загальну кількість звільнень по країнам. Де звільнень було найбільше? 
-- На діапазон дат. Динаміку по рокам, місяцям. Що відбувалося у ці роки?
SELECT country, SUM(total_laid_off) FROM layoffs_staging2
GROUP BY country
ORDER BY 2 DESC;

SELECT MIN(`date`), MAX(`date`)
FROM layoffs_staging2;

SELECT YEAR(`date`), SUM(total_laid_off) FROM layoffs_staging2
GROUP BY YEAR(`date`)
ORDER BY 1 DESC;

SELECT SUBSTRING(`date`, 1, 7) AS Month, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY Month
ORDER BY 2 DESC;

 -- Які галузі постраждали найбільше? Особливо роздрібна торгівля. Потім транспорт, фінансова свера, догляд за собою...
 -- Найменше виробництво у сфері технологій, аерокосмічна голузь
SELECT industry, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY industry
ORDER BY 2 DESC;

-- По стадіям. Де помітно, що найбільше звільнень після виходу компаніх на IPO
SELECT stage, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY stage
ORDER BY stage;

-- Дивлюсь на загальну кількість звільнених працівників по кожній компанії
SELECT company, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY company
ORDER BY 2 DESC;
-- По рокам
SELECT company, YEAR(`date`), SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY company, YEAR(`date`)
ORDER BY 3 DESC;
-- ранжую
WITH company_year (comapany, years, total_laid_off) AS(
SELECT company, YEAR(`date`), SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY company, YEAR(`date`)
)
SELECT *, DENSE_RANK() OVER(PARTITION BY years ORDER BY total_laid_off DESC) AS Ranking
FROM company_year
WHERE years IS NOT NULL
ORDER BY Ranking;