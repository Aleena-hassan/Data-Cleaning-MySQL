--REMOVE DUPLICATES
--STANDERDIZE THE DATA
--NULL VALUES OR BLANK VALUES
--REMOVE ANY COLUMNS OR ROWS




SELECT * FROM LAYOFFS;

CREATE TABLE layoffs_staging
like layoffs;
select * from layoffs_staging;

insert into layoffs_staging
select * from layoffs;

with duplicate_cte as(
select *,
row_number() over(
partition by company,location,industry, total_laid_off,`date`,percentage_laid_off,stage,country,funds_raised_millions) as row_num
from layoffs_staging)
select * from duplicate_cte where row_num>1;

select * from layoffs_staging where country='Casper';


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



select * from layoffs_staging2 where row_num>1;
delete from layoffs_staging2  where row_num>1;


DELETE from layoffs_staging2
WHERE row_num >1;

INSERT INTO LAYOFFS_STAGING2
select *,
row_number() over(
partition by company,location,industry, total_laid_off,`date`,percentage_laid_off,stage,country,funds_raised_millions) as row_num
from layoffs_staging;

select company,trim(company) from layoffs_staging2;
SET SQL_SAFE_UPDATES = 0;
UPDATE layoffs_staging2
SET COMPANY=trim(company);

UPDATE layoffs_staging2
SET INDUSTRY ='Crypto' WHERE 
INDUSTRY LIKE 'Crypto%';

select distinct industry from layoffs_staging2;

select distinct country , trim(trailing '.' from country)
from layoffs_staging2;

update layoffs_staging2
set country =trim(trailing '.' from country) 
where country like 'United States%';

select `date`
from layoffs_staging2;
update layoffs_staging2 
set `date` =str_to_date(`date`,'%m/%d/%Y');

Alter table layoffs_staging2
modify column `date` date;

Select * from layoffs_staging2 where total_laid_off is null and percentage_laid_off is null;

SELECT * FROM layoffs_staging2 T1
JOIN 
layoffs_staging2 T2 
	ON 
	T1.company=T2.company
WHERE (T1.industry IS NULL OR T1.INDUSTRY ='')
AND
T2.industry IS NOT NULL;

update layoffs_staging t1
join layoffs_staging2 T2 
	ON 
	T1.company=T2.company
set t1.industry=t2.industry
WHERE (T1.industry IS NULL OR T1.INDUSTRY ='')
and t2.industry is not null;

update layoffs_staging 
set industry= null where 
industry='';

Select * from layoffs_staging2;
Alter table layoffs_staging2
drop column row_num;
Select * from layoffs_staging2
 where total_laid_off is null 
 and 
 percentage_laid_off is null;


Delete from layoffs_staging2
 where total_laid_off is null 
 and 
 percentage_laid_off is null;











