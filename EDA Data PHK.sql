-- **Eksplorasi Data Awal (EDA)**
use world_layoffs;
-- Eksplorasi data untuk menemukan tren, pola, atau outlier yang menarik

-- Biasanya, saat memulai EDA, kita sudah memiliki gambaran mengenai apa yang ingin dicari

-- Dengan informasi ini, kita akan melihat-lihat data dan mencari wawasan menarik

SELECT * 
FROM world_layoffs.layoffs_staging2;

-- **Query Sederhana**

-- Mencari jumlah karyawan yang paling banyak terkena PHK dalam satu kejadian
SELECT MAX(total_laid_off)
FROM world_layoffs.layoffs_staging2;

-- Melihat persentase PHK tertinggi dan terendah
SELECT MAX(percentage_laid_off), MIN(percentage_laid_off)
FROM world_layoffs.layoffs_staging2
WHERE percentage_laid_off IS NOT NULL;

-- Mencari perusahaan yang mengalami PHK 100% (seluruh karyawan di-PHK)
SELECT *
FROM world_layoffs.layoffs_staging2
WHERE percentage_laid_off = 1;
-- Kebanyakan adalah startup yang bangkrut selama periode ini

-- Melihat perusahaan besar yang mengalami PHK 100%, diurutkan berdasarkan dana yang telah mereka kumpulkan
SELECT *
FROM world_layoffs.layoffs_staging2
WHERE percentage_laid_off = 1
ORDER BY funds_raised_millions DESC;
-- BritishVolt terlihat seperti perusahaan EV, dan Quibi yang pernah mengumpulkan 2 miliar dolar tetapi akhirnya bangkrut


-- **Query yang Lebih Kompleks (Menggunakan GROUP BY)**

-- Mencari perusahaan dengan jumlah PHK terbesar dalam satu kejadian
SELECT company, total_laid_off
FROM world_layoffs.layoffs_staging
ORDER BY 2 DESC
LIMIT 5;
-- Hanya berlaku untuk satu hari tertentu

-- Mencari perusahaan dengan total PHK terbanyak secara keseluruhan
SELECT company, SUM(total_laid_off)
FROM world_layoffs.layoffs_staging2
GROUP BY company
ORDER BY 2 DESC
LIMIT 10;

-- Mencari lokasi dengan jumlah PHK terbanyak
SELECT location, SUM(total_laid_off)
FROM world_layoffs.layoffs_staging2
GROUP BY location
ORDER BY 2 DESC
LIMIT 10;

-- Mencari total PHK berdasarkan negara
SELECT country, SUM(total_laid_off)
FROM world_layoffs.layoffs_staging2
GROUP BY country
ORDER BY 2 DESC;

-- Mencari total PHK berdasarkan tahun
SELECT YEAR(date), SUM(total_laid_off)
FROM world_layoffs.layoffs_staging2
GROUP BY YEAR(date)
ORDER BY 1 ASC;

-- Mencari industri dengan jumlah PHK terbanyak
SELECT industry, SUM(total_laid_off)
FROM world_layoffs.layoffs_staging2
GROUP BY industry
ORDER BY 2 DESC;

-- Mencari tahap pertumbuhan perusahaan dengan jumlah PHK terbanyak
SELECT stage, SUM(total_laid_off)
FROM world_layoffs.layoffs_staging2
GROUP BY stage
ORDER BY 2 DESC;


-- **Query yang Lebih Kompleks**

-- Sebelumnya, kita telah mencari perusahaan dengan jumlah PHK terbanyak secara keseluruhan
-- Sekarang, kita ingin melihat perusahaan dengan jumlah PHK terbanyak berdasarkan tahun

WITH Company_Year AS 
(
  SELECT company, YEAR(date) AS years, SUM(total_laid_off) AS total_laid_off
  FROM layoffs_staging2
  GROUP BY company, YEAR(date)
),
Company_Year_Rank AS (
  SELECT company, years, total_laid_off, DENSE_RANK() OVER (PARTITION BY years ORDER BY total_laid_off DESC) AS ranking
  FROM Company_Year
)
SELECT company, years, total_laid_off, ranking
FROM Company_Year_Rank
WHERE ranking <= 3
AND years IS NOT NULL
ORDER BY years ASC, total_laid_off DESC;


-- **Total PHK Bergulir Per Bulan**

-- Mencari total PHK per bulan
SELECT SUBSTRING(date,1,7) as dates, SUM(total_laid_off) AS total_laid_off
FROM layoffs_staging2
GROUP BY dates
ORDER BY dates ASC;

-- Menggunakan CTE agar dapat dilakukan analisis lebih lanjut
WITH DATE_CTE AS 
(
SELECT SUBSTRING(date,1,7) as dates, SUM(total_laid_off) AS total_laid_off
FROM layoffs_staging2
GROUP BY dates
ORDER BY dates ASC
)
SELECT dates, SUM(total_laid_off) OVER (ORDER BY dates ASC) as rolling_total_layoffs
FROM DATE_CTE
ORDER BY dates ASC;
