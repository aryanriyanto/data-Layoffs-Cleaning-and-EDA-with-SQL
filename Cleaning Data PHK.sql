-- SQL Proyek - Pembersihan Data (Data Cleaning)

-- Dataset: https://www.kaggle.com/datasets/swaptr/layoffs-2022

-- Menampilkan seluruh data dari tabel utama
SELECT * 
FROM world_layoffs.layoffs;

-- 1. Membuat tabel staging untuk bekerja dengan data yang akan dibersihkan
-- Ini dilakukan untuk menjaga data mentah tetap utuh jika terjadi kesalahan
USE world_layoffs;

CREATE TABLE layoffs_staging 
LIKE layoffs;

INSERT INTO layoffs_staging 
SELECT * FROM layoffs;
;

-- Langkah-langkah dalam proses pembersihan data:
-- 1. Mengecek dan menghapus duplikasi data
-- 2. Standarisasi data dan memperbaiki kesalahan
-- 3. Mengevaluasi nilai NULL
-- 4. Menghapus kolom dan baris yang tidak diperlukan

-- 1. Menghapus Duplikasi

-- Mengecek data yang kemungkinan memiliki duplikasi
SELECT *
FROM world_layoffs.layoffs_staging;

-- Mengecek duplikasi berdasarkan beberapa kolom utama
SELECT company, industry, total_laid_off, `date`,
		ROW_NUMBER() OVER (
			PARTITION BY company, industry, total_laid_off, `date`
		) AS row_num
	FROM world_layoffs.layoffs_staging;

-- Menampilkan duplikasi yang teridentifikasi
SELECT *
FROM (
	SELECT company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions,
		ROW_NUMBER() OVER (
			PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions
		) AS row_num
	FROM world_layoffs.layoffs_staging
) duplicates
WHERE row_num > 1;

-- Menghapus data duplikat dengan metode CTE (Common Table Expression)
WITH DELETE_CTE AS (
	SELECT company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions, 
    ROW_NUMBER() OVER (PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
	FROM world_layoffs.layoffs_staging
)
DELETE FROM world_layoffs.layoffs_staging
WHERE (company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions, row_num) IN (
	SELECT company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions, row_num
	FROM DELETE_CTE
) AND row_num > 1;

-- Alternatif lain: menambahkan kolom row_num untuk membantu proses penghapusan
ALTER TABLE world_layoffs.layoffs_staging ADD row_num INT;

-- Mengecek data setelah penambahan kolom
SELECT * FROM world_layoffs.layoffs_staging;

-- Membuat tabel baru untuk menyimpan data tanpa duplikasi
CREATE TABLE `world_layoffs`.`layoffs_staging2` (
`company` TEXT,
`location` TEXT,
`industry` TEXT,
`total_laid_off` INT,
`percentage_laid_off` TEXT,
`date` TEXT,
`stage` TEXT,
`country` TEXT,
`funds_raised_millions` INT,
row_num INT
);

-- Memasukkan data ke tabel baru dengan row_num untuk identifikasi duplikasi
INSERT INTO `world_layoffs`.`layoffs_staging2`
(`company`, `location`, `industry`, `total_laid_off`, `percentage_laid_off`, `date`, `stage`, `country`, `funds_raised_millions`, `row_num`)
SELECT `company`, `location`, `industry`, `total_laid_off`, `percentage_laid_off`, `date`, `stage`, `country`, `funds_raised_millions`,
		ROW_NUMBER() OVER (
			PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions
		) AS row_num
	FROM world_layoffs.layoffs_staging;

-- Menghapus duplikasi dengan row_num lebih dari 1
DELETE FROM world_layoffs.layoffs_staging2
WHERE row_num >= 2;

-- 2. Standarisasi Data

-- Mengecek data setelah proses penghapusan duplikasi
SELECT * FROM world_layoffs.layoffs_staging2;

-- Mengecek nilai NULL atau kosong dalam kolom industry
SELECT DISTINCT industry FROM world_layoffs.layoffs_staging2 ORDER BY industry;

-- Menampilkan baris dengan industry kosong atau NULL
SELECT * FROM world_layoffs.layoffs_staging2 WHERE industry IS NULL OR industry = '' ORDER BY industry;

-- Mengubah nilai kosong menjadi NULL agar lebih mudah diolah
UPDATE world_layoffs.layoffs_staging2
SET industry = NULL
WHERE industry = '';

-- Mengisi industry yang kosong berdasarkan data perusahaan yang sama
UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL;

-- Standarisasi nama industry yang tidak konsisten (misalnya Crypto)
UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry IN ('Crypto Currency', 'CryptoCurrency');

-- Mengecek kembali nilai unik dalam kolom industry setelah standarisasi
SELECT DISTINCT industry FROM world_layoffs.layoffs_staging2 ORDER BY industry;

-- Mengecek perbedaan dalam penulisan country (misalnya "United States" vs "United States.")
SELECT DISTINCT country FROM world_layoffs.layoffs_staging2 ORDER BY country;

-- Menghapus titik di akhir nama negara jika ada
UPDATE layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country);

-- Mengecek hasil standarisasi nama negara
SELECT DISTINCT country FROM world_layoffs.layoffs_staging2 ORDER BY country;

-- Standarisasi format tanggal
UPDATE layoffs_staging2
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

-- Mengubah tipe data kolom tanggal menjadi DATE
ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;

-- Mengecek data setelah perubahan format tanggal
SELECT * FROM world_layoffs.layoffs_staging2;

-- 3. Mengevaluasi Nilai NULL

-- Mengecek nilai NULL dalam kolom total_laid_off, percentage_laid_off, dan funds_raised_millions
-- Tidak ada perubahan yang perlu dilakukan karena nilai NULL dapat berguna dalam analisis data

-- 4. Menghapus Baris atau Kolom yang Tidak Diperlukan

-- Mengecek baris di mana total_laid_off bernilai NULL
SELECT * FROM world_layoffs.layoffs_staging2 WHERE total_laid_off IS NULL;

-- Menghapus baris yang tidak memiliki nilai total_laid_off dan percentage_laid_off
DELETE FROM world_layoffs.layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

-- Menghapus kolom row_num yang tidak lagi diperlukan
ALTER TABLE layoffs_staging2
DROP COLUMN row_num;

-- Menampilkan data akhir setelah proses pembersihan selesai
SELECT * FROM world_layoffs.layoffs_staging2;
