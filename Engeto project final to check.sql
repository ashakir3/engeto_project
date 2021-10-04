#t_Azizbek_Shakirov_projekt_SQL_fINal
# Updatovani nazvu countries, pridavani sloupce weekend,ctvrtleti a spojovani pomocnych tabulek

CREATE TABLE t_Azizbek_Shakirov_projekt_SQL_final (
SELECT
	cbd.country AS country,
	cbd.`date` AS date,
	cbd.confirmed AS confirmed,
	cbd.weekend AS weekned,
	cbd.period AS period,
	ct.tests_performed AS tests_performed,
	cv.population_density AS population_density,
	cv.median_age_2018 AS median_age_2018,
	evg.gdp_per_person AS gdp_per_person,
	ev.average_gini AS average_gini,
	ev.average_mor_under5 AS average_mor_under5,
	rd.Buddhism AS Buddhism,
	rd.Christianity AS Christianity,
	rd.Folk_Religions AS Folk_Religions,
	rd.Hinduism AS Hinduism,
	rd.Islam AS Islam,
	rd.Judaism AS Judaism,
	rd.Other_Religions AS Other_Religions,
	ROUND(lev.diff_2015_1956,2) AS diff_2015_1956,
	dtv.average_temp AS average_temperature,
	hv.hours AS hours_rain,
	wsp.max_wind_speed AS max_wind_speed
FROM covid19_b_d_view cbd
JOIN covid19_tests_view ct 
ON cbd.country = ct.country  AND cbd.`date` = ct.`date` 
JOIN countries_view cv
ON cbd.country =cv.country
JOIN economies_view_gdp evg
ON cbd.country = evg.country
JOIN economies_view ev
ON cbd.country  = ev.country
JOIN religions_div rd
ON cbd.country = rd.country 
JOIN life_expectancy_view lev
ON cbd.country = lev.country 
JOIN day_temp_view dtv
ON cbd.country = dtv.country AND cbd.`date` = dtv.`date` 
JOIN hours_view hv
ON cbd.country = hv.country AND cbd.`date` = hv.`date`
JOIN wind_speed_view wsp
ON cbd.country = wsp.country AND cbd.`date` = wsp.`date`
GROUP BY cbd.country,cbd.`date`);

# StANDartizovani nazvu statu
CREATE TABLE covid19_b_d_view(
	SELECT 
		CASE WHEN country = 'Burma' THEN 'Myanmar'
		WHEN country = 'Cabo Verde' THEN 'Cape Verde'
		WHEN country = 'Congo (Brazzaville)' THEN 'Congo'
		WHEN country = 'Congo (Kinshasa)' THEN 'The Democratic Republic of Congo'
		WHEN country = 'Cote d"Ivoire' THEN 'Ivory Coast'
		WHEN country = 'Czechia' THEN 'Czech Republic' 
		WHEN country = 'Eswatini' THEN 'Swaziland'
		WHEN country = 'Fiji' THEN 'Fiji Islands'
		WHEN country = 'Holy See' THEN 'Holy See (Vatican City State)'
		WHEN country = 'Korea, South' THEN 'South Korea'
		WHEN country = 'Libya' THEN 'Libyan Arab Jamahiriya'
		WHEN country = 'Micronesia' THEN 'Micronesia, Federated States of'
		WHEN country = 'Russia' THEN 'Russian Federation'
		WHEN country = 'US' THEN 'United States'
		ELSE country 
		END AS country,
		confirmed,
		`date`,
	CASE 
		WHEN WEEKDAY(`date`) IN (5,6) THEN 1
		ELSE 0
	END AS weekend,
	CASE
		WHEN MONTH(`date`) IN (1,2,3) THEN  0
		WHEN MONTH(`date`) IN (4,5,6) THEN  1
		WHEN MONTH(`date`) IN (7,8,9) THEN  2
		ELSE 3
	end AS period
	FROM covid19_basic_differences);



# Beru z Countries jen to co potreba pro zadani
CREATE TABLE countries_view (
	SELECT 
	country,
	ROUND(population_density,2) AS population_density,
	ROUND(median_age_2018,2) AS median_age_2018
	FROM countries);

# Beru z Economies jen to co potreba(krome HDP) pro zadani a omezuji pocet countries, pouzil jsem tady AVG, jelikoz nekterym countries chybi INformace pro nektere roky.
CREATE TABLE economies_view (
	SELECT country,
	ROUND(AVG(gini),2) AS average_gini,
	ROUND(AVG(mortaliy_under5),2) AS average_mor_under5
	FROM economies 
	WHERE country IN (SELECT country FROM covid19_basic_differences)
	GROUP BY country );



# Pocitani HDP
CREATE TABLE economies_view_gdp (
	SELECT country,
	ROUND(GDP/population,2) AS gdp_per_person
	FROM economies 
	WHERE
	`year` = 2020 
	AND
	country IN (SELECT country FROM covid19_basic_differences)
	GROUP BY country 
);


# Beru INformace z Religions za posledni rok.
CREATE TABLE religions_view (
	WITH base AS( 
	SELECT 
		country,
		religion,
		population,
		SUM(population) OVER(PARTITION BY country) total_population
	FROM religions
	WHERE `year` = '2020'
	)
	SELECT
		country,
		religion,
		population,
		ROUND(population/total_population*100,2) AS percentage,
		total_population
	FROM base
	GROUP BY country,religion );

# Radky -> Sloupce
CREATE TABLE religions_div
SELECT 
	country,
	MAX(CASE WHEN religion = 'Buddhism' THEN percentage END) AS Buddhism,
	MAX(CASE WHEN religion = 'Christianity' THEN percentage END) AS Christianity,
	MAX(CASE WHEN religion = 'Folk Religions' THEN percentage END) AS Folk_Religions,
	MAX(CASE WHEN religion = 'Hinduism' THEN percentage END) AS Hinduism,
	MAX(CASE WHEN religion = 'Islam' THEN percentage END) AS Islam,
	MAX(CASE WHEN religion = 'Judaism' THEN percentage END) AS Judaism,
	MAX(CASE WHEN religion = 'Other Religions' THEN percentage END) AS Other_Religions
FROM religions_view rv 
GROUP by country ;


# Pocitam rozdil z tabulky life_expectancy (2015-1965)
CREATE TABLE life_expectancy_view ( 
	SELECT
		le1.country,
		(le1.life_expectancy - le2.life_expectancy) AS diff_2015_1956
	FROM life_expectancy le1 
	CROSS JOIN life_expectancy le2
	WHERE le1.`year` = 2015 AND le2.`year` = 1965
	GROUP BY country) ;


# Prumerna denni teplota
CREATE TABLE day_temp_view (
SELECT
c.country,
CAST(w.`date` AS DATE) AS date,
AVG(CAST(REPLACE(temp,'°c','') AS INT)) AS average_temp
FROM weather w 
JOIN countries c 
ON w.city = c.capital_city
WHERE `time` between '06:00' AND '18:00'
GROUP BY c.country, w.`date`) ;


# Pocitani hodIN v danem dni, kdy byly srazky nenulove
CREATE TABLE hours_view (
SELECT
c.country,
CAST(w.`date` AS DATE) AS date,
COUNT(w.rain) * 3 AS hours
FROM weather w 
JOIN countries c 
ON w.city = c.capital_city
WHERE w.rain > '0.0 mm'
GROUP BY c.country,w.`date`) ;

# Maximalni sila vetru v narazech
CREATE TABLE wind_speed_view (
WITH base AS(
SELECT 
c.country,
CAST(w.`date` AS DATE) AS date,
LEFT(w.gust,2) AS gust_speed
FROM weather w 
JOIN countries c 
ON w.city = c.capital_city
)
SELECT 
country,
`date`,
MAX(CAST(gust_speed AS INT)) AS max_wind_speed
FROM base
GROUP BY country,`date`);

# Zmenen format date a mene sloupcu
CREATE TABLE covid19_tests_view(
	SELECT 
	country,
	CAST(`date` AS DATE) AS date,
	tests_performed 
	FROM covid19_tests 
	GROUP BY country,`date`);



