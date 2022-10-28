/*checking if I have accurate data in new_deaths/cases and total_deaths/cases columns*/
/*for some reasons I cannot create a temporary table and I don't know why, mb because I use a free version if BigQuery without billing*/
CREATE OR REPLACE TABLE
  CovidData.cases_deaths_accuracy( location string,
    --population int64,
    cases_accuracy float64,
    deaths_accuracy float64 ) AS (
  WITH
    aggregated_data AS (
    SELECT
      location,
      --MAX(population) AS population,
      SUM(new_cases) AS sum_new_cases,
      MAX(total_cases) AS total_cases,
      SUM(new_deaths) AS sum_new_deaths,
      MAX(total_deaths) AS total_deaths
    FROM
      `CovidData.deaths_clustered`
    GROUP BY
      location )
  SELECT
    location,
    --population,
    CovidData.get_percent(sum_new_cases,
      total_cases) AS cases_accuracy,
    CovidData.get_percent(sum_new_deaths,
      total_deaths) AS deaths_accuracy,
  FROM
    aggregated_data ); 
    

/*what countries have low accuracy? and what countries do not have any data about cases and deaths?*/
SELECT
  location,
  --population,
  cases_accuracy,
  deaths_accuracy,
FROM
  CovidData.cases_deaths_accuracy
WHERE
  cases_accuracy < 95
  OR deaths_accuracy < 95
  OR cases_accuracy IS NULL
  OR deaths_accuracy IS NULL; 
  
/*changing the deaths table and replace nulls*/
CREATE OR REPLACE TABLE
  `CovidData.deaths_clustered`
CLUSTER BY
  continent,
  location AS (
  SELECT
    iso_code,
    continent,
    location,
    creation_date,
    COALESCE( 
      LAST_VALUE(population IGNORE NULLS) 
      OVER(PARTITION BY location ORDER BY creation_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),
      0) AS population,

    COALESCE(
      LAST_VALUE(total_cases IGNORE NULLS) 
      OVER(PARTITION BY location ORDER BY creation_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),
      0) AS total_cases,

    COALESCE(new_cases, 0) AS new_cases,
    COALESCE(
      LAST_VALUE(total_deaths IGNORE NULLS) 
      OVER(PARTITION BY location ORDER BY creation_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), 
      0) AS total_deaths,
    COALESCE(new_deaths,0) AS new_deaths,
  FROM
    `CovidData.deaths`); 
    
    
/*cheking what I have in the vaccinations table*/ 
CREATE OR REPLACE TABLE
  CovidData.vaccinations_accuracy AS (
  WITH
    aggregated_data AS(
    SELECT
      location,
      MAX(total_vaccinations) AS total_vaccinations,
      SUM(new_vaccinations) AS sum_new_vaccinations,
    FROM
      `CovidData.vaccinations_clustered`
    WHERE
      CONTINENT IS NOT NULL
    GROUP BY
      location)
  SELECT
    LOCATION,
    total_vaccinations,
    sum_new_vaccinations,
    CovidData.get_percent(sum_new_vaccinations,
      total_vaccinations) AS total_sum_ratio,
  FROM
    aggregated_data); 
    
/*what countries do not have any data about vaccinations?*/
SELECT
  location
FROM
  `CovidData.vaccinations_accuracy`
WHERE
  total_vaccinations IS NULL; 
  
/*what countries have low accuracy for the new_vaccinations parameter?*/ 
/*it's a lot!*/
SELECT
  CovidData.get_percent(COUNTIF(total_sum_ratio < 95),
    COUNT(*)) AS number_of_countries_with_low_vaccinations_accuracy
FROM
  `CovidData.vaccinations_accuracy`; 
  
/*create a table with info about data*/
CREATE OR REPLACE TABLE
  CovidData.locations_info_table AS (
  SELECT
    v.location,
  IF
    (cd.deaths_accuracy < 95, TRUE, FALSE) AS recalculate_new_deaths,
  IF
    (cd.deaths_accuracy IS NULL, TRUE, FALSE) AS no_deaths_data,
  IF
    (cd. cases_accuracy IS NULL, TRUE, FALSE) AS no_cases_data,
  IF
    (v.total_sum_ratio IS NULL, TRUE, FALSE) AS no_vaccinations_data,
  IF
    (v.total_sum_ratio < 95
      OR (v.total_vaccinations IS NOT NULL
        AND v.sum_new_vaccinations IS NULL), TRUE, FALSE) AS recalculate_new_vaccinations,
  FROM
    CovidData.cases_deaths_accuracy cd
  JOIN
    CovidData.vaccinations_accuracy v
  USING
    (location)); 
    
/*recalculate new vaccinations for countries with low accuracy and fill nulls*/
CREATE OR REPLACE TABLE
  `CovidData.vaccinations_clustered`( 
    iso_code STRING,
    continent STRING,
    location STRING,
    creation_date DATE,
    total_vaccinations INT64,
    people_vaccinated INT64,
    people_fully_vaccinated INT64,
    total_boosters INT64,
    new_vaccinations INT64,
    )
CLUSTER BY
  continent,
  location AS(
  WITH
    tmp AS(
    SELECT
      iso_code,
      continent,
      location,
      creation_date,
      LAST_VALUE(total_vaccinations IGNORE NULLS) 
      OVER(PARTITION BY location ORDER BY creation_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS total_vaccinations,

      LAST_VALUE(people_vaccinated IGNORE NULLS) 
      OVER(PARTITION BY location ORDER BY creation_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS people_vaccinated,

      LAST_VALUE(people_fully_vaccinated IGNORE NULLS) 
      OVER(PARTITION BY location ORDER BY creation_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS people_fully_vaccinated,

      LAST_VALUE(total_boosters IGNORE NULLS) 
      OVER(PARTITION BY location ORDER BY creation_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS total_booster,

      new_vaccinations
    FROM
      `CovidData.vaccinations_clustered` )
  SELECT
    iso_code,
    continent,
    location,
    creation_date,
    COALESCE(total_vaccinations,0) AS total_vaccinations,
    COALESCE(people_vaccinated,0) AS people_vaccinated,
    COALESCE(people_fully_vaccinated,0) AS people_fully_vaccinated,
    COALESCE(total_booster,0) AS total_booster,
    COALESCE(
      CASE
        WHEN location IN ( SELECT location 
                           FROM CovidData.locations_info_table 
                           WHERE recalculate_new_vaccinations IS TRUE) 
        THEN 
        total_vaccinations - LAG(total_vaccinations) OVER(PARTITION BY location ORDER BY creation_date)
        ELSE NEW_VACCINATIONS
      END
    ,0) AS new_vaccinations
  FROM
    tmp );


DROP TABLE IF EXISTS
  CovidData.cases_deaths_accuracy;
DROP TABLE IF EXISTS
  CovidData.vaccinations_accuracy;