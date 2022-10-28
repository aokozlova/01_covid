  --
/*CREATE TABLE
  deaths --
CREATE TABLE
  vaccines*/

ALTER TABLE
  `CovidData.deaths` 
RENAME COLUMN date TO creation_date;


ALTER TABLE
  `CovidData.vaccinations` 
RENAME COLUMN date TO creation_date;

/*Clustering the data by location since I am going to do  a lot of aggregations and filtrations by these two columns*/
CREATE OR REPLACE TABLE
  `CovidData.deaths_clustered` (iso_code STRING,
    continent STRING,
    location STRING,
    creation_date DATE,
    population INT64,
    total_cases INT64,
    new_cases INT64,
    total_deaths INT64,
    new_deaths INT64,)
CLUSTER BY
  continent,
  location AS (
  SELECT
    iso_code,
    continent,
    location,
    creation_date,
    population,
    total_cases,
    new_cases,
    total_deaths,
    new_deaths,
  FROM
    `CovidData.deaths`);


CREATE OR REPLACE TABLE
  `CovidData.vaccinations_clustered` ( iso_code STRING,
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
  location AS (
  SELECT
    iso_code,
    continent,
    location,
    creation_date,
    total_vaccinations,
    people_vaccinated,
    people_fully_vaccinated,
    total_boosters,
    new_vaccinations,
  FROM
    `CovidData.vaccinations`);

  
  CREATE  FUNCTION if not exists
  CovidData.get_percent(x INT64,
    y INT64)
  RETURNS FLOAT64 AS (ROUND(SAFE_DIVIDE(x,y)*100, 2));