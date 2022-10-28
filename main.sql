/*world data*/   
WITH
  continents_data AS (
  SELECT
    location,
    MAX(population) AS population,
    MAX(total_cases) AS total_cases,
    MAX(total_deaths) AS total_deaths,
  FROM
    `CovidData.deaths_clustered`
  WHERE
    continent IS NULL
    AND location NOT LIKE '%income'
    AND LOWER(location) NOT LIKE 'european union'
  GROUP BY
    location)
SELECT
  location,
  population,
  total_cases,
  total_deaths,
  CovidData.get_percent(total_deaths,
    total_cases) AS death_rate,
  CovidData.get_percent(total_cases,
    population) AS infection_rate,
FROM
  continents_data
ORDER BY
  population DESC;
  
  
/*world daily data*/
WITH tmp AS 
(SELECT
  creation_date,
  SUM(new_cases) AS new_cases,
  SUM(new_deaths) AS new_deaths
FROM
  `CovidData.deaths_clustered`
WHERE
  continent IS NOT NULL /*having only countries*/
GROUP BY
  creation_date)
SELECT 
  creation_date, 
  new_cases, 
  new_deaths, 
  COALESCE(CovidData.get_percent(new_deaths, new_cases),0) as death_rate
FROM tmp
ORDER BY
  creation_date;
  

/*breaking by countries*/
CREATE OR REPLACE VIEW
  CovidData.locations_with_no_deaths_cases_data AS (
  SELECT
    location
  FROM
    `CovidData.locations_info_table`
  WHERE
    no_deaths_data IS TRUE
    OR no_cases_data IS TRUE);

  
WITH
  countries_data AS (
  SELECT
    location,
    MAX(population) AS population,
    MAX(total_cases) AS total_cases,
    MAX(total_deaths) AS total_deaths,
  FROM
    `CovidData.deaths_clustered`
  WHERE
    continent IS NOT NULL
    AND location NOT IN (
    SELECT
      location
    FROM
      CovidData.locations_with_no_deaths_cases_data) /*zeroes mean there was no covid in the country*/
  GROUP BY
    location)
SELECT
  location,
  population,
  total_cases,
  CovidData.get_percent(total_cases,
    population) AS infection_rate,
  total_deaths,
  CovidData.get_percent(total_deaths,
    total_cases) AS death_rate,
FROM
  countries_data
ORDER BY
  population DESC,
  infection_rate DESC; 
  
/*looking at total_cases vs total_deaths and population vs total_cases daily*/
SELECT
  location,
  creation_date,
  population,
  total_cases,
  new_cases,
  CovidData.get_percent(total_cases,
    population) AS infection_rate,
  total_deaths,
  new_deaths,
  CovidData.get_percent(total_deaths,
    total_cases) AS death_rate,
FROM
  `CovidData.deaths_clustered`
WHERE
  location NOT IN (
  SELECT
    location
  FROM
    CovidData.locations_with_no_deaths_cases_data)
ORDER BY
  location,
  creation_date;
  
  
/*what about gibraltar?*/
SELECT
  location,
  creation_date,
  population,
  total_cases AS total_cases,
  CovidData.get_percent(total_cases,
    population) AS infection_rate,
  total_deaths,
  CovidData.get_percent(total_deaths,
    total_cases),
FROM
  `CovidData.deaths_clustered`
WHERE
  location='Gibraltar'
ORDER BY
  creation_date;



/*lets look into vaccinations data*/
/*global data*/
CREATE OR REPLACE VIEW
  CovidData.deaths_vaccinations_join AS (
  SELECT
    v.continent,
    v.location,
    d.population,
    d.creation_date,
    v.total_vaccinations,
    v.new_vaccinations,
    v.people_vaccinated,
    v.people_fully_vaccinated,
    v.total_boosters,
  FROM
    `CovidData.vaccinations_clustered` v
  JOIN
    `CovidData.deaths_clustered` d
  ON
    v.location = d.location
    AND v.creation_date = d.creation_date );


WITH
  tmp AS (
  SELECT
    location,
    MAX(population) AS population,
    MAX(total_vaccinations) AS total_vaccinations,
    MAX(people_vaccinated) AS people_vaccinated,
    MAX(people_fully_vaccinated) AS people_fully_vaccinated,
    MAX(total_boosters) AS total_boosters,
  FROM
    CovidData.deaths_vaccinations_join
  WHERE
    continent IS NULL
    AND location NOT LIKE '%income'
    AND LOWER(location) NOT LIKE 'european union'
  GROUP BY
    location)
SELECT
  location,
  population,
  CovidData.get_percent(people_vaccinated,
    population) AS percent_of_people_vaccinated,
  CovidData.get_percent(people_fully_vaccinated,
    people_vaccinated) AS percent_of_people_fully_vaccinated,
  SAFE_DIVIDE(total_vaccinations, people_vaccinated) AS number_vaccinations_per_person,
FROM
  tmp
ORDER BY
  population DESC;
  
/*breaking by cointries*/ 
WITH
  tmp AS (
  SELECT
    location,
    MAX(population) AS population,
    MAX(total_vaccinations) AS total_vaccinations,
    MAX(people_vaccinated) AS people_vaccinated,
    MAX(people_fully_vaccinated) AS people_fully_vaccinated,
    MAX(total_boosters) AS total_boosters,
  FROM
    CovidData.deaths_vaccinations_join
  WHERE
    continent IS NOT NULL
    AND location NOT IN (SELECT 
                          location 
                        FROM `CovidData.locations_info_table` 
                        WHERE no_vaccinations_data is TRUE) 
  GROUP BY
    location)
SELECT
  location,
  population,
  CovidData.get_percent(people_vaccinated,
    population) AS percent_of_people_vaccinated,
  CovidData.get_percent(people_fully_vaccinated,
    people_vaccinated) AS percent_of_people_fully_vaccinated,
  SAFE_DIVIDE(total_vaccinations, people_vaccinated) AS number_vaccinations_per_person,
FROM
  tmp
ORDER BY
  percent_of_people_vaccinated DESC,
  population DESC;
  
/*dayly data*/
SELECT
  location,
  population,
  creation_date,
  new_vaccinations,
  CovidData.get_percent(people_vaccinated,
    population) AS percent_of_people_vaccinated,
  COALESCE(SAFE_DIVIDE(total_vaccinations, people_vaccinated),0) AS number_vaccinations_per_person,
FROM
  CovidData.deaths_vaccinations_join
WHERE
  continent IS NOT NULL
  AND location NOT IN (SELECT 
                          location 
                        FROM `CovidData.locations_info_table` 
                        WHERE no_vaccinations_data is TRUE) 
ORDER BY
  location,
  creation_date;


DROP FUNCTION IF EXISTS
  CovidData.get_percent;
DROP VIEW IF EXISTS
  CovidData.deaths_vaccinations_join;

DROP VIEW IF EXISTS
  CovidData.locations_with_no_deaths_cases_data;