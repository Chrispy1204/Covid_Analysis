-- Databricks notebook source
-- MAGIC %python
-- MAGIC
-- MAGIC df_deaths = spark.read.format("csv").option("header", "true").load("dbfs:/FileStore/shared_uploads/cp1204@utexas.edu/owid_covid_data_deaths-1.csv")
-- MAGIC
-- MAGIC df_vaccinations = spark.read.format("csv").option("header", "true").load("dbfs:/FileStore/shared_uploads/cp1204@utexas.edu/owid_covid_data_vaccinations-1.csv")
-- MAGIC
-- MAGIC df_deaths.createOrReplaceTempView("covid_data_deaths")
-- MAGIC df_vaccinations.createOrReplaceTempView("covid_data_vaccinations")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC The data is seperated into 2 tables, one including the deaths information of Covid and the other with vaccinations information

-- COMMAND ----------

-- double check that the data was imported
SHOW TABLES;

-- COMMAND ----------

-- Check data range

SELECT MIN(date), MAX(date)
FROM covid_data_deaths;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC The data is from 2020-01-01 to 2024-02-26

-- COMMAND ----------

SELECT * FROM covid_data_deaths 
LIMIT 10;

-- COMMAND ----------

DESCRIBE covid_data_deaths;

-- COMMAND ----------

DESCRIBE covid_data_vaccinations;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ### PART 1 - Data Exploration 

-- COMMAND ----------

-- select the data we want for analysis
SELECT location, date, total_cases, new_cases, total_deaths, population
FROM covid_data_deaths
ORDER BY 1,2;

-- COMMAND ----------

-- Total Cases VS Total Deaths
-- Shows the likelihood of dying if you get covid
SELECT location, date, total_cases, total_deaths, (total_deaths/total_cases)*100 AS DeathPercentage
FROM covid_data_deaths
WHERE location like '%States%'
ORDER BY 1;

-- COMMAND ----------

-- Looking at Total Cases VS Population
-- Shows the percentage of population that got Covid
SELECT location, date, total_cases, population, (total_cases/population)*100 AS CovidPercentage
FROM covid_data_deaths
WHERE location like '%States%'
ORDER BY 1;

-- COMMAND ----------

-- Looking at Countries with the highest Infection Rate compared to Population

SELECT location, population, MAX(total_cases) AS HighestInfectionCount, ((MAX(total_cases))/population)*100 AS PercentagePopulationInfected
FROM covid_data_deaths
GROUP BY location, population
ORDER BY PercentagePopulationInfected DESC;


-- COMMAND ----------

-- Showing the Countries with Highest Death Count per Population

SELECT location, MAX(CAST(total_deaths AS INT)) AS TotalDeathCount
FROM covid_data_deaths
WHERE continent IS NOT NULL
GROUP BY location
ORDER BY TotalDeathCount DESC;


-- COMMAND ----------

-- Looking into Continents

SELECT continent, MAX(CAST(total_deaths AS INT)) AS TotalDeathCount
FROM covid_data_deaths
WHERE continent IS NOT NULL
GROUP BY continent
ORDER BY TotalDeathCount DESC;

-- Seems like data isn't correct (Canada is not included in NA)

-- COMMAND ----------

-- This is the correct way to find the data grouped by continent
-- this shows continents with highest death count
SELECT location, MAX(CAST(total_deaths AS INT)) AS TotalDeathCount
FROM covid_data_deaths
WHERE continent IS NULL
GROUP BY location
ORDER BY TotalDeathCount DESC;

-- COMMAND ----------

-- Global view

SELECT 
  date, 
  SUM(CAST(new_cases AS INT)) AS GlobalNewCases, 
  SUM(CAST(new_deaths AS INT)) AS GlobalNewDeaths,
  SUM(CAST(new_deaths AS INT))/SUM(new_cases) *100 AS DeathPercentage
FROM covid_data_deaths
WHERE continent IS NOT NULL
GROUP BY date
ORDER BY 1,2;

-- COMMAND ----------

-- Death Percentage of the world

SELECT 
  SUM(CAST(new_cases AS INT)) AS GlobalNewCases, 
  SUM(CAST(new_deaths AS INT)) AS GlobalNewDeaths,
  SUM(CAST(new_deaths AS INT))/SUM(new_cases) *100 AS DeathPercentage
FROM covid_data_deaths
WHERE continent IS NOT NULL
ORDER BY 1,2

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ### PART 2 - Combining with Vaccinations file

-- COMMAND ----------

SELECT *
FROM covid_data_deaths D
JOIN covid_data_vaccinations V
  ON D.location  = V.location
  and D.date = V.date;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ### PART 3 - Data Exploration with Joined files

-- COMMAND ----------

-- Looking into Vaccination Percentage of Total Population

SELECT 
  D.continent,
  D.location,
  D.date,
  D.population,
  V.new_vaccinations,
  SUM(CAST(V.new_vaccinations AS INT)) OVER (PARTITION BY D.location ORDER BY D.location, D.date) AS CumulatedVaccination
FROM covid_data_deaths D
JOIN covid_data_vaccinations V
  ON D.location  = V.location
  and D.date = V.date
WHERE D.continent IS NOT NULL
ORDER BY 2,3;

-- COMMAND ----------

-- USE CTE

WITH PopVSVac (continent, location, date, population, new_vaccinations, CumulatedVaccination)
AS
(
  SELECT 
  D.continent,
  D.location,
  D.date,
  D.population,
  V.new_vaccinations,
  SUM(CAST(V.new_vaccinations AS INT)) OVER (PARTITION BY D.location ORDER BY D.location, D.date) AS CumulatedVaccination
FROM covid_data_deaths D
JOIN covid_data_vaccinations V
  ON D.location  = V.location
  and D.date = V.date
WHERE D.continent IS NOT NULL
ORDER BY 2,3
)
SELECT *, (CumulatedVaccination/population)*100
FROM PopVSVac

-- COMMAND ----------

-- Creating a TEMP TABLE

DROP TABLE IF EXISTS PercentPopulationVaccinated;
CREATE TABLE PercentPopulationVaccinated(
  continent STRING,
  location STRING,
  date STRING,
  population STRING,
  new_vaccinations STRING,
  CumulatedVaccination DOUBLE
) 
USING PARQUET;

INSERT INTO PercentPopulationVaccinated
SELECT 
  D.continent,
  D.location,
  D.date,
  D.population,
  V.new_vaccinations,
  SUM(CAST(V.new_vaccinations AS INT)) OVER (PARTITION BY D.location ORDER BY D.location, D.date) AS CumulatedVaccination
FROM covid_data_deaths D
JOIN covid_data_vaccinations V
  ON D.location  = V.location
  and D.date = V.date
WHERE D.continent IS NOT NULL
ORDER BY 2,3;

SELECT *, (CumulatedVaccination/population)*100
FROM PercentPopulationVaccinated;


-- COMMAND ----------

-- CREATING VIEW for data visualization

--DROP VIEW IF EXISTS PercentPopulationVaccinated;
CREATE TEMP VIEW PercentPopulationVaccinated AS
SELECT 
  D.continent,
  D.location,
  D.date,
  D.population,
  V.new_vaccinations,
  SUM(CAST(V.new_vaccinations AS INT)) OVER (PARTITION BY D.location ORDER BY D.location, D.date) AS CumulatedVaccination
FROM covid_data_deaths D
JOIN covid_data_vaccinations V
  ON D.location  = V.location
  and D.date = V.date
WHERE D.continent IS NOT NULL
;

-- COMMAND ----------

SELECT * FROM PercentPopulationVaccinated;

-- COMMAND ----------


