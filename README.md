# OpenWeather Data Engineering Pipeline

## Data Source
[OpenWeather](https://openweathermap.org/)

1. [Historical Weather Data](https://openweathermap.org/history)

Time Range: 02-2024 ~ 02-2025
Location: Boston


2. [Air Pollution Data](https://openweathermap.org/api/air-pollution)

Time Range: 02-2024 ~ 02-2025
Location: Boston
Frequency: Hourly 

## ADF

**Objective**:  Ingest weekly historical weather and air pollution data from the OpenWeather APIs into Azure Data Lake Storage using Azure Data Factory.

### Data Ingestion

Historical Air Pollution Data Ingestion Pipeline can be found [here](https://github.com/jh000107/OpenWeather/blob/master/data_factory/pipeline/Historical%20Air%20Pollution%20Ingest%20Pipeline.json)

Historical Weather Data Ingestion Pipeline can be found [here](https://github.com/jh000107/OpenWeather/blob/master/data_factory/pipeline/Historical%20Air%20Pollution%20Ingest%20Pipeline.json)

### Data Storage

## Synapse

### Data Analysis