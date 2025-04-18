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

**Objective**: Ingest weekly historical weather and air pollution data from the OpenWeather APIs into Azure Data Lake Storage using Azure Data Factory.

Historical Air Pollution Data Ingestion Pipeline can be found [here](https://github.com/jh000107/OpenWeather/blob/master/data_factory/pipeline/Historical%20Air%20Pollution%20Ingest%20Pipeline.json)

Historical Weather Data Ingestion Pipeline can be found [here](https://github.com/jh000107/OpenWeather/blob/master/data_factory/pipeline/Historical%20Air%20Pollution%20Ingest%20Pipeline.json)

## Synapse

**Objective**: Using the notebook environment in Synapse Analytics Serverless Spark Cluster, connect to storage and perform an exploratory data analysis (EDA) on the historical weather and air pollution data.

EDA Notebook can be found [here](https://github.com/jh000107/OpenWeather/blob/master/synapse/notebook/OpenWeather%20EDA.json)

## Azure Function + Event Hub

**Objective**: Ingest real-time weather and air pollution data from the OpenWeather APIs into Azure Event Hubs using Azure Functions. This involves setting up an Event Hubs namespace and event hub, creating an Azure Function to fetch real-time data and send it to Event Hubs, and verifying the data ingestion process.

Function script can be found [here](https://github.com/jh000107/OpenWeather/blob/master/azure_function/function_app.py)

## Stream Analytics

**Objective**: Set up a real-time data processing pipeline using Azure Stream Analytics to store real-time weather and air pollution data from Azure Event Hubs. The goal is to have the data will be processed on the fly and sent into PowerBI for a live dashboard.

## Data Flow

**Objective**: Clean and preprocess the historical/stream data ingested from the OpenWeather APIs using Azure Data Factory. This involves handling missing values, correcting data types, and transforming the data to make it suitable for analysis. The processed data will be stored in the Silver layer of Azure Data Lake Storage.

Historical Air Pollution Data Flow can be found [here](https://github.com/jh000107/OpenWeather/blob/master/data_factory/dataflow/PreprocessAirPollutionData.json).

Historical Weather Data Flow can be found [here](https://github.com/jh000107/OpenWeather/blob/master/data_factory/dataflow/PreprocessWeatherData.json).

## Batch Processing

**Objective**: Perform batch processing on the preprocessed data to handle large-scale data transformations and aggregations using Azure Synapse with PySpark. The processed data will be stored in the Gold layer of Azure Data Lake Storage.

Source to be added

## Data Loading

**Objective**: Move the processed data from the Gold layer in Azure Data Lake Storage (ADLS) to the data warehouse using Azure Synapse Analytics. Specifically, we are ingesting data from our storage account first into external tables, then branching off those external tables to create internal synapse dimension, fact, and aggregated tables.

## Data Visualization with Power BI

**Objective**: Create interactive visualizations to analyze the processed data using Power BI connected to Azure Synapse Analytics. This involves setting up the connection between Power BI and Synapse, creating dashboards, and generating insights from the visualized historical data.