# Carbon Black API Data Fetcher

This repository contains scripts designed to fetch data from the Carbon Black API. The data pulled includes various types of observations, audit logs, device details, and vulnerability assessments. 
Each script is tailored to extract specific data and process it into a structured format such as a DataFrame, which can then be saved to a SQL database or an Excel file.

## Prerequisites

- Python 3.6 or higher
- Required libraries:
  - `requests`
  - `pandas`
  - `sqlalchemy`
  - `json`
  - `datetime`
  - `time`

You can install the required libraries using pip:
```bash
pip install requests pandas sqlalchemy
