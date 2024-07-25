import requests
import pandas as pd
import sqlalchemy as sa
from datetime import datetime, timedelta
import json

# Replace these variables with your actual values
cbc_hostname = "https://your-hostname.com"
org_key = "your_org_key"
api_secret = "your_api_secret"
api_id = "your_api_id"

# Construct the search endpoint URL
search_url = f"{cbc_hostname}/appservices/v6/orgs/{org_key}/devices/_search"

# Set up the headers
headers = {
    "X-AUTH-TOKEN": f"{api_secret}/{api_id}",
    "Content-Type": "application/json"
}

# Define the time range for the previous day from 1am to 1am
end_date = datetime.now().replace(hour=1, minute=0, second=0, microsecond=0)
start_date = end_date - timedelta(days=1)

# Convert to ISO format
time_range = {
    "start": start_date.strftime("%Y-%m-%dT%H:%M:%SZ"),
    "end": end_date.strftime("%Y-%m-%dT%H:%M:%SZ")
}

# Parameters for the request
params = {
    "time_range": time_range,
    "start": 0,
    "rows": 10000
}

# Make the API call
response = requests.post(search_url, headers=headers, json=params)

# Debugging info
print(f"Requesting URL: {search_url}")
print(f"Params sent: {params}")

all_devices = []
if response.status_code == 200:
    try:
        devices = response.json().get("results", [])
        all_devices.extend(devices)
        print(f"Fetched {len(devices)} devices from {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}.")
    except requests.exceptions.JSONDecodeError:
        print("Error decoding JSON.")
else:
    print(f"Error: {response.status_code} - {response.text}")

# Convert to DataFrame if there are results
if all_devices:
    devices_df = pd.DataFrame(all_devices)
else:
    print("No devices fetched.")

def load_to_sql(df, table_name):
    MAX_VARCHAR_LENGTH = 255

    # Convert any dictionary or list columns to JSON strings
    for col in df.columns:
        if df[col].dtype == 'object':
            if df[col].apply(lambda x: isinstance(x, (dict, list))).any():
                df[col] = df[col].apply(lambda x: json.dumps(x) if isinstance(x, (dict, list)) else x)

    # Truncate string columns to the max length
    for col in df.select_dtypes(include=['object']).columns:
        df[col] = df[col].apply(lambda x: x[:MAX_VARCHAR_LENGTH] if isinstance(x, str) else x)

    # Define data types for each column
    dtype = {col: sa.types.String(length=MAX_VARCHAR_LENGTH) for col in df.columns}

    connection_string = (
        "Driver={ODBC Driver 17 for SQL Server};"
        "Server=your_server;"
        "Database=your_database;"
        "Integrated Security=SSPI;"
        "port=1433;"
        "Trusted_Connection=yes;"
    )

    connection_url = sa.engine.URL.create("mssql+pyodbc", query=dict(odbc_connect=connection_string))
    engine = sa.create_engine(connection_url, fast_executemany=True)

    try:
        df.to_sql(table_name, engine, if_exists="replace", index=False, dtype=dtype, schema="your_schema")
        print("Data loaded to SQL successfully.")
    except Exception as e:
        print(f"Error loading data to SQL: {e}")

# Load data to SQL
if not devices_df.empty:
    load_to_sql(devices_df, "Devices_Data")
