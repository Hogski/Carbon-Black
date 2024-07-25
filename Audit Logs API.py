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
search_url = f"{cbc_hostname}/audit_log/v1/orgs/{org_key}/logs/_search"

# Set up the headers
headers = {
    "X-AUTH-TOKEN": f"{api_secret}/{api_id}",
    "Content-Type": "application/json"
}

# Define the time range for the last 180 days
end_date = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
start_date = end_date - timedelta(days=180)

current_date = end_date

all_audit_logs = []

while current_date > start_date:
    # Calculate the start and end dates for the range
    range_end = current_date
    range_start = current_date - timedelta(days=1)
    
    if range_start < start_date:
        range_start = start_date

    # Convert to ISO format
    time_range = {
        "start": range_start.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "end": range_end.strftime("%Y-%m-%dT%H:%M:%SZ")
    }

    # Parameters for the request
    params = {
        "criteria": {
            "create_time": {
                "start": time_range["start"],
                "end": time_range["end"]
            }
        },
        "rows": 10000,  # Maximum number of rows per request
        "start": 0,  # Start from the first result
        "sort": [{
            "field": "create_time",
            "order": "asc"
        }]
    }

    # Make the API call
    response = requests.post(search_url, headers=headers, json=params)

    # Debugging info
    print(f"Requesting URL: {search_url}")
    print(f"Params sent: {json.dumps(params)}")

    if response.status_code == 200:
        try:
            audit_logs = response.json().get("results", [])
            all_audit_logs.extend(audit_logs)
            print(f"Fetched {len(audit_logs)} audit logs from {range_start.strftime('%Y-%m-%d %H:%M:%S')} to {range_end.strftime('%Y-%m-%d %H:%M:%S')}.")
        except requests.exceptions.JSONDecodeError:
            print("Error decoding JSON.")
    else:
        print(f"Error: {response.status_code} - {response.text}")

    # Move to the next 6-hour period
    current_date = range_start
    # To avoid rate-limiting issues, consider adding a sleep time between requests
    # time.sleep(1)  # Uncomment this line if necessary

# Convert to DataFrame if there are results
if all_audit_logs:
    audit_logs_df = pd.DataFrame(all_audit_logs)
else:
    print("No audit logs fetched.")
    audit_logs_df = pd.DataFrame()

'''
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
if not audit_logs_df.empty:
    load_to_sql(audit_logs_df, "Audit_Log_Data")

'''
# Optionally, save to Excel
audit_logs_df.to_excel("C:/path/to/your/directory/audit_Logs_Data.xlsx", index=False)
