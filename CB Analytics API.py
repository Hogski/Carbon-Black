import requests
import pandas as pd
from datetime import datetime, timedelta
import time
import sqlalchemy as sa
import json

# Replace these variables with your actual values
cbc_hostname = "https://your-hostname.com"
org_key = "your_org_key"
api_secret = "your_api_secret"
api_id = "your_api_id"

# Construct the search endpoint URL
search_url = f"{cbc_hostname}/api/investigate/v2/orgs/{org_key}/observations/search_jobs"

# Set up the headers
headers = {
    "X-AUTH-TOKEN": f"{api_secret}/{api_id}",
    "Content-Type": "application/json"
}

def start_search_job(params):
    response = requests.post(search_url, headers=headers, json=params)
    if response.status_code == 200:
        try:
            return response.json().get("job_id")
        except requests.exceptions.JSONDecodeError:
            print("Error decoding JSON.")
    else:
        print(f"Error: {response.status_code} - {response.text}")
    return None

def get_results(job_id):
    results_url = f"{cbc_hostname}/api/investigate/v2/orgs/{org_key}/observations/search_jobs/{job_id}/results"
    all_observations = []
    start_time = time.time()
    timeout = 180  # 3 minutes

    while True:
        # Check job status
        response = requests.get(f"{results_url}?start=0&rows=0", headers=headers)
        data = response.json()

        # If job is complete, check if there are any results
        if data['contacted'] == data['completed']:
            if data['num_found'] == 0:
                print(f"No results found for job {job_id}.")
                return all_observations

            response = requests.get(f"{results_url}?start=0&rows=10000", headers=headers)
            if response.status_code == 200:
                try:
                    observations = response.json().get("results", [])
                    all_observations.extend(observations)
                    if not observations:
                        print(f"No observations found in the results for job {job_id}.")
                    break
                except requests.exceptions.JSONDecodeError:
                    print("Error decoding JSON.")
            else:
                print(f"Error fetching results for job {job_id}: {response.status_code} - {response.text}")
                break

        # Timeout check
        if time.time() - start_time > timeout:
            raise TimeoutError(f"Search job {job_id} did not complete within the expected time frame.")
        
        time.sleep(0.5)
    
    return all_observations

# Set the time range for the previous day and break it into 2-hour chunks
all_observations = []
end_date = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
start_date = end_date - timedelta(days=1)

# Loop through each 2-hour period of the previous day
for i in range(0, 24, 2):
    period_start = start_date + timedelta(hours=i)
    period_end = period_start + timedelta(hours=2)
    query = f"device_timestamp:[{period_start.strftime('%Y-%m-%dT%H:%M:%SZ')} TO {period_end.strftime('%Y-%m-%dT%H:%M:%SZ')}] AND (observation_type:CB_ANALYTICS)"
    
    params = {
        "query": query,
        "rows": 10000,
        "start": 0
    }

    while True:
        job_id = start_search_job(params)
        if job_id:
            response = requests.get(f"{cbc_hostname}/api/investigate/v2/orgs/{org_key}/observations/search_jobs/{job_id}/results?start=0&rows=0", headers=headers)
            if response.status_code == 200:
                data = response.json()
                if data['num_found'] > 0:
                    observations = get_results(job_id)
                    all_observations.extend(observations)
                    print(f"Fetched {len(observations)} observations for period starting {period_start.strftime('%Y-%m-%d %H:%M')}")
                else:
                    print(f"No data for job {job_id} from {period_start.strftime('%Y-%m-%d %H:%M')} to {period_end.strftime('%Y-%m-%d %H:%M')}, moving to next period.")
                break
            else:
                print(f"Error fetching initial results for job {job_id}: {response.status_code} - {response.text}")
        else:
            print(f"Failed to start job for {period_start.strftime('%Y-%m-%d %H:%M')}, retrying.")

# Convert to DataFrame if there are results
if all_observations:
    observations_df = pd.DataFrame(all_observations)
    print(f"Fetched a total of {len(all_observations)} observations.")
else:
    print("No observations fetched.")
    observations_df = pd.DataFrame()

# Save to Excel
# observations_df.to_excel("C:/path/to/your/directory/CB_Analytics_Observations.xlsx", index=False)

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
if not observations_df.empty:
    load_to_sql(observations_df, "CB_Analytics_Observations_Data")
