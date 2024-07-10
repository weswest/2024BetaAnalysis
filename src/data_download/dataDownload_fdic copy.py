#%%
# Imports

import requests
from collections import Counter
import pandas as pd
import time
import os

#%%
# 1A: Test function returning specific element per date-institution

def get_financial_field_value(report_date, cert_id, field_name):
    """
    Fetches the value of a specified financial field for a given reporting date and certificate ID.
    
    Args:
    report_date (str): The reporting date in YYYYMMDD format.
    cert_id (int): The certificate ID of the institution.
    field_name (str): The name of the financial field to retrieve.
    
    Returns:
    The value of the specified financial field if found, otherwise None.
    """
    # Define the base URL
    base_url = "https://banks.data.fdic.gov/api/financials"
    
    # Define the parameters
    params = {
        "filters": f"CERT:{cert_id} AND REPDTE:{report_date}",
        "fields": f"CERT,REPDTE,{field_name}",
        "limit": 1,
        "format": "json",
        "download": "false"
    }
    
    # Make the API request
    response = requests.get(base_url, params=params)
    
    # Check if the request was successful
    if response.status_code == 200:
        data = response.json()
        if 'data' in data and len(data['data']) > 0:
            field_value = data['data'][0]['data'].get(field_name, None)
            return field_value
        else:
            print("No data found for the specified criteria.")
            return None
    else:
        print(f"Failed to retrieve data. Status code: {response.status_code}")
        return None

run_test_1a = True
if run_test_1a:
    # Test the function with the report date 20231231, cert ID 628, and field name DEPDOM
    test_date = "20231231"
    test_cert_id = 628
    test_field_name = "DEPDOM"
    field_value = get_financial_field_value(test_date, test_cert_id, test_field_name)
    print(f"Value for field '{test_field_name}' on {test_date} for cert ID {test_cert_id}: {field_value}")


    # Compare these results against the reference file downloaded
    # File path to the reference CSV file
    ref_file_path = "../00Ref/628_Financials_L2_5_22_2024.csv"

    # Read the CSV file, skipping the initial metadata rows
    ref_df = pd.read_csv(ref_file_path, skiprows=6, header=None)

    # Extracting the data from columns C and D (0-indexed, C is 2, D is 3)
    ref_data = ref_df.iloc[0:26, 2:4]  # Adjust the range as necessary

    # Creating a dictionary from the DataFrame
    ref_jpmc_2023Q4_dict = dict(zip(ref_data.iloc[:, 0], ref_data.iloc[:, 1]))

    # Print the value stored in "DEPDOM"
    print("From ref file, DEPDOM on 20231231 for cert ID 628 (in $000s):", ref_jpmc_2023Q4_dict["DEPDOM"])

#%%
#1B Return all dates from FDIC Reporting

def get_all_report_dates():
    """
    Retrieves a list of all available reporting dates from the FDIC financials database, sorted with the latest dates first.
    
    Returns:
    list: A sorted list of available reporting dates (latest first).
    """
    # Define the base URL
    base_url = "https://banks.data.fdic.gov/api/financials"
    
    # Define the parameters
    params = {
        "fields": "ID",
        "limit": 1000,  # Adjust as necessary to get more dates
        "format": "json",
        "download": "false"
    }
    
    # Make the API request
    response = requests.get(base_url, params=params)
    
    # Check if the request was successful
    if response.status_code == 200:
        data = response.json()
        if 'data' in data and len(data['data']) > 0:
            # Extract and sort the dates
            dates = list({entry['data']['ID'].split('_')[1] for entry in data['data']})
            sorted_dates = sorted(dates, reverse=True)
            return sorted_dates
        else:
            print("No data found.")
            return []
    else:
        print(f"Failed to retrieve data. Status code: {response.status_code}")
        return []

run_test_1b = False
if run_test_1b:
    # Test the function
    report_dates = get_all_report_dates()
    print(f"All available report dates (latest first): {report_dates}")

#%%
# 1C. Return all certs reporting on a certain date

def get_certs_by_date(report_date):
    """
    Retrieves a list of all certificate IDs (Certs) for a given reporting date.
    
    Args:
    report_date (str): The reporting date in YYYYMMDD format.
    
    Returns:
    list: A list of certificate IDs (Certs) for the given reporting date.
    """
    # Define the base URL
    base_url = "https://banks.data.fdic.gov/api/financials"
    
    # Initialize parameters
    params = {
        "filters": f"REPDTE:{report_date}",
        "fields": "CERT",
        "limit": 10000,  # Maximum limit for FDIC system
        "format": "json",
        "download": "false",
        "offset": 0  # Start with offset 0
    }
    
    certs = []
    more_data = True
    
    # Loop to handle pagination and retrieve all data
    while more_data:
        response = requests.get(base_url, params=params)
        if response.status_code == 200:
            data = response.json()
            if 'data' in data and len(data['data']) > 0:
                certs.extend([entry['data']['CERT'] for entry in data['data']])
                params['offset'] += 10000  # Increment offset to get next batch
            else:
                more_data = False  # No more data available
        else:
            print(f"Failed to retrieve data. Status code: {response.status_code}")
            more_data = False  # Exit loop if request fails
    
    return certs

# Helper function to find and print duplicate certs
def find_duplicate_certs(certs):
    """
    Finds and prints certificate IDs (Certs) that appear more than once in the dataset.
    
    Args:
    certs (list): A list of certificate IDs (Certs).
    
    Returns:
    None
    """
    
    cert_counts = Counter(certs)
    duplicates = {cert: count for cert, count in cert_counts.items() if count > 1}
    
    if duplicates:
        print("Duplicate Certs found:")
        for cert, count in duplicates.items():
            print(f"Cert ID: {cert}, Number of Entries: {count}")
    else:
        print("No duplicate Certs found.")

run_test_1c = False
if run_test_1c:
    # Test the function with the report date 19901231
    test_date = "19901231"
    certs = get_certs_by_date(test_date)
    print(f"Number of Certs for report date {test_date}: {len(certs)}")

    # Find and print duplicate certs
    find_duplicate_certs(certs)

#%%
# 1D. Download data for all certs on a given date

def build_dataframe_for_date(report_date, cert_ids, fields):
    """
    Builds a DataFrame of data elements for each certificate ID on a given date.
    
    Args:
    report_date (str): The reporting date in YYYYMMDD format.
    cert_ids (list): A list of certificate IDs (Certs) to include.
    fields (list): A list of field names to retrieve for each certificate ID.
    
    Returns:
    pd.DataFrame: A DataFrame with columns ['Date', 'Cert', 'Field', 'Value'].
    """
    # Define the base URL
    base_url = "https://banks.data.fdic.gov/api/financials"
    
    # Initialize an empty list to store the rows
    rows = []
    batch_size = 100  # Set batch size
    total_batches = (len(cert_ids) + batch_size - 1) // batch_size
    
    # Track time
    start_time = time.time()
    
    # Process cert_ids in batches
    for i in range(0, len(cert_ids), batch_size):
        batch_certs = cert_ids[i:i + batch_size]
        batch_filter = " OR ".join([f"CERT:{cert}" for cert in batch_certs])
        
        # Define the parameters for the API request
        params = {
            "filters": f"({batch_filter}) AND REPDTE:{report_date}",
            "fields": ",".join(["CERT", "REPDTE"] + fields),
            "limit": batch_size,
            "format": "json",
            "download": "false"
        }
        
        # Make the API request
        response = requests.get(base_url, params=params)
        
        # Check if the request was successful
        if response.status_code == 200:
            data = response.json()
            if 'data' in data and len(data['data']) > 0:
                for entry in data['data']:
                    cert_data = entry['data']
                    for field in fields:
                        if field in cert_data:
                            rows.append({
                                "Date": report_date,
                                "Cert": cert_data["CERT"],
                                "Field": field,
                                "Value": cert_data[field]
                            })
        else:
            print(f"Failed to retrieve data for certs in batch {i//batch_size + 1}. Status code: {response.status_code}")
        
        # Print progress update every 10 batches
        if (i // batch_size + 1) % 10 == 0 or (i // batch_size + 1) == total_batches:
            elapsed_time = time.time() - start_time
            estimated_total_time = (elapsed_time / (i // batch_size + 1)) * total_batches
            estimated_remaining_time = estimated_total_time - elapsed_time
            print(f"{i // batch_size + 1} out of {total_batches} batches running. Seconds run: {elapsed_time:.2f}; Estimated seconds remaining: {estimated_remaining_time:.2f}")
    
    # Create a DataFrame from the collected rows
    df = pd.DataFrame(rows, columns=['Date', 'Cert', 'Field', 'Value'])
    return df

run_test_1d = True
if run_test_1d:
    # Test the function with report date 19901231, and fields DEP and DEPDOM
    test_date = "20221231"
#    test_certs = get_certs_by_date(test_date)
#    test_fields = ["DEP", "DEPDOM"]
    test_certs = [14, 628]
    test_fields = ['DEPSMAMT', 'DEPSMB', 'NTRCDSM', 'NTRTMMED', 'NTRTMLGJ', 'DEPLGAMT', 'DEPDOM', 
                   'DEP', 'DEPFOR', 'DDT', 'NTRSMMDA', 'NTRSOTH', 'TS', 'DEPNIDOM', 'DEPIDOM', 'COREDEP', 
                   'BRO', 'DEPINS', 'EDEPDOM', 'EINTEXP', 'EDEPFOR', 'INTINCY', 'INTEXPY', 'NIMY', 'LNLSNET']


    df = build_dataframe_for_date(test_date, test_certs, test_fields)
    print(df)
    print(len(df))
#%%
# 2. Download Data

# Define Inputs
fields = ['DEPSMAMT', 'DEPSMB', 'NTRCDSM', 'NTRTMMED', 'NTRTMLGJ', 'DEPLGAMT', 'DEPDOM', 
            'DEP', 'DEPFOR', 'DDT', 'NTRSMMDA', 'NTRSOTH', 'TS', 'DEPNIDOM', 'DEPIDOM', 'COREDEP', 
            'BRO', 'DEPINS', 'EDEPDOM', 'EINTEXP', 'EDEPFOR', 'INTINCY', 'INTEXPY', 'NIMY', 'LNLSNET']

# 

# Execute code
# report_dates = get_all_report_dates()
report_dates = get_all_report_dates()
for report_date in report_dates:
    filename = f"{report_date}.csv"
    if os.path.isfile(filename):
        print(f"{filename} exists; skipping")
    else:
        print(f"{filename} doesn't exist. Time to download")   
        certs =  get_certs_by_date(report_date)
        df = build_dataframe_for_date(report_date, certs, fields)
        df.to_csv(f"{report_date}.csv", index=False)