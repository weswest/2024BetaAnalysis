#%%
# Imports

import requests
from collections import Counter
import pandas as pd
import time
import logging
import os

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

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
    

#%%
#1B Return all dates from FDIC Reporting

def get_all_report_dates():
    """
    Retrieves a list of all available reporting dates from the FDIC financials database, sorted with the latest dates first.
    """
    base_url = "https://banks.data.fdic.gov/api/financials"
    params = {
        "fields": "ID",
        "limit": 1000,
        "format": "json",
        "download": "false"
    }
    response = requests.get(base_url, params=params)
    if response.status_code == 200:
        data = response.json()
        if 'data' in data and len(data['data']) > 0:
            dates = list({entry['data']['ID'].split('_')[1] for entry in data['data']})
            sorted_dates = sorted(dates, reverse=True)
            return sorted_dates
        else:
            print("No data found.")
            return []
    else:
        print(f"Failed to retrieve data. Status code: {response.status_code}")
        return []
    

def get_certs_by_date(report_date, limit=10000):
    """
    Retrieves a list of all certificate IDs (Certs) for a given reporting date.
    
    Args:
    report_date (str): The reporting date in YYYYMMDD format.
    limit (int): The maximum number of records to retrieve per API request (default is 10000).
    
    Returns:
    list: A list of certificate IDs (Certs) for the given reporting date.
    """
    base_url = "https://banks.data.fdic.gov/api/financials"
    
    params = {
        "filters": f"REPDTE:{report_date}",
        "fields": "CERT",
        "limit": limit,
        "format": "json",
        "download": "false",
        "offset": 0
    }
    
    certs = []
    
    while True:
        try:
            response = requests.get(base_url, params=params)
            response.raise_for_status()
        except requests.RequestException as e:
            logger.error(f"Failed to retrieve data: {e}")
            break
        
        data = response.json()
        if 'data' in data and len(data['data']) > 0:
            certs.extend(entry['data']['CERT'] for entry in data['data'])
            params['offset'] += limit
        else:
            break
    
    return certs

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
        logger.info("Duplicate Certs found:")
        for cert, count in duplicates.items():
            logger.info(f"Cert ID: {cert}, Number of Entries: {count}")
    else:
        logger.info("No duplicate Certs found.")



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


#%%
# 2. Download Data

# 2. Download Data

if __name__ == "__main__":
    # Define Inputs
    fields = [
        # Existing fields with descriptions
        'DEPSMAMT',  # Small Time Deposits
        'DEPSMB',  # Money Market Deposit Accounts (MMDAs)
        'NTRCDSM',  # Negotiable Order of Withdrawal (NOW) Accounts
        'NTRTMMED',  # Time Deposits of $100,000 or More
        'NTRTMLGJ',  # Jumbo Certificates of Deposit
        'DEPLGAMT',  # Large Time Deposits
        'DEPDOM',  # Domestic Deposits
        'DEP',  # Total Deposits
        'DEPFOR',  # Foreign Deposits
        'DDT',  # Demand Deposits
        'NTRSMMDA',  # Savings Deposits
        'NTRSOTH',  # Other Savings Deposits
        'TS',  # Transaction Accounts
        'DEPNIDOM',  # Non-interest-bearing Domestic Deposits
        'DEPIDOM',  # Interest-bearing Domestic Deposits
        'COREDEP',  # Core Deposits
        'BRO',  # Brokered Deposits
        'DEPINS',  # Insured Deposits
        'EDEPDOM',  # Estimated Insured Deposits - Domestic
        'EINTEXP',  # Estimated Interest Expense
        'EDEPFOR',  # Estimated Insured Deposits - Foreign
        'INTINCY',  # Total Interest Income
        'INTEXPY',  # Total Interest Expense
        'NIMY',  # Net Interest Margin
        'LNLSNET',  # Net Loans and Leases

        # Additional fields for suggested metrics with descriptions
        'SC',  # Securities
        'ASSET',  # Total Assets
        'LNCON',  # Construction and Development Loans
        'LNRECON',  # Commercial Real Estate Loans
        'NONII',  # Non-interest Income
        'ROA',  # Return on Assets
        'ROE',  # Return on Equity
        'CAPRATE',  # Capital Adequacy Ratio
        'EFFRATIO',  # Efficiency Ratio
        'NPL',  # Non-performing Loans
        'T1CAPR',  # Tier 1 Capital Ratio
        'DIVPAYOUT',  # Dividend Payout Ratio
        'COF',  # Cost of Funds
        'NCO',  # Net Charge-offs
        'LIQRATIO',  # Liquidity Ratio
        'MKTDEP',  # Market Share of Total Deposits
        'IRR',  # Interest Rate Risk Sensitivity
        'OPINC',  # Operating Income
        'OPEXP',  # Operating Expenses
        'DEFLOAN',  # Defaulted Loans
        'SHORTDEBT',  # Short-term Debt
        'DEBT'  # Total Debt
    ]

    # Define the output directory
    output_dir = './data/raw/fdic'
    os.makedirs(output_dir, exist_ok=True)

    # Execute code
    report_dates = get_all_report_dates()
    for report_date in report_dates:
        filename = os.path.join(output_dir, f"{report_date}.csv")
        if os.path.isfile(filename):
            print(f"{filename} exists; skipping")
        else:
            print(f"{filename} doesn't exist. Time to download")
            certs = get_certs_by_date(report_date)
            df = build_dataframe_for_date(report_date, certs, fields)
            df.to_csv(filename, index=False)