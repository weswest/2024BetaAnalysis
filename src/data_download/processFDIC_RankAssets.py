import os
import pandas as pd
from collections import defaultdict
import time

# Import functions from dataDownload_fdic.py
from dataDownload_fdic import get_all_report_dates, get_certs_by_date, build_dataframe_for_date

# Define paths
FDIC_DATA_PATH = './data/raw/fdic'
PROCESSED_DATA_PATH = './data/processed'
os.makedirs(PROCESSED_DATA_PATH, exist_ok=True)

def get_best_ranks(fdic_data_path):
    """
    Iterate through each CSV file in the fdic data directory and calculate the best rank for each Cert.
    
    Args:
    fdic_data_path (str): Path to the directory containing FDIC data CSV files.
    
    Returns:
    pd.DataFrame: DataFrame with Cert, Best_Asset_Rank, Asset_Value, Filename, and Institution_Name.
    """
    # Initialize a dictionary to store the best rank for each Cert
    best_ranks = defaultdict(lambda: (float('inf'), None, None, None))

    # Get the list of all CSV files
    files = [file for file in os.listdir(fdic_data_path) if file.endswith('.csv')]
    total_files = len(files)

    start_time = time.time()

    # Iterate through each CSV file in the fdic data directory
    for i, file_name in enumerate(files, start=1):
        print(f"Analyzing file {i} out of {total_files}; expected time remaining: {int((time.time() - start_time) / i * (total_files - i))} seconds...")
        file_path = os.path.join(fdic_data_path, file_name)
        df = pd.read_csv(file_path)

        # Filter for rows where Field is ASSET and create a copy
        asset_data = df.loc[df['Field'] == 'ASSET'].copy()

        # Rank Certs based on the Value column in descending order
        asset_data['Rank'] = asset_data['Value'].rank(method='min', ascending=False)

        # Update the best rank for each Cert
        for _, row in asset_data.iterrows():
            cert = row['Cert']
            rank = row['Rank']
            value = row['Value']
            if rank < best_ranks[cert][0]:
                best_ranks[cert] = (rank, value, file_name, None)

    # Convert the best ranks dictionary to a DataFrame
    best_ranks_df = pd.DataFrame.from_dict(best_ranks, orient='index', columns=['Best_Asset_Rank', 'Asset_Value', 'Filename', 'Institution_Name']).reset_index()
    best_ranks_df.rename(columns={'index': 'Cert'}, inplace=True)

    return best_ranks_df

def update_institution_names(best_ranks_df):
    """
    Update the institution names in the best_ranks DataFrame.
    
    Args:
    best_ranks_df (pd.DataFrame): DataFrame with Cert, Best_Asset_Rank, Asset_Value, Filename, and Institution_Name.
    
    Returns:
    pd.DataFrame: Updated DataFrame with institution names filled in.
    """
    report_dates = get_all_report_dates()
    
    for i in range(0, len(report_dates), 4):  # Iterate over dates, skipping 4 each time (~1 year)
        report_date = report_dates[i]
        print(f"Processing report date: {report_date}")

        certs = get_certs_by_date(report_date)
        certs_to_name = best_ranks_df[best_ranks_df['Institution_Name'].isnull() & best_ranks_df['Cert'].isin(certs)]['Cert'].tolist()

        if not certs_to_name:
            continue

        institution_data = build_dataframe_for_date(report_date, certs_to_name, ['NAME'])  # Assuming 'NAME' is the field for institution name
        for _, row in institution_data.iterrows():
            cert = row['Cert']
            name = row['Value']
            best_ranks_df.loc[best_ranks_df['Cert'] == cert, 'Institution_Name'] = name

    return best_ranks_df

def save_best_ranks(best_ranks_df, output_file_path):
    """
    Save the best ranks DataFrame to a CSV file.
    
    Args:
    best_ranks_df (pd.DataFrame): DataFrame with Cert, Best_Asset_Rank, Asset_Value, Filename, and Institution_Name.
    output_file_path (str): Path to save the output CSV file.
    """
    best_ranks_df.to_csv(output_file_path, index=False)
    print(f"Best asset ranks with institution names saved to {output_file_path}")

if __name__ == "__main__":
    # Calculate the best ranks for each Cert
    best_ranks_df = get_best_ranks(FDIC_DATA_PATH)

    # Update institution names
    best_ranks_df = update_institution_names(best_ranks_df)

    # Define the output file path
    output_file_path = os.path.join(PROCESSED_DATA_PATH, 'institution_details.csv')

    # Save the best ranks to a CSV file
    save_best_ranks(best_ranks_df, output_file_path)
