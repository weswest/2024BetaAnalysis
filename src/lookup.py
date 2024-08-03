import pandas as pd
import argparse
from fuzzywuzzy import process
import os

# Get the directory of the current script
script_dir = os.path.dirname(os.path.abspath(__file__))

# Define the path to the CSV file relative to the script directory
CSV_FILE_PATH = os.path.join(script_dir, '../data/processed/institution_details.csv')

def load_data(file_path):
    """
    Load the data from the CSV file.
    
    Args:
    file_path (str): Path to the CSV file.
    
    Returns:
    pd.DataFrame: DataFrame containing the data.
    """
    return pd.read_csv(file_path)

def fuzzy_search(df, query, column='Institution_Name', threshold=80):
    """
    Perform a fuzzy search on the specified column.
    
    Args:
    df (pd.DataFrame): DataFrame containing the data.
    query (str): The search string.
    column (str): The column to search on.
    threshold (int): The matching threshold (0-100).
    
    Returns:
    pd.DataFrame: DataFrame containing the matched rows.
    """
    choices = df[column].tolist()
    matches = process.extract(query, choices, limit=10)
    matches = [match for match in matches if match[1] >= threshold]
    matched_values = [match[0] for match in matches]
    return df[df[column].isin(matched_values)]

def main():
    parser = argparse.ArgumentParser(description="Fuzzy search for institution names.")
    parser.add_argument('query', type=str, help='The search string')
    args = parser.parse_args()

    # Load data
    df = load_data(CSV_FILE_PATH)

    # Perform fuzzy search
    results = fuzzy_search(df, args.query)

    # Print results
    if results.empty:
        print(f"No matches found for '{args.query}'.")
    else:
        print(f"Matches found for '{args.query}':\n")
        print(results.to_string(index=False))

if __name__ == "__main__":
    main()
