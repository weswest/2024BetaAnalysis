import pandas as pd

# Define paths
PROCESSED_DATA_PATH = './data/processed/bank_data_rank200_unsorted.csv'
OUTPUT_SORTED_PATH = './data/processed/bank_data_rank200.csv'

def calculate_deposit_expense(df):
    """
    Calculate deposit expense as annualized_EDEPDOM / DEPDOM.
    
    Args:
    df (pd.DataFrame): DataFrame containing the data.
    
    Returns:
    pd.DataFrame: DataFrame with the deposit expense calculated.
    """
    df['deposit_expense'] = df.apply(
        lambda row: row['annualized_EDEPDOM'] / row['DEPDOM'] if pd.notnull(row['annualized_EDEPDOM']) and pd.notnull(row['DEPDOM']) and row['DEPDOM'] != 0 else None,
        axis=1
    )
    return df

def main():
    # Load the processed data
    df = pd.read_csv(PROCESSED_DATA_PATH)

    # Calculate deposit expense
    df = calculate_deposit_expense(df)

    # Sort by cert and date
    df = df.sort_values(by=['cert', 'date'])

    # Save the sorted DataFrame
    df.to_csv(OUTPUT_SORTED_PATH, index=False)
    print(f"Sorted data with deposit expense saved to {OUTPUT_SORTED_PATH}")

if __name__ == "__main__":
    main()
