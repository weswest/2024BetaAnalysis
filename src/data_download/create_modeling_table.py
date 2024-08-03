import os
import pandas as pd
import time

FDIC_DATA_PATH = './data/raw/fdic'
FRED_DATA_PATH = './data/raw/rates/fred_data.csv'
BEST_RANKS_PATH = './data/processed/institution_details.csv'
OUTPUT_PATH_TEMPLATE = './data/processed/bank_data_rank{}.csv'

def process_fdic_data(fdic_data_path, annualize_fields, non_annualize_fields, start_year):
    """
    Process FDIC CSV files to aggregate specified fields.
    
    Args:
    fdic_data_path (str): Path to the directory containing FDIC data CSV files.
    annualize_fields (list): List of fields that need to be annualized.
    non_annualize_fields (list): List of fields that don't need annualization.
    start_year (int): The starting year to process files from.
    
    Returns:
    pd.DataFrame: DataFrame with date, cert, and specified fields.
    """
    records = []

    files = [f for f in os.listdir(fdic_data_path) if f.endswith('.csv') and int(f[:4]) >= start_year]
    files.sort()  # Ensure files are processed in chronological order
    total_files = len(files)
    start_time = time.time()

    for i, file_name in enumerate(files, start=1):
        if i < 6 or i % 15 == 0 or i == total_files:
            elapsed_time = time.time() - start_time
            average_time_per_file = elapsed_time / i
            remaining_files = total_files - i
            estimated_time_remaining = average_time_per_file * remaining_files
            print(f"Processing file {i} out of {total_files}; elapsed time: {elapsed_time:.2f}s; expected time remaining: {estimated_time_remaining:.2f}s")

        file_path = os.path.join(fdic_data_path, file_name)
        df = pd.read_csv(file_path)
        date = file_name.split('.')[0]

        for cert in df['Cert'].unique():
            cert_data = df[df['Cert'] == cert]
            record = {'date': date, 'cert': cert}

            for field in annualize_fields + non_annualize_fields:
                if field in annualize_fields:
                    value = cert_data.loc[cert_data['Field'] == field, 'Value'].sum()
                    record[f'raw_{field}'] = value
                else:
                    value = cert_data.loc[cert_data['Field'] == field, 'Value'].sum()
                    record[field] = value

            records.append(record)

    fdic_aggregated_df = pd.DataFrame(records)
    return fdic_aggregated_df

def annualize_ytd_fields(df, annualize_fields):
    """
    Annualize a list of year-to-date fields, adjusting values for non-March quarters.
    
    Args:
    df (pd.DataFrame): DataFrame containing the data.
    annualize_fields (list): List of field names to annualize.
    
    Returns:
    pd.DataFrame: DataFrame with additional columns for the annualized fields.
    """
    for field in annualize_fields:
        annualized_field_name = f'annualized_{field.split("_")[-1]}'
        df[annualized_field_name] = None

        # Group by cert to process each bank's data separately
        for cert, group in df.groupby('cert'):
            group = group.sort_values(by='date')
            annualized_values = []
            prev_value = None

            for date, value in zip(group['date'], group[f'raw_{field}']):
                if pd.to_datetime(date).month == 3:
                    annualized_values.append(value * 4)
                else:
                    if prev_value is not None:
                        annualized_values.append((value - prev_value) * 4)
                    else:
                        annualized_values.append(value * 4)  # Default case for the first quarter
                prev_value = value

            df.loc[group.index, annualized_field_name] = annualized_values

    return df

def merge_with_fred_data(fdic_df, fred_data_path, fred_fields):
    """
    Merge FDIC aggregated data with FRED data.
    
    Args:
    fdic_df (pd.DataFrame): DataFrame with FDIC aggregated data.
    fred_data_path (str): Path to the FRED data CSV file.
    fred_fields (list): List of FRED fields to merge.
    
    Returns:
    pd.DataFrame: Merged DataFrame.
    """
    # Read the FRED data without specifying column names
    fred_df = pd.read_csv(fred_data_path)

    # Rename the first column to 'date'
    fred_df.rename(columns={fred_df.columns[0]: 'date'}, inplace=True)

    # Set 'date' as index and parse dates
    fred_df['date'] = pd.to_datetime(fred_df['date'])
    fred_df.set_index('date', inplace=True)

    # Divide values by 100
    fred_df[fred_fields] = fred_df[fred_fields] / 100

    # Forward fill each FRED field separately
    for field in fred_fields:
        fred_df[field] = fred_df[field].ffill()

    # Trim precision to 6 decimal places
    for field in fred_fields:
        fred_df[field] = fred_df[field].round(6)

    # Convert FDIC date to datetime
    fdic_df['date'] = pd.to_datetime(fdic_df['date'], format='%Y%m%d')

    # Merge the data, forward filling missing ff_e values
    merged_df = pd.merge_asof(fdic_df.sort_values('date'), fred_df[fred_fields].sort_index(), left_on='date', right_index=True, direction='backward')

    return merged_df

def calculate_percentage(df, numerator, denominator, new_column):
    if numerator in df.columns and denominator in df.columns:
        df[new_column] = df.apply(
            lambda row: row[numerator] / row[denominator] if pd.notnull(row[numerator]) and pd.notnull(row[denominator]) and row[denominator] != 0 else None, axis=1
        )

def process_and_merge_data(fdic_data_path, fred_data_path, best_ranks_path, output_path_template, annualize_fields, non_annualize_fields, fred_fields, rank_threshold, start_year):
    # Process FDIC data
    fdic_df = process_fdic_data(fdic_data_path, annualize_fields, non_annualize_fields, start_year)

    # Load best asset ranks
    best_ranks_df = pd.read_csv(best_ranks_path)
    
    # Filter institutions based on rank threshold
    high_rank_certs = best_ranks_df[best_ranks_df['Best_Asset_Rank'] <= rank_threshold]['Cert'].tolist()
    
    # Separate high rank and low rank data
    high_rank_df = fdic_df[fdic_df['cert'].isin(high_rank_certs)]
    low_rank_df = fdic_df[~fdic_df['cert'].isin(high_rank_certs)]
    
    # Aggregate low rank data
    low_rank_aggregated_df = low_rank_df.groupby('date').sum().reset_index()
    low_rank_aggregated_df['cert'] = 'Aggregated_Small_Banks'
    
    # Combine high rank and low rank data
    combined_df = pd.concat([high_rank_df, low_rank_aggregated_df], ignore_index=True)
    
    # Annualize the specified fields
    combined_df = annualize_ytd_fields(combined_df, annualize_fields)
    
    # Calculate additional fields
    calculate_percentage(combined_df, 'DEPINS', 'DEPDOM', 'insured_deposit_percentage')
    calculate_percentage(combined_df, 'DEPNIDOM', 'DEPDOM', 'nib_deposit_percentage')
    calculate_percentage(combined_df, 'BRO', 'DEPDOM', 'brokered_deposit_percentage')
    calculate_percentage(combined_df, 'SC', 'ASSET', 'securities_asset_percentage')
    calculate_percentage(combined_df, 'annualized_NONII', 'annualized_INCY', 'nonii_revenue_percentage')
    calculate_percentage(combined_df, 'annualized_EDEPDOM', 'DEPDOM', 'deposit_expense_rate')

    # Merge with FRED data
    merged_df = merge_with_fred_data(combined_df, fred_data_path, fred_fields)
    
    # Sort by cert and date
    merged_df = merged_df.sort_values(by=['cert', 'date'])

    # Save the merged data to CSV
    output_path = output_path_template.format(rank_threshold)
    merged_df.to_csv(output_path, index=False)
    print(f"Merged data saved to {output_path}")

if __name__ == "__main__":
    annualize_fields = ['EDEPDOM', 'INTINCY', 'NONII']
    non_annualize_fields = ['DEPDOM', 'DEP', 'DEPFOR', 'DEPNIDOM', 'DEPIDOM', 'BRO', 'DEPINS', 'LNLSNET', 'SC', 'ASSET', 'LNCON']
    fred_fields = ['ff_t', 'ff_e', 't_1m', 't_3m', 't_6m', 't_12m', 't_2y', 't_3y', 't_5y', 't_7y', 't_10y', 't_30y']
    rank_threshold = 200
    start_year = 1950

    process_and_merge_data(FDIC_DATA_PATH, FRED_DATA_PATH, BEST_RANKS_PATH, OUTPUT_PATH_TEMPLATE, annualize_fields, non_annualize_fields, fred_fields, rank_threshold, start_year)
