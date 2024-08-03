#%%

import requests
import pandas as pd
import configparser
import os

#%%
# Read configuration file
config = configparser.ConfigParser()
config.read('config.ini')
API_KEY = config['FRED']['API_KEY']

#%%

# Define the series IDs for the required data
series_ids = {
    'ff_t': 'DFEDTAR',
    'ff_e': 'DFF',
    't_1m': 'DGS1MO',
    't_3m': 'DGS3MO',
    't_6m': 'DGS6MO',
    't_12m': 'DGS1',
    't_2y': 'DGS2',
    't_3y': 'DGS3',
    't_5y': 'DGS5',
    't_7y': 'DGS7',
    't_10y': 'DGS10',
    't_30y': 'DGS30'
}

# Function to fetch data from FRED API
def fetch_fred_data(series_id, api_key):
    url = f"https://api.stlouisfed.org/fred/series/observations"
    params = {
        'series_id': series_id,
        'api_key': api_key,
        'file_type': 'json'
    }
    response = requests.get(url, params=params)
    data = response.json()
    observations = data['observations']
    dates = [obs['date'] for obs in observations]
    values = [float(obs['value']) if obs['value'] != '.' else None for obs in observations]
    return pd.Series(data=values, index=pd.to_datetime(dates), name=series_id)

# Main function to execute data download
if __name__ == "__main__":
    # Fetch data for all specified series
    data_frames = {}
    for name, series_id in series_ids.items():
        print(f"Fetching data for {name} (series ID: {series_id})...")
        data_frames[name] = fetch_fred_data(series_id, API_KEY)

    # Combine all series into a single DataFrame
    df = pd.concat(data_frames, axis=1)
    
    # Define the output directory
    output_dir = './data/raw/rates'
    os.makedirs(output_dir, exist_ok=True)
    
    # Save the DataFrame to a CSV file
    df.to_csv(os.path.join(output_dir, 'fred_data.csv'))
    print("Data saved to './data/raw/rates/fred_data.csv'.")

    # If you want to preview the data
    print(df.head())