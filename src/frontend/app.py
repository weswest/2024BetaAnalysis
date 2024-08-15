import boto3
import os
import logging
from flask import Flask, render_template
from dotenv import load_dotenv
import pandas as pd

# Load environment variables
load_dotenv()

app = Flask(__name__)

# Set up logging
logging.basicConfig(level=logging.WARNING, format='%(asctime)s - %(levelname)s - %(message)s')

def get_s3_client():
    aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID')
    aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')
    aws_region = 'us-east-2'  # Hardcoded region

    if aws_access_key_id and aws_secret_access_key:
        logging.warning("Using AWS credentials from environment variables.")
        return boto3.client(
            's3',
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region_name=aws_region
        ), aws_access_key_id, aws_secret_access_key
    else:
        logging.warning("Using IAM role for AWS credentials.")
        session = boto3.Session(region_name=aws_region)
        return session.client('s3'), None, None

def load_bank_data():
    s3_client, aws_access_key_id, aws_secret_access_key = get_s3_client()
    bucket_name = 'deposit-betas'
    object_key = 'data/processed/institution_details.csv'

    # Download the file from S3
    s3_client.download_file(bucket_name, object_key, 'institution_details.csv')

    # Load the CSV file into a DataFrame
    df = pd.read_csv('institution_details.csv')
    df = df[df['Best_Asset_Rank'] < 200].sort_values(by='Asset_Value', ascending=False)
    options = df.apply(lambda row: {'name': row['Institution_Name'], 'cert': row['Cert'], 'assets': row['Asset_Value']}, axis=1).tolist()
    logging.warning(f"First few bank options: {options[:5]}")
    return options, aws_access_key_id, aws_secret_access_key

bank_options, aws_access_key_id, aws_secret_access_key = load_bank_data()

@app.route('/')
def index():
    return render_template('index.html', bank_options=bank_options, aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)

@app.route('/checkin')
def checkin():
    return "Frontend is running."

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080, debug=True)
