from flask import Flask, render_template, request, jsonify
import requests
import pandas as pd
import boto3
import os
import logging
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

app = Flask(__name__)

# Set up logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

def get_s3_client():
    aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID')
    aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')
    aws_region = os.getenv('AWS_REGION')

    if aws_access_key_id and aws_secret_access_key and aws_region:
        logging.debug("Using AWS credentials from environment variables.")
        return boto3.client(
            's3',
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region_name=aws_region
        )
    else:
        logging.debug("Using IAM role for AWS credentials.")
        session = boto3.Session(region_name=aws_region if aws_region else None)
        return session.client('s3')

def load_bank_data():
    s3_client = get_s3_client()
    bucket_name = 'deposit-betas'
    object_key = 'data/processed/institution_details.csv'

    # Download the file from S3
    s3_client.download_file(bucket_name, object_key, 'institution_details.csv')

    # Load the CSV file into a DataFrame
    df = pd.read_csv('institution_details.csv')
    df = df[df['Best_Asset_Rank'] < 200].sort_values(by='Asset_Value', ascending=False)
    options = df.apply(lambda row: {'name': row['Institution_Name'], 'cert': row['Cert'], 'assets': row['Asset_Value']}, axis=1).tolist()
    logging.debug(f"First few bank options: {options[:5]}")
    return options

bank_options = load_bank_data()

@app.route('/')
def index():
    return render_template('index.html', bank_options=bank_options)

@app.route('/checkin')
def checkin():
    return "Frontend is running."

@app.route('/get_model', methods=['POST'])
def get_model():
    print("Frontend: received POST request at /get_model")
    data = request.get_json()
    print(f"Request JSON data: {data}")

    bank_name = data['bankName']
    cert = data['cert']
    assets = data['assets']
    model = data['model']

    logging.debug(f"Sending request to backend with bank_name: {bank_name}, cert: {cert}, assets: {assets}, model: {model}")

    response = requests.post('http://backend:8000/process', json={
        'bank_name': bank_name,
        'cert': cert,
        'assets': assets,
        'model': model
    })

    logging.debug(f"Received response from backend: {response.json()}")
    return jsonify(response.json())

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080, debug=True)
