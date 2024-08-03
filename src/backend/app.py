from flask import Flask, request, jsonify
import json
import boto3
import os
from dotenv import load_dotenv
import pandas as pd
from sklearn.linear_model import LinearRegression
import logging

# Load environment variables
load_dotenv()

app = Flask(__name__)

# Set up logging to log to the console
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def get_s3_client():
    if os.getenv('AWS_ACCESS_KEY_ID') and os.getenv('AWS_SECRET_ACCESS_KEY'):
        logger.debug("Using AWS credentials from environment variables.")
        return boto3.client(
            's3',
            aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
            aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
            region_name=os.getenv('AWS_REGION')
        )
    else:
        logger.debug("Using IAM role for AWS credentials.")
        return boto3.client('s3')

@app.route('/process', methods=['POST'])
def process():
    data = request.json
    bank_name = data['bank_name']
    cert = data['cert']
    assets = data['assets']
    model_type = data['model']
    result = f"Bank: {bank_name}; Cert: {cert}; Assets: {assets}; Model: {model_type}"

    logger.debug(f"Received request with data: {data}")

    # Log the request
    with open('log.json', 'a') as log_file:
        log_entry = {'bank_name': bank_name, 'cert': cert, 'assets': assets, 'model': model_type, 'result': result}
        json.dump(log_entry, log_file)
        log_file.write('\n')

    # Check if the bank_data_rank200.csv file exists on S3
    s3_client = get_s3_client()
    bucket_name = 'deposit-betas'
    object_key = 'data/processed/bank_data_rank200.csv'

    try:
        s3_client.head_object(Bucket=bucket_name, Key=object_key)
        s3_message = "I can see bank_data_rank200.csv"
        logger.debug(s3_message)
        
        # Download the file
        s3_client.download_file(bucket_name, object_key, '/app/bank_data_rank200.csv')
        logger.debug("Downloaded bank_data_rank200.csv from S3")
        
        # Load the CSV file
        df = pd.read_csv('/app/bank_data_rank200.csv')
        logger.debug(f"Loaded CSV file with columns: {df.columns.tolist()}")
        
        # Filter records by cert
        bank_data = df[df['cert'] == int(cert)]
        logger.debug(f"Filtered data for cert {cert}: {bank_data.head()}")

        # Ensure the necessary columns are present
        if 'deposit_expense_rate' in bank_data.columns and 'ff_t' in bank_data.columns:
            # Run the model
            X = bank_data[['ff_t']].values
            y = bank_data['deposit_expense_rate'].values
            model = LinearRegression()
            model.fit(X, y)
            
            # Extract model coefficients
            intercept = model.intercept_
            coef = model.coef_[0]
            model_results = {
                'intercept': intercept,
                'coefficient': coef,
                'model_type': 'Linear Regression'
            }
            logger.debug(f"Model results: {model_results}")
        else:
            model_results = {'error': 'Necessary columns not found in data'}
            logger.debug(model_results['error'])
    except Exception as e:
        s3_message = f"Error accessing bank_data_rank200.csv: {str(e)}"
        logger.debug(s3_message)
        model_results = {'error': str(e)}

    return jsonify({'result': result, 's3_message': s3_message, 'model_results': model_results})

@app.route('/logs', methods=['GET'])
def get_logs():
    try:
        with open('log.json', 'r') as log_file:
            logs = [json.loads(line) for line in log_file]
        return jsonify(logs)
    except FileNotFoundError:
        return jsonify({"error": "Log file not found"}), 404

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8000)
