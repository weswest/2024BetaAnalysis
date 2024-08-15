from flask import Flask, jsonify
import boto3
import json
import logging
import os
import pandas as pd
from dotenv import load_dotenv
from sklearn.linear_model import LinearRegression
import threading
#from prometheus_client import Counter, generate_latest, CONTENT_TYPE_LATEST

# Load environment variables
load_dotenv()

# Set up logging to log to the console
logging.basicConfig(level=logging.WARNING, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

app = Flask(__name__)

'''
# Define Prometheus Counter
REQUEST_COUNTER = Counter('backend_requests_total', 'Total number of requests processed by the backend')  # Add this line
'''

def get_s3_client():
    aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID')
    aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')
    aws_region = 'us-east-2'  # Hardcoded region

    if aws_access_key_id and aws_secret_access_key:
        logging.debug("Using AWS credentials from environment variables.")
        return boto3.client(
            's3',
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region_name=aws_region
        )
    else:
        logging.debug("Using IAM role for AWS credentials.")
        session = boto3.Session(region_name=aws_region)
        return session.client('s3')

# AWS SQS configuration
SQS_REGION = 'us-east-2'
REQUEST_QUEUE_URL = 'https://sqs.us-east-2.amazonaws.com/471112830027/get_a_model_queue'
RESPONSE_QUEUE_URL = 'https://sqs.us-east-2.amazonaws.com/471112830027/model_results_queue'

sqs_client = boto3.client('sqs', region_name=SQS_REGION)

def consume_model_requests():
    while True:
        try:
            response = sqs_client.receive_message(
                QueueUrl=REQUEST_QUEUE_URL,
                MaxNumberOfMessages=1,
                WaitTimeSeconds=10
            )
            if 'Messages' in response:
                for message in response['Messages']:
                    logging.debug(f"Consumed model request: {message['Body']}")
#                    REQUEST_COUNTER.inc()  # Increment the request counter  # Add this line
                    process_model_request(json.loads(message['Body']))
                    sqs_client.delete_message(
                        QueueUrl=REQUEST_QUEUE_URL,
                        ReceiptHandle=message['ReceiptHandle']
                    )
        except Exception as e:
            logging.error(f"Error consuming model requests from SQS: {str(e)}")

def process_model_request(data):
    session_data = data['session_data']
    bank_name = data['bank_name']
    cert = data['cert']
    assets = data['assets']
    model_type = data['model']
    result = f"Bank: {bank_name}; Cert: {cert}; Assets: {assets}; Model: {model_type}"

    logger.debug(f"Processing data: {data} and session data: {session_data}")

    # Log the request
    with open('log.json', 'a') as log_file:
        log_entry = {'session_data': session_data, 'bank_name': bank_name, 'cert': cert, 'assets': assets, 'model': model_type, 'result': result}
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

    response_message = {
        'session_data': session_data,
        'model_results': model_results
    }

    try:
        response = sqs_client.send_message(
            QueueUrl=RESPONSE_QUEUE_URL,
            MessageBody=json.dumps(response_message)
        )
        logging.debug(f"Published model results to SQS: {response['MessageId']}")
    except Exception as e:
        logging.error(f"Failed to publish model results to SQS: {str(e)}")

@app.route('/checkin')
def checkin():
    return "Backend is running."

@app.route('/logs')
def get_logs():
    try:
        with open('log.json', 'r') as log_file:
            logs = [json.loads(line) for line in log_file]
        return jsonify(logs)
    except FileNotFoundError:
        return jsonify({"error": "Log file not found"}), 404

'''
@app.route('/metrics')
def metrics():
    return generate_latest(), 200, {'Content-Type': CONTENT_TYPE_LATEST}
'''

if __name__ == '__main__':
    threading.Thread(target=consume_model_requests, daemon=True).start()
    app.run(host='0.0.0.0', port=8000)
