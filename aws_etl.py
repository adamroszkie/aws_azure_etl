import logging
import pandas as pd
import boto3
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
import requests.exceptions

# Konfiguracja loggingu
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

#Konfigurujemy retry przy spodziewanych błędach
@retry(
    retry=retry_if_exception_type((requests.exceptions.Timeout, requests.exceptions.ConnectionError)),
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10)
)

#Określamy funkcję, w której bierzemy plik z AWS S3
def extract_from_s3(bucket_name, object_keys, aws_access_key, aws_secret_key):
    for object_key in object_keys:
        try:
            logging.info(f'Trying to extract data from Amazon S3 with key: {object_key}')

            # tworzymy tzw. klientam który pozwala nam się połączyć z AWS S3
            s3_client = boto3.client(
                's3',
                aws_access_key_id=aws_access_key,
                aws_secret_access_key=aws_secret_key
            )

            # pobieramy plik
            response = s3_client.get_object(Bucket=bucket_name, Key=object_key)

            # przekształcamy nasz plik na dataframe
            data = pd.read_csv(response['Body'])

            logging.info('Data extracted successfully from S3')
            return data
        except Exception as e:
            logging.error(f'Error during extraction from S3 with key {object_key}: {e}')
    raise Exception("All extraction attempts failed.")


# określamy funkcję transformacji fanych
def transform_data(data):
    try:
        logging.info('Transforming data...')

        # przykładowa transformacja
        data['processed_column'] = data['column1'] * 2 
        data = data.dropna()

        logging.info('Data transformed successfully')
        return data
    except Exception as e:
        logging.error(f'Error during transformation: {e}')
        raise
