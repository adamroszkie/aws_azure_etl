import logging
import azure.functions as func
from aws_etl import extract_from_s3, transform_data
from azure_etl import load_data_to_blob
from constants import AWS_S3_CONFIG, AZURE_BLOB_CONFIG

# Konfiguracja loggingu
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def main(req: func.HttpRequest, debug=False) -> func.HttpResponse:
    logging.info('ETL function triggered manually.')

    try:
        # egzekucja funkcji, w której bierzemy plik z AWS S3
        data = extract_from_s3(
            AWS_S3_CONFIG['bucket_name'], 
            AWS_S3_CONFIG['object_keys'], 
            AWS_S3_CONFIG['aws_access_key'], 
            AWS_S3_CONFIG['aws_secret_key']
        )

        # egzekucja funkcji, w której przekształcamy dane
        transformed_data = transform_data(data)

        # egzekucja funkcji, w której ładujemy przekształcony plik do storage
        load_data_to_blob(
            transformed_data, 
            AZURE_BLOB_CONFIG['blob_service_url'], 
            AZURE_BLOB_CONFIG['storage_account_key'], 
            AZURE_BLOB_CONFIG['container_name'], 
            AZURE_BLOB_CONFIG['blob_name']
        )

        return func.HttpResponse("ETL completed successfully.", status_code=200)
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")
        return func.HttpResponse("An unexpected error occurred.", status_code=500)
