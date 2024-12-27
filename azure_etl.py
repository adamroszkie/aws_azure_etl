import logging
from azure.storage.blob import BlobServiceClient
from io import StringIO
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

#Określamy funkcję, w ładujemy przekształcony plik do storage
def load_data_to_blob(data, blob_service_url, credential, container_name, blob_name):
    try:
        logging.info('Loading data to Azure Blob Storage...')

        # przekształcamy dane na csv
        data_as_csv = data.to_csv(index=False)

        # tworzymy połączenie z blob storage 1
        blob_service_client = BlobServiceClient(blob_service_url, credential=credential)

        # tworzymy połączenie z blob storage 2
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)

        # ładujemy nasze csv w określone wcześniej miejsce
        blob_client.upload_blob(data_as_csv, overwrite=True)

        logging.info('Data loaded successfully to Azure Blob Storage')
    except Exception as e:
        logging.error(f'Error during load to Azure Blob Storage: {e}')
        raise
