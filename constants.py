# Amazon S3 configuration
AWS_S3_CONFIG = {
    "bucket_name": "your-s3-bucket-name",
    "object_keys": ["example_etl_input.csv", "backup_etl_input.csv"],  # Lista kluczy obiektów
    "aws_access_key": "your_aws_access_key",
    "aws_secret_key": "your_aws_secret_key"
}

# Azure Blob Storage configuration
AZURE_BLOB_CONFIG = {
    "blob_service_url": "https://<your_storage_account_name>.blob.core.windows.net",
    "container_name": "your-container-name",
    "blob_name": "processed_example_etl_input.csv",
    "storage_account_key": "your_storage_account_key"
}
