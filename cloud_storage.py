import os
from google.cloud import storage
from google.cloud.exceptions import NotFound
from dotenv import load_dotenv

# Load environment variables from .env file.
load_dotenv()

# Set the credentials file path.
credentials_file = os.environ.get('GOOGLE_APPLICATION_CREDENTIALS')

# Create a storage client instance.
storage_client = storage.Client.from_service_account_json(credentials_file)

def handle_exception(func):
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except NotFound as e:
            print(f"Bucket or blob not found: {e}")
        except Exception as e:
            print(f"An error occurred: {e}")
    return wrapper

# Prints the metadata of a GCP bucket_name.
@handle_exception
def bucket_metadata(bucket_name):
    bucket = storage_client.get_bucket(bucket_name)
    metadata = {
        "ID": bucket.id,
        "Name": bucket.name,
        "Storage Class": bucket.storage_class,
        "Location": bucket.location,
        "Location Type": bucket.location_type,
        "Cors": bucket.cors,
        "Default Event Based Hold": bucket.default_event_based_hold,
        "Default KMS Key Name": bucket.default_kms_key_name,
        "Metageneration": bucket.metageneration,
        "Public Access Prevention": bucket.iam_configuration.public_access_prevention,
        "Retention Effective Time": bucket.retention_policy_effective_time,
        "Retention Period": bucket.retention_period,
        "Retention Policy Locked": bucket.retention_policy_locked,
        "Requester Pays": bucket.requester_pays,
        "Self Link": bucket.self_link,
        "Time Created": bucket.time_created,
        "Versioning Enabled": bucket.versioning_enabled,
        "Labels": bucket.labels
    }
    for key, value in metadata.items():
        print(f"{key}: {value}")

# Lists all the blobs in the bucket.
@handle_exception
def list_blobs(bucket_name):
    blobs = list(storage_client.list_blobs(bucket_name))
    if not blobs:
        print(f"No blobs found in {bucket_name} bucket")
    else:
        for blob in blobs:
            print(blob.name)

# Uploads a file to the bucket.
@handle_exception
def upload_blob(bucket_name, source_file_name, destination_blob_name):
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(source_file_name)
    print(f"File {source_file_name} uploaded to {destination_blob_name}.")


# Downloads the source_file_name file from the bucket_name bucket.
@handle_exception
def download_blob(bucket_name, source_file_name):
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(source_file_name)
    blob.download_to_filename(source_file_name)

# Deletes the source_file_name file from the bucket_name bucket.
@handle_exception
def delete_blob(bucket_name, source_file_name):
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(source_file_name)
    blob.delete()
    print(f"File {source_file_name} deleted from {bucket_name}.")