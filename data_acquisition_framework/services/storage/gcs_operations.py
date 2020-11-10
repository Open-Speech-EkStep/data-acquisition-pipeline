from google.cloud import storage
import os
import json


def set_gcs_credentials(dictionary):
    json_object = json.dumps(dictionary, indent=4)

    # Writing to sample.json
    with open("temp.json", "w") as outfile:
        outfile.write(json_object)
    print("Setting GOOGLE_APPLICATION_CREDENTIALS ....")
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'temp.json'


def upload_blob(bucket_name, source_file_name, destination_blob_name):
    """Uploads a file to the bucket."""
    # bucket_name = "your-bucket-name"
    # source_file_name = "local/path/to/file"
    # destination_blob_name = "storage-object-name"

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    blob.upload_from_filename(source_file_name)

    print(
        "File {} uploaded to {}/{}.".format(
            source_file_name, bucket_name, destination_blob_name
        )
    )


def download_blob(bucket_name, source_blob_name, destination_file_name):
    """Downloads a blob from the bucket."""
    # bucket_name = "your-bucket-name"
    # source_blob_name = "storage-object-name"
    # destination_file_name = "local/path/to/file"

    storage_client = storage.Client()

    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(source_blob_name)
    blob.download_to_filename(destination_file_name)

    print(
        "Blob {} from Bucket {} downloaded to {}.".format(
            source_blob_name, bucket_name, destination_file_name
        )
    )


def check_blob(bucket_name, file_prefix):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    stats = storage.Blob(bucket=bucket, name=file_prefix).exists(storage_client)
    return stats


if __name__ == "__main__":
    pass
