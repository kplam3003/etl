from google.cloud import storage

def list_files(bucket_name, bucket_folder):
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    files = bucket.list_blobs(prefix=bucket_folder)
    fileList = [file.name for file in files if '.' in file.name]
    return fileList        

def download_files(bucket_name, bucket_folder, file_name, local_folder):
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(bucket_folder + '/' + file_name)
    blob.download_to_filename(local_folder + file_name)

def upload_file(bucket_name, bucket_folder, file_name, local_folder):    
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(bucket_folder + '/' + file_name)
    blob.upload_from_filename(local_folder + file_name)

def upload_google_storage(bucket_name, src_file, dst_file):
    client = storage.Client()
    bucket = client.get_bucket(bucket_name)
    blob = bucket.blob(dst_file)
    blob.upload_from_filename(src_file)


def download_google_storage(bucket_name, src_file, dst_file):
    client = storage.Client()
    bucket = client.get_bucket(bucket_name)
    blob = bucket.blob(src_file)
    blob.download_to_filename(dst_file)    