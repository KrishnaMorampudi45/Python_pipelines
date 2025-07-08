import boto3
import os

def archive(source_bucket,source_file):
    dest_bucket='kri4545bucky'
    dest_path='archives/'
    
    dest_key = os.path.join(dest_path, os.path.basename(source_file))
    s3=boto3.resource('s3')
    copy_file={
        'Bucket':source_bucket,
        'Key':source_file
    }
    s3.meta.client.copy(copy_file,dest_bucket,dest_key)
    s3.Object(source_bucket,source_file).delete()