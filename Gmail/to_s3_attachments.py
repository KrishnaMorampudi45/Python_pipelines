import boto3
import os
from configparser import ConfigParser

def attach(sender,time,id,folder_attachments,j):
    s3=boto3.client('s3')
    config=ConfigParser()
    config.read('config.ini')
    bucket=config['s3']['bucket']
    local_path=os.path.join(folder_attachments,j)
    if os.path.exists(local_path):
        s3.upload_file(local_path,bucket,f'gmail/{sender}{time}/{j}')