import boto3
import os
from configparser import ConfigParser

def s3(sender,time,id,folder_txt):
    s3=boto3.client('s3')
    config=ConfigParser()
    config.read('config.ini')
    bucket=config['s3']['bucket']
    txt_file=os.path.join(folder_txt,f'{id}.txt')
    if os.path.exists(txt_file):
        s3.upload_file(txt_file,bucket,f'gmail/{sender}{time}/{id}.txt')