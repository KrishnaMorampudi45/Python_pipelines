import boto3
import os
from configparser import ConfigParser

def link(sender,time,id,folder_attachments,j):
    s3=boto3.client('s3')
    config=ConfigParser()
    config.read('config.ini')
    bucket=config['s3']['bucket']
    sender=sender.replace('@','%40')
    sender=sender.replace(' ','+')
    sender=sender.replace(':','%3A')
    time=str(time)
    time = time.replace(' ', '+').replace(':', '%3A')
    key=f'gmail/{sender}{time}/{j}'
    region=config['region']['region']
    url=f"https://{bucket}.s3.{region}.amazonaws.com/{key}"
    return url
