import boto3
import fitz
from io import BytesIO
import configparser
import os
from to_archive import archive
from transform import transform

#connect to the s3 
config=configparser.ConfigParser()
config.read('config.ini')
s3=boto3.client('s3')
bucket=config['s3']['bucket']
prefix=config['prefix']['path']

#list all the pdfs
files=s3.list_objects_v2(Bucket=bucket,Prefix=prefix)
print("S3 files listed:", [f['Key'] for f in files.get('Contents', [])])
pdf_list=[file['Key'] for file in files.get('Contents',[]) if file['Key'].endswith('.pdf')]


for key in pdf_list:

    pdf_stream=BytesIO()
    s3.download_fileobj(bucket,key,pdf_stream)
    pdf_stream.seek(0)

    doc=fitz.open(stream=pdf_stream, filetype='pdf')
    text = ''
    for page in doc:
        text += page.get_text("text")
    
    output_dir = "pdf_text_output"
    os.makedirs(output_dir, exist_ok=True)

    filename = os.path.basename(key).replace('.pdf', '.txt')
    output_path = os.path.join(output_dir, filename)
    print(output_path)

    with open(output_path, 'w', encoding='utf-8') as file:
        file.write(text)

    archive(bucket,key)
    transform(output_path)
