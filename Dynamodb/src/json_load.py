import json
import boto3

client=boto3.resource('dynamodb',region_name='us-east-1')
table=client.Table('Projects')

with open('data2.json','r') as f:
    items=json.load(f)

with table.batch_writer() as batch:
    for item in items:
        batch.put_item(Item=item)
