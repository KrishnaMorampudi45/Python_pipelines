from sqlalchemy import create_engine
import pandas as pd
import boto3
from mysql_conn import mysql_conn

client=boto3.resource('dynamodb')
table=client.Table('Projects')
engine=create_engine(mysql_conn())

response=table.scan()
data=response['Items']
df=pd.DataFrame(data)


df1=df.drop('technologies',axis=1)
df1.to_sql('projects',engine,if_exists='replace',index=False)

df = df.explode('technologies').reset_index(drop=True)
print(df)
df2=df[['project_id','technologies']]
df2.to_sql('technologies',engine,if_exists='replace',index=False)
