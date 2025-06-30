#impotr all the nessecary packages
from sqlalchemy import create_engine
from sqlalchemy.sql import text
import pandas as pd
import numpy as np
from  mysql_conn import mysql_conn
from pymongo import MongoClient


client=MongoClient('mongodb://localhost:27017/')
db=client['my-mongo']
my_collection=db['companies ']

engine=create_engine(mysql_conn())
#seperate technologies into seperate dataframe
cursor=my_collection.find({},{'_id':0})
data=list(cursor)
df=pd.DataFrame(data)
# df['project_id'] = df['project_id'].astype(str)
df1=pd.DataFrame(columns=['project_id','technology'])
print(df)

df=df[['project_id','technologies']]
for _,row in df.iterrows():
    id=row['project_id']
    for i in row['technologies']:
        df1.loc[len(df1)]=[id,i]



#store all data into a dataframe exclude the technologies and drop the _id column as it was created  by mongo
df2=pd.read_json('data2.json')
df2=df2.drop(['technologies'],axis=1)
df2.to_sql('companies',engine,if_exists='fail',index=False)

#convert dataframe into sql table
df1.to_sql('technologies',engine,if_exists='fail',index=False)

