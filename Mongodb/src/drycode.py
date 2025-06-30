from sqlalchemy import create_engine
from sqlalchemy.sql import text
import pandas as pd
import numpy as np
from  mysql_conn import mysql_conn
from mongo_conn import MongoClient

client=MongoClient('mongodb://localhost:27017/')
db=client['my-mongo']
my_collection=db['itc']
engine=create_engine('mysql+pymysql://root:99128@localhost:3306/mongo1')
#create a main table
cursor=my_collection.find({},{'_id':0})
cursor=list(cursor)
df=pd.DataFrame(cursor)
df=pd.json_normalize(df)
print(df)
    
