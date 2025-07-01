from pymongo import MongoClient
import pandas as pd
from dynamo_pandas import put_df,get_df
from dynamo_pandas.transactions import put_item,put_items,get_all_items

client=MongoClient('mongodb://localhost:27017/')
db=client['my-mongo']
my_collection=db['companies ']

cursor=list(my_collection.find({},{'_id':0}))
df=pd.DataFrame(cursor)
df = df.drop_duplicates(subset=['project_id', 'project_name', 'status'])


put_df(df,table='companies')
print(df)
items=get_all_items(table='companies')
put_items(items=items,table='companies')