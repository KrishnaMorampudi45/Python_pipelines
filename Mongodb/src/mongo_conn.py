from pymongo import MongoClient
import json

client=MongoClient('mongodb://localhost:27017/')
db=client['my-mongo']
my_collection=db['itc']
with open('data1.json','r') as f:
    data=json.loads(f.read())
my_collection.insert_many(data)


my_collection=db['companies ']
with open('data2.json','r') as f:
    data=json.loads(f.read())
my_collection.insert_many(data)