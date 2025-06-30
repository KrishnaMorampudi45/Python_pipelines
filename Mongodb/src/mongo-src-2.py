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
df = df.drop_duplicates(subset=['project_id', 'project_name', 'status'])
print(df)

df1=df[['project_id','project_name','status']]
df1.to_sql('project_details',engine,if_exists='replace',index=False)


#creating table to the client
client_=[]
for x,row in df.iterrows():
    client=row['client']
    location=client.get('location',{})
    client_.append({
        'project_id':row['project_id'],
        'client_name':client.get('name'),
        'Industry':client.get('industry'),
        'city':location.get('city'),
        'country':location.get('country')
    })
client_=pd.DataFrame(client_)
client_.to_sql('client_details',engine,if_exists='replace',index=False)

#technologies
tech=pd.DataFrame(columns=['project_id','technologies'])
for x,row in df.iterrows():
    for i in row['technologies']:
        tech.loc[len(tech)]=[row['project_id'],i]
tech.to_sql('technology_details',engine,if_exists='replace',index=False)

#team 
team=pd.DataFrame(columns=['project_id','project_manager','employee_name','employee_role'])
for x,row in df.iterrows():
    team_=row['team']
    members=team_.get('members',{})
    for i in members:
        team.loc[len(team)]=[row['project_id'],team_.get('project_manager'),i.get('name'),i.get('role')]
team.to_sql('team_details',engine,if_exists='replace',index=False)

#milestones

stones=pd.DataFrame(columns=['project_id','milestone','due_date'])
for x,row in df.iterrows():
    stone_=row['milestones']
    for stone in stone_:
        stones.loc[len(stones)]=[row['project_id'],stone.get('name'),stone.get('due_date')]
stones.to_sql('milestone_details',engine,if_exists='replace',index=False)

  
