from office365.sharepoint.client_context import ClientContext
from office365.runtime.auth.user_credential import UserCredential
import configparser
import pandas as pd
from database import conn

df=[]
config=configparser.ConfigParser()
config.read('config.ini')
site_url = config['credentials']['your_site']
username = config['credentials']['username']
password = config['credentials']['password']

ctx = ClientContext(site_url).with_credentials(UserCredential(username, password))
web = ctx.web
items_=web.lists.get_by_title('Project_details')

items=items_.items.select(['Title','Status','Project_x0020_Manager','Start_x0020_Date','End_x0020_Date','Budget','Department']).get().execute_query()

for i in items:
    df.append({
        'Project Name':i.properties.get('Title'),
        'Status':i.properties.get('Status'),
        'Project Manager':i.properties.get('Project_x0020_Manager'),
        'Start Date':i.properties.get('Start_x0020_Date'),
        'End Date':i.properties.get('End_x0020_Date'),
        'Budget':i.properties.get('Budget'),
        'Department':i.properties.get('Department')
    })

df=pd.DataFrame(df)
conn=conn()
df.to_sql('Sharepoint_data',conn,if_exists='replace',index=False)
