import pandas as pd
from mysql_conn import mysql_conn
from ssms_conn import ssms_conn
from sqlalchemy import create_engine, inspect
from sqlalchemy.sql import text

engine1=create_engine(ssms_conn())
engine2=create_engine(mysql_conn())
inspector=inspect(engine2)


if not (inspector.has_table('customer_data')):
    df=pd.read_sql('select * from customer_data',engine1)
    df['status']='Active'
    df.to_sql('customer_data',engine2,if_exists='fail',index=False)
else:
    df1=pd.read_sql('select * from customer_data',engine1)
    df2=pd.read_sql('select * from customer_data',engine2)
    new_rows = df1[~df1['customer_id'].isin(df2['customer_id'])]
    new_rows.to_sql('customer_data',engine2,if_exists='append',index=False)
    print(new_rows)

    ud=[]
    for _,row in df1.iterrows():
        cid=row['customer_id']
        old=df2[df2['customer_id']==cid]
        if not old.empty:
            ssms=row.drop('customer_id')
            mysql=old.iloc[0].drop(['customer_id','status'])
            if not ssms.equals(mysql):
                ud.append(row)

                with engine2.connect() as conn:
                    conn.execute(text(f"update customer_data set status='Inactive' where customer_id={cid}"))
                    conn.commit()
    if ud:
        update=pd.DataFrame(ud)
        update['status']='Active'
        update.to_sql('customer_data',engine2,if_exists='append',index=False)
