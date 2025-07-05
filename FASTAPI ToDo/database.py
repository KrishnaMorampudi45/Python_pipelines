from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

import configparser

def sql_conn():
    config=configparser.ConfigParser()
    config.read(r'C:\Users\Krishna\Desktop\To-Do\config.ini')
    engine=config['mysql']['engine']
    conn=create_engine(engine)
    return conn.raw_connection()


    