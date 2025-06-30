from sqlalchemy import create_engine
from sqlalchemy.sql import text
import pandas as pd
import numpy as np
import configparser

def mysql_conn():
    config=configparser.ConfigParser()
    config.read(r'C:\Users\Krishna\Desktop\Python\MYSQL\config.ini')
    engine=config['mysql']['engine']
    mysql_engine=engine
    return mysql_engine