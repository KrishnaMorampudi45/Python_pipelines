from sqlalchemy import create_engine
import configparser

def conn():
    client=configparser.ConfigParser()
    client.read('config.ini')
    engine=create_engine(client['ssms']['engine'])
    return engine.raw_connection()