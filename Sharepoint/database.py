from sqlalchemy import create_engine
import configparser

def conn():
    config=configparser.ConfigParser()
    config.read('config.ini')
    engine=create_engine(config['ssms']['engine'])
    return engine