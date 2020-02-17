import configparser
import mysql.connector
from mysql.connector import Error, errorcode

config = configparser.ConfigParser()
config.read('config.ini')

def connect():
    return mysql.connector.connect(host= config['mysqlDB']['host'],
                                    database= config['mysqlDB']['db'],
                                    user= config['mysqlDB']['user'],
                                    password= config['mysqlDB']['pass'])