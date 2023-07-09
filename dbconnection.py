import configparser
import warnings
from configparser import ConfigParser
import mysql.connector as sql
from mysql.connector import error
import pymysql


Warning.simplefilter(action= 'ignore')

from cryptography.fernet import Fernet

config = configparser.ConfigParser()
config.read("config.ini")


class MySQLConnection:
    def __init__(self, host, port, user, pwd):
        self.host = host
        self.port = port
        self.user = user
        self.pwd = pwd

    def connect(self):
        try:
            db = pymysql.connect(
                host = self.host,
                port = self.port,
                user = self.user,
                password = self.pwd
                )
            return db
        except Exception as e:
            print(f"Could not connect to db : {e}")

    def readData(self, db, query):
        try:
            db.execute(query)
        except Exception as e:
            pass
