#!/usr/bin/python
# -*- coding: utf-8 -*-
from psycopg2 import pool
import configparser

config = configparser.ConfigParser()
config.read('/u01/ETL/CONFIG/config.ini')

class Database:
    connection_pool = None

    @classmethod
    def initialise(cls):
        cls.connection_pool = pool.ThreadedConnectionPool(1, 1000, database=config['DB']['Database'], user=config['DB']['User'],
                                                            password=config['DB']['Password'], host=config['DB']['Host'])

    @classmethod
    def get_connection(cls):
        conn = cls.connection_pool.getconn()
        conn.set_client_encoding('UTF8')
        return conn

    @classmethod
    def return_connection(cls, conn):
        cls.connection_pool.putconn(conn)

    @classmethod
    def close_all_connections(cls):
        Database.connection_pool.closeall()


class CursorFromPool:
    def __init__(self):
        self.connection = None
        self.cursor = None

    def __enter__(self):
        self.connection = Database.get_connection()
        self.cursor = self.connection.cursor()
        return self.cursor

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_val is not None:
            print(exc_type)
            print(exc_val)
            print(exc_tb)
            self.connection.rollback()
        else:
            self.cursor.close()
            self.connection.commit()
        Database.return_connection(self.connection)
