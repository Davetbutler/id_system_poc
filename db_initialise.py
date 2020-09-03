import os
import sys
import yaml
import json
import records
import logging
import psycopg2
import psycopg2.extras
from psycopg2.extras import Json, DictCursor

logging.basicConfig(format='%(name)s - %(asctime)s - %(message)s',
    datefmt='%d-%b-%y %H:%M:%S', level=logging.INFO)
logger = logging.getLogger('ID System')

class DatabaseInitialLogin(object):

    def __init__(self):

        u = 'davidbutler'
        p = 'dave'
        h = '127.0.0.1'
        o = 5432
        d = 'davidbutler'

        logger.info(f'connected to database {d} with username {u}')

        self.URL = f"postgres://{u}:{p}@{h}:{o}/{d}"
        self.dbargs = {'dbname': d, 'user': u, 'password': p, 'host': h, 'port': o}

        # records driver (useful for SELECT statements)
        self.db = records.Database(self.URL)

        # psycopg2 driver (useful for batch-INSERT statetment)
        self.conn = psycopg2.connect(**self.dbargs)
        self.cur = self.conn.cursor(cursor_factory=DictCursor)

    def send_query(self, query):
        try:
            ans = self.db.query(query)
        except psycopg2.OperationalError as e:
            # 'psycopg2.OperationalError: Attempting one more time'
            self.db = records.Database(self.URL)
            ans = self.db.query(query)
        return ans

    def execute_values(self, query, values):
        try:
            psycopg2.extras.execute_values(self.cur, query, values)
            self.conn.commit()
        except psycopg2.OperationalError as e:
            # 'psycopg2.OperationalError: Attempting one more time'
            self.conn = psycopg2.connect(**self.dbargs)
            self.cur = self.conn.cursor(cursor_factory=DictCursor)
            psycopg2.extras.execute_values(self.cur, query, values)
            self.conn.commit()
        return
