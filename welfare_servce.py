import json
import os
import sys
from random import randrange
import logging
import argparse
import ast
import datetime
from jsonschema import validate
from typing import List, Dict, Any
from database_operations import DatabaseQueries
from health_service import HealthServiceClient


logging.basicConfig(
    format="%(name)s - %(asctime)s - %(message)s",
    datefmt="%d-%b-%y %H:%M:%S",
    level=logging.INFO,
)
logger = logging.getLogger("Health Service")

class WelfareServiceClient(DatabaseQueries):

    WELFARE_AUTH_LOG = "logs/welfare_serice_authenticate_log.json"
    WELFARE_QUERY_LOG = "logs/welfare_query_log.json"
    DEPT_NAME = 'welfare_dept'
    PASSWORD = 'welfare'

    def __init__(self):
        self.logger = logging.getLogger('Welfare Service')

    def welfare_disability_authenticate(self, id):

        '''
        Method for check if a user is  registered as having a disability
        Inputs: user id
        Outputs: has_registered_disability value
        '''

        attribute = 'has_registered_disability'

        hc = HealthServiceClient()

        query_output = hc.health_table_query(self.DEPT_NAME, self.PASSWORD, attribute, id)

        query_log = {'querier': self.DEPT_NAME,
                     'dept_queried': 'health  dept',
                     'id_queried': id,
                     'atttribute_queried': attribute,
                     'query_time': datetime.datetime.now()}

        logger.info(f'Successfully made query. Writing query details to {self.WELFARE_QUERY_LOG} Query output to follow.')

        # write log to file
        with open(self.WELFARE_QUERY_LOG, mode="a+", encoding="utf-8") as f:
            f.write(f"{query_log} \r\n")

        return query_output

if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument('-id', '--id',
            dest='id',
            type=int,
            help='ID of user')

    args = parser.parse_args()

    id = args.id

    wc = WelfareServiceClient()

    print(wc.welfare_disability_authenticate(id))
