import sys
import os
import json
import records
import psycopg2
import logging
import datetime
import psycopg2.extras
from jsonschema import validate
from typing import List, Dict, Any
from db_initialise import DatabaseInitialLogin

logging.basicConfig(format='%(name)s - %(asctime)s - %(message)s',
        datefmt='%d-%b-%y %H:%M:%S', level=logging.INFO)
logger = logging.getLogger('database client')

class DatabaseQueries(DatabaseInitialLogin):

    def __init__(self):
        super(DatabaseQueries, self).__init__()

    def insert_health_records(self, record : Dict) -> None:

        '''
        Method to insert a record into health table
        Inputs: a dictionary containing all column values required for table
        e.g. {'id': 13,
              'registered_doctor': 'doctor2',
              'has_asthma': False,
              'has_registered_disability': False}
        '''

        with open("json_validators/health_table_input.json", 'r') as f:
            health_table_input_schema = json.load(f)

        logger.info(f'Aboout to validate health table input: {record}')

        validate(instance=record, schema=health_table_input_schema)

        logger.info(f'Health table input passed validation')

        keys = ['id', 'registered_doctor', 'has_asthma', 'has_registered_disability']

        insert_tuple = ()
        for key in keys:
            insert_tuple += (record[key],)

        now = str(datetime.datetime.now())
        insert_tuple += (now,)

        query = f'''
        INSERT INTO health_table
            (id, registered_doctor, has_asthma, has_registered_disability, record_updated_at)
        VALUES
            {insert_tuple}
        '''
        self.send_query(query)

        return

    def update_health_record(self, id_to_update : int, record_to_update : Dict[str,Any]) -> None:

        '''
        Method to update a record in the health table
        Inputs: id_to_update - the primary key of the table, record_to_update - see next line
        records to update of form {'registered_doctor': 'doctor2',
                                    'has_asthma': None,
                                    'has_registered_disability': False}
        Note: cannot update 'id' field.
        '''

        with open("json_validators/health_table_update_input.json", 'r') as f:
            health_table_update_input_schema = json.load(f)

        logger.info(f'About to validate health table update input: {record_to_update}')

        validate(instance=record_to_update, schema=health_table_update_input_schema)

        logger.info(f'Health table update input passed validation')

        # this is ugly, have to be careful with quotes when have string or bool in f-string
        if record_to_update['registered_doctor'] is not None:
            set_update = f"registered_doctor = '{record_to_update['registered_doctor']}', "

        if record_to_update['has_asthma'] is not None:
            set_update += f"has_asthma = {record_to_update['has_asthma']}, "
        if record_to_update['has_registered_disability'] is not None:
            set_update += f"has_registered_disability = {record_to_update['has_registered_disability']}, "

        set_update = set_update + f"record_updated_at = '{datetime.datetime.now()}'"

        query = f'''UPDATE health_table SET {set_update} WHERE id = {id_to_update};
                '''
        logger.info(f'Sending following query: {query}')

        self.send_query(query)

        return

    def health_table_deduplication(self, record : Dict) -> None:

        '''
        Method to check the ID is not already in the health table
        Inputs: a dictionary containing all column values required for table
        e.g. {'id': 13,
              'registered_doctor': 'doctor2',
              'has_asthma': False,
              'has_registered_disability': False}
        '''

        query = f'''SELECT id FROM health_table WHERE id = {record['id']}'''

        is_health_table_empty_query = '''SELECT COUNT(*) FROM health_table'''

        if self.send_query(is_health_table_empty_query).export('df')['count'].to_list()[0] == 0:
            output = True
        elif self.send_query(query).export('df')['id'].to_list():
            output = False
        else:
            output = True

        return output

    def query_health_table(self, query):

        return self.send_query(query).export('df')
