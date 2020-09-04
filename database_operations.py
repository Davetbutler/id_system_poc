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
        print(query)
        logger.info(f'Sending following query: {query}')

        self.send_query(query)

        return

    def id_exists_health_table(self, id : int) -> None:

        '''
        Method to check the ID is not already in the health table
        Inputs: a dictionary containing all column values required for table
        e.g. {'id': 13,
              'registered_doctor': 'doctor2',
              'has_asthma': False,
              'has_registered_disability': False}
        '''

        query = f'''SELECT COUNT(*) FROM health_table WHERE id = {id};'''

        if self.send_query(query).export('df')['count'].to_list()[0] > 0:
            id_exists = True
        else:
            id_exists = False

        return id_exists

    def query_health_table(self, query):

        return self.send_query(query).export('df')

    def is_id_in_use(self, id_to_check):

        is_id_register_table_empty_query = '''SELECT COUNT(*) FROM id_register'''
        query = f'''SELECT CAST(CASE WHEN COUNT(*) > 0 THEN 1 ELSE 0 END AS BIT)
                    FROM id_register WHERE id = {id_to_check};'''

        if self.send_query(query).export('df')['bit'].to_list()[0] == '1':
            id_in_use = True
        else:
            id_in_use = False

        return id_in_use

    def register_insert_record(self, id, record):

        # check the inputted recoord is valid.
        with open("json_validators/register_input_to_id_table.json", 'r') as f:
            registation_input_schema = json.load(f)

        try:

            validate(instance=record, schema=registation_input_schema)
            logger.info(f'record is valid input')

        except:

            logger.info(f'inputed records are not valid: {record}')
            return None

        record['id'] = id
        insert_tuple = (record['id'], record['name'])

        query = f'''
        INSERT INTO id_register
            (id, name)
        VALUES
            {insert_tuple}
        '''
        self.send_query(query)

        return

    def health_dept_access_granted(self, name_wanting_access : str, password : str) -> bool:

        '''
        Method to check if the party wanting to query the db has access, and if the password is valid
        Input: name_wanting_acess - name of entity wanting acccess
               password - password stored against the name in the health_dept_access table
        Output: boolean depending on whether access is granted or not
        '''

        # check name is in db
        query_name = f"SELECT COUNT(*) FROM health_dept_access WHERE name = '{name_wanting_access}' "

        name_in_db = self.send_query(query_name).export('df')['count'].to_list()[0]

        if not name_in_db:
            logger.info(f'{name_wanting_access} is not registered as having autthorise access to this db')
            return False

        # check password is correct for given name

        query_password = f"SELECT password FROM health_dept_access WHERE name = '{name_wanting_access}'"

        password_in_db = self.send_query(query_password).export('df')['password'].to_list()[0]

        if password_in_db == password:
            access_granted = True
        else:
            logger.info('incorrect password given')
            access_granted = False

        return access_granted




        return
