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


logging.basicConfig(format='%(name)s - %(asctime)s - %(message)s',
        datefmt='%d-%b-%y %H:%M:%S', level=logging.INFO)
logger = logging.getLogger('database operations')

REGISTRATION_RECORD_LOG = 'registration_record_log.json'
HEALTH_TABLE_INSERT_LOG = 'health_table_insert_log.json'
HEALTH_TABLE_QUERY_LOG = 'health_table_query_log.json'
HEALTH_TABLE_UPDATE_LOG = 'health_table_update_log.json'
SIZE_OF_ID_SPACE = 10000

# --------------
# Registration phase
# --------------

# A user must present their 'record'. record = {'name': 'david'}
# They are then issued an id number (id)
# Their updated record i.e. record = {'id': id, 'name': 'david'} is inserted into the id_register table

# We define two components, then combine them as follows:
# 1. generate_user_id
# 2. registration_insert_record
# 3. combine above two components into registration

def generate_user_id(record : Dict[str, str]) -> int:

    db = DatabaseQueries()

    with open("json_validators/register_inputs.json", 'r') as f:
        registation_input_schema = json.load(f)

    try:

        validate(instance=record, schema=registation_input_schema)
        logger.info(f'record is valid input')

    except:

        logger.info(f'inputed records are not valid: {record}')
        return None

    id = randrange(SIZE_OF_ID_SPACE)

    # check this ID is not already in use.
    id_in_use = db.is_id_in_use(id)

    if id_in_use:
        logger.info(f'id is already in use, please rerun')
        output = None
    else:
        output = id

    return  output

def registration_insert_record(id : int, record: Dict[str, str]) -> str:

    db = DatabaseQueries()

    id_in_use = db.is_id_in_use(id)

    record['id'] = id
    registration_insert_log = record

    if id_in_use:
        logger.info(f'id is already in use')
        registration_insert_log['successful'] = False
        logger.info(f'logging record insertion attempt: {registration_insert_log}')

        # write log to file
        with open(REGISTRATION_RECORD_LOG, mode = 'a+', encoding='utf-8') as f:
            f.write(f'{registration_insert_log} \r\n')
    else:
        db.register_insert_record(id, record)
        logger.info(f'successfully inputted record into id_register table')
        registration_insert_log['successful'] = True

        logger.info(f'logging record insertion attempt: {registration_insert_log}')

        # write log to file
        with open(REGISTRATION_RECORD_LOG, mode = 'a+', encoding='utf-8') as f:
            f.write(f'{registration_insert_log} \r\n')
    return

def registration(record : Dict[str, str]) -> str:

    logger.info(f'generating user id')
    id = generate_user_id(record)

    if id is not None:
        logger.info(f'inserting record into database')
        registration_insert_record(id, record)
    else:
        logger.info(f'no id present (id is None)')

    return


# --------------
# Health table operations
# --------------

def health_table_insert(record: Dict) -> str:

    '''
    Method to insert records into table
    Inputs: a list of tuples containing records to be inserted.
    e.g. record = {'id': 1,
                   'registered_doctor': 'doctor1',
                   'has_asthma': False,
                   'has_registered_disability': False}
    Note1: the tuple must be ordered in the same order as the columns.
    Note2: every column must have an associated entry in the tuple.
    '''

    # initialise DB
    db = DatabaseQueries()

    # check id component exists in id_register table

    id = record['id']

    id_has_been_registered = db.is_id_in_use(id)

    if id_has_been_registered:

        logger.info(f'Records to be inserted are: {record}')

        insert_log = record

        # check in health table for already existing id
        id_exists = db.id_exists_health_table(record)

        # if no duplicate then insert records
        if not id_exists:

            db.insert_health_records(record)

            # update insert log
            insert_log['successful'] = True

            logger.info(f'Successfully inserted records, logging insert: {insert_log}')

            # write log to file
            with open(HEALTH_TABLE_INSERT_LOG, mode = 'a+', encoding='utf-8') as f:
                f.write(f'{insert_log} \r\n')

            output = 'insert successful'

        # if duplicate found do nothing
        else:
            logger.info(f'Duplicate found in health table')

            # update insert log
            insert_log['successful'] = False

            logger.info(f'logging unsuccessful insert: {insert_log}')

            # write log to file
            with open(HEALTH_TABLE_INSERT_LOG, mode = 'a+', encoding='utf-8') as f:
                f.write(f'{insert_log} \r\n')

            output = 'insert unsuccessful'

    else:
        logger.info('id is not registered in id_register table')
        output = None

    return output

def health_table_update(updated_by : int, id_to_update : int, records_to_update : Dict[str, Any]) -> str:

    '''
    Method to update records in health records table
    Inputs: id_to_update - the id of the record to be updated,
            records_to_update - dictionary of updated records.
                                if field is not to be updated then must contain None e.g.
                                {'registered_doctor': 'doctor2',
                                 'has_asthma': None,
                                 'has_registered_disability': True}
    '''

    # initialise DB

    db = DatabaseQueries()

    # construct basic update log
    update_log = {'updated_by': updated_by,
                  'record_updated': id_to_update,
                  'updated_to': records_to_update}

    logger.info(f'Updating the following records: {records_to_update}')

    # try to update record
    try:

        db.update_health_record(id_to_update, records_to_update)

        logger.info(f'Successfully updated records')

        # update log
        update_log['successful'] = True

        logger.info(f'Logging update: {update_log}')

        # write log to file
        with open(HEALTH_TABLE_UPDATE_LOG, mode = 'a+', encoding='utf-8') as f:
            f.write(f'{update_log} \r\n')

        output = 'update successfully completed'

    # if update is unsuccessful
    except:

        # update log
        update_log['successful'] = False

        logger.info(f'Failed to update table, logging failed update: {update_log}')

        with open(HEALTH_TABLE_UPDATE_LOG, mode = 'a+', encoding='utf-8') as f:
            f.write(f'{update_log} \r\n')

        output = 'update failed'

    return output

# TODO: eventually we want to put limits on the query e.g. only one column at a time.

def health_table_query(queried_by : int, query: str) -> None:

    db = DatabaseQueries()

    query_log = {'queried_by': queried_by,
                 'query': query}

    try:
        query_output = db.query_health_table(query)

        query_log['successful'] = True

        with open(HEALTH_TABLE_QUERY_LOG, mode = 'a+', encoding='utf-8') as f:
            f.write(f'{query_log} \r\n')

    except:
        logger.info(f'query failed')

        query_log['successful'] = False

        with open(HEALTH_TABLE_QUERY_LOG, mode = 'a+', encoding='utf-8') as f:
            f.write(f'{query_log} \r\n')

        query_output = None

    return query_output

if __name__ == '__main__':

    user_record = {
                    "name": "david",
                }

    #registration(user_record)
    #sys.exit()
    #print(generate_user_id(user_record))


    # --------------
    # basic functionality tests
    # --------------

    record = {'id': 1,
              'registered_doctor': 'doctor1',
              'has_asthma': False,
              'has_registered_disability': False}

    # insert records, first time should insert, second should thrown error

    #print('inserting record, should be successful')
    print(health_table_insert(record))
    sys.exit()
    #print('inserting duplicate record test')
    #print(health_table_insert(record))

    record_to_update = {'registered_doctor': 'doctor2',
                         'has_asthma': None,
                         'has_registered_disability': True}

    record_to_update_fail = {'registered_doctor': 'doctor2',
                             'has_diabeties': None,
                             'has_registered_disability': True}

    #print('updating health table, should be successful')
    #health_table_update(1, 1, record_to_update)
    #print('updating health table log, should be unsuccessful')
    #health_table_update(1, 1, record_to_update_fail)

    query = "SELECT * FROM health_table WHERE idd = '3';"

    print('quering health table')
    print(health_table_query(1, query))
