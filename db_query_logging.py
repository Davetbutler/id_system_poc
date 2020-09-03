import json
import os
import sys
import random
import logging
import argparse
import ast
import datetime
from typing import List, Dict, Any
from database_insert import DatabaseQueries


logging.basicConfig(format='%(name)s - %(asctime)s - %(message)s',
        datefmt='%d-%b-%y %H:%M:%S', level=logging.INFO)
logger = logging.getLogger('database operations')

HEALTH_TABLE_INSERT_LOG = 'health_table_insert_log.json'
HEALTH_TABLE_QUERY_LOG = 'health_table_query_log.json'
HEALTH_TABLE_UPDATE_LOG = 'health_table_update_log.json'

def health_table_insert(record: Dict) -> str:

    '''
    Method to insert records into table
    Inputs: a list of tuples containing records to be inserted.
    Note1: the tuple must be ordered in the same order as the columns.
    Note2: every column must have an associated entry in the tuple.
    '''

    # initialise DB
    db = DatabaseQueries()

    logger.info(f'Records to be inserted are: {record}')

    insert_log = record

    # check in health table for already existing id
    no_duplicate = db.health_table_deduplication(record)

    # if no duplicate then insert records
    if no_duplicate:

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

    # --------------
    # basic functionality tests
    # --------------

    # 1. Delete from all tables

    record = {'id': 1,
              'registered_doctor': 'doctor1',
              'has_asthma': False,
              'has_registered_disability': False}

    # insert records, first time should insert, second should thrown error

    #print('inserting record, should be successful')
    #print(health_table_insert(record))

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
