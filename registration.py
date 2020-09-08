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


logging.basicConfig(
    format="%(name)s - %(asctime)s - %(message)s",
    datefmt="%d-%b-%y %H:%M:%S",
    level=logging.INFO,
)
logger = logging.getLogger("Registration")

class Registration(DatabaseQueries):

    REGISTRATION_RECORD_LOG = "registration_record_log.json"
    HEALTH_TABLE_INSERT_LOG = "health_table_insert_log.json"
    HEALTH_TABLE_QUERY_LOG = "health_table_query_log.json"
    HEALTH_TABLE_UPDATE_LOG = "health_table_update_log.json"
    SIZE_OF_ID_SPACE = 10000

    def __init__(self):
        self.logger = logging.getLogger('Registration')

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


    def generate_user_id(self, name : str) -> int:

        record ={"name": name}

        db = DatabaseQueries()

        with open("json_validators/register_inputs.json", "r") as f:
            registation_input_schema = json.load(f)

        try:

            validate(instance=record, schema=registation_input_schema)
            logger.info(f"record is valid input")

        except:
            logger.info(f"inputed records are not valid: {record}")
            return None

        id = randrange(self.SIZE_OF_ID_SPACE)

        # check this ID is not already in use.
        id_in_use = db.is_id_in_use(id)

        if id_in_use:
            logger.info(f"id is already in use, please rerun")
            output = None
        else:
            output = id

        return output


    def registration_insert_record(self, id: int, name : str) -> str:

        record = {"name": name}

        db = DatabaseQueries()

        id_in_use = db.is_id_in_use(id)

        record["id"] = id
        registration_insert_log = record

        if id_in_use:
            logger.info(f"id is already in use")
            registration_insert_log["successful"] = False
            logger.info(f"logging record insertion attempt: {registration_insert_log}")

            # write log to file
            with open(self.REGISTRATION_RECORD_LOG, mode="a+", encoding="utf-8") as f:
                f.write(f"{registration_insert_log} \r\n")
        else:
            db.register_insert_record(record)
            logger.info(f"successfully inputted record into id_register table")
            registration_insert_log["successful"] = True

            logger.info(f"logging record insertion attempt: {registration_insert_log}")

            # write log to file
            with open(self.REGISTRATION_RECORD_LOG, mode="a+", encoding="utf-8") as f:
                f.write(f"{registration_insert_log} \r\n")
        return


    def registration(self, name : str) -> str:

        logger.info(f"generating user id")

        id = self.generate_user_id(name)

        if id is not None:
            logger.info(f"inserting record into database")
            self.registration_insert_record(id, name)
            logger.info(f"successfully registered name: {name} with id: {id}")
        else:
            logger.info(f"no id present (id is None)")

        return



if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument('-n', '--name',
            dest='name',
            help='Name of user to be regstered')
    parser.add_argument('-id', '--id',
            dest='id',
            help='ID of user')
    parser.add_argument('-t', '--test',
            dest='test',
            default=False,
            help='Test id already in db')

    args = parser.parse_args()

    name = args.name
    id = args.id
    test = args.test

    if not test:
        r = Registration()
        r.registration(name)

    else:
        r = Registration()
        r.registration_insert_record(id, name)
    # We will now walk through the functionalities of the system by populating the db_tables
    # The commands are currenttly commented, I recommed as you go down the page you recomment them so only the part of interest is being run.

    # -------------
    # Registration
    # -------------

    # First let us register our first user.
    # We will use the registration method, this in turn calls generate_user_id and then register_insert_record
    # The process is as follows:
    #    1. the user inputs their name, we create the record: 'record' - record = {"name": 'david'} - consitsting of their name
    #    2. a random unique id (integer) is assignedd to them, we check against the db to ensure it has not already been assigned
    #    3. the updated user record - {"id": id, "name": name} is inserte into the database

    # RUN: python registration.py -n dave

    # RUN: python registration.py -n dave -id 984 -t True
