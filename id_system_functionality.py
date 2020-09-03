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
logger = logging.getLogger("database operations")

REGISTRATION_RECORD_LOG = "registration_record_log.json"
HEALTH_TABLE_INSERT_LOG = "health_table_insert_log.json"
HEALTH_TABLE_QUERY_LOG = "health_table_query_log.json"
HEALTH_TABLE_UPDATE_LOG = "health_table_update_log.json"
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


def generate_user_id(record: Dict[str, str]) -> int:

    db = DatabaseQueries()

    with open("json_validators/register_inputs.json", "r") as f:
        registation_input_schema = json.load(f)

    try:

        validate(instance=record, schema=registation_input_schema)
        logger.info(f"record is valid input")

    except:

        logger.info(f"inputed records are not valid: {record}")
        return None

    id = randrange(SIZE_OF_ID_SPACE)

    # check this ID is not already in use.
    id_in_use = db.is_id_in_use(id)

    if id_in_use:
        logger.info(f"id is already in use, please rerun")
        output = None
    else:
        output = id

    return output


def registration_insert_record(id: int, record: Dict[str, str]) -> str:

    db = DatabaseQueries()

    id_in_use = db.is_id_in_use(id)

    record["id"] = id
    registration_insert_log = record

    if id_in_use:
        logger.info(f"id is already in use")
        registration_insert_log["successful"] = False
        logger.info(f"logging record insertion attempt: {registration_insert_log}")

        # write log to file
        with open(REGISTRATION_RECORD_LOG, mode="a+", encoding="utf-8") as f:
            f.write(f"{registration_insert_log} \r\n")
    else:
        db.register_insert_record(id, record)
        logger.info(f"successfully inputted record into id_register table")
        registration_insert_log["successful"] = True

        logger.info(f"logging record insertion attempt: {registration_insert_log}")

        # write log to file
        with open(REGISTRATION_RECORD_LOG, mode="a+", encoding="utf-8") as f:
            f.write(f"{registration_insert_log} \r\n")
    return


def registration(record: Dict[str, str]) -> str:

    logger.info(f"generating user id")
    id = generate_user_id(record)

    if id is not None:
        logger.info(f"inserting record into database")
        registration_insert_record(id, record)
    else:
        logger.info(f"no id present (id is None)")

    return


# --------------
# Health table operations
# --------------


def health_table_insert(record: Dict) -> str:

    """
    Method to insert records into table
    Inputs: a list of tuples containing records to be inserted.
    e.g. record = {'id': 1,
                   'registered_doctor': 'doctor1',
                   'has_asthma': False,
                   'has_registered_disability': False}
    Note1: the tuple must be ordered in the same order as the columns.
    Note2: every column must have an associated entry in the tuple.
    """

    # initialise DB
    db = DatabaseQueries()

    # initialise log
    insert_log = record

    # check id component exists in id_register table

    id = record["id"]

    id_has_been_registered = db.is_id_in_use(id)

    if id_has_been_registered:

        logger.info(f"Records to be inserted are: {record}")

        # check in health table for already existing id
        id_exists = db.id_exists_health_table(record)

        # if no duplicate then insert records
        if not id_exists:

            db.insert_health_records(record)

            # update insert log
            insert_log["successful"] = True

            logger.info(f"Successfully inserted records, logging insert: {insert_log}")

            # write log to file
            with open(HEALTH_TABLE_INSERT_LOG, mode="a+", encoding="utf-8") as f:
                f.write(f"{insert_log} \r\n")

            output = "insert successful"

        # if duplicate found do nothing
        else:
            logger.info(f"Duplicate found in health table")

            # update insert log
            insert_log["successful"] = False

            logger.info(
                f"logging unsuccessful insert: {insert_log} in {HEALTH_TABLE_INSERT_LOG}"
            )

            # write log to file
            with open(HEALTH_TABLE_INSERT_LOG, mode="a+", encoding="utf-8") as f:
                f.write(f"{insert_log} \r\n")

            output = "insert unsuccessful"

    else:
        logger.info("id is not registered in id_register table")

        # update insert log
        insert_log["successful"] = False

        logger.info(
            f"logging unsuccessful insert: {insert_log} in {HEALTH_TABLE_INSERT_LOG}"
        )
        # write log to file
        with open(HEALTH_TABLE_INSERT_LOG, mode="a+", encoding="utf-8") as f:
            f.write(f"{insert_log} \r\n")
        output = None

    return output


def health_table_update(
    updated_by: int, id_to_update: int, records_to_update: Dict[str, Any]
) -> str:

    """
    Method to update records in health records table
    Inputs: id_to_update - the id of the record to be updated,
            records_to_update - dictionary of updated records.
                                if field is not to be updated then must contain None e.g.
                                {'registered_doctor': 'doctor2',
                                 'has_asthma': None,
                                 'has_registered_disability': True}
    """

    # initialise DB

    db = DatabaseQueries()

    # construct basic update log
    update_log = {
        "updated_by": updated_by,
        "record_updated": id_to_update,
        "updated_to": records_to_update,
    }

    logger.info(f"Attempting to update the following records: {records_to_update}")

    # check to see if the id is in the table

    id_exists = db.id_exists_health_table(id)

    if id_exists:

        # try to update record
        try:

            db.update_health_record(id_to_update, records_to_update)

            logger.info(f"Successfully updated records")

            # update log
            update_log["successful"] = True

            logger.info(f"Logging update: {update_log} in {HEALTH_TABLE_UPDATE_LOG}")

            # write log to file
            with open(HEALTH_TABLE_UPDATE_LOG, mode="a+", encoding="utf-8") as f:
                f.write(f"{update_log} \r\n")

            output = "update successfully completed"

        # if update is unsuccessful
        except:

            # update log
            update_log["successful"] = False

            logger.info(
                f"Failed to update table, logging failed update: {update_log} in {HEALTH_TABLE_UPDATE_LOG}"
            )

            with open(HEALTH_TABLE_UPDATE_LOG, mode="a+", encoding="utf-8") as f:
                f.write(f"{update_log} \r\n")

            output = "update failed"
    else:

        logger.info(f"id: {id} does not exist in health_table")
        update_log["successful"] = False

        logger.info(
            f"Failed to update table, logging failed update: {update_log} in {HEALTH_TABLE_UPDATE_LOG}"
        )

        with open(HEALTH_TABLE_UPDATE_LOG, mode="a+", encoding="utf-8") as f:
            f.write(f"{update_log} \r\n")

        output = "update failed"

    return output


# TODO: eventually we want to put limits on the query e.g. only one column at a time.


def health_table_query(queried_by: str, password: str, query: str):

    db = DatabaseQueries()

    query_log = {"queried_by": queried_by, "query": query}

    access_granted = db.health_dept_access_granted(queried_by, password)

    if access_granted:
        logger.info("access granted, query being processed now")
    else:
        logger.info("access not granted to make this query")
        return None

    try:
        query_output = db.query_health_table(query)

        query_log["successful"] = True

        with open(HEALTH_TABLE_QUERY_LOG, mode="a+", encoding="utf-8") as f:
            f.write(f"{query_log} \r\n")

    except:
        logger.info(f"query failed")

        query_log["successful"] = False

        with open(HEALTH_TABLE_QUERY_LOG, mode="a+", encoding="utf-8") as f:
            f.write(f"{query_log} \r\n")

        query_output = None

    return query_output


if __name__ == "__main__":

    # Follow the README and create the three tables given in db_tables.sql

    # We will now walk through the functionalities of the system by populating the db_tables
    # The commands are currenttly commented, I recommed as you go down the page you recomment them so only the part of interest is being run.

    # -------------
    # Registration
    # -------------

    # First let us register our first user.
    # We will use the registration method, this in turn calls generate_user_id and then register_insert_record
    # The process is as follows:
    #    1. the user inputs their 'record' - record = {"name": 'david'} - consitsting of their name
    #    2. a random unique id (integer) is assignedd to them, we check against the db to ensure it has not already been assigned
    #    3. the updated user record - {"id": id, "name": name} is inserte into the database

    initial_user_record = {"name": "david"}

    # registration(initial_user_record)

    # If you run SELECT * FROM id_register; you should see that the id_register table has its first entry

    # Now let us sanity check the system. We should not be able to input the same id again for a different user
    # id is the primary key of the data base, therefore this is not possible in any case, however the register_insert_record
    # method does not allow for this. Run the following to see this in action.
    # Note you will need to change this for the id that is in the table (they are randomly generated)

    id = 7459
    # registration_insert_record(id, initial_user_record)

    # hopefully you will have seen that things are being 'logged' into a json file.
    # open the relevant files and you should see that insertions are being logged as successes of failures.

    # -------------
    # Insert into the health table
    # -------------

    # In this proof of concept we have a 'health department' who stores health data
    # We first want to input some data into this table
    # The one rule is that the user must be registered in id_register
    # Let us first try to enter a record into the health table where the id not registered.
    # NB: if id = 1 happens to be in the table this example wont make much sense!
    initial_health_record_fail = {
        "id": 1,
        "registered_doctor": "doctor1",
        "has_asthma": False,
        "has_registered_disability": False,
    }

    # health_table_insert(initial_health_record_fail)

    # Now let us insert a valid health record in the health table
    # NB: you will have to change the id to the id that is in the id_register table

    initial_health_record = {
        "id": 7459,
        "registered_doctor": "doctor1",
        "has_asthma": False,
        "has_registered_disability": False,
    }

    # health_table_insert(initial_health_record)

    # If you check the health_table you will see the inserted record

    # Next, we may want to update a health record. To do this we use the health_table_update method
    # To do this we create the updated record as follows, all fields must be present, but can be null
    updated_record = {
        "registered_doctor": "doctor243",
        "has_asthma": None,
        "has_registered_disability": True,
    }

    # The inputs to the health_table_update method are:
    # 1. updated_by: this can largely be ignored, the idea is for future functionality to allow tracking of each entity (think govt dept)
    # 2. id_to_update: this is the id that is to be updated
    # 3. records_to_update: the dictionary given above.

    # health_table_update(1, 7459, updated_record)

    # If you check the health_table you should see that the record has been updated.

    # Let us try to update with a record with incoorrect keys
    updated_record_fail = {
        "registered_doctor": "doctor2",
        "has_diabeties": None,
        "has_registered_disability": True,
    }

    # health_table_update(1, 7459, updated_record_fail)

    # We see that the update is not sucessful, and is logged.

    # Let us also check that the update is unsuccessful if the id does not exist in the health_table
    # Again we assume that id = 1 is not in the table

    # health_table_update(1, 1, updated_record_fail)

    # Now we move onto how another govt department may request access and be able too query another database
    # To do this we use the following method, we assume the welfare dept wants to query the health dept database (health_table):
    # 1. The welfare dept must have some 'aggreement' with the health dept to be allowed to query their database/
    # 2. Here we abstract away what this process acutally looks like, but it may involve agreements on the amount of queries at one time, or the type of datathat is alloowed to be queried
    # 3. We abstract this process with the table health_dept_access. Here we list the names and passwords of the enties that have been granted access.
    # 4. We then only allow those who have been granted access to query the database.

    # If you have not aleady populated the health_dept_access run the following in psql:
    # INSERT INTO health_dept_access (name, password) VALUES ('welfare_dept', 'welfare');

    # Now let us use these credentials to query the health_table.
    #  First let us see what a successful query looks like, change the id to the one in your table

    id = 7459
    query = f"SELECT * FROM health_table WHERE id = {id};"
    print(health_table_query("welfare_dept", "welfare", query))

    # Let us see what happens when the wrong password or wrong user name

    print(health_table_query("wrong_user_name", "welfare", query))

    print(health_table_query("welfare_dept", "wrong password", query))

    # There are likley to be many bugs for edge cases, but hopefully this has given you an idea on what the functionality is at present,
    # and gives enough for you to come to a conclusion about whether this is useful or not to expand on.
