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
logger = logging.getLogger("Health Service")

class HealthServiceClient(DatabaseQueries):

    HEALTH_TABLE_INSERT_LOG = "logs/health_table_insert_log.json"
    HEALTH_TABLE_QUERY_LOG = "logs/health_table_query_log.json"
    HEALTH_TABLE_UPDATE_LOG = "logs/health_table_update_log.json"

    def __init__(self):
        self.logger = logging.getLogger('Health Service')

    def health_table_insert(self, id : int, registered_doctor : str, has_asthma : bool, has_registered_disability : bool) -> str:

        """
        Method to insert records into table
        Inputs: user atttributes tto be inserted
        Note1: the tuple must be ordered in the same order as the columns.
        Note2: every column must have an associated entry in the tuple.
        """

        record = {'id': id,
                  'registered_doctor': registered_doctor,
                  'has_asthma': has_asthma,
                  'has_registered_disability': has_registered_disability}

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
            id_exists = db.id_exists_health_table(id)

            # if no duplicate then insert records
            if not id_exists:

                db.insert_health_records(record)

                # update insert log
                insert_log["successful"] = True

                logger.info(f"Successfully inserted records, logging insert: {insert_log}")

                # write log to file
                with open(self.HEALTH_TABLE_INSERT_LOG, mode="a+", encoding="utf-8") as f:
                    f.write(f"{insert_log} \r\n")

                output = "insert successful"

            # if duplicate found do nothing
            else:
                logger.info(f"Duplicate found in health table")

                # update insert log
                insert_log["successful"] = False

                logger.info(
                    f"logging unsuccessful insert: {insert_log} in {self.HEALTH_TABLE_INSERT_LOG}"
                )

                # write log to file
                with open(self.HEALTH_TABLE_INSERT_LOG, mode="a+", encoding="utf-8") as f:
                    f.write(f"{insert_log} \r\n")

                output = "insert unsuccessful"

        else:
            logger.info("id is not registered in id_register table")

            # update insert log
            insert_log["successful"] = False

            logger.info(
                f"logging unsuccessful insert: {insert_log} in {self.HEALTH_TABLE_INSERT_LOG}"
            )
            # write log to file
            with open(self.HEALTH_TABLE_INSERT_LOG, mode="a+", encoding="utf-8") as f:
                f.write(f"{insert_log} \r\n")
            output = None

        return output


    def health_table_update(self, updated_by: int, id_to_update: int, doctor : str = None, has_asthma : bool =  None, has_registered_disability : bool = None) -> str:

        """
        Method to update records in health records table
        Inputs: updated_by - currently a redundant input
                id_to_update - the id of the record to be updated,
                records_to_update, if None then do not get updated.

        """

        records_to_update  = {'registered_doctor': doctor,
                              'has_asthma': has_asthma,
                              'has_registered_disability': has_registered_disability}

        # construct basic update log
        update_log = {
            "updated_by": updated_by,
            "record_updated": id_to_update,
            "updated_to": records_to_update,
            "updated_at": datetime.datetime.now()
        }

        inputs_all_none = (doctor is None) and (has_asthma is None) and (has_registered_disability is None)

        if inputs_all_none:

            logger.info('All inputs are None so nothing to update')

            logger.info(f"Logging update: {update_log} in {self.HEALTH_TABLE_UPDATE_LOG}")

            # update log
            update_log["successful"] = False
            # write log to file
            with open(self.HEALTH_TABLE_UPDATE_LOG, mode="a+", encoding="utf-8") as f:
                f.write(f"{update_log} \r\n")

            return None


        # initialise DB

        db = DatabaseQueries()

        logger.info(f"Attempting to update the following records: {records_to_update}")

        # check to see if the id is in the table

        id_exists = db.id_exists_health_table(id_to_update)

        if id_exists:

            # try to update record
            try:

                db.update_health_record(id_to_update, records_to_update)

                logger.info(f"Successfully updated records")

                # update log
                update_log["successful"] = True

                logger.info(f"Logging update: {update_log} in {self.HEALTH_TABLE_UPDATE_LOG}")

                # write log to file
                with open(self.HEALTH_TABLE_UPDATE_LOG, mode="a+", encoding="utf-8") as f:
                    f.write(f"{update_log} \r\n")

                output = "update successfully completed"

            # if update is unsuccessful
            except:

                # update log
                update_log["successful"] = False

                logger.info(
                    f"Failed to update table, logging failed update: {update_log} in {self.HEALTH_TABLE_UPDATE_LOG}"
                )

                with open(self.HEALTH_TABLE_UPDATE_LOG, mode="a+", encoding="utf-8") as f:
                    f.write(f"{update_log} \r\n")

                output = "update failed"
        else:

            logger.info(f"id: {id_to_update} does not exist in health_table")
            update_log["successful"] = False

            logger.info(
                f"Failed to update table, logging failed update: {update_log} in {self.HEALTH_TABLE_UPDATE_LOG}"
            )

            with open(self.HEALTH_TABLE_UPDATE_LOG, mode="a+", encoding="utf-8") as f:
                f.write(f"{update_log} \r\n")

            output = "update failed"

        return output


    # TODO: eventually we want to put limits on the query e.g. only one column at a time.

    def health_table_query(self, queried_by: str, password: str, attribute : str, id : int):

        db = DatabaseQueries()

        query = f"SELECT id, {attribute} FROM health_table WHERE id = {id};"

        query_log = {"queried_by": queried_by,
                     "query": query,
                     "queried_at": datetime.datetime.now()}

        access_granted = db.health_dept_access_granted(queried_by, password)

        if access_granted:
            logger.info("access granted, query being processed now")
        else:
            logger.info("access not granted to make this query")
            return None

        try:
            query_output = db.query_health_table(query)

            query_log["successful"] = True

            with open(self.HEALTH_TABLE_QUERY_LOG, mode="a+", encoding="utf-8") as f:
                f.write(f"{query_log} \r\n")

        except:
            logger.info(f"query failed")

            query_log["successful"] = False

            with open(self.HEALTH_TABLE_QUERY_LOG, mode="a+", encoding="utf-8") as f:
                f.write(f"{query_log} \r\n")

            query_output = None

        return query_output



if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument('-n', '--name',
            dest='name',
            help='Name of user to be regstered')
    parser.add_argument('-id', '--id',
            dest='id',
            type=int,
            help='ID of user')
    parser.add_argument('-d', '--doctor',
            dest='doctor',
            help='name of doctor')
    parser.add_argument('-a', '--asthma',
            dest='asthma',
            type=bool,
            help='has_asthma attribute, bool')
    parser.add_argument('-dis', '--disability',
            dest='disability',
            type=bool,
            help='has_registered_disability attribute, bool')
    parser.add_argument('-qby', '--queried_by',
            dest='queried_by',
            help='organisation requesting query')
    parser.add_argument('-p', '--password',
            dest='password',
            help='password of organisation requesting query')
    parser.add_argument('-att', '--attribute',
            dest='attribute',
            help='attribute queried')
    parser.add_argument('-i', '--insert',
            dest='insert',
            default=False,
            help='Set to true to run health_table_insert function')
    parser.add_argument('-u', '--update',
            dest='update',
            default=False,
            help='Update health records')
    parser.add_argument('-q', '--query',
            dest='query',
            default=False,
            help='Query health table')


    args = parser.parse_args()

    name = args.name
    id = args.id
    doctor = args.doctor
    asthma = args.asthma
    disability = args.disability
    queried_by = args.queried_by
    attribute = args.attribute
    password = args.password
    insert = args.insert
    update = args.update
    query = args.query

    h = HealthServiceClient()

    if insert:
        h.health_table_insert(id, doctor, asthma, disability)

    if update:
        h.health_table_update(1, id, doctor, asthma, disability)

    if query:
        print(h.health_table_query(queried_by, password, attribute, id))
