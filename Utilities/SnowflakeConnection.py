# @Author  : Yogesh Patil
# @Date    : 16/06/2023
# @File    : SnowflakeConnection.py

import os
import sys
import json
from Utilities import Utility as im, LoggerFile as log


# ---------------------------- Connecting to the Snowflake Database ----------------------------#
class Snow_DB_Connection:
    def __init__(self, base_dir, environment, failed_files_dir, log):
        self.base_dir = base_dir
        self.environment = environment
        self.failed_files_dir = failed_files_dir
        self.log = log

    def connecting_to_snowflake_db(self):
        snowflake_connection_file = open(os.path.join(self.base_dir, "Snowflake_connection.json"))
        read_connection_field = json.load(snowflake_connection_file)

        for item in read_connection_field.keys():
            if item == self.environment:
                username = read_connection_field[self.environment][0]['username']
                password = read_connection_field[self.environment][0]['password']
                account = read_connection_field[self.environment][0]['account']
                warehouse = read_connection_field[self.environment][0]['warehouse']
                database = read_connection_field[self.environment][0]['database']
                schema = read_connection_field[self.environment][0]['schema']
                role = read_connection_field[self.environment][0]['role']
        try:
            connection_obj = im.Snowflake_Connection(username, password, account,
                                                     warehouse, database, schema,
                                                     role, self.base_dir, log)
            connection = connection_obj.develope_connection()
            return connection
        except:
            connection_obj.connection_fail()
            sys.exit(-1)
