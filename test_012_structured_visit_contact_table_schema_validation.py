# @Author  : Yogesh Patil
# @Date    : 15/05/2023
# @File    : test_012_structured_visit_contact_table_schema_validation.py

import os
from Utilities import Utility as im, LoggerFile as log, ReadProperties as rp, SnowflakeConnection as snowfl

log = log.BaseClass().getlogger()

base_dir = rp.ReadConfig.getSystemPath()
environment = rp.ReadConfig.getEnvironment()

# Do not change these parameter values
sql_base_dir = os.path.join(base_dir, "SQL")
schema_base_dir = os.path.join(base_dir, "SCHEMA")
failed_files_dir = os.path.join(base_dir, "Failed_Files")
line = '-' * 154

# ---------------------------- Connecting to the Snowflake Database ----------------------------#
# Code change by: @Yogesh Patil
# Date of change: 16/06/2023
connection = snowfl.Snow_DB_Connection(base_dir, environment, failed_files_dir, log).connecting_to_snowflake_db()


# ----------------------------  STRUCTURED VISIT_CONTACT TABLE ----------------------------#
def test_structured_visit_event_table_schema_validation():
    log.info('Execution Started On: {} Environment'.format(environment))
    registration_type = 'STRUCTURED'
    namespace = 'DFO'
    table_name = 'VISIT_CONTACT'
    sql_query = 'Describe Table "DATAHUB"."DFO_STRUCTURED"."{}"'.format(table_name)
    log.info("Execution Query: {}".format(sql_query))
    im.Schema_Validation(schema_base_dir, registration_type, table_name, namespace, sql_query, connection, log) \
        .schema_validation()


# ----------------------------  END OF SNOWFLAKE CONNECTION ----------------------------#
def test_close_snowflake_connection():
    connection.close()