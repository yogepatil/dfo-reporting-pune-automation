# @Author  : Yogesh Patil
# @Date    : 28/08/2023
# @File    : test_014_message_tags_refined_table_schema_validation.py

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
connection = snowfl.Snow_DB_Connection(base_dir, environment, failed_files_dir, log).connecting_to_snowflake_db()


# ----------------------------  REFINED TABLE: ATTACHED_ONLY_MESSAGE_TAG ----------------------------#
def test_attached_only_message_tag_refined_table_schema():
    registration_type = 'REFINED'
    namespace = 'DFO'
    table_name = 'ATTACHED_ONLY_MESSAGE_TAG'
    sql_query = 'Describe Table "DATAHUB"."DFO_REFINED"."{}"'.format(table_name)

    im.Schema_Validation(schema_base_dir, registration_type, table_name, namespace, sql_query, connection, log) \
        .schema_validation()


# ----------------------------   END OF SNOWFLAKE CONNECTION ----------------------------#
def test_close_snowflake_connection():
    connection.close()