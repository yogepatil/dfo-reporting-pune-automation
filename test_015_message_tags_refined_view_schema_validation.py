# @Author  : Yogesh Patil
# @Date    : 28/08/2023
# @File    : test_015_message_tags_refined_view_schema_validation.py

import os
import shutil
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

# Deleting failed files folder and re-creating for new run
shutil.rmtree(failed_files_dir, ignore_errors=True)
os.mkdir(failed_files_dir)


# ----------------------------  REFINED VIEW - ATTACHED_ONLY_MESSAGE_TAG_VIEW_V001 ----------------------------#
def test_refined_attached_only_message_tab_v001_schema():
    registration_type = 'REFINED_VIEW'
    namespace = 'VIEW'
    view_name = 'ATTACHED_ONLY_MESSAGE_TAG_VIEW_V001'
    sql_query = 'DESCRIBE VIEW "DATAHUB"."DFO_REFINED"."{}"'.format(view_name)

    im.Schema_Validation(schema_base_dir, registration_type, view_name, namespace, sql_query, connection, log) \
        .schema_validation()


# ----------------------------  END OF SNOWFLAKE CONNECTION ----------------------------#
def test_close_snowflake_connection():
    connection.close()