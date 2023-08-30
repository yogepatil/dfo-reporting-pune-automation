# @Author  : Yogesh Patil
# @Time    : 28/08/2023
# @File    : test_017_message_tags_refined_view_count_validation.py

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


# ----------------------------- REFINED VIEW - ATTACHED_ONLY_MESSAGE_TAG_VIEW_V001 COUNT ----------------------------#
def test_refined_attached_only_message_tag_view_v001_record_validation():
    registration_type = 'REFINED_VIEW'
    table_name = 'ATTACHED_ONLY_MESSAGE_TAG_VIEW_V001'
    field_name = 'ATTACHED_ONLY_MESSAGE_TAG_VIEW_V001'

    im.Data_Validation(base_dir, sql_base_dir, failed_files_dir, registration_type, table_name, field_name, connection,
                       log).check_records_count()


# ------------------------------- END OF SNOWFLAKE CONNECTION ------------------------------#
def test_close_snowflake_connection():
    connection.close()
