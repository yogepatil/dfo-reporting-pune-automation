# @Author  : Yogesh Patil
# @Date    : 21/03/2023
# @File    : test_008_visit_conversion_fact_refiner_validation.py

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
# Code change by: @Yogesh Patil
# Date of change: 16/06/2023
connection = snowfl.Snow_DB_Connection(base_dir, environment, failed_files_dir, log).connecting_to_snowflake_db()
# Deleting failed files folder and re-creating for new run
shutil.rmtree(failed_files_dir, ignore_errors=True)
os.mkdir(failed_files_dir)


# ------------------------------- REFINED VISIT_CONVERSION_FACT VALIDATION ------------------------------#
def test_visit_conversion_type_name_records_validation_01():
    registration_type = 'REFINED'
    table_name = 'VISIT_CONVERSION_FACT'
    field_name = 'CONVERSION_TYPE_NAME'

    im.Data_Validation(base_dir, sql_base_dir, failed_files_dir, registration_type, table_name, field_name, connection,
                       log).check_matching_records()


def test_visit_conversion_timestamp_records_validation_02():
    registration_type = 'REFINED'
    table_name = 'VISIT_CONVERSION_FACT'
    field_name = 'CONVERSION_TIMESTAMP'

    im.Data_Validation(base_dir, sql_base_dir, failed_files_dir, registration_type, table_name, field_name, connection,
                       log).check_matching_records()


def test_visit_conversion_amount_records_validation_03():
    registration_type = 'REFINED'
    table_name = 'VISIT_CONVERSION_FACT'
    field_name = 'CONVERSION_AMOUNT'

    im.Data_Validation(base_dir, sql_base_dir, failed_files_dir, registration_type, table_name, field_name, connection,
                       log).check_matching_records()


def test_visit_conversion_seconds_records_validation_04():
    registration_type = 'REFINED'
    table_name = 'VISIT_CONVERSION_FACT'
    field_name = 'CONVERSION_SECONDS'

    im.Data_Validation(base_dir, sql_base_dir, failed_files_dir, registration_type, table_name, field_name, connection,
                       log).check_matching_records()


def test_visitor_duplicate_visit_id_allowed_record_validation_05():
    registration_type = 'REFINED'
    table_name = 'VISIT_CONVERSION_FACT'
    field_name = 'DUPLICATE_RECORD_VALIDATION'

    im.Data_Validation(base_dir, sql_base_dir, failed_files_dir, registration_type, table_name, field_name, connection,
                       log).check_duplicate_records()


# ----------------------------  END OF SNOWFLAKE CONNECTION ----------------------------#
def test_close_snowflake_connection():
    connection.close()