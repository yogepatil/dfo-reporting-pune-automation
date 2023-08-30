# @Author  : Yogesh Patil
# @Date    : 16/03/2023
# @File    : test_007_visitor_dim_refiner_validation.py

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


# ------------------------------- REFINED VISITOR_DIM VALIDATION ------------------------------#
def test_visitor_browser_name_records_validation_01():
    registration_type = 'REFINED'
    table_name = 'VISITOR_DIM'
    field_name = 'VISITOR_BROWSER_NAME'

    im.Data_Validation(base_dir, sql_base_dir, failed_files_dir, registration_type, table_name, field_name, connection,
                       log).check_matching_records()


def test_visitor_browser_version_records_validation_02():
    registration_type = 'REFINED'
    table_name = 'VISITOR_DIM'
    field_name = 'VISITOR_BROWSER_VERSION'

    im.Data_Validation(base_dir, sql_base_dir, failed_files_dir, registration_type, table_name, field_name, connection,
                       log).check_matching_records()


def test_visitor_browser_lang_records_validation_03():
    registration_type = 'REFINED'
    table_name = 'VISITOR_DIM'
    field_name = 'VISITOR_BROWSER_LANGUAGE'

    im.Data_Validation(base_dir, sql_base_dir, failed_files_dir, registration_type, table_name, field_name, connection,
                       log).check_matching_records()


def test_visitor_device_type_records_validation_04():
    registration_type = 'REFINED'
    table_name = 'VISITOR_DIM'
    field_name = 'VISITOR_DEVICE_TYPE'

    im.Data_Validation(base_dir, sql_base_dir, failed_files_dir, registration_type, table_name, field_name, connection,
                       log).check_matching_records()


def test_visitor_device_type_records_validation_05():
    registration_type = 'REFINED'
    table_name = 'VISITOR_DIM'
    field_name = 'VISITOR_APPLICATION_TYPE_NAME'

    im.Data_Validation(base_dir, sql_base_dir, failed_files_dir, registration_type, table_name, field_name, connection,
                       log).check_matching_records()


def test_visitor_OS_name_records_validation_06():
    registration_type = 'REFINED'
    table_name = 'VISITOR_DIM'
    field_name = 'VISITOR_OS_NAME'

    im.Data_Validation(base_dir, sql_base_dir, failed_files_dir, registration_type, table_name, field_name, connection,
                       log).check_matching_records()


def test_visitor_OS_version_records_validation_07():
    registration_type = 'REFINED'
    table_name = 'VISITOR_DIM'
    field_name = 'VISITOR_OS_VERSION'

    im.Data_Validation(base_dir, sql_base_dir, failed_files_dir, registration_type, table_name, field_name, connection,
                       log).check_matching_records()


def test_visitor_address_records_validation_08():
    registration_type = 'REFINED'
    table_name = 'VISITOR_DIM'
    field_name = 'VISITOR_IP_ADDRESS'

    im.Data_Validation(base_dir, sql_base_dir, failed_files_dir, registration_type, table_name, field_name, connection,
                       log).check_matching_records()


def test_visitor_location_records_validation_09():
    registration_type = 'REFINED'
    table_name = 'VISITOR_DIM'
    field_name = 'VISITOR_LOCATION_NAME'

    im.Data_Validation(base_dir, sql_base_dir, failed_files_dir, registration_type, table_name, field_name, connection,
                       log).check_matching_records()


def test_visitor_country_records_validation_10():
    registration_type = 'REFINED'
    table_name = 'VISITOR_DIM'
    field_name = 'VISITOR_COUNTRY_NAME'

    im.Data_Validation(base_dir, sql_base_dir, failed_files_dir, registration_type, table_name, field_name, connection,
                       log).check_matching_records()


# def test_visitor_createdtimestamp_validation_11():
#     registration_type = 'REFINED'
#     table_name = 'VISITOR_DIM'
#     field_name = 'CREATED_TIMESTAMP'
#
#     im.Data_Validation(base_dir, sql_base_dir, failed_files_dir, registration_type, table_name, field_name, connection,
#                        log).check_matching_records()


def test_visitor_duplicate_record_validation_12():
    registration_type = 'REFINED'
    table_name = 'VISITOR_DIM'
    field_name = 'DUPLICATE_RECORD_VALIDATION'

    im.Data_Validation(base_dir, sql_base_dir, failed_files_dir, registration_type, table_name, field_name, connection,
                       log).check_duplicate_records()


# ----------------------------  END OF SNOWFLAKE CONNECTION ----------------------------#
def test_close_snowflake_connection():
    connection.close()
