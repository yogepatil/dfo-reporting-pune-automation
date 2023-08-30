# @Author  : Yogesh Patil
# @Date    : 04/04/2023
# @File    : test_009_SUITE_VIEW_count_validation.py

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


# ------------------------------- SUITE_VIEW - VISITOR_DIM_VIEW_V001 COUNT ------------------------------#
def test_suite_visitor_dim_view_v001_record_validation():
    registration_type = 'SUITE_VIEW'
    table_name = 'VISITOR_DIM_VIEW_V001'
    field_name = 'VISITOR_DIM_VIEW_V001'

    im.Data_Validation(base_dir, sql_base_dir, failed_files_dir, registration_type, table_name, field_name, connection,
                       log).check_records_count()


# ------------------------------- SUITE_VIEW - VISIT_CONVERSION_FACT_VIEW_V001 COUNT -------------------------#
def test_suite_visit_conversion_fact_view_v001_record_validation():
    registration_type = 'SUITE_VIEW'
    table_name = 'VISIT_CONVERSION_FACT_VIEW_V001'
    field_name = 'VISIT_CONVERSION_FACT_VIEW_V001'

    im.Data_Validation(base_dir, sql_base_dir, failed_files_dir, registration_type, table_name, field_name, connection,
                       log).check_records_count()


# ------------------------------- SUITE_VIEW - VISITOR_TAG_VIEW_V001 COUNT ------------------------------#
def test_suite_visitor_tag_view_v001_record_validation():
    registration_type = 'SUITE_VIEW'
    table_name = 'VISITOR_TAG_VIEW_V001'
    field_name = 'VISITOR_TAG_VIEW_V001'

    im.Data_Validation(base_dir, sql_base_dir, failed_files_dir, registration_type, table_name, field_name, connection,
                       log).check_records_count()


# ------------------------------- SUITE_VIEW - VISIT_ACTIVITY_FACT_VIEW_V001 COUNT ----------------------------#
def test_suite_visit_activity_fact_view_v001_record_validation():
    registration_type = 'SUITE_VIEW'
    table_name = 'VISIT_ACTIVITY_FACT_VIEW_V001'
    field_name = 'VISIT_ACTIVITY_FACT_VIEW_V001'

    im.Data_Validation(base_dir, sql_base_dir, failed_files_dir, registration_type, table_name, field_name, connection,
                       log).check_records_count()


# ------------------------------- SUITE_VIEW - VISIT_FACT_VIEW_V001 COUNT ------------------------------#
def test_suite_visit_fact_view_v001_record_validation():
    registration_type = 'SUITE_VIEW'
    table_name = 'VISIT_FACT_VIEW_V001'
    field_name = 'VISIT_FACT_VIEW_V001'

    im.Data_Validation(base_dir, sql_base_dir, failed_files_dir, registration_type, table_name, field_name, connection,
                       log).check_records_count()


# ------------------------------- SUITE_VIEW - VISIT_CONTACT_BRIDGE_VIEW_V001 COUNT ----------------------------#
def test_suite_visit_contact_bridge_view_v001_record_validation():
    registration_type = 'SUITE_VIEW'
    table_name = 'VISIT_CONTACT_BRIDGE_VIEW_V001'
    field_name = 'VISIT_CONTACT_BRIDGE_VIEW_V001'

    im.Data_Validation(base_dir, sql_base_dir, failed_files_dir, registration_type, table_name, field_name, connection,
                       log).check_records_count()


# ------------------------------- END OF SNOWFLAKE CONNECTION ------------------------------#
def test_close_snowflake_connection():
    connection.close()
