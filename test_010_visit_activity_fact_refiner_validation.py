# @Author  : Yogesh Patil
# @Date    : 27/03/2023
# @File    : test_010_visit_activity_fact_refiner_validation.py

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


# ------------------------------- REFINED VISIT_ACTIVITY_FACT VALIDATION ------------------------------#
# def test_visit_activity_start_timestamp_records_validation_01():
#     registration_type = 'REFINED'
#     table_name = 'VISIT_ACTIVITY_FACT'
#     field_name = 'VISIT_ACTIVITY_START_TIMESTAMP'
#
#     im.Data_Validation(base_dir, sql_base_dir, failed_files_dir, registration_type, table_name, field_name, connection,
#                        log).check_matching_records()


def test_visit_activity_type_name_records_validation_02():
    registration_type = 'REFINED'
    table_name = 'VISIT_ACTIVITY_FACT'
    field_name = 'VISIT_ACTIVITY_TYPE_NAME'

    im.Data_Validation(base_dir, sql_base_dir, failed_files_dir, registration_type, table_name, field_name, connection,
                       log).check_matching_records()


def test_visit_destination_id_records_validation_03():
    registration_type = 'REFINED'
    table_name = 'VISIT_ACTIVITY_FACT'
    field_name = 'VISIT_DESTINATION_ID'

    im.Data_Validation(base_dir, sql_base_dir, failed_files_dir, registration_type, table_name, field_name, connection,
                       log).check_matching_records()


def test_proactive_action_id_records_validation_04():
    registration_type = 'REFINED'
    table_name = 'VISIT_ACTIVITY_FACT'
    field_name = 'PROACTIVE_ACTION_ID'

    im.Data_Validation(base_dir, sql_base_dir, failed_files_dir, registration_type, table_name, field_name, connection,
                       log).check_matching_records()


def test_proactive_action_name_records_validation_05():
    registration_type = 'REFINED'
    table_name = 'VISIT_ACTIVITY_FACT'
    field_name = 'PROACTIVE_ACTION_NAME'

    im.Data_Validation(base_dir, sql_base_dir, failed_files_dir, registration_type, table_name, field_name, connection,
                       log).check_matching_records()


def test_proactive_action_type_name_records_validation_06():
    registration_type = 'REFINED'
    table_name = 'VISIT_ACTIVITY_FACT'
    field_name = 'PROACTIVE_ACTION_TYPE_NAME'

    im.Data_Validation(base_dir, sql_base_dir, failed_files_dir, registration_type, table_name, field_name, connection,
                       log).check_matching_records()


def test_proactive_workflow_id_records_validation_07():
    registration_type = 'REFINED'
    table_name = 'VISIT_ACTIVITY_FACT'
    field_name = 'PROACTIVE_WORKFLOW_ID'

    im.Data_Validation(base_dir, sql_base_dir, failed_files_dir, registration_type, table_name, field_name, connection,
                       log).check_matching_records()


def test_proactive_workflow_name_records_validation_08():
    registration_type = 'REFINED'
    table_name = 'VISIT_ACTIVITY_FACT'
    field_name = 'PROACTIVE_WORKFLOW_NAME'

    im.Data_Validation(base_dir, sql_base_dir, failed_files_dir, registration_type, table_name, field_name, connection,
                       log).check_matching_records()


def test_page_viewed_url_records_validation_09():
    registration_type = 'REFINED'
    table_name = 'VISIT_ACTIVITY_FACT'
    field_name = 'PAGE_VIEWED_URL'

    im.Data_Validation(base_dir, sql_base_dir, failed_files_dir, registration_type, table_name, field_name, connection,
                       log).check_matching_records()


def test_page_viewed_title_records_validation_10():
    registration_type = 'REFINED'
    table_name = 'VISIT_ACTIVITY_FACT'
    field_name = 'PAGE_VIEWED_TITLE'

    im.Data_Validation(base_dir, sql_base_dir, failed_files_dir, registration_type, table_name, field_name, connection,
                       log).check_matching_records()


def test_page_viewed_seconds_validation_records_11():
    registration_type = 'REFINED'
    table_name = 'VISIT_ACTIVITY_FACT'
    field_name = 'PAGE_VIEWED_SECONDS'

    im.Data_Validation(base_dir, sql_base_dir, failed_files_dir, registration_type, table_name, field_name, connection,
                       log).check_matching_records()


def test_knowledge_base_article_id_records_validation_12():
    registration_type = 'REFINED'
    table_name = 'VISIT_ACTIVITY_FACT'
    field_name = 'KNOWLEDGE_BASE_ARTICLE_ID'

    im.Data_Validation(base_dir, sql_base_dir, failed_files_dir, registration_type, table_name, field_name, connection,
                       log).check_matching_records()


def test_knowledge_base_article_url_records_validation_13():
    registration_type = 'REFINED'
    table_name = 'VISIT_ACTIVITY_FACT'
    field_name = 'KNOWLEDGE_BASE_ARTICLE_URL'

    im.Data_Validation(base_dir, sql_base_dir, failed_files_dir, registration_type, table_name, field_name, connection,
                       log).check_matching_records()


def test_knowledge_base_article_title_records_validation_14():
    registration_type = 'REFINED'
    table_name = 'VISIT_ACTIVITY_FACT'
    field_name = 'KNOWLEDGE_BASE_ARTICLE_TITLE'

    im.Data_Validation(base_dir, sql_base_dir, failed_files_dir, registration_type, table_name, field_name, connection,
                       log).check_matching_records()


def test_duplicate_records_validation_15():
    registration_type = 'REFINED'
    table_name = 'VISIT_ACTIVITY_FACT'
    field_name = 'DUPLICATE_RECORD_VALIDATION'

    im.Data_Validation(base_dir, sql_base_dir, failed_files_dir, registration_type, table_name, field_name, connection,
                       log).check_duplicate_records()


# ----------------------------  END OF SNOWFLAKE CONNECTION ----------------------------#
def test_close_snowflake_connection():
    connection.close()