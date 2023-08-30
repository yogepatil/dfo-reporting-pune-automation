# @Author  : Yogesh Patil
# @Date    : 27/03/2023
# @File    : test_010_VISIT_FACT_refiner_validation.py

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


# ------------------------------- REFINED VISIT_FACT VALIDATION ------------------------------#
def test_duplicate_records_validation_01():
    registration_type = 'REFINED'
    table_name = 'VISIT_FACT'
    field_name = 'DUPLICATE_RECORD_VALIDATION'

    im.Data_Validation(base_dir, sql_base_dir, failed_files_dir, registration_type, table_name, field_name, connection,
                       log).check_duplicate_records()


def test_visit_referrer_search_engine_records_validation_02():
    registration_type = 'REFINED'
    table_name = 'VISIT_FACT'
    field_name = 'VISIT_REFERRER_SEARCH_ENGINE'

    im.Data_Validation(base_dir, sql_base_dir, failed_files_dir, registration_type, table_name, field_name, connection,
                       log).check_matching_records()


# def test_visit_referrer_url_records_validation_03():
#     registration_type = 'REFINED'
#     table_name = 'VISIT_FACT'
#     field_name = 'VISIT_REFERRER_URL'
#
#     im.Data_Validation(base_dir, sql_base_dir, failed_files_dir, registration_type, table_name, field_name, connection,
#                        log).check_matching_records()


# def test_visit_start_timestamp_records_validation_04():
#     registration_type = 'REFINED'
#     table_name = 'VISIT_FACT'
#     field_name = 'VISIT_START_TIMESTAMP'
#
#     im.Data_Validation(base_dir, sql_base_dir, failed_files_dir, registration_type, table_name, field_name, connection,
#                        log).check_matching_records()


def test_visit_utm_campaign_name_records_validation_05():
    registration_type = 'REFINED'
    table_name = 'VISIT_FACT'
    field_name = 'VISIT_UTM_CAMPAIGN_NAME'

    im.Data_Validation(base_dir, sql_base_dir, failed_files_dir, registration_type, table_name, field_name, connection,
                       log).check_matching_records()


def test_visit_utm_context_text_records_validation_06():
    registration_type = 'REFINED'
    table_name = 'VISIT_FACT'
    field_name = 'VISIT_UTM_CONTENT_TEXT'

    im.Data_Validation(base_dir, sql_base_dir, failed_files_dir, registration_type, table_name, field_name, connection,
                       log).check_matching_records()


def test_visit_utm_medium_name_records_validation_07():
    registration_type = 'REFINED'
    table_name = 'VISIT_FACT'
    field_name = 'VISIT_UTM_MEDIUM_NAME'

    im.Data_Validation(base_dir, sql_base_dir, failed_files_dir, registration_type, table_name, field_name, connection,
                       log).check_matching_records()


def test_visit_utm_source_name_records_validation_08():
    registration_type = 'REFINED'
    table_name = 'VISIT_FACT'
    field_name = 'VISIT_UTM_SOURCE_NAME'

    im.Data_Validation(base_dir, sql_base_dir, failed_files_dir, registration_type, table_name, field_name, connection,
                       log).check_matching_records()


def test_visit_utm_term_name_records_validation_09():
    registration_type = 'REFINED'
    table_name = 'VISIT_FACT'
    field_name = 'VISIT_UTM_TERM_NAME'

    im.Data_Validation(base_dir, sql_base_dir, failed_files_dir, registration_type, table_name, field_name, connection,
                       log).check_matching_records()


def test_visit_chat_opened_count_records_validation_12():
    registration_type = 'REFINED'
    table_name = 'VISIT_FACT'
    field_name = 'CHAT_OPENED_COUNT'

    im.Data_Validation(base_dir, sql_base_dir, failed_files_dir, registration_type, table_name, field_name, connection,
                       log).check_matching_records()


def test_visit_knowledge_base_article_records_validation_13():
    registration_type = 'REFINED'
    table_name = 'VISIT_FACT'
    field_name = 'KNOWLEDGEBASE_ARTICLE_CLICKED_COUNT'

    im.Data_Validation(base_dir, sql_base_dir, failed_files_dir, registration_type, table_name, field_name, connection,
                       log).check_matching_records()


def test_visit_proactive_displayed_records_validation_14():
    registration_type = 'REFINED'
    table_name = 'VISIT_FACT'
    field_name = 'PROACTIVE_DISPLAYED_COUNT'

    im.Data_Validation(base_dir, sql_base_dir, failed_files_dir, registration_type, table_name, field_name, connection,
                       log).check_matching_records()


def test_visit_proactive_clicked_records_validation_15():
    registration_type = 'REFINED'
    table_name = 'VISIT_FACT'
    field_name = 'PROACTIVE_CLICKED_COUNT'

    im.Data_Validation(base_dir, sql_base_dir, failed_files_dir, registration_type, table_name, field_name, connection,
                       log).check_matching_records()


def test_visit_proactive_success_records_validation_16():
    registration_type = 'REFINED'
    table_name = 'VISIT_FACT'
    field_name = 'PROACTIVE_SUCCESS_COUNT'

    im.Data_Validation(base_dir, sql_base_dir, failed_files_dir, registration_type, table_name, field_name, connection,
                       log).check_matching_records()


def test_visit_proactive_failed_records_validation_17():
    registration_type = 'REFINED'
    table_name = 'VISIT_FACT'
    field_name = 'PROACTIVE_FAILED_COUNT'

    im.Data_Validation(base_dir, sql_base_dir, failed_files_dir, registration_type, table_name, field_name, connection,
                       log).check_matching_records()


def test_visit_page_viewed_count_records_validation_18():
    registration_type = 'REFINED'
    table_name = 'VISIT_FACT'
    field_name = 'PAGE_VIEWED_COUNT'

    im.Data_Validation(base_dir, sql_base_dir, failed_files_dir, registration_type, table_name, field_name, connection,
                       log).check_matching_records()


# def test_visit_page_viewed_seconds_records_validation_19():
#     registration_type = 'REFINED'
#     table_name = 'VISIT_FACT'
#     field_name = 'PAGE_VIEWED_SECONDS'
#
#     im.Data_Validation(base_dir, sql_base_dir, failed_files_dir, registration_type, table_name, field_name, connection,
#                        log).check_matching_records()


# ----------------------------  END OF SNOWFLAKE CONNECTION ----------------------------#
def test_close_snowflake_connection():
    connection.close()
