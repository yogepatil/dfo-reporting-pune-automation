# @Author  : Yogesh Patil
# @Date    : 20/05/2023
# @File    : test_visit_objects_regression_suite.py

import os
import sys
import json
import shutil
from Utilities import Utility as im, LoggerFile as log
from Utilities import ReadProperties as rp

log = log.BaseClass().getlogger()

base_dir = rp.ReadConfig.getSystemPath()
environment = rp.ReadConfig.getEnvironment()

# Do not change these parameter values
sql_base_dir = os.path.join(base_dir, "SQL")
schema_base_dir = os.path.join(base_dir, "SCHEMA")
failed_files_dir = os.path.join(base_dir, "Failed_Files")
line = '-' * 154

# ---------------------------- Connecting to the Snowflake Database ----------------------------#
snowflake_connection_file = open(os.path.join(base_dir, "Snowflake_connection.json"))
read_connection_field = json.load(snowflake_connection_file)

for item in read_connection_field.keys():
    if item == environment:
        username = read_connection_field[environment][0]['username']
        password = read_connection_field[environment][0]['password']
        account = read_connection_field[environment][0]['account']
        warehouse = read_connection_field[environment][0]['warehouse']
        database = read_connection_field[environment][0]['database']
        schema = read_connection_field[environment][0]['schema']
        role = read_connection_field[environment][0]['role']
try:
    connection_obj = im.Snowflake_Connection(username, password, account,
                                             warehouse, database, schema,
                                             role, base_dir, log)
    connection = connection_obj.develope_connection()
except:
    connection_obj.connection_fail()
    sys.exit(-1)

# Deleting failed files folder and re-creating for new run
shutil.rmtree(failed_files_dir, ignore_errors=True)
os.mkdir(failed_files_dir)


# ----------------------------  STRUCTURED VISIT_EVENT TABLE ----------------------------#
def test_structured_visit_event_table_schema_validation():
    print("\n")
    log.info("Execution Started On: {} Environment".format(environment))
    registration_type = 'STRUCTURED'
    namespace = 'DFO'
    table_name = 'VISIT_EVENT'
    sql_query = 'Describe Table "DATAHUB"."DFO_STRUCTURED"."{}"'.format(table_name)
    log.info("Execution Query: {}".format(sql_query))
    im.Schema_Validation(schema_base_dir, registration_type, table_name, namespace, sql_query, connection, log) \
        .schema_validation()


#----------------------------  STRUCTURED VISIT_CONTACT TABLE ----------------------------#
def test_structured_visit_event_table_schema_validation():
    print("\n")
    log.info("Execution Started On: {} Environment".format(environment))
    registration_type = 'STRUCTURED'
    namespace = 'DFO'
    table_name = 'VISIT_CONTACT'
    sql_query = 'Describe Table "DATAHUB"."DFO_STRUCTURED"."{}"'.format(table_name)
    log.info("Execution Query: {}".format(sql_query))
    im.Schema_Validation(schema_base_dir, registration_type, table_name, namespace, sql_query, connection, log) \
        .schema_validation()


# ----------------------------  REFINED VISITOR_DIM TABLE ----------------------------#
def test_refined_visitor_dim_table_schema_validation():
    registration_type = 'REFINED'
    namespace = 'DFO'
    table_name = 'VISITOR_DIM'
    sql_query = 'Describe Table "DATAHUB"."DFO_REFINED"."{}"'.format(table_name)

    im.Schema_Validation(schema_base_dir, registration_type, table_name, namespace, sql_query, connection, log) \
        .schema_validation()


# ----------------------------  REFINED VISIT_CONVERSION_FACT TABLE ----------------------------#
def test_refined_visit_conversion_fact_table_schema_validation():
    registration_type = 'REFINED'
    namespace = 'DFO'
    table_name = 'VISIT_CONVERSION_FACT'
    sql_query = 'Describe Table "DATAHUB"."DFO_REFINED"."{}"'.format(table_name)

    im.Schema_Validation(schema_base_dir, registration_type, table_name, namespace, sql_query, connection, log) \
        .schema_validation()


# ----------------------------  REFINED VISITOR_TAG TABLE ----------------------------#
def test_refined_visitor_tag_table_schema_validation():
    registration_type = 'REFINED'
    namespace = 'DFO'
    table_name = 'VISITOR_TAG'
    sql_query = 'Describe Table "DATAHUB"."DFO_REFINED"."{}"'.format(table_name)

    im.Schema_Validation(schema_base_dir, registration_type, table_name, namespace, sql_query, connection, log) \
        .schema_validation()


# ----------------------------  REFINED VISIT_ACTIVITY_FACT TABLE ----------------------------#
def test_refined_visit_activity_fact_table_schema_validation():
    registration_type = 'REFINED'
    namespace = 'DFO'
    table_name = 'VISIT_ACTIVITY_FACT'
    sql_query = 'Describe Table "DATAHUB"."DFO_REFINED"."{}"'.format(table_name)

    im.Schema_Validation(schema_base_dir, registration_type, table_name, namespace, sql_query, connection, log) \
        .schema_validation()


# ----------------------------  REFINED VISIT_FACT TABLE ----------------------------#
def test_refined_visit_fact_table_schema_validation():
    registration_type = 'REFINED'
    namespace = 'DFO'
    table_name = 'VISIT_FACT'
    sql_query = 'Describe Table "DATAHUB"."DFO_REFINED"."{}"'.format(table_name)

    im.Schema_Validation(schema_base_dir, registration_type, table_name, namespace, sql_query, connection, log) \
        .schema_validation()


#----------------------------  REFINED VISIT_CONTACT_BRIDGE TABLE ----------------------------#
def test_refined_visit_contact_bridge_table_schema_validation():
    registration_type = 'REFINED'
    namespace = 'DFO'
    table_name = 'VISIT_CONTACT_BRIDGE'
    sql_query = 'Describe Table "DATAHUB"."DFO_REFINED"."{}"'.format(table_name)

    im.Schema_Validation(schema_base_dir, registration_type, table_name, namespace, sql_query, connection, log) \
        .schema_validation()


# ----------------------------  REFINED VIEW - VISITOR_DIM_VIEW_V001 ----------------------------#
def test_refined_visitor_dim_view_v001_schema_validation():
    registration_type = 'REFINED_VIEW'
    namespace = 'VIEW'
    view_name = 'VISITOR_DIM_VIEW_V001'
    sql_query = 'DESCRIBE VIEW "DATAHUB"."DFO_REFINED"."{}"'.format(view_name)

    im.Schema_Validation(schema_base_dir, registration_type, view_name, namespace, sql_query, connection, log) \
        .schema_validation()


# ----------------------------  REFINED VIEW - VISITOR_TAG_VIEW_V001 ----------------------------#
def test_refined_visitor_tag_view_v001_schema_validation():
    registration_type = 'REFINED_VIEW'
    namespace = 'VIEW'
    view_name = 'VISITOR_TAG_VIEW_V001'
    sql_query = 'DESCRIBE VIEW "DATAHUB"."DFO_REFINED"."{}"'.format(view_name)

    im.Schema_Validation(schema_base_dir, registration_type, view_name, namespace, sql_query, connection, log) \
        .schema_validation()


# ----------------------------  REFINED VIEW - VISIT_CONVERSION_FACT_VIEW_V001 ----------------------------#
def test_refined_visit_conversion_fact_view_v001_schema_validation():
    registration_type = 'REFINED_VIEW'
    namespace = 'VIEW'
    view_name = 'VISIT_CONVERSION_FACT_VIEW_V001'
    sql_query = 'DESCRIBE VIEW "DATAHUB"."DFO_REFINED"."{}"'.format(view_name)

    im.Schema_Validation(schema_base_dir, registration_type, view_name, namespace, sql_query, connection, log) \
        .schema_validation()


# ----------------------------  REFINED VIEW - VISIT_ACTIVITY_FACT_VIEW_V001 ----------------------------#
def test_refined_visit_activity_fact_view_v001_schema_validation():
    registration_type = 'REFINED_VIEW'
    namespace = 'VIEW'
    view_name = 'VISIT_ACTIVITY_FACT_VIEW_V001'
    sql_query = 'DESCRIBE VIEW "DATAHUB"."DFO_REFINED"."{}"'.format(view_name)

    im.Schema_Validation(schema_base_dir, registration_type, view_name, namespace, sql_query, connection, log) \
        .schema_validation()


# ----------------------------  REFINED VIEW - VISIT_FACT_VIEW_V001 ----------------------------#
def test_refined_visit_fact_view_v001_schema_validation():
    registration_type = 'REFINED_VIEW'
    namespace = 'VIEW'
    view_name = 'VISIT_FACT_VIEW_V001'
    sql_query = 'DESCRIBE VIEW "DATAHUB"."DFO_REFINED"."{}"'.format(view_name)

    im.Schema_Validation(schema_base_dir, registration_type, view_name, namespace, sql_query, connection, log) \
        .schema_validation()


# ----------------------------  REFINED VIEW - VISIT_CONTACT_BRIDGE_VIEW_V001 ----------------------------#
def test_refined_visit_contact_bridge_view_v001_schema_validation():
    registration_type = 'REFINED_VIEW'
    namespace = 'VIEW'
    view_name = 'VISIT_CONTACT_BRIDGE_VIEW_V001'
    sql_query = 'DESCRIBE VIEW "DATAHUB"."DFO_REFINED"."{}"'.format(view_name)

    im.Schema_Validation(schema_base_dir, registration_type, view_name, namespace, sql_query, connection, log) \
        .schema_validation()


# ------------------------------- REFINED VIEW - VISITOR_DIM_VIEW_V001 COUNT ------------------------------#
def test_refined_visitor_dim_view_v001_record_validation():
    registration_type = 'REFINED_VIEW'
    table_name = 'VISITOR_DIM_VIEW_V001'
    field_name = 'VISITOR_DIM_VIEW_V001'

    im.Data_Validation(base_dir, sql_base_dir, failed_files_dir, registration_type, table_name, field_name, connection,
                       log).check_records_count()


# ------------------------------- REFINED VIEW - VISIT_CONVERSION_FACT_VIEW_V001 COUNT -------------------------#
def test_refined_visit_conversion_fact_view_v001_record_validation():
    registration_type = 'REFINED_VIEW'
    table_name = 'VISIT_CONVERSION_FACT_VIEW_V001'
    field_name = 'VISIT_CONVERSION_FACT_VIEW_V001'

    im.Data_Validation(base_dir, sql_base_dir, failed_files_dir, registration_type, table_name, field_name, connection,
                       log).check_records_count()


# ------------------------------- REFINED VIEW - VISITOR_TAG_VIEW_V001 COUNT ------------------------------#
def test_refined_visitor_tag_view_v001_record_validation():
    registration_type = 'REFINED_VIEW'
    table_name = 'VISITOR_TAG_VIEW_V001'
    field_name = 'VISITOR_TAG_VIEW_V001'

    im.Data_Validation(base_dir, sql_base_dir, failed_files_dir, registration_type, table_name, field_name, connection,
                       log).check_records_count()


# ------------------------------- REFINED VIEW - VISIT_FACT_VIEW_V001 COUNT ------------------------------#
def test_refined_visit_fact_view_v001_record_validation():
    registration_type = 'REFINED_VIEW'
    table_name = 'VISIT_FACT_VIEW_V001'
    field_name = 'VISIT_FACT_VIEW_V001'

    im.Data_Validation(base_dir, sql_base_dir, failed_files_dir, registration_type, table_name, field_name, connection,
                       log).check_records_count()


# ------------------------------- REFINED VIEW - VISIT_ACTIVITY_FACT_VIEW_V001 COUNT ----------------------------#
def test_refined_visit_activity_fact_view_v001_record_validation():
    registration_type = 'REFINED_VIEW'
    table_name = 'VISIT_ACTIVITY_FACT_VIEW_V001'
    field_name = 'VISIT_ACTIVITY_FACT_VIEW_V001'

    im.Data_Validation(base_dir, sql_base_dir, failed_files_dir, registration_type, table_name, field_name, connection,
                       log).check_records_count()


# ------------------------------- REFINED VIEW - VISIT_CONTACT_BRIDGE_VIEW_V001 COUNT ----------------------------#
def test_refined_visit_contact_bridge_view_v001_record_validation():
    registration_type = 'REFINED_VIEW'
    table_name = 'VISIT_CONTACT_BRIDGE_VIEW_V001'
    field_name = 'VISIT_CONTACT_BRIDGE_VIEW_V001'

    im.Data_Validation(base_dir, sql_base_dir, failed_files_dir, registration_type, table_name, field_name, connection,
                       log).check_records_count()


# ------------------------------- SUITE VIEW - VISITOR_DIM_VIEW_V001 ----------------------------#
def test_suite_visitor_dim_view_v001_schema_validation():
    registration_type = 'SUITE_VIEW'
    namespace = 'VIEW'
    view_name = 'VISITOR_DIM_VIEW_V001'
    sql_query = 'DESCRIBE VIEW "DATAHUB"."SUITE_REFINED"."{}"'.format(view_name)

    im.Schema_Validation(schema_base_dir, registration_type, view_name, namespace, sql_query, connection, log) \
        .schema_validation()


# ------------------------------- SUITE VIEW - VISIT_CONVERSION_FACT_VIEW_V001 ----------------------------#
def test_suite_visit_conversion_fact_view_v001_schema_validation():
    registration_type = 'SUITE_VIEW'
    namespace = 'VIEW'
    view_name = 'VISIT_CONVERSION_FACT_VIEW_V001'
    sql_query = 'DESCRIBE VIEW "DATAHUB"."SUITE_REFINED"."{}"'.format(view_name)

    im.Schema_Validation(schema_base_dir, registration_type, view_name, namespace, sql_query, connection, log) \
        .schema_validation()


# ------------------------------- SUITE VIEW - VISITOR_TAG_VIEW_V001 ----------------------------#
def test_suite_visitor_tag_view_v001_schema_validation():
    registration_type = 'SUITE_VIEW'
    namespace = 'VIEW'
    view_name = 'VISITOR_TAG_VIEW_V001'
    sql_query = 'DESCRIBE VIEW "DATAHUB"."SUITE_REFINED"."{}"'.format(view_name)

    im.Schema_Validation(schema_base_dir, registration_type, view_name, namespace, sql_query, connection, log) \
        .schema_validation()


# ------------------------------- SUITE VIEW - VISIT_ACTIVITY_FACT_VIEW_V001 ----------------------------#
def test_suite_visit_activity_fact_view_v001_schema_validation():
    registration_type = 'SUITE_VIEW'
    namespace = 'VIEW'
    table_name = 'VISIT_ACTIVITY_FACT_VIEW_V001'
    sql_query = 'DESCRIBE VIEW "DATAHUB"."SUITE_REFINED"."{}"'.format(table_name)

    im.Schema_Validation(schema_base_dir, registration_type, table_name, namespace, sql_query, connection, log) \
        .schema_validation()


# ------------------------------- SUITE VIEW - VISIT_FACT_VIEW_V001 ----------------------------#
def test_suite_visit_fact_view_v001_schema_validation():
    registration_type = 'SUITE_VIEW'
    namespace = 'VIEW'
    view_name = 'VISIT_FACT_VIEW_V001'
    sql_query = 'DESCRIBE VIEW "DATAHUB"."SUITE_REFINED"."{}"'.format(view_name)

    im.Schema_Validation(schema_base_dir, registration_type, view_name, namespace, sql_query, connection, log) \
        .schema_validation()


# ----------------------------  SUITE VIEW - VISIT_CONTACT_BRIDGE_VIEW_V001 ----------------------------#
def test_suite_visit_contact_bridge_view_v001_schema_validation():
    registration_type = 'SUITE_VIEW'
    namespace = 'VIEW'
    view_name = 'VISIT_CONTACT_BRIDGE_VIEW_V001'
    sql_query = 'DESCRIBE VIEW "DATAHUB"."SUITE_REFINED"."{}"'.format(view_name)

    im.Schema_Validation(schema_base_dir, registration_type, view_name, namespace, sql_query, connection, log) \
        .schema_validation()


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


def test_visitor_createdtimestamp_validation_11():
    registration_type = 'REFINED'
    table_name = 'VISITOR_DIM'
    field_name = 'CREATED_TIMESTAMP'

    im.Data_Validation(base_dir, sql_base_dir, failed_files_dir, registration_type, table_name, field_name, connection,
                       log).check_matching_records()


def test_visitor_duplicate_record_validation_12():
    registration_type = 'REFINED'
    table_name = 'VISITOR_DIM'
    field_name = 'DUPLICATE_RECORD_VALIDATION'

    im.Data_Validation(base_dir, sql_base_dir, failed_files_dir, registration_type, table_name, field_name, connection,
                       log).check_duplicate_records()


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


# ------------------------------- REFINED VISITOR_TAG VALIDATION ------------------------------#
def test_visitor_createdtimestamp_validation_01():
    registration_type = 'REFINED'
    table_name = 'VISITOR_TAG'
    field_name = 'CREATEDTIMESTAMP'

    im.Data_Validation(base_dir, sql_base_dir, failed_files_dir, registration_type, table_name, field_name, connection,
                       log).check_matching_records()


# ------------------------------- REFINED VISIT_ACTIVITY_FACT VALIDATION ------------------------------#
def test_visit_activity_start_timestamp_records_validation_01():
    registration_type = 'REFINED'
    table_name = 'VISIT_ACTIVITY_FACT'
    field_name = 'VISIT_ACTIVITY_START_TIMESTAMP'

    im.Data_Validation(base_dir, sql_base_dir, failed_files_dir, registration_type, table_name, field_name, connection,
                       log).check_matching_records()


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


# ------------------------------- REFINED VISIT_CONTACT_BRIDGE VALIDATION ------------------------------#
# def test_visit_contact_key_records_validation_01():
#     registration_type = 'REFINED'
#     table_name = 'VISIT_CONTACT_BRIDGE'
#     field_name = 'CONTACT_KEY'
#
#     im.Data_Validation(base_dir, sql_base_dir, failed_files_dir, registration_type, table_name, field_name, connection,
#                        log).check_matching_records()
#
#
# def test_visit_contact_id_records_validation_02():
#     registration_type = 'REFINED'
#     table_name = 'VISIT_CONTACT_BRIDGE'
#     field_name = 'CONTACT_ID'
#
#     im.Data_Validation(base_dir, sql_base_dir, failed_files_dir, registration_type, table_name, field_name, connection,
#                        log).check_matching_records()
#
#
# def test_visit_contact_no_records_validation_03():
#     registration_type = 'REFINED'
#     table_name = 'VISIT_CONTACT_BRIDGE'
#     field_name = 'CONTACT_NO'
#
#     im.Data_Validation(base_dir, sql_base_dir, failed_files_dir, registration_type, table_name, field_name, connection,
#                        log).check_matching_records()
#
#
# def test_visit_interaction_id_records_validation_04():
#     registration_type = 'REFINED'
#     table_name = 'VISIT_CONTACT_BRIDGE'
#     field_name = 'INTERACTION_ID'
#
#     im.Data_Validation(base_dir, sql_base_dir, failed_files_dir, registration_type, table_name, field_name, connection,
#                        log).check_matching_records()
#
#
# def test_visit_interaction_key_records_validation_05():
#     registration_type = 'REFINED'
#     table_name = 'VISIT_CONTACT_BRIDGE'
#     field_name = 'INTERACTION_KEY'
#
#     im.Data_Validation(base_dir, sql_base_dir, failed_files_dir, registration_type, table_name, field_name, connection,
#                        log).check_matching_records()
#
#
# def test_visit_cust_contact_id_records_validation_06():
#     registration_type = 'REFINED'
#     table_name = 'VISIT_CONTACT_BRIDGE'
#     field_name = 'CUSTOMER_CONTACT_ID'
#
#     im.Data_Validation(base_dir, sql_base_dir, failed_files_dir, registration_type, table_name, field_name, connection,
#                        log).check_matching_records()
#
#
# def test_visit_cust_contact_key_records_validation_07():
#     registration_type = 'REFINED'
#     table_name = 'VISIT_CONTACT_BRIDGE'
#     field_name = 'CUSTOMER_CONTACT_KEY'
#
#     im.Data_Validation(base_dir, sql_base_dir, failed_files_dir, registration_type, table_name, field_name, connection,
#                        log).check_matching_records()
#
#
# def test_visit_tenant_id_records_validation_08():
#     registration_type = 'REFINED'
#     table_name = 'VISIT_CONTACT_BRIDGE'
#     field_name = '_TENANT_ID'
#
#     im.Data_Validation(base_dir, sql_base_dir, failed_files_dir, registration_type, table_name, field_name, connection,
#                        log).check_matching_records()
#
#
# def test_visit_id_records_validation_09():
#     registration_type = 'REFINED'
#     table_name = 'VISIT_CONTACT_BRIDGE'
#     field_name = 'VISIT_ID'
#
#     im.Data_Validation(base_dir, sql_base_dir, failed_files_dir, registration_type, table_name, field_name, connection,
#                        log).check_matching_records()
#
#
# def test_visitor_duplicate_record_validation_10():
#     registration_type = 'REFINED'
#     table_name = 'VISIT_CONTACT_BRIDGE'
#     field_name = 'DUPLICATE_RECORD_VALIDATION'
#
#     im.Data_Validation(base_dir, sql_base_dir, failed_files_dir, registration_type, table_name, field_name, connection,
#                        log).check_duplicate_records()


# ----------------------------  END OF SNOWFLAKE CONNECTION ----------------------------#
def test_close_snowflake_connection():
    connection.close()
