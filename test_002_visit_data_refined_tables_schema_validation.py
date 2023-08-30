# @Author  : Yogesh Patil
# @Date    : 09/02/2023
# @File    : test_002_visit_data_refined_tables_schema_validation.py

import os
import sys
import json
import shutil
import pytest
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


# ----------------------------  REFINED VISIT_CONTACT_BRIDGE TABLE ----------------------------#
def test_refined_visit_contact_bridge_table_schema_validation():
    registration_type = 'REFINED'
    namespace = 'DFO'
    table_name = 'VISIT_CONTACT_BRIDGE'
    sql_query = 'Describe Table "DATAHUB"."DFO_REFINED"."{}"'.format(table_name)

    im.Schema_Validation(schema_base_dir, registration_type, table_name, namespace, sql_query, connection, log) \
        .schema_validation()


# ----------------------------   END OF SNOWFLAKE CONNECTION ----------------------------#
def test_close_snowflake_connection():
    connection.close()