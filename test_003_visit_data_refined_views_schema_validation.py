# @Author  : Yogesh Patil
# @Date    : 10/03/2023
# @File    : test_003_visit_data_refined_views_schema_validation.py

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

# Deleting failed files folder and re-creating for new run
shutil.rmtree(failed_files_dir, ignore_errors=True)
os.mkdir(failed_files_dir)


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


# ----------------------------  END OF SNOWFLAKE CONNECTION ----------------------------#
def test_close_snowflake_connection():
    connection.close()