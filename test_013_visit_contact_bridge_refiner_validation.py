# @Author  : Yogesh Patil
# @Date    : 06/06/2023
# @File    : test_013_visit_contact_bridge_refiner_validation.py

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

# Connecting to the Snowflake Database
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


# ------------------------------- REFINED VISIT_CONTACT_BRIDGE VALIDATION ------------------------------#
def test_visit_contact_key_records_validation_01():
    registration_type = 'REFINED'
    table_name = 'VISIT_CONTACT_BRIDGE'
    field_name = 'CONTACT_KEY'

    im.Data_Validation(base_dir, sql_base_dir, failed_files_dir, registration_type, table_name, field_name, connection,
                       log).check_matching_records()


def test_visit_contact_id_records_validation_02():
    registration_type = 'REFINED'
    table_name = 'VISIT_CONTACT_BRIDGE'
    field_name = 'CONTACT_ID'

    im.Data_Validation(base_dir, sql_base_dir, failed_files_dir, registration_type, table_name, field_name, connection,
                       log).check_matching_records()


def test_visit_contact_no_records_validation_03():
    registration_type = 'REFINED'
    table_name = 'VISIT_CONTACT_BRIDGE'
    field_name = 'CONTACT_NO'

    im.Data_Validation(base_dir, sql_base_dir, failed_files_dir, registration_type, table_name, field_name, connection,
                       log).check_matching_records()


def test_visit_interaction_id_records_validation_04():
    registration_type = 'REFINED'
    table_name = 'VISIT_CONTACT_BRIDGE'
    field_name = 'INTERACTION_ID'

    im.Data_Validation(base_dir, sql_base_dir, failed_files_dir, registration_type, table_name, field_name, connection,
                       log).check_matching_records()


def test_visit_interaction_key_records_validation_05():
    registration_type = 'REFINED'
    table_name = 'VISIT_CONTACT_BRIDGE'
    field_name = 'INTERACTION_KEY'

    im.Data_Validation(base_dir, sql_base_dir, failed_files_dir, registration_type, table_name, field_name, connection,
                       log).check_matching_records()


def test_visit_cust_contact_id_records_validation_06():
    registration_type = 'REFINED'
    table_name = 'VISIT_CONTACT_BRIDGE'
    field_name = 'CUSTOMER_CONTACT_ID'

    im.Data_Validation(base_dir, sql_base_dir, failed_files_dir, registration_type, table_name, field_name, connection,
                       log).check_matching_records()


def test_visit_cust_contact_key_records_validation_07():
    registration_type = 'REFINED'
    table_name = 'VISIT_CONTACT_BRIDGE'
    field_name = 'CUSTOMER_CONTACT_KEY'

    im.Data_Validation(base_dir, sql_base_dir, failed_files_dir, registration_type, table_name, field_name, connection,
                       log).check_matching_records()


def test_visit_tenant_id_records_validation_08():
    registration_type = 'REFINED'
    table_name = 'VISIT_CONTACT_BRIDGE'
    field_name = '_TENANT_ID'

    im.Data_Validation(base_dir, sql_base_dir, failed_files_dir, registration_type, table_name, field_name, connection,
                       log).check_matching_records()


def test_visit_id_records_validation_09():
    registration_type = 'REFINED'
    table_name = 'VISIT_CONTACT_BRIDGE'
    field_name = 'VISIT_ID'

    im.Data_Validation(base_dir, sql_base_dir, failed_files_dir, registration_type, table_name, field_name, connection,
                       log).check_matching_records()


def test_visitor_duplicate_record_validation_10():
    registration_type = 'REFINED'
    table_name = 'VISIT_CONTACT_BRIDGE'
    field_name = 'DUPLICATE_RECORD_VALIDATION'

    im.Data_Validation(base_dir, sql_base_dir, failed_files_dir, registration_type, table_name, field_name, connection,
                       log).check_duplicate_records()


# ----------------------------  END OF SNOWFLAKE CONNECTION ----------------------------#
def test_close_snowflake_connection():
    connection.close()
