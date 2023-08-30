# @Author  : Yogesh Patil
# @Date    : 19/08/2023
# @File    : test_020_DAT_11637_MetaWhatsApp_billing.py

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


# DAT-11919: Verify that, correct billing data is available in 'METAWHATSAPP_BILLING' table.
def test_billing_data_added_correctly_in_metered_daily_usage_fact_table_for_metawhatsapp():
    registration_type = 'REFINED'
    table_name = 'METAWHATSAPP_BILLING'
    field_name = 'METAWHATSAPP_BILLING_DATA'

    im.Data_Validation(base_dir, sql_base_dir, failed_files_dir, registration_type, table_name, field_name, connection,
                       log).check_matching_records()


# DAT-11921: Verify that aggregated amount is received for a tenant only once in a day.
def test_metered_quantity_received_only_once_for_a_tenant_in_metered_daily_usage_fact_table_for_metawhatsapp():
    registration_type = 'REFINED'
    table_name = 'METAWHATSAPP_BILLING'
    field_name = 'METERED_QUANTITY_FOR_SINGLE_TENANT'

    im.Data_Validation(base_dir, sql_base_dir, failed_files_dir, registration_type, table_name, field_name, connection,
                       log).check_matching_records()


# DAT-12005: Verify that, metered_quantity is not coming as NULL.
def test_metered_quantity_is_not_null_in_metered_daily_usage_fact_table_for_metawhatsapp():
    registration_type = 'REFINED'
    table_name = 'METAWHATSAPP_BILLING'
    field_name = 'METERED_QUANTITY_NULL'

    im.Data_Validation(base_dir, sql_base_dir, failed_files_dir, registration_type, table_name, field_name, connection,
                       log).check_null_records()


# DAT-11933: Verify that if sending a back dated event the query date will be same as backdated date.
def test_query_date_is_same_as_backdated_date_for_events_in_metered_daily_usage_fact_table_for_metawhatsapp():
    registration_type = 'REFINED'
    table_name = 'METAWHATSAPP_BILLING'
    field_name = 'METERED_QUERY_DATE'

    im.Data_Validation(base_dir, sql_base_dir, failed_files_dir, registration_type, table_name, field_name, connection,
                       log).check_matching_records()


# DAT-11394: Verify that, duplicate billing records are not added in the 'METAWHATSAPP_BILLING' table for
# same Prod id, Tenant id, Bus no & for specific day.
def test_duplicate_records_in_metered_daily_usage_fact_table_for_metawhatsapp():
    registration_type = 'REFINED'
    table_name = 'METAWHATSAPP_BILLING'
    field_name = 'DUPLICATE_RECORD_VALIDATION'

    im.Data_Validation(base_dir, sql_base_dir, failed_files_dir, registration_type, table_name, field_name, connection,
                       log).check_duplicate_records()


# ----------------------------  END OF SNOWFLAKE CONNECTION ----------------------------#
def test_close_snowflake_connection():
    connection.close()
