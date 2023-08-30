import os
from datetime import *

import numpy as np
import pandas as pd
import snowflake.connector


class Snowflake_Connection:
    def __init__(self, username, password, account, warehouse, database, schema, role, base_dir, log):
        self.username = username
        self.password = password
        self.account = account
        self.warehouse = warehouse
        self.database = database
        self.schema = schema
        self.role = role
        self.base_dir = base_dir
        self.log = log

    def develope_connection(self):
        conn = snowflake.connector.connect(user=self.username, password=self.password, account=self.account,
                                           warehouse=self.warehouse, database=self.database, schema=self.schema,
                                           role=self.role)
        connection = conn.cursor()
        return connection

    def close_connection(self):
        self.develope_connection().connection.close()


class Open_SQL_File:
    def __init__(self, sql_base_dir, registration_type, table_name, field_name):
        self.sql_base_dir = sql_base_dir
        self.registration_type = registration_type
        self.table_name = table_name
        self.field_name = field_name

    def open_sql_file(self):
        os.chdir(self.sql_base_dir + "\\" + self.registration_type + "\\" + self.table_name)
        with open(self.field_name + '.sql', 'r') as file:
            sql_file = file.read()
        return sql_file


class SQL_Execute:
    def __init__(self, connection):
        self.connection = connection

    def execute_sql(self, sql_query):
        query = self.connection.execute(sql_query)
        result = query.fetchall()
        col_names = []
        for elt in self.connection.description:
            col_names.append(elt[0])
        df = pd.DataFrame(result, columns=col_names)
        return df


class Data_Validation(Open_SQL_File, SQL_Execute):
    def __init__(self, base_dir, sql_base_dir, failed_files_dir, registration_type, table_name, field_name, connection,
                 log):
        Open_SQL_File.__init__(self, sql_base_dir, registration_type, table_name, field_name)
        SQL_Execute.__init__(self, connection)
        self.base_dir = base_dir
        self.failed_files_dir = failed_files_dir
        self.log = log

    def check_matching_records(self):
        print("\n")
        self.log.info(
            "Objective - Validation of the {} field of the {} table with respect to source".format(self.field_name,
                                                                                                   self.table_name))
        try:
            sql_query = Open_SQL_File(self.sql_base_dir, self.registration_type, self.table_name,
                                      self.field_name).open_sql_file()
            try:
                df = SQL_Execute(self.connection).execute_sql(sql_query)
                total_records_count = int(df.shape[0])
                matching_records_count = int(df[(df.FLAG == True)]["FLAG"].count())
                failed_records_count = int(df[(df.FLAG == False)]["FLAG"].count())
                failed_records = df[df.FLAG == False]

                if total_records_count == matching_records_count:
                    # self.log.info("Validation passed for {} field of {} table, Total records: {}, Passed records: {
                    # }, " "Failed records: {}" .format(self.field_name, self.table_name, total_records_count,
                    # matching_records_count, failed_records_count))
                    self.log.info("---------------- Validation Passed ----------------")
                    self.log.info(
                        "Total records identified for {} field of {} table: {}".format(self.field_name, self.table_name,
                                                                                       total_records_count))
                    self.log.info("Passed records identified for {} field of {} table:  {}".format(self.field_name,
                                                                                                   self.table_name,
                                                                                                   matching_records_count))
                    self.log.info("Failed records identified for {} field of {} table:  {}".format(self.field_name,
                                                                                                   self.table_name,
                                                                                                   failed_records_count))
                    # Code to capture additional info in logs starts
                    with pd.option_context('display.max_rows', None, 'display.max_columns', None,
                                           'display.precision', 5):
                        print(
                            "---------------------------------- Result ----------------------------------------------")
                        # self.log.info(df)
                        # print(df)
                        # print(
                        #    "----------------------------------------------------------------------------------------")
                    # Code to capture additional info in logs ends
                    assert 1 == 1
                else:
                    self.log.info("---------------- Validation Failed ----------------")
                    self.log.info("Validation failed for {} field of {} table, Total records: {}, Passed records: {}, "
                                  "Failed records: {}"
                                  .format(self.field_name, self.table_name, total_records_count, matching_records_count,
                                          failed_records_count))
                    os.chdir(self.failed_files_dir)
                    file = self.table_name + '_' + self.field_name + '.csv'
                    self.log.info("Failed records: {}".format(os.path.join(self.failed_files_dir, file)))
                    with open(file, 'w') as file:
                        failed_records.to_csv(file)
                    assert 1 == 2
            except:
                self.log.info("Query: {}".format(sql_query))
                assert 1 == 2
        except:
            self.log.info("File {} has issues, File location: {}".format(self.field_name + '.sql',
                                                                         os.path.join(self.sql_base_dir,
                                                                                      self.registration_type,
                                                                                      self.table_name)))
            assert 1 == 2

    def check_duplicate_records(self):
        self.log.info(
            "Objective - Validation of duplicate records of the {} table".format(self.table_name))
        try:
            sql_query = Open_SQL_File(self.sql_base_dir, self.registration_type, self.table_name,
                                      self.field_name).open_sql_file()
            try:
                df = SQL_Execute(self.connection).execute_sql(sql_query)
                total_records_count = int(df.shape[0])
                matching_records_count = int(df[(df.FLAG == True)]["FLAG"].count())
                failed_records_count = int(df[(df.FLAG == False)]["FLAG"].count())
                failed_records = df[df.FLAG == False]
                if df.shape[0] == 0:
                    self.log.info("Validation Passed, No Duplicate records in the {} table".format(self.table_name))
                    assert 1 == 1
                else:
                    self.log.info("Validation failed, {} duplicate records in {} table".format(total_records_count,
                                                                                               self.table_name))
                    os.chdir(self.failed_files_dir)
                    file = self.table_name + '_' + self.field_name + '.csv'
                    self.log.info("Duplicate records: {}".format(os.path.join(self.failed_files_dir, file)))
                    with open(file, 'w') as file:
                        failed_records.to_csv(file)
                    assert 1 == 2
            except:
                self.log.info("Query: {}".format(sql_query))
                assert 1 == 2
        except:
            self.log.info("File {} has issues, File location: {}".format(self.field_name + '.sql',
                                                                         os.path.join(self.sql_base_dir,
                                                                                      self.registration_type,
                                                                                      self.table_name)))
            assert 1 == 2

    def check_null_records(self):
        self.log.info(
            "Objective - Validation of NULL records of {} field for {} table".format(self.field_name,
                                                                                     self.table_name))
        try:
            sql_query = Open_SQL_File(self.sql_base_dir, self.registration_type, self.table_name,
                                      self.field_name).open_sql_file()
            try:
                df = SQL_Execute(self.connection).execute_sql(sql_query)
                total_records_count = int(df.shape[0])
                matching_records_count = int(df[(df.FLAG == True)]["FLAG"].count())
                failed_records_count = int(df[(df.FLAG == False)]["FLAG"].count())
                failed_records = df[df.FLAG == False]
                if df.shape[0] == 0:
                    self.log.info(
                        "Validation Passed, No NULL records in the {} field of {} table".format(self.field_name,
                                                                                                self.table_name))
                    assert 1 == 1
                else:
                    self.log.info(
                        "Validation failed, {} NULL records in the {} field of {} table".format(total_records_count,
                                                                                                self.field_name,
                                                                                                self.table_name))
                    os.chdir(self.failed_files_dir)
                    file = self.table_name + '_' + self.field_name + '.csv'
                    self.log.info("Duplicate records: {}".format(os.path.join(self.failed_files_dir, file)))
                    with open(file, 'w') as file:
                        failed_records.to_csv(file)
                    assert 1 == 2
            except:
                self.log.info("Query: {}".format(sql_query))
                assert 1 == 2
        except:
            self.log.info("File {} has issues, File location: {}".format(self.field_name + '.sql',
                                                                         os.path.join(self.sql_base_dir,
                                                                                      self.registration_type,
                                                                                      self.table_name)))
            assert 1 == 2

    def check_records_count(self):
        print("\n")
        print(
            "Objective - Validation of records count for the {} table".format(self.table_name))
        try:
            sql_query = Open_SQL_File(self.sql_base_dir, self.registration_type, self.table_name,
                                      self.field_name).open_sql_file()
            try:
                df = SQL_Execute(self.connection).execute_sql(sql_query)
                total_records_count = int(df.shape[0])
                failed_records = df[df.FLAG == False]
                if df.shape[0] == 0:
                    self.log.info(
                        "Validation Passed, all records are added successfully in the {} table".format(self.table_name))
                    assert 1 == 1
                else:
                    self.log.info("Validation failed, {} records are missing in {} table".format(total_records_count,
                                                                                                 self.table_name))
                    os.chdir(self.failed_files_dir)
                    file = self.table_name + '_' + self.field_name + '.csv'
                    self.log.info("Records: {}".format(os.path.join(self.failed_files_dir, file)))
                    with open(file, 'w') as file:
                        failed_records.to_csv(file)
                    assert 1 == 2
            except:
                self.log.info("Query: {}".format(sql_query))
                assert 1 == 2
        except:
            self.log.info("File {} has issues, File location: {}".format(self.field_name + '.sql',
                                                                         os.path.join(self.sql_base_dir,
                                                                                      self.registration_type,
                                                                                      self.table_name)))
            assert 1 == 2


class Open_Schema_File:
    def __init__(self, schema_base_dir, registration_type, table_name):
        self.schema_base_dir = schema_base_dir
        self.registration_type = registration_type
        self.table_name = table_name

    def open_schema_file(self):
        os.chdir(self.schema_base_dir + "\\" + self.registration_type)
        schema_file = pd.read_excel(self.table_name + '.xlsx', sheet_name=0, engine='openpyxl')
        return schema_file


class Schema_Capture:
    def rename_schema(self, source_schema, target_schema):
        source_schema.rename(columns={'name': 'src_name', 'type': 'src_type', 'null': 'src_null'}, inplace=True)
        target_cols = target_schema[['name', 'type', 'null?']]
        target_cols.rename(columns={'name': 'tgt_name', 'type': 'tgt_type', 'null?': 'tgt_null'}, inplace=True)
        fin = pd.merge(source_schema, target_cols, left_on='src_name', right_on='tgt_name', how='outer')
        fin['FLAG'] = np.where((fin["src_type"] == fin["tgt_type"]) & (fin["src_null"] == fin["tgt_null"]), True, False)
        return fin, source_schema, target_cols


class Schema_Validation(Open_Schema_File, Schema_Capture, SQL_Execute):
    def __init__(self, schema_base_dir, registration_type, table_name, namespace, sql_query, connection, log):
        Open_Schema_File.__init__(self, schema_base_dir, registration_type, table_name)
        Schema_Capture.__init__(self)
        SQL_Execute.__init__(self, connection)
        self.connection = connection
        self.namespace = namespace
        self.sql_query = sql_query
        self.log = log

    def schema_validation(self):
        try:
            source_schema = Open_Schema_File(self.schema_base_dir, self.registration_type,
                                             self.table_name).open_schema_file()
            try:
                target_schema = SQL_Execute(self.connection).execute_sql(self.sql_query)
                fin, source_cols, target_cols = Schema_Capture().rename_schema(source_schema, target_schema)
                if source_cols.shape[0] == target_cols.shape[0]:
                    if source_cols.shape[0] == (fin[(fin.FLAG == True)]["FLAG"].count()):
                        self.log.info("Schema validation is successful for {}".format(self.table_name))

                        with pd.option_context('display.max_rows', None, 'display.max_columns', None,
                                               'display.precision', 3):
                            self.log.info(target_cols)
                        assert 1 == 1
                    else:
                        self.log.info("Mismatched Columns between Source & Target")
                        for index, row in fin.iterrows():
                            if row['FLAG'] == False:
                                self.log.info(
                                    'Source field: {}, type: {}, nullable?: {} \n '
                                    'Target Field: {}, type: {}, nullable?: {}'.format(
                                        row['src_name'], row['src_type'], row['src_null'], row['tgt_name'],
                                        row['tgt_type'], row['tgt_null']))
                        assert 1 == 2
                else:
                    self.log.info('Column count Mismatch between Source & Target')
                    self.log.info("Source Schema has columns: {}".format(source_cols.shape[0]))
                    self.log.info("Target Schema has columns: {}".format(target_cols.shape[0]))
                    with pd.option_context('display.max_rows', None, 'display.max_columns', None, 'display.precision',
                                           3):
                        for index, row in fin.iterrows():
                            if row['FLAG'] == False:
                                self.log.info(
                                    'Source field: {}, type: {}, nullable?: {} \n'
                                    'Target Field: {}, type: {}, nullable?: {}'.format(
                                        row['src_name'], row['src_type'], row['src_null'], row['tgt_name'],
                                        row['tgt_type'], row['tgt_null']))
                        assert 1 == 2
            except:
                self.log.info('Check query: {}'.format(self.sql_query))
                assert 1 == 2
        except:
            self.log.info("File {} has issues, File location: {}".format(self.table_name + '.xlsx',
                                                                         os.path.join(self.schema_base_dir,
                                                                                      self.registration_type)))
            assert 1 == 2


class Data_Delete:
    def __init__(self, base_dir, file_name, table_list):
        self.base_dir = base_dir
        self.file_name = file_name
        self.table_list = table_list
        # SQL_Execute.__init__(self, connection)

    def save_todays_date_to_file(self):
        os.chdir(self.base_dir)
        today_date = str(date.today())
        with open(self.file_name, 'w') as file:
            file.write(today_date)
