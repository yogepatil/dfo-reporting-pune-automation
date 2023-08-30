import os
import sys
import snowflake.connector
import pandas as pd
import numpy as np
import openpyxl
from datetime import *


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

    def develope_connection(self, username, password, account, warehouse, database, schema, role, base_dir, log):
        conn = snowflake.connector.connect(user=username, password=password, account=account,
                                           warehouse=warehouse, database=database, schema=schema,
                                           role=role)
        connection = conn.cursor()
        return connection