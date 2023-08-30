# @Author  : Yogesh Patil
# @Time    : 12/01/2023
# @File    : ReadProperties.py

import configparser

config = configparser.RawConfigParser()
configDirPath = r"C:\Users\yogepatil\Project\DR-Reporting\Automation_Testing\dfo-reporting-PyTest-Automation" \
                r"\Configurations\dforeporting_config.ini "
print('### Config. File Dir: ' + configDirPath)
config.read(configDirPath)


class ReadConfig:
    @staticmethod
    def getSystemPath():
        systemPath = config.get('common info', 'BASE_DIR')
        return systemPath

    @staticmethod
    def getEnvironment():
        env = config.get('common info', 'SNOW_ENV')
        return env

    @staticmethod
    def getSchemaPath():
        schema = config.get('common info', 'SCHEMA_DIR')
        return schema
