# @Author  : Yogesh Patil
# @Time    : 13/01/2023
# @File    : LoggerFile.py

import logging
import sys


class BaseClass:
    def getlogger(self):
        logger = logging.getLogger(__name__)
        file_handler = logging.FileHandler('LOGS\Data-Reporting-Execution.log')
        formatter = logging.Formatter("%(asctime)s :%(levelname)s : %(name)s : %(message)s")
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
        logger.setLevel(logging.DEBUG)
        logger = logging.getLogger(__name__)
        logger.setLevel(logging.DEBUG)
        log_format = "%(asctime)s-%(lineno)s:  %(message)s"
        log_handler = logging.StreamHandler(sys.stdout)
        log_handler.setFormatter(logging.Formatter(log_format))
        logger.addHandler(log_handler)
        logger.propagate = False
        return logger
