# @Author  : Yogesh Patil
# @Time    : 13/01/2023
# @File    : ProjDirectorySetup.py

import os
import sys


class ProjDirSetup:
    @staticmethod
    def getTestProjDirectory(path, val):
        prjBaseDir = sys.path[val]
        print('PROJECT BASE DIRECTORY: ' + prjBaseDir)
        if 'TPTestSuite' not in prjBaseDir:
            requestDirPath = os.path.join(prjBaseDir, path)
            print('************* ' + requestDirPath + ' **************')
        else:
            requestDirPath = os.path.join(os.path.normpath(prjBaseDir + os.sep + os.pardir), path)
            print('************* ' + requestDirPath + ' **************')
        return requestDirPath

    @staticmethod
    def getProjDirectory(path, val):
        prjBaseDir = sys.path[val]
        print('PROJECT BASE DIRECTORY: ' + prjBaseDir)
        if '\\TEST\\Script' in prjBaseDir:
            requestDirPath = os.path.join(os.path.normpath(prjBaseDir + os.sep + os.pardir + os.sep + os.pardir), path)
            print('************* ' + requestDirPath + ' **************')
        else:
            requestDirPath = os.path.join(prjBaseDir, path)
            print('************* ' + requestDirPath + ' **************')
        return requestDirPath
