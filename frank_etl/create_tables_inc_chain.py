#!/usr/bin/python
# -*- coding: UTF-8 -*-
import sys
import os
import datetime
import time
import re
import subprocess
import shlex
import json


jsonConfigList = []
PROJECT_NAME = ""
etlDate = ""
db2hiveColTypeMap = {}
businessReadyTime = ""
SLEEP_SECONDS = 600
colsDefInConfFileStr = ""

FIELD_MAPPING_SPARK2HIVE_DIC = {
    "date":"DATE",
    "datetime":"TIMESTAMP",
    "boolean":"BOOLEAN",
    "int":"INT",
    "long":"BIGINT",
    "float":"FLOAT",
    "double":"DOUBLE",
    "string":"STRING",
    "clob":"STRING",
    "blob":"STRING"
}



def parseDB2HiveColMap(mappingStr):

     if mappingStr != "":
         pairs = mappingStr.split(",")
         for rec in pairs:
              field = rec.split("=")
              name = field[0]
              type = field[1]
              db2hiveColTypeMap[name] = type


def get_argv(args):
    if len(args) == 3:
        return True
    else:
        print "Please provide json config file dir ,project name , then re-run the script."
        return False



def parseconfs(dirName):



    configList  = os.listdir(dirName)
    currentDir = os.getcwd()
    for fileName in configList:
        absFileName  = currentDir + "/" + dirName + "/"+ fileName

        dic = {}

        print "db json config file:  " + absFileName
        if os.path.exists(absFileName):
            configfile = file(absFileName)
            jsonobj = json.load(configfile)
            print "config json: " + json.dumps(jsonobj)
            configfile.close


            dic["usechain"] = jsonobj["usechain"]

            dic["--connect"] = jsonobj["db.url"]
            # 使用的用户名
            dic["--username"] = jsonobj["db.username"]
            # 使用的密码
            dic["--password"] = jsonobj["db.password"]

            dic["hdfs.root"] = jsonobj["hdfs.root"]
            dic["hdfs.category.input"] = jsonobj["hdfs.category.input"]
            dic["hdfs.category.output"] = jsonobj["hdfs.category.output"]
            dic["hdfs.db_name"] = jsonobj["hdfs.db_name"]
            dic["hdfs.table_name"] = jsonobj["hdfs.table_name"]
            dic["hdfs.schema_version"] = jsonobj["hdfs.schema_version"]

            dbName = jsonobj["db.database"]
            dic["--table"] = dbName + ".rel_"+ jsonobj["db.table_name"]


            dic["hiveDBName"] = jsonobj["hive_db"]
            dic["hiveTableName"] = jsonobj["hive_table"]

            dB2HiveColumnMappingStr = jsonobj["sqoop.map-column-hive"]

            parseDB2HiveColMap(dB2HiveColumnMappingStr)

            colsList = jsonobj["columns"]
            if len(colsList)== 0:
               print "table %s:,columns is empty!exit program!"%(dic["hiveTableName"])
               exit(-1)

            colsStr = ""

            colsList = sorted(colsList, key=lambda column: column["columnid"])

            for col in colsList:
                colName = col["name"]
                hiveColType = db2hiveColTypeMap.get(colName)

                if hiveColType != None:
                  colType = hiveColType
                else:
                  colType = FIELD_MAPPING_SPARK2HIVE_DIC[col["type"]]

                if colsStr =="":
                  colsStr = colName + " " + colType
                else:
                  colsStr = colsStr + ","+ colName + " " + colType



            hischainColsStr = colsStr + ", " + "s_dt" + " " + "STRING"


            dic["columns"] = colsStr
            dic["hischainCols"] = "etl_dt STRING, start_dt STRING," + colsStr
             
	    dic["chainMidCols"] = colsStr  

            print "colsStr:%s"%(colsStr,)
            jsonConfigList.append(dic)

    return jsonConfigList



def dropOneTable(hiveTableName):

    # use ods database
    cmd = "hive -e \" use ods\" "
    subprocess.call(shlex.split(cmd.encode('ascii')))


    # drop ods table
    cmd = "hive -e \" DROP TABLE IF EXISTS %s\" "%(hiveTableName,)
    print cmd
    flag = subprocess.call(shlex.split(cmd.encode('ascii')))
    return flag


def createOneTable(hiveTableName,hivefields,partitionfield):

    # use ods database
    cmd = "hive -e \" use ods\" "
    subprocess.call(shlex.split(cmd.encode('ascii')))


    # drop ods table
    cmd = "hive -e \" DROP TABLE IF EXISTS %s\" "%(hiveTableName,)
    print cmd
    subprocess.call(shlex.split(cmd.encode('ascii')))


    createHiveSql = "create  TABLE  IF NOT EXISTS %s( \
     %s)  partitioned by (%s string)  \
      "%(hiveTableName,hivefields,partitionfield)

    cmd = "hive -e \" %s\" "%(createHiveSql,)

    print "create hive table cmd is:  %s"%(cmd,)
    flag = subprocess.call(shlex.split(cmd.encode('ascii')))
    return flag

def createTables(dic):

    dbname = dic["hiveDBName"]
    tablename =dic["hiveTableName"]


    hischainTable = dbname +  "." + tablename 
    hischainFields = dic["hischainCols"]
    flag = createOneTable(hischainTable,hischainFields,"end_dt")

    
    chainMidTable = dbname +  "." + tablename[0:-1]+ "m" 
    chainMidFields = dic["chainMidCols"]  
    flag = createOneTable(chainMidTable,chainMidFields,"etl_dt")

    return flag



if __name__ == "__main__":
    if not get_argv(sys.argv):
        sys.exit(1)

    importDir = sys.argv[1]
    PROJECT_NAME = sys.argv[2]

    parseconfs(importDir)

    for dic in jsonConfigList:
        flag = createTables(dic)
        print "flag :", flag

    exit(flag)
