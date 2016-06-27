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
import etl_utils


jsonConfigList = []
PROJECT_NAME = ""
etlDate = ""
businessReadyTime = ""

# sqoop increment import sql
inc_query_sql = "select %s,'19000000' from %s  where  $CONDITIONS"
# sqoop all data import sql
all_query_sql = " "

sqoop_import_sqlDic = {}

sqoop_import_sqlDic["inc"] = inc_query_sql
sqoop_import_sqlDic["all"] = all_query_sql




def get_argv(args):
    if len(args) == 5:
        return True
    else:
        print "Please provide json config file name ,  project name and ETLDate,ReadyTime , then re-run the script."
        return False


def getconfs(dbConfig):

    jsonobj = etl_utils.parseconfs(dbConfig)
    dic = {}

    dic["--connect"] = jsonobj["db.url"]
    # 使用的用户名
    dic["--username"] = jsonobj["db.username"]
    # 使用的密码
    dic["--password"] = '"%s"'%(jsonobj["db.password"])


    dic["-m"]= "1"
    dic["--hive-import"]= " "
    partitionname ="e_dt" 
    partitionvalue ="30001231" 
    partitionstr = " --hive-partition-key %s  --hive-partition-value %s "%(partitionname,partitionvalue)
    dic["--hive-table"]= jsonobj["hive_table"]+"_his " + partitionstr 

    tablename = jsonobj["hive_table"]+ "_his" 
    
    truncateTable(tablename)

    dic["--split-by"]= jsonobj["sqoop.split-by"]

    querySql =  buildQuerySql(jsonobj)
    dic["--query"]= '" %s "'%(querySql,)
    print "sqoop import sql:" + dic["--query"]

    current_time = datetime.datetime.now()

    date_str = current_time.strftime("%Y%m%d")
    date_str = etlDate
    time_str = current_time.strftime("%Y%m%d%H%M")

    dic["--fields-terminated-by"] = " '\\001' "
    dic["--null-string"] = " '\\\N' "
    dic["--null-non-string"] = " '\\\N' "
    #dic["--as-parquetfile"] = " "
    dic["--verbose"] = " "



    tableBasePath = "%s/%s/%s/%s%s/%s/"%(jsonobj["hdfs.root"],jsonobj["hdfs.category.input"],"init",jsonobj["hdfs.db_name"], jsonobj["hdfs.table_name"],jsonobj["hdfs.schema_version"])

    dic["--target-dir"] = tableBasePath + "%s/%s"%(date_str,time_str)
    print "temp dir:%s"%(dic["--target-dir"],)


    jsonConfigList.append(dic)
    return dic


def buildQuerySql(jsonobj):

    importType = jsonobj["sqoop.import-type"]

    if importType == "inc":

        tableName=jsonobj["db.table_name"]

        (colsStr,joinColumns,keyColumns) =  etl_utils.buildComlunmnsStrForInit(jsonobj)

        importType = jsonobj["sqoop.import-type"]

        query_template  = sqoop_import_sqlDic[importType]

        query_sql = query_template%(colsStr,tableName)

        print "query_sql:" + query_sql

    return query_sql

def truncateTable(tablename):
     # use ods database
     cmd = "hive -e \" use ods\" "
     subprocess.call(shlex.split(cmd.encode('ascii')))


     sql = "truncate table %s"%(tablename)

     cmd = "hive -e \" %s\" "%(sql,)

     print "create hive table cmd is:  %s"%(cmd,)
     flag = subprocess.call(shlex.split(cmd.encode('ascii')))




def execimport(dic):
   
    cmd = "sqoop import  "
    for key, value in dic.items():
         cmd = cmd + "  %s %s " % (key, value)
    print "sqoop commond: "+ cmd
    flag = subprocess.call(shlex.split(cmd.encode('ascii')))
    return flag


if __name__ == "__main__":
    if not get_argv(sys.argv):
        sys.exit(1)

    dbConfig = sys.argv[1]
    PROJECT_NAME = sys.argv[2]
    etlDate = sys.argv[3]
    businessReadyTime = sys.argv[4]

    getconfs(dbConfig)

    for dic  in jsonConfigList:
        flag = execimport(dic)
        print "flag :", flag
        exit(flag)
