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
qp_dt = ""
businessReadyTime = ""

# sqoop increment import sql
inc_query_sql = "select  %s from %s  where  $CONDITIONS"
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

    print "================================="+qp_dt


    dic["-m"]= "1"
    dic["--hive-import"]= " "
    partitionname ="end_dt"
    partitionvalue ="30001231"
   # partitionvalue = qp_dt
    partitionstr = " --hive-partition-key %s  --hive-partition-value %s "%(partitionname,partitionvalue)
    dic["--hive-table"]=  jsonobj["hive_db"]+ "." + jsonobj["hive_table"] + partitionstr

    tablename = jsonobj["hive_db"]+ "." + jsonobj["hive_table"]

    truncateTable(tablename,partitionvalue)

    dic["--split-by"]= jsonobj["sqoop.split-by"]

    querySql =  buildQuerySql(jsonobj,qp_dt)
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

    dic["--inline-lob-limit"] = "16777216"


    tableBasePath = "%s/%s/%s/%s%s/%s/"%(jsonobj["hdfs.root"],jsonobj["hdfs.category.input"],"init",jsonobj["hdfs.db_name"], jsonobj["hdfs.table_name"],jsonobj["hdfs.schema_version"])

    dic["--target-dir"] = tableBasePath + "%s/%s"%(date_str,time_str)
    print "temp dir:%s"%(dic["--target-dir"],)


    jsonConfigList.append(dic)
    return dic


def buildQuerySql(jsonobj,qp_dt):

    importType = jsonobj["sqoop.import-type"]

    if importType == "inc":
        tableName=etl_utils.buildSrcTableName(jsonobj)
       # tableName=jsonobj["db.table_name"]

       # (colsStr,joinColumns,keyColumns) =  etl_utils.buildComlunmnsStrForInit(jsonobj)
        colsList = jsonobj["columns"]
        colsStr = ""
        colsList = sorted(colsList, key=lambda column: column["columnid"])
        db_type = etl_utils.parseDBType(jsonobj)
        count = 0
 
        if len(colsList)==0:
           exit(-1)

        now_time = datetime.datetime.now()
        etl_date_str = now_time.strftime('%Y%m%d')

        for col in colsList:  
            
            colname =  etl_utils.convertColType(col,db_type)
            if count == 0:
              colsStr = colsStr + etl_date_str + ", 19000101 ," + colname
            else:
              colsStr = colsStr + "," + colname
            count = count + 1

        importType = jsonobj["sqoop.import-type"]

        query_template  = sqoop_import_sqlDic[importType]
        qp_dtStr = "'%s'"%(qp_dt,)
        query_sql = query_template%(colsStr,tableName)

        print "query_sql:" + query_sql

    return query_sql

def truncateTable(tablename,etl_dt):
     # use ods database
     cmd = "hive -e \" use ods\" "
     subprocess.call(shlex.split(cmd.encode('ascii')))


     #sql = "truncate table %s"%(tablename)
     sql = "alter table %s drop if exists partition (end_dt='%s')"%(tablename,etl_dt)
     print "================================="+sql

     cmd = "hive -e \" %s\" "%(sql,)

     print "create hive table cmd is:  %s"%(cmd,)
     flag = subprocess.call(shlex.split(cmd.encode('ascii')))




def execimport(dic):

    cmd = "sqoop import --hive-drop-import-delims "
    for key, value in dic.items():
         cmd = cmd + "  %s %s " % (key, value)
    print "sqoop commond: "+ cmd
    flag = subprocess.call(shlex.split(cmd.encode('ascii')))
    return flag

def setETLDate(etl_date_str):
    dateFormat = '%Y%m%d'
    now_time = datetime.datetime.now()
    yes_time = now_time + datetime.timedelta(days=-1)

    if not cmp('tx_dt',etl_date_str):

        etl_date_str = yes_time.strftime(dateFormat)
    elif not cmp('sys_dt',etl_date_str):
        etl_date_str = now_time.strftime(dateFormat)

    qp_dt = etl_date_str
    print "ETL process time:%s"%(qp_dt)
    return qp_dt

if __name__ == "__main__":
    if not get_argv(sys.argv):
        sys.exit(1)

    dbConfig = sys.argv[1]
    PROJECT_NAME = sys.argv[2]
    etlDate = sys.argv[3]
    businessReadyTime = sys.argv[4]
    qp_dt=setETLDate(etlDate)
    print qp_dt+ "::::::::::::::::::::::::::::::"
    getconfs(dbConfig)

    for dic  in jsonConfigList:
        flag = execimport(dic)
        print "flag :", flag
        exit(flag)

