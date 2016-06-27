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
inc_query_sql = "select %s from ( \
    select %s ,t.cdc_type,row_number() \
    over (partition by t.key_id order by id desc) as rn \
    from %s_cdc t  \
    where to_char(t.cdc_ts,'YYYYMMDD')='%s') b \
    left join %s a \
    on %s \
    where b.rn=1 "

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

    dic["--split-by"]= jsonobj["sqoop.split-by"]

    querySql =  buildQuerySql(jsonobj)
    dic["--query"]= '"%sand $CONDITIONS"'%(querySql,)
    print "sqoop import sql:" + dic["--query"]

    current_time = datetime.datetime.now()

    date_str = current_time.strftime("%Y%m%d")
    date_str = etlDate
    time_str = current_time.strftime("%Y%m%d%H%M")

    tableBasePath = "%s/%s/%s/%s/%s/"%(jsonobj["hdfs.root"],jsonobj["hdfs.category.input"],jsonobj["hdfs.db_name"], jsonobj["hdfs.table_name"],jsonobj["hdfs.schema_version"])

    dic["--target-dir"] = tableBasePath + "%s/%s"%(date_str,time_str)

    print "target dir: " +  dic["--target-dir"]


    dic["--fields-terminated-by"] = " '\001' "
    dic["--hive-drop-import-delims"] = "  "


    #dic["--fields-terminated-by"] = " '\\001' "
    dic["--null-string"] = " '\\\N' "
    dic["--null-non-string"] = " '\\\N' "
    #dic["--as-parquetfile"] = " "
    dic["--verbose"] = " "

    dic["--inline-lob-limit"] = "16777216"

    jsonConfigList.append(dic)

    return dic


def buildQuerySql(jsonobj):

    importType = jsonobj["sqoop.import-type"]

    if importType == "inc":

        tableName=jsonobj["db.table_name"]

        (colsStr,joinColumns,keyColumns) =  etl_utils.buildComlunmnsStr(jsonobj)

        importType = jsonobj["sqoop.import-type"]

        query_template  = sqoop_import_sqlDic[importType]

        query_sql = query_template%(colsStr,keyColumns,tableName,etlDate,tableName,joinColumns)

        print "query_sql:" + query_sql

    return query_sql


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
