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
import etl_checkdata


jsonConfigList = []
PROJECT_NAME = ""
etlDate = ""
qp_dt = ""
businessReadyTime = ""

whereCondition = "" 

# sqoop increment import sql
inc_query_sql = "select  %s from %s  where %s  and $CONDITIONS"
# sqoop all data import sql
all_query_sql = " "

sqoop_import_sqlDic = {}

sqoop_import_sqlDic["inc"] = inc_query_sql
sqoop_import_sqlDic["all"] = all_query_sql




def get_argv(args):
    if len(args) == 6:
        return True
    else:
        print "Please provide json config file name ,  project name and ETLDate,ReadyTime,whereCondition , then re-run the script."
        return False


def buildQuerySql(jsonobj,qp_dt):

    importType = jsonobj["sqoop.import-type"]

    if importType == "inc":

        tableName=etl_utils.buildSrcTableName(jsonobj)

        (colsStr,joinColumns,keyColumns) =  etl_utils.buildComlunmnsStrForInit(jsonobj)

        importType = jsonobj["sqoop.import-type"]

        query_template  = sqoop_import_sqlDic[importType]
        qp_dtStr = "'%s'"%(qp_dt,)

        whereConditionFinal = whereCondition%(qp_dt,)

        query_sql = query_template%(colsStr,tableName,whereConditionFinal)

        print "query_sql:" + query_sql

    return query_sql

if __name__ == "__main__":
    if not get_argv(sys.argv):
        sys.exit(1)

    dbConfig = sys.argv[1]
    PROJECT_NAME = sys.argv[2]
    etlDate = sys.argv[3]
    businessReadyTime = sys.argv[4]
    whereCondition = sys.argv[5]
    qp_dt=etl_utils.setETLDate(etlDate)
    print qp_dt+ "::::::::::::::::::::::::::::::"

    (sqoop_dic,hiveDic) = etl_utils.buildConfDics(dbConfig,"Y","etl_dt",qp_dt,buildQuerySql)


    now_time = datetime.datetime.now()
    run_time = now_time.strftime('%Y%m%d %H:%M:%S')

    etl_checkdata.check_data(dbConfig,qp_dt,run_time,"","source",1)


    flag = etl_utils.execimport(sqoop_dic)
    print "=========== sqoop flag :", flag
    if flag != 0:
      exit(flag)

    flag = etl_utils.loadData2HiveTable(hiveDic)

    if flag == 0:
       etl_checkdata.check_data(dbConfig,qp_dt,qp_dt,"","source",2)

    exit(flag)


