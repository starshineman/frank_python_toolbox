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



def buildQuerySql(jsonobj,qp_dt):

    importType = jsonobj["sqoop.import-type"]

    if importType == "inc":

        tableName=etl_utils.buildSrcTableName(jsonobj)
 
       # tableName=jsonobj["db.table_name"]

        (colsStr,joinColumns,keyColumns) =  etl_utils.buildComlunmnsStrForInit(jsonobj)

        importType = jsonobj["sqoop.import-type"]

        query_template  = sqoop_import_sqlDic[importType]
        qp_dtStr = "'%s'"%(qp_dt,)
        query_sql = query_template%(colsStr,tableName)

        print "query_sql:" + query_sql

    return query_sql


if __name__ == "__main__":
    if not get_argv(sys.argv):
        sys.exit(1)

    dbConfig = sys.argv[1]
    PROJECT_NAME = sys.argv[2]
    etlDate = sys.argv[3]
    businessReadyTime = sys.argv[4]
    qp_dt=etl_utils.setETLDate(etlDate)
    print qp_dt+ "::::::::::::::::::::::::::::::"

    (sqoop_dic,hiveDic) = etl_utils.buildConfDics(dbConfig,"N","etl_dt",qp_dt,buildQuerySql)

    #flag = etl_utils.execimport(sqoop_dic)
    #print "=========== sqoop flag :", flag
    #if flag != 0:
    #  exit(flag)

    flag = etl_utils.loadData2HiveTableAllDeleteAllInsert(hiveDic)
     
    exit(flag)
