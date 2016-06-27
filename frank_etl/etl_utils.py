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


SQOOP_CON_NUM="4"


def getSplitNum(jsonobj):
     
   dbtype = parseDBType(jsonobj)
   if dbtype == "mysql":
      return "1"
   else:
      return SQOOP_CON_NUM  

def parseDBType(jsonobj):
   # source DB type: oracle / mysql
    db_url = jsonobj["db.url"]
    g_source_type = db_url.split(":")[1]
    return g_source_type    

def parseSysConfs(dbConfig):
    dbConfigFileName  =  dbConfig
    print "sys json config file:  " + dbConfigFileName
    if os.path.exists(dbConfigFileName):
        configfile = file(dbConfigFileName)
        jsonobj = json.load(configfile)
        print jsonobj
        configfile.close
        return jsonobj


def parseTableConfs(dbConfig):

    dbConfigFileName  =  dbConfig
    print "table  json config file:  " + dbConfigFileName
    if os.path.exists(dbConfigFileName):
        configfile = file(dbConfigFileName)
        jsonobj = json.load(configfile)
        print jsonobj
        configfile.close
        return jsonobj


def parseconfs(dbConfig):

    currentDir = os.getcwd()
    dbConfigFileName  = currentDir + "/" + dbConfig
    print "db json config file:  " + dbConfigFileName
    if os.path.exists(dbConfigFileName):
        configfile = file(dbConfigFileName)
        jsonobj = json.load(configfile)
        print jsonobj
        configfile.close
        return jsonobj



def buildSrcTableName(jsonobj):
    srcTableName = jsonobj["db.database"]+"."+jsonobj["db.table_name"] 
    return srcTableName


def buildComlunmnsStr(jsonobj):
     colsList = jsonobj["columns"]
     colsStr = ""
     joinColumns = ""
     keyColumns = ""
     colsList = sorted(colsList, key=lambda column: column["columnid"])
     count = 0
     joincount = 0
     if len(colsList)==0:
        exit(-1)

     for col in colsList:
         if col["primary_key"]=="true":
             colname = "b.key_" + col["name"]
             joinCol = "b.key_" + col["name"] + "=" + "a." + col["name"]
             keyCol = "key_" + col["name"] 
             
             if joincount == 0:
                joinColumns =  joinColumns + " " +joinCol
                keyColumns =  keyColumns + " " +keyCol
             else:
                joinColumns = joinColumns + " and " + joinCol
                keyColumns =  keyColumns + " ,  " +keyCol

             joincount = joincount +1
         else:
             colname = "a." + col["name"]

         if count == 0:
           colsStr = colsStr + colname
         else:
           colsStr = colsStr + "," + colname
         count = count + 1

     colsStr = colsStr + "," + "b.cdc_type"
     print "colStr: " + colsStr
     print "joinColumns: " + joinColumns
     print "keyColumns: " + keyColumns
     return (colsStr,joinColumns,keyColumns)


def convertColType(col,db_type):
   colName = col["name"]
   convertedColName = ""
   colType = col["type"]
   if colType in ['datetime','timestamp','date'] and db_type == "mysql":
     # convertedColName = "to_char(%s,'yyyymmdd')"%(colName,)
      convertedColName = "date_format(%s,'%%Y-%%m-%%d %%H:%%i:%%s') as "%(colName,) + colName
      print "mysql time/date col:%s"%(convertedColName)
   elif colType == "clob" or colType == "blob" or colType == "raw":
      convertedColName = " null as" +  colName
   elif db_type == "oracle" and colType == "rowid":
      convertedColName = "ROWIDTOCHAR(%s) as g_%s "%(colName,colName)
   elif db_type == "mysql" and colType == "tinyint":
      convertedColName = "cast(%s as  SIGNED) as %s"%(colName,colName)
   else:
      convertedColName = colName

   return convertedColName


def formatList(srcList):
  destList = []
  for srcStr in srcList:
     destStr = srcStr.strip().upper()
     destList.append(destStr)
  return destList


def buildComlunmnsStrForInit(jsonobj):
     colsList = jsonobj["columns"]
     srcTbKeys = jsonobj["db.table_keys"]
     srcTbKeysList = srcTbKeys.split(",") 
     srcTbKeysList = formatList(srcTbKeysList) 
     colsStr = ""
     joinColumns = ""
     keyColumns = ""
     colsList = sorted(colsList, key=lambda column: column["columnid"])
     count = 0
     joincount = 0
     if len(colsList)==0:
        exit(-1)

     
     db_type = parseDBType(jsonobj)
     print "============db type is : %s"%(db_type)
     for col in colsList:
         
         #if col["primary_key"]=="true" or (col["name"] in srcTbKeysList):
         if  (col["name"].upper() in srcTbKeysList):
             colname =  col["name"]
             joinCol = "b.key_" + col["name"] + "=" + "a." + col["name"]
             keyCol = "key_" + col["name"] 
             
             if joincount == 0:
                joinColumns =  joinColumns + " " +joinCol
                keyColumns =  keyColumns + " " +keyCol
             else:
                joinColumns = joinColumns + " and " + joinCol
                keyColumns =  keyColumns + " ,  " +keyCol

             joincount = joincount +1
         else:
             colname =  col["name"]

         
         colname =  convertColType(col,db_type)
         if count == 0:
           colsStr = colsStr + colname
         else:
           colsStr = colsStr + "," + colname
         count = count + 1

     print "colStr: " + colsStr
     print "joinColumns: " + joinColumns
     print "keyColumns: " + keyColumns
     return (colsStr,joinColumns,keyColumns)




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



def buildSqoopTargetDir(jsonobj,date_str):
    tableBasePath = "%s/%s/%s/%s/%s/"%(jsonobj["hdfs.root"],jsonobj["hdfs.category.input"],jsonobj["hdfs.db_name"], jsonobj["hdfs.table_name"],jsonobj["hdfs.schema_version"])
    tableBaseTmpPath = "%s/%s_tmp/%s/%s/%s/"%(jsonobj["hdfs.root"],jsonobj["hdfs.category.input"],jsonobj["hdfs.db_name"], jsonobj["hdfs.table_name"],jsonobj["hdfs.schema_version"])
    targetDirPath = tableBasePath + "%s"%(date_str,)
    targetTmpDirPath = tableBaseTmpPath + "%s"%(date_str,)
    print "target path is : " + targetDirPath
    print "target tmp  path is : " + targetTmpDirPath
    return (targetDirPath,targetTmpDirPath)


def hdfs_path_exists(path):
    res = subprocess.call(["hadoop", "fs", "-test", "-e", path])
    if res != 0:
        return False
    else:
        return True



def reloadStgData2HiveTable(dic):

    stgTmpPath = dic["stgTmpPath"]
    stgPath = dic["stgPath"]
    hivetable = dic["hivetable"]
    partitionName = dic["partitionName"]
    partitionValue = dic["partitionValue"]

    if  not hdfs_path_exists(stgPath):
       print "stg path is not exist! job terminated!"
       exit(-1)

    truncateTable(hivetable,partitionName,partitionValue)
    if  hdfs_path_exists(stgTmpPath):
       subprocess.call(["hadoop", "fs", "-rm", "-R", stgTmpPath])
    
    subprocess.call(["hadoop", "fs", "-mkdir", "-p", stgTmpPath])
    flag =subprocess.call(["hadoop", "fs", "-cp",stgPath+"/*",stgTmpPath])
    if flag !=0:
      print "copy %s data into  %s failed! job terminated!"%(stgPath,stgTmpPath)
      exit(-1)

    loadData2HiveSql = "load data inpath  '%s'  overwrite  \
    into table %s  partition (%s='%s')"%(stgTmpPath,hivetable,partitionName,partitionValue)

    cmd = "hive -e \" %s\" "%(loadData2HiveSql,)

    print "load data into hive table cmd is:  %s"%(cmd,)
    flag = subprocess.call(shlex.split(cmd.encode('ascii')))
   
    subprocess.call(["hadoop", "fs", "-rm", "-R", stgTmpPath])
    return flag



def loadData2HiveTable(dic):

    stgTmpPath = dic["stgTmpPath"]
    stgPath = dic["stgPath"]
    hivetable = dic["hivetable"]
    partitionName = dic["partitionName"]
    partitionValue = dic["partitionValue"]

    if  not hdfs_path_exists(stgTmpPath):
       print "stg tmp path is not exist! job terminated!"
       exit(-1)

    truncateTable(hivetable,partitionName,partitionValue)
    
    if  hdfs_path_exists(stgPath):
       subprocess.call(["hadoop", "fs", "-rm", "-R", stgPath])
    
    subprocess.call(["hadoop", "fs", "-mkdir", "-p", stgPath])
    flag =subprocess.call(["hadoop", "fs", "-cp",stgTmpPath+"/*",stgPath])
    if flag !=0:
      print "copy %s data into  %s failed! job terminated!"%(stgTmpPath,stgPath)
      exit(-1)
    
    loadData2HiveSql = "load data inpath  '%s'  overwrite  \
    into table %s  partition (%s='%s')"%(stgTmpPath,hivetable,partitionName,partitionValue)

    cmd = "hive -e \" %s\" "%(loadData2HiveSql,)

    print "load data into hive table cmd is:  %s"%(cmd,)
    flag = subprocess.call(shlex.split(cmd.encode('ascii')))
    
    subprocess.call(["hadoop", "fs", "-rm", "-R", stgTmpPath])
    return flag


def loadData2HiveTableAllDeleteAllInsert(dic):

    stgTmpPath = dic["stgTmpPath"]
    stgPath = dic["stgPath"]
    hivetable = dic["hivetable"]
    partitionName = dic["partitionName"]
    partitionValue = dic["partitionValue"]

    if  not hdfs_path_exists(stgTmpPath):
       print "stg tmp path is not exist! job terminated!"
       exit(-1)
    
    truncateWholeTable(hivetable)

    if  hdfs_path_exists(stgPath):
       subprocess.call(["hadoop", "fs", "-rm", "-R", stgPath])

    subprocess.call(["hadoop", "fs", "-mkdir", "-p", stgPath])
    flag =subprocess.call(["hadoop", "fs", "-cp",stgTmpPath+"/*",stgPath])
    if flag !=0:
      print "copy %s data into  %s failed! job terminated!"%(stgTmpPath,stgPath)
      exit(-1)

    loadData2HiveSql = "load data inpath  '%s'  overwrite  \
    into table %s  "%(stgTmpPath,hivetable)

    cmd = "hive -e \" %s\" "%(loadData2HiveSql,)

    print "load data into hive table cmd is:  %s"%(cmd,)
    flag = subprocess.call(shlex.split(cmd.encode('ascii')))

    subprocess.call(["hadoop", "fs", "-rm", "-R", stgTmpPath])
    return flag






def truncateTable(tableName,partitionName,partitionValue):

     sql = "alter table %s drop if exists partition (%s='%s')"%(tableName,partitionName,partitionValue)
     print "================================="+sql

     cmd = "hive -e \" %s\" "%(sql,)

     print "truncate table cmd is:  %s"%(cmd,)
     flag = subprocess.call(shlex.split(cmd.encode('ascii')))
     return flag



def truncateWholeTable(tableName):

     sql = "TRUNCATE TABLE  %s "%(tableName,)
     print "================================="+sql

     cmd = "hive -e \" %s\" "%(sql,)

     print "truncate table cmd is:  %s"%(cmd,)
     flag = subprocess.call(shlex.split(cmd.encode('ascii')))
     return flag







def buildConfDics(dbConfig,isChain,parName,parValue,buildQuerySqlFunc):

    jsonobj = parseconfs(dbConfig)
    dic = {}
    hiveDic = {}
 



    qp_dt = parValue

    dic["--connect"] = jsonobj["db.url"]
    # 使用的用户名
    dic["--username"] = jsonobj["db.username"]
    # 使用的密码
    dic["--password"] = '"%s"'%(jsonobj["db.password"])

    print "================================="+qp_dt

    dic["-m"]= getSplitNum(jsonobj) 

    dic["--delete-target-dir"]= " "
    partitionname = parName
    partitionvalue = qp_dt

    hive_table_name = jsonobj["hive_table"]


    if isChain=="Y":

       tablename = jsonobj["hive_db"]+ "." + hive_table_name[0:-1]+"m"
    else:
       tablename = jsonobj["hive_db"]+ "." + jsonobj["hive_table"]

    dic["--split-by"]= jsonobj["sqoop.split-by"]

    querySql =  buildQuerySqlFunc(jsonobj,qp_dt)
    dic["--query"]= '" %s "'%(querySql,)
    print "sqoop import sql:" + dic["--query"]

    date_str = qp_dt

    dic["--fields-terminated-by"] = " '\\001' "
    dic["--null-string"] = " '\\\N' "
    dic["--null-non-string"] = " '\\\N' "
    dic["--verbose"] = " "

    dic["--inline-lob-limit"] = "16777216"

    (hiveDic["stgPath"],dic["--target-dir"]) = buildSqoopTargetDir(jsonobj,date_str)

    hiveDic["stgTmpPath"] = dic["--target-dir"]
    hiveDic["hivetable"] = tablename
    hiveDic["partitionName"] = partitionname
    hiveDic["partitionValue"] = partitionvalue

    return (dic,hiveDic)


def execimport(dic):

    cmd = "sqoop import --hive-drop-import-delims "
    for key, value in dic.items():
         cmd = cmd + "  %s %s " % (key, value)
    print "======================= sqoop commond: "+ cmd
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

    str = "select %s from ( \
    select id,key_id as data_id,t.cdc_type,t.cdc_ts ,row_number() \
    over (partition by t.key_id order by id desc) as rn \
    from %s_cdc t  \
    where to_char(t.cdc_ts,'YYYYMMDD')='%s') b \
    left join %s a \
    on %s \
    where b.rn=1 "

    tableColumns = "a,b"
    etl_date = "20160418"
    tableName = "aaa"
    joinColumns =""
    str1 = str%(tableColumns,tableName,etl_date,tableName,joinColumns)
    print str1

    #jsonobj = parseconfs("wormhole_config.json")

    #(colsStr,joinColumns) = buildComlunmnsStr(jsonobj)

