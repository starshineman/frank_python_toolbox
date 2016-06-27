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
    if len(args) == 5:
        return True
    else:
        print "Please provide json config file name ,  project name and ETLDate,ReadyTime , then re-run the script."
        return False



def hdfs_path_exists(path):
    res = subprocess.call(["hadoop", "fs", "-test", "-e", path])
    if res != 0:
        return False
    else:
        return True




def parseconfs(dbConfig,tbSchema):


    currentDir = os.getcwd()

    dbConfigFileName  = currentDir + "/" + dbConfig
    tbSchemaFileName  = currentDir + "/" + tbSchema
    dic = {}

    print "db json config file:  " + dbConfigFileName
    if os.path.exists(dbConfigFileName):
        configfile = file(dbConfigFileName)
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




    print "table json Schema file:  " + tbSchemaFileName
    if os.path.exists(tbSchemaFileName):
        configfile = file(tbSchemaFileName)
        jsonobj = json.load(configfile)

        print "Schema json: " + json.dumps(jsonobj)
        configfile.close

        dbName = jsonobj["db.database"]
        dic["--table"] = dbName + "."+ jsonobj["db.table_name"]


        dic["hiveTableName"] = jsonobj["hive_table"]


        dB2HiveColumnMappingStr = jsonobj["sqoop.map-column-hive"]

        parseDB2HiveColMap(dB2HiveColumnMappingStr)

        colsList = jsonobj["columns"]

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


        # sqoop increment import
        if jsonobj["usechain"]=="false" and jsonobj[ "sqoop.import-type"]== "inc":
            colsStr =  colsStr + ", " + "cdc_type" + " " + "STRING"

        dic["columns"] = colsStr
        print "length of  colsList:%s "%(len(colsList),)
        dic["columnsList"] = colsList

        print "colsStr:%s"%(colsStr,)


    jsonConfigList.append(dic)
    return dic


def checkConfig(dic):
   colsList = dic["columnsList"] 
   tableName = dic["hiveTableName"] 
   if len(colsList)==0:
       print "table :%s columns is empty,exit program!"%(tableName)
       exit(-1)


def get_all_paths(basepath):
    path_catas = []
    path = basepath
    if hdfs_path_exists(path):
        # Get biz_end_date
        beds = subprocess.Popen(["hadoop", "fs", "-ls", path], stdout=subprocess.PIPE).communicate()
        bed_list = beds[0].split("\n")
        for bed in bed_list:
            sub_bed = re.search(r'/.+', bed)
            if sub_bed:
                base_psd = sub_bed.group()
                if base_psd.strip():
                    base_psd = r"hdfs:"+ base_psd
                    path_catas.append(base_psd)
                    print base_psd
    return path_catas



def createCurrentTableForAllData(dic):

    # copy data from stg to ods
    stgPath = dic["stg_path"]
    odsPath = dic["ods_path"]


    if not hdfs_path_exists(odsPath):
       subprocess.call(["hadoop", "fs", "-mkdir", "-p", odsPath])

    stgDatePaths=get_all_paths(stgPath)

    for stgPath in stgDatePaths:
        pathParts=stgPath.split(r"/")
        pathTimePart = pathParts[len(pathParts)-1]
        if "ed" in pathTimePart:
            pass
        else:
            subprocess.call(["hadoop", "fs", "-cp",stgPath,odsPath])
            stgedPath = stgPath + "ed"
            subprocess.call(["hadoop", "fs", "-mv",stgPath,stgedPath])


    # use ods database
    cmd = "hive -e \" use ods\" "
    subprocess.call(shlex.split(cmd.encode('ascii')))


    hiveTableName = dic["hiveTableName"]

    hiveTableName = hiveTableName + "_curr"

    # drop ods table
    cmd = "hive -e \" DROP TABLE IF EXISTS %s\" "%(hiveTableName,)
    print cmd
    subprocess.call(shlex.split(cmd.encode('ascii')))

    # create hive table

    hivefields = dic["columns"]

    odsDataPaths = get_all_paths(odsPath)

    bizDates = []
    for odspath in odsDataPaths:
        pathParts=odspath.split(r"/")
        pathTimePart = pathParts[len(pathParts)-1]
        bizDate  = datetime.datetime.strptime(pathTimePart,"%Y%m%d%H%M")
        bizDates.append(bizDate)

    maxTime = datetime.datetime.strptime("190001010000","%Y%m%d%H%M")
    for biztime in bizDates:
        if biztime > maxTime:
            maxTime = biztime

    maxTimeStr = maxTime.strftime("%Y%m%d%H%M")
    hdfsPath = odsPath + "/" + maxTimeStr

    createHiveSql = "CREATE EXTERNAL TABLE %s( \
    %s) STORED AS PARQUET \
    location '%s' "%(hiveTableName,hivefields,hdfsPath)

    createHiveSql = "CREATE EXTERNAL TABLE %s( \
    %s) \
    location '%s' "%(hiveTableName,hivefields,hdfsPath)

    cmd = "hive -e \" %s\" "%(createHiveSql,)

    print "create hive table cmd is:  %s"%(cmd,)
    flag = subprocess.call(shlex.split(cmd.encode('ascii')))
    return flag


def createHisSnapTableForAllData(dic):

    # copy data from stg to ods
    stgPath = dic["stg_path"]
    odsPath = dic["ods_path"]

    tableBasePath = dic["ods_base_path"]

    if not hdfs_path_exists(odsPath):
       subprocess.call(["hadoop", "fs", "-mkdir", "-p", odsPath])

    stgDatePaths=get_all_paths(stgPath)

    for stgPath in stgDatePaths:
        pathParts=stgPath.split(r"/")
        pathTimePart = pathParts[len(pathParts)-1]
        if "ed" in pathTimePart:
            pass
        else:
            subprocess.call(["hadoop", "fs", "-cp",stgPath,odsPath])
            stgedPath = stgPath + "ed"
            subprocess.call(["hadoop", "fs", "-mv",stgPath,stgedPath])


    # use ods database
    cmd = "hive -e \" use ods\" "
    subprocess.call(shlex.split(cmd.encode('ascii')))


    hiveTableName = dic["hiveTableName"]

    hiveTableName = hiveTableName + "_all"

    # create hive table

    hivefields = dic["columns"]

    odsDataPaths = get_all_paths(odsPath)

    bizDates = []
    for odspath in odsDataPaths:
        pathParts=odspath.split(r"/")
        pathTimePart = pathParts[len(pathParts)-1]
        bizDate  = datetime.datetime.strptime(pathTimePart,"%Y%m%d%H%M")
        bizDates.append(bizDate)

    maxTime = datetime.datetime.strptime("190001010000","%Y%m%d%H%M")
    for biztime in bizDates:
        if biztime > maxTime:
            maxTime = biztime

    maxTimeStr = maxTime.strftime("%Y%m%d%H%M")
    hdfsPath = odsPath + "/" + maxTimeStr



    createHiveSql = "create EXTERNAL TABLE  IF NOT EXISTS %s( \
     %s)  partitioned by (biz_end_date string) STORED AS PARQUET \
     location '%s' "%(hiveTableName,hivefields,tableBasePath)


    createHiveSql = "create EXTERNAL TABLE  IF NOT EXISTS %s( \
     %s)  partitioned by (biz_end_date string)  \
     location '%s' "%(hiveTableName,hivefields,tableBasePath)

    cmd = "hive -e \" %s\" "%(createHiveSql,)

    print "create hive table cmd is:  %s"%(cmd,)
    flag = subprocess.call(shlex.split(cmd.encode('ascii')))

    dt2PatitionDic = {}

    current_time = datetime.datetime.now()

    date_str = current_time.strftime("%Y%m%d")

    date_str = etlDate

    dt2PatitionDic[date_str] = hdfsPath

    partitions = collate_partitions(dt2PatitionDic)

    drop_one_partition(hiveTableName,date_str)

    # Add all partitions
    flag = add_all_partitions(hiveTableName, partitions)
    return flag

def allDataTableAlgo(dic):


    current_time = datetime.datetime.now()
    date_str = current_time.strftime("%Y%m%d")
    date_str = etlDate

    print "etl_date:%s"%(date_str)

    stgBasePath = "%s/%s/%s/%s/%s/"%(dic["hdfs.root"],dic["hdfs.category.input"],dic["hdfs.db_name"], dic["hdfs.table_name"],dic["hdfs.schema_version"])
    odsBasePath = "%s/%s/%s/%s/%s/"%(dic["hdfs.root"],dic["hdfs.category.output"],dic["hdfs.db_name"], dic["hdfs.table_name"],dic["hdfs.schema_version"])

    dic["stg_path"] = stgBasePath + "%s"%(date_str)
    dic["ods_path"] = odsBasePath + "%s"%(date_str)
    dic["ods_base_path"] = odsBasePath


    createCurrentTableForAllData(dic)
    flag =  createHisSnapTableForAllData(dic)
    return flag


def getMaxProcessTimeStamp(chainStausFileContent):

    process_list = chainStausFileContent[0].split("\n")

    bizDates = {}
    for processStr in process_list:

        if processStr =="":
            continue
        onetimestampList = processStr.split("\t")
        timeIdStr = onetimestampList[0]
        if len(onetimestampList)==2:
            changedTableSchema = onetimestampList[1]
        else:
            changedTableSchema = ""
        timeIdList = timeIdStr.split("_")
        timestampStr = timeIdList[0]
        idStr = timeIdList[1]
        processTime = datetime.datetime.strptime(timestampStr,"%Y%m%d%H%M%S")

        if bizDates.get(processTime) is None:
            bizDates[processTime] = {}

        bizDates[processTime][idStr] = changedTableSchema

    maxTime = datetime.datetime.strptime("19000101000000","%Y%m%d%H%M%S")
    for biztime in bizDates.keys():
       if biztime > maxTime:
         maxTime = biztime

    return (maxTime,bizDates)




def getHisMaxProcessTimeStamp(oneDatehisFilelist):

    bizDates = {}
    for hisTimepathItem in oneDatehisFilelist:

        templist = hisTimepathItem.split(r"/")
        hisDateIdStr = templist[len(templist)-1]

        timeIdList = hisDateIdStr.split("_")
        timestampStr = timeIdList[0]
        idStr = timeIdList[1]

        processTime  = datetime.datetime.strptime(timestampStr,"%Y%m%d%H%M%S")

        if bizDates.get(processTime) is None:
            bizDates[processTime] = []

        bizDates[processTime].append(idStr)

    maxTime = datetime.datetime.strptime("19000101000000","%Y%m%d%H%M%S")
    for biztime in bizDates.keys():
       if biztime > maxTime:
         maxTime = biztime

    return (maxTime,bizDates)



def convertSchema2dic(schemaColList):
    dic = {}
    if len(schemaColList)>0:
       for col in schemaColList:
          colName = col["name"]
          colType = col["type"]
          dic[colName] = colType
    return dic


def tableSchemaChanged(srcColList,destColList):

    destDic =  convertSchema2dic(destColList)
    srcDic =  convertSchema2dic(srcColList)

    srcJsonStr = json.dumps(srcDic,sort_keys = True)
    destJsonStr = json.dumps(destDic,sort_keys = True)

    print "srcJsonStr:%s\n,destJsonStr:%s\n"%(srcJsonStr,destJsonStr)
    for (key,value) in srcDic.items():
       if destDic.get(key)==None or destDic.get(key)!=value:
         return True
    return False



def getChainStatusFile(path):
   
    successFileList = get_all_paths(path)

    bizDates = {}
    for processStr in successFileList:

       tempList = processStr.split(r"/")
       filename  = tempList[len(tempList)-1]

       timeIdList=filename.split(r'.')[0].split(r'_')
       timestampStr = timeIdList[0]
       idStr = timeIdList[1]
       processTime = datetime.datetime.strptime(timestampStr,"%Y%m%d%H%M%S")

       if bizDates.get(processTime) is None:
          bizDates[processTime] = {}

       bizDates[processTime][idStr] = processStr

    maxTime = datetime.datetime.strptime("19000101000000","%Y%m%d%H%M%S")
    for biztime in bizDates.keys():
       if biztime > maxTime:
         maxTime = biztime

    ids = bizDates[maxTime]
    maxid = max(ids.keys())
    chainFileName = ids[maxid]
    return chainFileName




def chainDataTableAlgo(dic):




    tableBasePath = "%s/%s/%s/%s/%s/"%(dic["hdfs.root"],dic["hdfs.category.output"],dic["hdfs.db_name"], dic["hdfs.table_name"],dic["hdfs.schema_version"])

    snapshotBasePath = tableBasePath + "20991231"
    historyBasePath = tableBasePath
    chainStausBase = tableBasePath + "success"
    hiveTableName =  dic["hiveTableName"]
    hivefields = dic["columns"]

    while True:

        chainStausFileList=getChainStatusFile(chainStausBase).split(r'/')
        chainStausFilePath = chainStausBase + "/"+ chainStausFileList[len(chainStausFileList)-1]
        print chainStausFilePath
        if hdfs_path_exists(chainStausFilePath):
             chainStausFileContent = subprocess.Popen(["hadoop", "fs", "-cat",chainStausFilePath], stdout=subprocess.PIPE).communicate()

             (maxTime,bizDatesdic) = getMaxProcessTimeStamp(chainStausFileContent)

             todayStr = datetime.datetime.now().strftime("%Y%m%d")
             readyTimeStr = todayStr + businessReadyTime
             readyTime = datetime.datetime.strptime(readyTimeStr, "%Y%m%d%H%M%S")

             print "maxTime:%s,readyTime:%s!"%(maxTime,readyTime)
             if maxTime <= readyTime:
                 time.sleep(SLEEP_SECONDS)
             #chain 工作状态文件中最大的时间记录大于业务系统的跑批结束时间时,开始建表
             else:


                 ids = bizDatesdic[maxTime].keys()
                 maxid=max(ids)
                 schemaJson = bizDatesdic[maxTime][maxid]
                 jsonDestObj = json.loads(schemaJson)
                 destColList = jsonDestObj['columns']
                 srcColList = dic["columnsList"]


                 tableSchemaChangedFlag = tableSchemaChanged(srcColList,destColList)
                 #tableSchemaChangedFlag = False
                 if tableSchemaChangedFlag:
                     tableName = "%s.%s"%(dic["hdfs.db_name"],dic["hdfs.table_name"])
                     print "src table: %s schema changed,job terminated!"%tableName
                     exit(-2)
                 # 构建histroy 和  snapshot 分区 和 存储路径 的对应关系的dic
                 dt2PatitionDic = {}
                 historyDateList =  get_all_paths(historyBasePath)


                 for hisDateItem in historyDateList:

                     dtlist = hisDateItem.split(r"/")
                     hisDatess = dtlist[len(dtlist)-1]
                     if "success" in hisDatess or "20991231" in hisDatess:
                         continue
                     historyDatePath = historyBasePath + hisDatess
                     oneDatehisFilelist =  get_all_paths(historyDatePath)

                     (hismaxTime,hisbizDatesdic) = getHisMaxProcessTimeStamp(oneDatehisFilelist)


                     hismaxTimeStr = hismaxTime.strftime("%Y%m%d%H%M%S")
                     snapMaxDayStr =  maxTime.strftime("%Y%m%d")
                     snapMaxTimeStr = maxTime.strftime("%Y%m%d%H%M%S")
                     snapMaxTimeIdList = bizDatesdic[maxTime]
                     snapMaxTimeMaxid = max(snapMaxTimeIdList.keys())

                     hisMaxTimeIdList = hisbizDatesdic[hismaxTime]
                     hisMaxTimeMaxid = max(hisMaxTimeIdList)


                     if snapMaxDayStr == todayStr:
                         hisOnePatitionHdfsPath = historyDatePath + "/" + snapMaxTimeStr+ "_" + snapMaxTimeMaxid
                     else:
                         hisOnePatitionHdfsPath = historyDatePath + "/" + hismaxTimeStr + "_" + hisMaxTimeMaxid

                     dt2PatitionDic[hisDatess] = hisOnePatitionHdfsPath

                 dt2PatitionDic["20991231"] = snapshotBasePath + "/" + snapMaxTimeStr+ "_" + snapMaxTimeMaxid


                 print "partition info:"
                 for key,value in dt2PatitionDic.items():
                     print "partition name:%s,partition location:%s"%(key,value)

                 createHisHiveTable(hiveTableName,hivefields,tableBasePath,dt2PatitionDic)

                 createCurrentHiveTable(hiveTableName,hivefields,dt2PatitionDic)

                 return 0




def createHisHiveTable(hiveTableName,hivefields,tableBasePath,dt2PatitionDic):
     #建表

     # use ods database
     cmd = "hive -e \" use ods\" "
     subprocess.call(shlex.split(cmd.encode('ascii')))


     createHiveSql = "create EXTERNAL TABLE  IF NOT EXISTS %s( \
     %s)  partitioned by (biz_end_date string) STORED AS PARQUET \
     location '%s' "%(hiveTableName,hivefields,tableBasePath)

     cmd = "hive -e \" %s\" "%(createHiveSql,)

     print "create hive table cmd is:  %s"%(cmd,)
     flag = subprocess.call(shlex.split(cmd.encode('ascii')))

     #drop partition
     drop_all_partitions(hiveTableName)

     # construct patitions infomation

     partitions = collate_partitions(dt2PatitionDic)

     # Add all partitions
     add_all_partitions(hiveTableName, partitions)


# current snapshot table ,hourly update
def createCurrentHiveTable(hiveTableName,hivefields,dt2PatitionDic):

    hdfsFileLocation = dt2PatitionDic["20991231"]
    CurrentHiveTable = hiveTableName + "_curr"


    # use ods database
    cmd = "hive -e \" use ods\" "
    subprocess.call(shlex.split(cmd.encode('ascii')))


    # drop ods table
    cmd = "hive -e \" DROP TABLE IF EXISTS %s\" "%(CurrentHiveTable,)
    print cmd
    subprocess.call(shlex.split(cmd.encode('ascii')))


    createHiveSql = "CREATE EXTERNAL TABLE %s( \
    %s) STORED AS PARQUET \
    location '%s' "%(CurrentHiveTable,hivefields,hdfsFileLocation)


    cmd = "hive -e \" %s\" "%(createHiveSql,)

    print "create CurrentHiveTable  cmd is:  %s"%(cmd,)
    subprocess.call(shlex.split(cmd.encode('ascii')))


# snapshot tables, daily accumulation
def createHisSnapShotHiveTable():
     pass

# histroy chain tables, daliy accumulation
def createHisChainHiveTable():
    pass



# drop one partition .
def drop_one_partition(tab_name,partition_name):
    table = "hive -e \"alter table " + tab_name + " drop IF EXISTS partition(biz_end_date='%s')\""%(partition_name,)
    print "drop one patition:" + table
    print "drop one partition: %s,%s"%(tab_name,partition_name)
    os.popen(table)
    return



# Drop all partitions before add the latest.
def drop_all_partitions(tab_name):
    table = "hive -e \"alter table " + tab_name + " drop IF EXISTS partition(biz_end_date!='1')\""
    print "drop all partitions: %s"%(tab_name,)
    os.popen(table)
    return


# Transfer the path to table partition
def collate_partitions(pathDic):
    partitions = ''
    for key,value in pathDic.items():

        sub_partitions = " PARTITION (biz_end_date='" + key + "') location '" + value + "' "
        partitions += sub_partitions
        print "partitions" + partitions
    return partitions


def add_all_partitions(tab_name, partitions):
    if partitions.strip():
        table = 'hive -e "alter table ' + tab_name + ' add ' + partitions + ';"'
        print "mount all partitions: %s"%(table,)
        os.popen(table)
    return 0


def execAlgoSelect(dic):
    useChain = dic["usechain"]

    if useChain== "true":
        flag = chainDataTableAlgo(dic)
    else:
        flag = allDataTableAlgo(dic)
    return flag


if __name__ == "__main__":
    if not get_argv(sys.argv):
        sys.exit(1)


    dbConfig = sys.argv[1]
    tbSchema = sys.argv[1]
    PROJECT_NAME = sys.argv[2]
    etlDate = sys.argv[3]
    businessReadyTime = sys.argv[4]

    parseconfs(dbConfig,tbSchema)


    for dic  in jsonConfigList:
        checkConfig(dic)
        flag = execAlgoSelect(dic)
        print "flag :", flag
        exit(flag)
