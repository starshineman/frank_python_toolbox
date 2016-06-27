#!/usr/bin/python
# -*- coding: UTF-8 -*-
import sys
import os
import subprocess
import shlex
import json


jsonConfigList = []
cmlArgs = []


def get_argv(args):
    if len(args) == 4:
        return True
    else:
        print "Please provide  import  json config file name , json schema file name  and project name , then re-run the script."
        return False


def parseconfs(dbConfig,tbSchema):


    currentDir = os.getcwd()

    dbConfigFileName  = currentDir + "/" + dbConfig
    tbSchemaFileName  = currentDir + "/" + tbSchema
    dic = {}

    print "db json config file:  " + dbConfigFileName
    if os.path.exists(dbConfigFileName):
        configfile = file(dbConfigFileName)
        jsonobj = json.load(configfile)
        print jsonobj
        configfile.close


        dic["--connect"] = jsonobj["db.url"]
        # 使用的用户名
        dic["--username"] = jsonobj["db.username"]
        # 使用的密码
        dic["--password"] = jsonobj["db.password"]



    print "table json config file:  " + tbSchemaFileName
    if os.path.exists(tbSchemaFileName):
        configfile = file(tbSchemaFileName)
        jsonobj = json.load(configfile)
        print jsonobj
        configfile.close



        dic["--table"] = jsonobj["table"]
        dbName = jsonobj["database"]
        tableName = jsonobj["hive_table"]
        dic["-m"]= "5"

        dic["--export-dir"] = "hdfs://ns1/user/hive/warehouse/dm.db/%s"%(tableName)

        dic["--input-fields-terminated-by"] = " '\\001' "
        dic["--input-lines-terminated-by"] = " '\\001' "
        dic["--input-null-string"] = " '\\\N' "
        dic["--input-null-non-string"] = " '\\\N' "

    jsonConfigList.append(dic)
    return dic



def execexport(dic):

    cmd = "sqoop export  "
    for key, value in dic.items():
         cmd = cmd + "  %s %s " % (key, value)
    print "cmd is:  %s"%(cmd,)

    flag = subprocess.call(shlex.split(cmd.encode('ascii')))
    return flag



if __name__ == "__main__":
    if not get_argv(sys.argv):
        sys.exit(1)

    cmlArgs = sys.argv

    dbConfig = sys.argv[1]
    tbSchema = sys.argv[2]
    PROJECT_NAME = sys.argv[3]

    parseconfs(dbConfig,tbSchema)

    for dic  in jsonConfigList:
        flag = execexport(dic)
        print "flag :", flag
        exit(flag)
