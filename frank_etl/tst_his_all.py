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



def get_argv(args):
    if len(args) == 5:
        return True
    else:
        print "Please input etl date parameter,ex: 'YESTERDAY', 'TODAY' or '2015-01-01' and etl sql file path . then re-run the script."
        return False

def parseconfs(dbConfig):


    currentDir = os.getcwd()

    ##dbConfigFileName  = currentDir + "/" + dbConfig
    dbConfigFileName  = dbConfig
    
    dic = {}

    

    print "table json Schema file:  " + dbConfigFileName
    if os.path.exists(dbConfigFileName):
        configfile = file(dbConfigFileName)
        jsonobj = json.load(configfile)

        print "Schema json: " + json.dumps(jsonobj)
        configfile.close

        

        colsList = jsonobj["columns"]
        
        colsStr = ""
        
        
        count = 0
        colStr = ""
        joinColStr = ""
        for col in colsList:
            colName = col["name"]
            if count == 0:
            	colStr = colStr + "a.%s"%(colName)
            	joinColStr = joinColStr + "a.%s=b.%s"%(colName,colName) 
            else:
            	colStr = colStr + ",a.%s"%(colName)
            	joinColStr = joinColStr + " and a.%s=b.%s"%(colName,colName)
            count = count + 1
        
        
        dic["joinColStr"] = joinColStr
        dic["colsStr"] = colStr
        print "colsStr:%s"%(colsStr,)


    jsonConfigList.append(dic)
    return dic
if __name__ == "__main__":
    if not get_argv(sys.argv):
        sys.exit(1)

    tx_date= sys.argv[1]
    tb_name = sys.argv[3]
    etl_sql_path = sys.argv[2]
    max_date = '30001231'
    
    dbConfig = sys.argv[4]
    parseconfs(dbConfig)
    
    dic = jsonConfigList[0]
    
    joinColStr =  dic["joinColStr"]
    colsStr = dic["colsStr"]
    
    print colsStr + "==========================="

    flag= 0
    cmd = 'hive' + ' \
-hiveconf tx_date=' + tx_date  +' -hiveconf tb_name='+ tb_name +' -hiveconf max_date=' + max_date +' -hiveconf colsStr=' + colsStr + ' -hiveconf joinColStr=' + joinColStr +' \
-f ' +  etl_sql_path + ' '
    print cmd + "==========================="


    flag = subprocess.call(shlex.split(cmd.encode('ascii')))



    print "flag :", flag
    exit(flag)
