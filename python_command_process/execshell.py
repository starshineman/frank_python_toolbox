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


cmdlists = []



def get_argv(args):
    if len(args) == 3:
        return True
    else:
        print "Please provide  json config file name param and project name, then re-run the script."
        return False



def execcmd(cmdlist):
    res = 0
    print cmdlist
    #res = subprocess.call(cmdlist)
    #res = subprocess.call(shlex.split(cmdlist.encode('ascii')))

    if res != 0:
        return False
    else:
        return True


def execcmdWithReturnStr(cmdlist):
    path_catas = []

    beds = subprocess.Popen(cmdlist, stdout=subprocess.PIPE).communicate()
    bed_list = beds[0].split("\n")
    for bed in bed_list:
        sub_bed = re.search(r'/.+', bed)
        if sub_bed:
            base_psd = sub_bed.group()
            if base_psd.strip():
                path_catas.append(base_psd)
                print base_psd
    return path_catas


def execcmds(cmdlists):
    for cmdlist in cmdlists:
        execcmd(cmdlist)

def buildcmdlistForGetHdfsFile(hivedb,hivetable,partitionName,partitionValue):
    if hivedb =="ods":
       cmdstr = "hadoop fs -get hdfs://%s/%s/%s=%s/part-m-00000 \
/data/hive_tmp/%s__%s"%(hivedb,hivetable,partitionName,partitionValue,hivetable,partitionValue)
    elif hivedb =="export_tmp":
        cmdstr = "hadoop fs -get hdfs://%s.db/%s/%s=%s/000000_0 \
/data/hive_tmp/%s__%s"%(hivedb,hivetable,partitionName,partitionValue,hivetable,partitionValue)

    print "get hdfs file cmd is :%s"%(cmdstr,)
    #cmdlist = cmdstr.split(' ')
    # return cmdlist
    return cmdstr



def buildcmdlistForImportFile2Hive(hivedb,hivetable,partitionName,partitionValue):

    cmdstr ="hive -e \" load data local inpath '/app/hive_load/%s__%s' \
into table ods.%s partition (%s='%s')\"  " \
    %(hivetable,partitionValue,hivetable,partitionName,partitionValue)

    print "load hdfs file into hive table cmd is :%s"%(cmdstr,)
    return cmdstr


# copy hdfs files of hive table to local files
def execExportHiveData2LocalFiles():
    dt = "20160525"
    paramslist = [
        ["export_tmp","","end_dt","30001231"]
        ,["export_tmp","","end_dt",dt]
        ,["export_tmp","","data_dt",dt]
        ,["export_tmp","","data_dt",dt]
        ,["ods","","data_dt",dt]
        ,["ods","","data_dt",dt]
    ]
    for param in paramslist:

        cmdlist = buildcmdlistForGetHdfsFile(param[0],param[1],param[2],param[3])
        cmdlists.append(cmdlist)

    execcmds(cmdlists)


# copy hdfs files of hive table to local files
def execImportLocalFiles2HiveTable():
    dt = "20160525"
    paramslist = [
        ["export_tmp","","end_dt","30001231"]
        ,["export_tmp","","end_dt",dt]
        ,["export_tmp","","data_dt",dt]
        ,["export_tmp","","data_dt",dt]
        ,["ods","","data_dt",dt]
        ,["ods","","data_dt",dt]
    ]
    for param in paramslist:

        cmdlist = buildcmdlistForImportFile2Hive(param[0],param[1],param[2],param[3])
        cmdlists.append(cmdlist)

    execcmds(cmdlists)


if __name__ == "__main__":
     # execExportHiveData2LocalFiles()
     execImportLocalFiles2HiveTable()

