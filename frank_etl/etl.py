#!/usr/bin/python
# -*- coding: UTF-8 -*-
import sys
import os
import datetime
import time
import re
import subprocess
import shlex



def get_argv(args):
    if len(args) == 3:
        return True
    else:
        print "Please input etl date parameter,ex: 'YESTERDAY', 'TODAY' or '2015-01-01' and etl sql file path . then re-run the script."
        return False


if __name__ == "__main__":
    if not get_argv(sys.argv):
        sys.exit(1)

    etl_date_str = sys.argv[1]
    etl_sql_path = sys.argv[2]

    #print etl_date_str
    if not cmp('YESTERDAY',etl_date_str):
        now_time = datetime.datetime.now()
        yes_time = now_time + datetime.timedelta(days=-1)
        etl_date_str = yes_time.strftime('%Y-%m-%d')
    elif not cmp('TODAY',etl_date_str):
        etl_date_str = time.strftime('%Y-%m-%d',time.localtime(time.time()))

    timeArray = time.strptime(etl_date_str, "%Y-%m-%d")
    #转换为时间戳:
    etl_timestamp = int(time.mktime(timeArray))
    print "ETL process time:%s"%(etl_date_str)

    timeArray = time.localtime(etl_timestamp)
    otherStyleTime = time.strftime("%Y-%m-%d", timeArray)
    #print otherStyleTime

    # 执行命令
    """
    hive \
 -hiveconf yesterday_timestamp=`date -d yesterday +%s` \
 -f /home/bi/hive_scripts/merge.sql

    """


    etl_date = ""
    flag= 0
    # etl_sql_path = ' ./hive_scripts/merge.sql'

    cmd = 'hive' + ' \
-hiveconf etl_time=' + str(etl_timestamp) + ' \
-f ' +  etl_sql_path + ' '


    flag = subprocess.call(shlex.split(cmd.encode('ascii')))



    print "flag :", flag
    exit(flag)
