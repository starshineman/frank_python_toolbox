#!/usr/bin/python
# -*- coding: UTF-8 -*-
import MySQLdb
import sys
import os
import datetime
import time
import re
import subprocess
import shlex
import json


def get_argv(args):
    if len(args) == 4:
        return True
    else:
        print "Please provide system name , step name and data_dt, then re-run the script."
        return False

def date_turning(system,step,data_dt):
    conn = MySQLdb.connect(host='', user='',passwd='', port= 3503)
    conn.select_db('bi')
    cursor = conn.cursor()

    #sql = 'insert into date_turning (sys,step,data_dt,complete_time) values("%s",%s,"%s",sysdate())'%(system,step,data_dt)
    sql = 'replace date_turning (sys,step,data_dt,complete_time) values("%s",%s,"%s",sysdate())'%(system,step,data_dt)
    print sql
    cursor.execute(sql)
    cursor.close()
    conn.commit()
    conn.close()

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

    system = sys.argv[1]
    step = sys.argv[2]
    data_dt = sys.argv[3]
    dt = setETLDate(data_dt)
    date_turning(system,step,dt)

