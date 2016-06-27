#!/usr/bin/python
# -*- coding: UTF-8 -*-

import cx_Oracle
import MySQLdb
import json
import sys
import subprocess
import os
import datetime
import time
import re
import shlex
import etl_utils



# 知识库
ETL_LOG_HOST = ''
ETL_LOG_PORT = 3503
ETL_LOG_DB = ''
ETL_LOG_USER = ''
ETL_LOG_PASS = ''


#connect database
def get_source_db_conn(dbtype, host, port, db, user, password):
    try:
        if dbtype == 'oracle':
            return get_oracle_conn(host, port, db, user, password)
        elif dbtype == 'mysql':
            return get_mysql_conn(host, int(port), db, user, password)
        else:
            print "ERROR: Source database type(in JSON file) is not supported: " + dbtype
            exit(1)
    except Exception, ex:
        print ex
        exit(1)


#connect oracle
def get_oracle_conn(host, port, db, user, password):
    conn = cx_Oracle.connect(user, password, host + ":" + str(port) + "/" + db)
    return conn

#connect mysql
def get_mysql_conn(host, port, db, user, password):
    conn = MySQLdb.connect(host=host, port=port, db=db, user=user, passwd=password)
    return conn

#execute_sql
def execute_sql(conn, sql):
    print "=================================" + sql
    try:
        cursor = conn.cursor()
        cursor.execute(sql)
        conn.commit()
        cursor.close()
    except Exception, ex:
        print ex
        exit(1)
    finally:
        conn.close()


def get_source_count(g_source_db,g_source_type, g_source_host, g_source_port, g_source_tns, g_source_user, g_source_pass,g_source_table,g_where_condition):
    sql_string = "select count(*) from " + g_source_db + "." +g_source_table
    if len(g_where_condition) > 0:
        sql_string += " where " + g_where_condition
        
    
    
    try:
    	print "=================================get_source_count:"+ sql_string
        print g_source_type + ","+ g_source_host + ","+ g_source_port + ","+ g_source_tns + ","+ g_source_user + ","+ g_source_pass
        # 源数据库获取行数
        source_conn = get_source_db_conn(g_source_type, g_source_host, g_source_port, g_source_tns, g_source_user, g_source_pass)
        cursor = source_conn.cursor()
        cursor.execute(sql_string)
        row = cursor.fetchone()
        count = row[0]
        cursor.close()
        return count
    except Exception, ex:
        print ex
        exit(1)
    finally:
        source_conn.close()


def get_target_count(g_hive_db,g_hive_table,g_where_condition):
    sql_string = "select count(*) from " + g_hive_table
    if len(g_where_condition) > 0:
        sql_string += " where " + g_where_condition
        
    print "=================================get_target_count:" + sql_string
    
    cmd = "hive -e \"use {db}; {sql};\"".format(db=g_hive_db, sql=sql_string)
    pipe = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE)
    pipe.wait()
    (stdout_data, stderr_data) = pipe.communicate()

    tgt_row_count = -1
    try:
        tgt_row_count = int(stdout_data)
    except ValueError, error:
        print "ERROR: Failed to get row count by command: " + cmd
        print error
    return tgt_row_count



def get_count(conn, db, table, date, run):
    sql = "select count(*) from etl_log where db_name='{db}' and table_name='{table}' and data_date=str_to_date('{date}','%Y%m%d') and run_time=str_to_date('{run}', '%Y%m%d %H:%i:%s')"\
        .format(db=db, table=table, date=date, run=run)

    print "=================================" + sql
    try:
        cursor = conn.cursor()
        cursor.execute(sql)
        data = cursor.fetchone()
        return data[0]
    except Exception, ex:
        print ex
        return 1
    finally:
        cursor.close()


def insert_etl_log(conn, db, table, date, run, count):
    if get_count(conn, db, table, date, run) > 0:
      sql = "update etl_log set source_count_1={count},source_count_2=null,target_count=null where db_name='{db}' and table_name='{table}' and data_date=str_to_date('{date}','%Y%m%d') and run_time=str_to_date('{run}', '%Y%m%d %H:%i:%s')"\
          .format(round=round, count=count, db=db, table=table, date=date, run=run)
    else:
      sql = "insert into etl_log(db_name, table_name, data_date, run_time, source_count_1) values('{db}', '{table}', str_to_date('{date}','%Y%m%d'), str_to_date('{run}', '%Y%m%d %H:%i:%s'), {count}) "\
          .format(count=count, db=db, table=table, date=date, run=run)
    execute_sql(conn, sql)



def update_source_etl_log(conn, db, table, date, run, count):
    if get_count(conn, db, table, date, run) > 0:
        sql = "update etl_log set source_count_2={count} where db_name='{db}' and table_name='{table}' and data_date=str_to_date('{date}','%Y%m%d') and run_time=str_to_date('{run}', '%Y%m%d %H:%i:%s')"\
            .format(round=round, count=count, db=db, table=table, date=date, run=run)
    else:
        sql = "insert into etl_log(db_name, table_name, data_date, run_time, source_count_2) values('{db}', '{table}', str_to_date('{date}','%Y%m%d'), str_to_date('{run}', '%Y%m%d %H:%i:%s'), {count}) "\
            .format(count=count, db=db, table=table, date=date, run=run)
    execute_sql(conn, sql)


def update_target_etl_log(conn, db, table, date, run, count):
    if get_count(conn, db, table, date, run) > 0:
        sql = "update etl_log set target_count={count} where db_name='{db}' and table_name='{table}' and data_date=str_to_date('{date}','%Y%m%d') and run_time=str_to_date('{run}', '%Y%m%d %H:%i:%s')" \
            .format(count=count, db=db, table=table, date=date, run=run)
    else:
        sql = "insert into etl_log(db_name, table_name, data_date, run_time, target_count) values('{db}', '{table}', str_to_date('{date}','%Y%m%d'), str_to_date('{run}', '%Y%m%d %H:%i:%s'), {count}) " \
            .format(count=count, db=db, table=table, date=date, run=run)
    execute_sql(conn, sql)





def check_data(dbConfig,qp_dt,sys_dt,g_where_condition,fromdb,round_time):
    jsonobj = etl_utils.parseconfs(dbConfig)

    g_source_table = jsonobj["db.table_name"]
    g_source_user = jsonobj["db.username"]
    g_source_pass = jsonobj["db.password"]
    g_hive_db = jsonobj["hive_db"]
    g_hive_table = jsonobj["hive_table"]
    g_source_db = jsonobj["db.database"]
    # source DB type: oracle / mysql
    db_url = jsonobj["db.url"]
    g_source_type = db_url.split(":")[1]

    db_url = db_url[db_url.find("//") + 2:]
    url_items = db_url.split("/")
    g_source_tns = url_items[1]
    g_source_host = url_items[0].split(":")[0]
    g_source_port = url_items[0].split(":")[1]

    g_run_time=sys_dt
    g_data_date=qp_dt
    g_round=round_time
    g_run_type=fromdb


    if g_run_type == 'source':
        # 源数据库记录个数
        row_count = get_source_count(g_source_db,g_source_type, g_source_host, g_source_port, g_source_tns, g_source_user, g_source_pass,g_source_table,g_where_condition)

        # 写入知识库
        log_conn = get_mysql_conn(ETL_LOG_HOST, ETL_LOG_PORT, ETL_LOG_DB, ETL_LOG_USER, ETL_LOG_PASS)
        if g_round == 1:
            insert_etl_log(log_conn, g_source_tns, g_source_table, g_data_date, g_run_time, row_count)
        elif g_round == 2:
            update_source_etl_log(log_conn, g_source_tns, g_source_table, g_data_date, g_run_time, row_count)
    else:
        # 目标数据库记录个数
        row_count = get_target_count(g_hive_db,g_hive_table,g_where_condition)

        # 写入知识库
        log_conn = get_mysql_conn(ETL_LOG_HOST, ETL_LOG_PORT, ETL_LOG_DB, ETL_LOG_USER, ETL_LOG_PASS)
        update_target_etl_log(log_conn, g_source_tns, g_source_table, g_data_date, g_run_time, row_count)

    return 0

