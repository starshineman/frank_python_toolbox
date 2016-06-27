#!usr/bin/env python
#-*-coding:utf-8 -*-

#导入SQLITE3模块
import sqlite3

#SQLite数据库名
DB_SQLITE_NAME="test.db"

def sqliteHandler():


    #连接数据库
    try:
        sqlite_conn=sqlite3.connect(DB_SQLITE_NAME)
    except sqlite3.Error,e:
        print "连接sqlite3数据库失败", "\n", e.args[0]
        return

    #获取游标
    sqlite_cursor=sqlite_conn.cursor()

    #如果存在表先删除
    sql_del="DROP TABLE IF EXISTS tbl_test;"
    try:
        sqlite_cursor.execute(sql_del)
    except sqlite3.Error,e:
        print "删除数据库表失败！", "\n", e.args[0]
        return
    sqlite_conn.commit()

    #创建表
    sql_add='''CREATE TABLE tbl_test(
    i_index INTEGER PRIMARY KEY,
    sc_name VARCHAR(32)
    );'''
    try:
        sqlite_cursor.execute(sql_add)
    except sqlite3.Error,e:
        print "创建数据库表失败！", "\n", e.args[0]
        return
    sqlite_conn.commit()

    #添加一条记录
    sql_insert="INSERT INTO tbl_test(sc_name) values('mac');"
    try:
        sqlite_cursor.execute(sql_insert)
    except sqlite3.Error,e:
        print "添加数据失败！", "\n", e.args[0]
        return
    sqlite_conn.commit()

    #查询记录
    sql_select="SELECT * FROM tbl_test;"
    sqlite_cursor.execute(sql_select)
    for row in sqlite_cursor:
        i=1;
        print "数据表第%s" %i,"条记录是：", row,

if __name__=='__main__':
    #调用数据库操作方法
    sqliteHandler()
