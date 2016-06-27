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
import etl_checkdata

reload(sys)  
sys.setdefaultencoding('utf8')  

jsonConfigList = []

def get_argv(args):
    if len(args) == 3:
        return True
    else:
        print "Please input etl date parameter,ex: 'YESTERDAY', 'TODAY' or '2015-01-01' and etl sql file path . then re-run the script."
        return False


def parseconfs(dbConfig):

    jsonobj = etl_utils.parseconfs(dbConfig)
    dbConfigFileName  = dbConfig

    print "table json Schema file:  " + dbConfigFileName
    dic = {}

    dic["hive_table"]=jsonobj["hive_table"]
    dic["hive_db"] = jsonobj["hive_db"]
    dic["table_name"] = jsonobj["db.table_name"]


    colsList = jsonobj["columns"]
    srcTbKeys = jsonobj["db.table_keys"]
    srcTbKeysList = srcTbKeys.split(",")

    srcTbKeysList = etl_utils.formatList(srcTbKeysList)
    colsStr = ""



    count = 0
    joincount = 0

    colStr_md5 = ""
    colStr_as_h = ""
    colStr_as_m = ""
    colStr_h = ""
    colStr_coalesce = ""
    colStr_m = ""

    joinColStr = ""
    p_k  = ""

    for col in colsList:


            #if col["primary_key"]=="true" or (col["name"] in srcTbKeysList):
            if  (col["name"].upper() in srcTbKeysList):

               joinCol = "h.h_" + col["name"] + "=" + "m.m_" + col["name"]
               p_k =  col["name"]

               if joincount == 0:
                 joinColStr =  joinColStr + joinCol
               else:
                 joinColStr = joinColStr + " and " + joinCol

               joincount = joincount +1



            colName = col["name"]
            colType = col["type"]
            if count == 0:
                
                #用于拼H表字段串
                colStr_as_h = colStr_as_h + "%s"%(colName) + " as " + "h_%s"%(colName)
                #用于拼m表字段串
                colStr_as_m = colStr_as_m + "%s"%(colName) + " as " + "m_%s"%(colName)
                #用于拼第一个插入字段串
                colStr_h = colStr_h   + "h_%s"%(colName)
                #用于拼第二个字段串
                colStr_coalesce = colStr_coalesce + "coalesce(h_%s"%(colName) + " , " + "m_%s)"%(colName)
                #用于拼第二个字段串
                colStr_m = colStr_m  + "m_%s"%(colName)
                #用于拼md5串
                if colType[0:7]=="decimal": 
                   colName = "cast(" + colName + " as string)"
                   colStr_md5 = colStr_md5 + "%s"%(colName)
                else:
                	 colStr_md5 = colStr_md5 + "%s"%(colName)
            else:

                 
                #用于拼H表字段串
                colStr_as_h = colStr_as_h + ",%s"%(colName) + " as " + "h_%s"%(colName)
                #用于拼m表字段串
                colStr_as_m = colStr_as_m + ",%s"%(colName) + " as " + "m_%s"%(colName)
                #用于拼第一个插入字段串
                colStr_h = colStr_h   + ",h_%s"%(colName)
                #用于拼第二个字段串
                colStr_coalesce = colStr_coalesce + ",coalesce(h_%s"%(colName) + " , " + "m_%s)"%(colName)
                #用于拼第三个字段串
                colStr_m = colStr_m  + ",m_%s"%(colName)
                #用于拼md5串
                if colType[0:7]=="decimal": 
                   colName = "cast(" + colName + " as string)"
                   colStr_md5 = colStr_md5 + ",%s"%(colName)
                else:
                	 colStr_md5 = colStr_md5 + ",%s"%(colName) 
            count = count + 1

    print "joinColStr: " + joinColStr
    dic["joinColStr"] = joinColStr

    print "p_k: " + p_k
    dic["p_k"] = p_k


    print "colStr_md5:%s"%(colStr_md5,)
    dic["colStr_md5"] = colStr_md5

    print "colStr_as_h:%s"%(colStr_as_h,)
    dic["colStr_as_h"] = colStr_as_h

    print "colStr_as_m:%s"%(colStr_as_m,)
    dic["colStr_as_m"] = colStr_as_m


    print "colStr_h:%s"%(colStr_h,)
    dic["colStr_h"] = colStr_h

    print "colStr_coalesce:%s"%(colStr_coalesce,)
    dic["colStr_coalesce"] = colStr_coalesce

    print "colStr_m:%s"%(colStr_m,)
    dic["colStr_m"] = colStr_m
    


    jsonConfigList.append(dic)
    return dic


def chain_sql(hive_db,tb_name,tx_date,max_date,joinColStr,p_k,colStr_md5,colStr_as_h,colStr_as_m,colStr_h,colStr_coalesce,colStr_m):

    tb_name_m = tb_name[0:-1]+ "m"
    print "tb_name_m:%s"%(tb_name_m,)

    sql_string="""
                set hive.hadoop.supports.splittable.combineinputformat=true;
                set hive.exec.dynamic.partition.mode=nonstrict;
                set hive.mapjoin.smalltable.filesize=125000000;
                set hive.exec.mode.local.auto=true;
                set hive.optimize.skewjoin = true;

                --注意md5算法不支持decimal类型,需要做cast转换

                drop table if exists %s_full;
                create temporary table %s_full as
                select *
                from
                    (select
                            etl_dt as h_etl_dt, start_dt as h_start_dt,end_dt as h_end_dt,
                            ods.dw_md5(%s) as h_md5_str,
                            %s
                    from ods.%s  a
                    where a.start_dt < '%s') h
                    full join
                    (select etl_dt as m_etl_dt,
                            ods.dw_md5(%s) as m_md5_str,
                            %s
                    from ods.%s  a
                    where etl_dt = '%s') m
                    on %s
                 ;

                --未变化的数据,重新插入
                insert overwrite table ods.%s partition (end_dt)
                select h_etl_dt,h_start_dt,
                       %s
                       ,h_end_dt as end_dt
                from %s_full
                where m_%s is null
                --两种操作：1.新增的数据插入；2.变化的数据闭链（全字段做md5比对）
                union all
                select coalesce(h_etl_dt,m_etl_dt),coalesce(h_start_dt,'%s'),
                       %s
                       ,case when h_end_dt='%s' and h_md5_str<>m_md5_str then '%s' else coalesce(h_end_dt,'%s')  end as end_dt
                from %s_full
                where m_%s is not null
                --插入更新的数据
                union all
                select m_etl_dt,'%s',
                       %s
                       ,'%s' as end_dt
                from %s_full
                where h_%s is not null
                  and m_%s is not null
                  and h_md5_str<>m_md5_str
                  and (h_end_dt='%s' or h_end_dt = '%s')
                  ;"""%(tb_name,tb_name,colStr_md5,colStr_as_h,tb_name,tx_date,colStr_md5,colStr_as_m,tb_name_m,tx_date,joinColStr,tb_name,colStr_h,tb_name,p_k,tx_date,colStr_coalesce,max_date,tx_date,max_date,tb_name,p_k,tx_date,colStr_m,max_date,tb_name,p_k,p_k,max_date,tx_date)

    print "===========================" + sql_string

    cmd = "hive -v -e  \"use {db}; {sql};\"".format(db=hive_db, sql=sql_string)


    print "===========================" + cmd 
    flag= 0
    flag = subprocess.call(shlex.split(cmd.encode('UTF-8')))
    print "flag :", flag

    return flag

if __name__ == "__main__":
    if not get_argv(sys.argv):
        sys.exit(1)

    tx_date= etl_utils.setETLDate(sys.argv[1])
    dbConfig = sys.argv[2]

    max_date = '30001231'

    dic = parseconfs(dbConfig)

    hive_db = dic["hive_db"]
    tb_name = dic["hive_table"]

    joinColStr =  dic["joinColStr"]
    p_k = dic["p_k"]


    colStr_md5 = dic["colStr_md5"]
    colStr_as_h = dic["colStr_as_h"]
    colStr_as_m = dic["colStr_as_m"]
    colStr_h = dic["colStr_h"]
    colStr_coalesce = dic["colStr_coalesce"]
    colStr_m = dic["colStr_m"]


    flag = chain_sql(hive_db,tb_name,tx_date,max_date,joinColStr,p_k,colStr_md5,colStr_as_h,colStr_as_m,colStr_h,colStr_coalesce,colStr_m)
   
    qp_dt=tx_date
    hive_whereCondion="start_dt <= '%s' and end_dt > '%s'" %(qp_dt,qp_dt)
    
    if flag == 0:
       etl_checkdata.check_data(dbConfig,qp_dt,qp_dt,hive_whereCondion,"target","")

    exit(flag)

