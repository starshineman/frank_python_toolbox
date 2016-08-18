# -*- coding: utf-8 -*-

import os
os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'

import cx_Oracle


conn = cx_Oracle.connect('bi_ods/cedb')
print conn.encoding
cursor = conn.cursor()
cursor.execute("select * from dual")

sql = "select * from dba_tables t where t.table_name='ODS_T_LITIGATION_FEE'"
sql = "select * from NPLM_LOAN_CONTRACT"
cursor.execute(sql);
result = cursor.fetchall()
print("Total: " + str(cursor.rowcount))


#print li[0][3].decode('utf-8')

for row in result:
    for field in row:
      if type(field) == str:
        print(field.decode('utf-8'))
      else:
        print (field)

cursor.close
conn.close



