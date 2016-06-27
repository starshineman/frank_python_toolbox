#!/usr/bin/env python
#encoding:utf8

import sys

reload(sys)
sys.setdefaultencoding('utf-8')
import optparse
import os
import datetime
from ConfigParser import ConfigParser


class Option:
    @staticmethod
    def usage():
        USAGE = r"python %s -conf configFile " % sys.argv[0]
        return USAGE
    @staticmethod
    def procOpt():
        parser = optparse.OptionParser(usage=Option.usage())
        parser.add_option('-c', '--conf', dest="configFile", default = None, help="config file",)
        options,remainder = parser.parse_args()
        return options

def main():
    starttime = datetime.datetime.now()
    options = Option.procOpt()
    if options.configFile is None:
        print "add conf: " + Option.usage()
        return

    #从配置文件中获取参数
    config = ConfigParser()
    config.read(options.configFile)

    #获取sql
    exec_sql_lst = []
    scsql = SourceSql()

    for i in range(1,12):
        exec_sql_lst.append(scsql.get_step(config, i))

    #执行sql语句
    status = 0
    for exec_sql in exec_sql_lst:
        print exec_sql
        status = os.system("hive -e \"set mapred.job.priority=VERY_HIGH;%s\"" % exec_sql)
        if status == 0:
            continue
        else:
            status = -1
            break
    endtime = datetime.datetime.now()
    print "执行时间：" + str((endtime - starttime).seconds) + "秒"
    sys.exit(status)

class SourceSql():
    def __init__(self):
        pass

    def get_step(self, config, step):
        sql = config.get("GET_SQL", 'step'+str(step))
        return sql

if __name__ =="__main__": 
    main()
