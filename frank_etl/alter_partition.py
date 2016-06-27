#!/usr/bin/python
# -*- coding: UTF-8 -*-
import os
import re
import subprocess
import sys

def hdfs_path_exists(path):
    res = subprocess.call(["hadoop", "fs", "-test", "-e", path])
    if res != 0:
        return False
    else:
        return True

def get_all_paths(tab_name):
    path_catas = []
    path = '/' + tab_name + '/ods'
    if hdfs_path_exists(path):
        # Get biz_end_date
        beds = subprocess.Popen(["hadoop", "fs", "-ls", path], stdout=subprocess.PIPE).communicate()
        bed_list = beds[0].split("\n")
        for bed in bed_list:
            sub_bed = re.search(r'/.+', bed)
            if sub_bed:
                # Get biz_end_date
                psds = subprocess.Popen(["hadoop", "fs", "-ls", sub_bed.group()], stdout=subprocess.PIPE).communicate()
                psd_list = psds[0].split("\n")
                print psd_list
                base_psd = ''
                for psd in psd_list:
                    sub_psd = re.search(r'/.+', psd)
                    if sub_psd and (sub_psd.group() > base_psd):
                        base_psd = sub_psd.group()
                if base_psd.strip():
                    path_catas.append(base_psd)
    return path_catas

# Transfer the path to table partition
def collate_partitions(path_list):
    partitions = ''
    for path in path_list:
        args = path.split('/')
        sub_partitions = " PARTITION (biz_end_date='" + args[3] + "', process_start_datetime='"+ args[4] +"') location '" + path + "' "
        partitions += sub_partitions
    return partitions



# Drop all partitions before add the latest.
def drop_all_partitions(tab_name):
    table = "hive -e \"alter table " + tab_name + " drop IF EXISTS partition(biz_end_date!='1')\""
    os.popen(table)
    return

def add_all_partitions(tab_name, partitions):
    if partitions.strip():
        table = 'hive -e "alter table ' + tab_name + ' add ' + partitions + ';"'
        print table
        os.popen(table)
    return

def get_argv(args):
    if len(args) == 2:
        return True
    else:
        print "Please import_input one parameter table name, then re-run the script."
        return False

if __name__ == "__main__":
    if not get_argv(sys.argv):
        sys.exit(1)

    tab_name = sys.argv[1]
    # tab_name = 'tt'

    # 1. Init: Drop all partitions
    drop_all_partitions(tab_name)

    # 2. Get all partitions
    paths = get_all_paths(tab_name)
    partitions = collate_partitions(paths)

    # 3. Add all partitions
    add_all_partitions(tab_name, partitions)
