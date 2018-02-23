#!/usr/bin/python
#coding=utf-8
import MySQLdb
import json
import datetime
import os

now = datetime.datetime.now()
#curr_date = now.strftime("%Y-%m-%d")
curr_date='2017-08-01'
log_dir = '/home/wangdi/test/'+curr_date

files= os.listdir(log_dir)
conn =  MySQLdb.connect(host='10.17.64.180',user='dm_xps_rw',passwd='b4ne8hIdaLMej3+',db='dm_xps', port=3306,charset='utf8')
conn.select_db('dm_xps')
curs = conn.cursor()
for file in files:
    if not os.path.isdir(file):
        f = open(log_dir+"/"+file);
        iter_f = iter(f);
        for line in iter_f:
            print line.strip()
            try:
                curs.execute(line.strip())
            except Exception, exception:
                print exception



conn.commit()
curs.close()
conn.close()
f.close()