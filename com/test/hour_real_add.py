#!/usr/bin/python
#coding=utf-8
import MySQLdb
import json
import datetime
import os
import sys


now = datetime.datetime.now()
time_format = now.strftime("%Y%m%d%H%M%S")
log_format = now.strftime("%Y%m%d%H")
serial_num = 'REAL-NO-RE'+time_format
curr_timestamp = now.strftime("%Y-%m-%d %H:%M:%S")
#curr_time = now.strftime("%Y-%m-%d %H:00:00")
curr_time = '2018-07-26 11:00:00'
#param_hour= sys.argv[2]
#curr_time = sys.argv[1]+' '+param_hour

if curr_time.strip()=="":
    exit();

print curr_time

conn = MySQLdb.connect(host='10.121.48.86',user='dm_xps_rw',passwd='NjyMKkK+vc4Ks3k',db='dm_xps', port=3306,charset='utf8')
conn.select_db('dm_xps')
curs = conn.cursor()

curs_start = conn.cursor()
curs_start.execute('select from_unixtime(UNIX_TIMESTAMP(),"%Y-%m-%d %H:%m:%s");')
curr_time_db = curs_start.fetchone()


#2.开始比对离线 实时小时表数据
#1.离线实时 交集对比,更新操作, 差集 离线,新增操作(离线有, 实时没有)
#                         0         1               2               3           4       5           6       7       8       9           10
hour_crid_sql = 'select a.`id`, a.`click_amount`, a.`imp_amount`, a.`consume_amount`, a.`aid`, a.`campid`, a.`adgid`, a.`crid`, a.`hour`, a.`data_time`, a.`create_time` from t_dm_xps_advertiser_consume_hour_crid a left join t_dm_xps_advertiser_consume_hour_crid_real_time b  on a.aid = b.aid AND a.campid = b.campid AND a.adgid = b.adgid AND a.crid = b.crid AND a.data_time = b.data_time where a.data_time="%s" limit 0,10000;' %(curr_time);

off_line_hour_list = curs.execute(hour_crid_sql);
results = curs.fetchmany(off_line_hour_list)
print 'hour_crid_sql:',hour_crid_sql

print json.dumps(results)
curs2 = conn.cursor();
print type(results)

if any(results) == False:
    exit();

for r in results:
    print '#######################################'
    print "off:",r[0],r[1],r[2],r[3],r[4],r[5],r[6],r[7],r[8],r[9],r[10]
    #f.write(str(r[0])+'   '+str(r[1])+'   '+str(r[2])+ '\n')
    real_hour_crid_sql = 'select  `id`,`click_amount`, `imp_amount`, `consume_amount` from t_dm_xps_advertiser_consume_hour_crid_real_time WHERE `data_time`="%s" and `aid`= "%d" and `campid`= "%d" and `adgid`="%d" and `crid`="%d" ;'  %(r[9],r[4],r[5],r[6],r[7]);
    print 'real_hour_crid_sql:',real_hour_crid_sql
    curs2.execute(real_hour_crid_sql);
    real_result = curs2.fetchone();

    print type(real_result)

    if any(real_result) ==False:
        print 'False'


    print type(real_result)
    if real_result == None:
        print '1'

    else:
        print '2'




print 'end'


conn.commit()
curs.close()
curs2.close()
curs_start.close()
conn.close()


