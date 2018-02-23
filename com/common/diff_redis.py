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
#curr_time = '2017-07-26 11:00:00'
param_hour= sys.argv[2]
curr_time = sys.argv[1]+' '+param_hour

if curr_time.strip()=="":
    exit();

print curr_time


#curr_date = now.strftime("%Y-%m-%d")
curr_date = sys.argv[1]
d2 = now - datetime.timedelta(days=1)
yes_time =  d2.strftime("%Y-%m-%d %H:%M:%S")

log_dir = '/home/wangdi/test/'+curr_date
log_name = log_dir+'/info-'+param_hour[0:2]+'.log'
if os.path.exists(log_dir):
    print log_dir+' exists'
else:
    os.makedirs(log_dir)

f = open(log_name,"w")
conn =  MySQLdb.connect(host='10.17.64.180',user='dm_xps_rw',passwd='b4ne8hIdaLMej3+',db='dm_xps', port=3306,charset='utf8')
conn.select_db('dm_xps')
curs = conn.cursor()

curs_start = conn.cursor()
curs_start.execute('select from_unixtime(UNIX_TIMESTAMP(),"%Y-%m-%d %H:%m:%s");')
curr_time_db = curs_start.fetchone()


#1.备份数据
back_curs = conn.cursor()
back_real_sql = 'INSERT INTO t_dm_xps_advertiser_consume_hour_crid_real_time_history (`id`, `click_amount`, `imp_amount`, `consume_amount`, `aid`, `campid`, `adgid`, `crid`, `hour`, `data_time`, `create_time` ) select `id`, `click_amount`, `imp_amount`, `consume_amount`, `aid`, `campid`, `adgid`, `crid`, `hour`, `data_time`,"%s" from t_dm_xps_advertiser_consume_hour_crid where data_time="%s" limit 0,10000;' %(curr_time_db[0],curr_time);
f.write(back_real_sql+ '\n')
#back_curs.execute(back_real_sql)

#2.开始比对离线 实时小时表数据
#1.离线实时 交集对比,更新操作, 差集 离线,新增操作(离线有, 实时没有)
#                         0         1               2               3           4       5           6       7       8       9           10
hour_crid_sql = 'select a.`id`, a.`click_amount`, a.`imp_amount`, a.`consume_amount`, a.`aid`, a.`campid`, a.`adgid`, a.`crid`, a.`hour`, a.`data_time`, a.`create_time` from t_dm_xps_advertiser_consume_hour_crid a left join t_dm_xps_advertiser_consume_hour_crid_real_time b  on a.aid = b.aid AND a.campid = b.campid AND a.adgid = b.adgid AND a.crid = b.crid AND a.data_time = b.data_time where a.data_time="%s" limit 0,10000;' %(curr_time);

off_line_hour_list = curs.execute(hour_crid_sql);
results = curs.fetchmany(off_line_hour_list)
print 'hour_crid_sql:',hour_crid_sql
for r in results:
    print '#######################################'
    print "off:",r[0],r[1],r[2],r[3],r[4],r[5],r[6],r[7],r[8],r[9],r[10]
    #f.write(str(r[0])+'   '+str(r[1])+'   '+str(r[2])+ '\n')
    real_hour_crid_sql = 'select  `id`,`click_amount`, `imp_amount`, `consume_amount` from t_dm_xps_advertiser_consume_hour_crid_real_time WHERE `data_time`="%s" and `aid`= "%d" and `campid`= "%d" and `adgid`="%d" and `crid`="%d" ;'  %(r[9],r[4],r[5],r[6],r[7]);
    print 'real_hour_crid_sql:',real_hour_crid_sql
    curs2 = conn.cursor();
    curs2.execute(real_hour_crid_sql);
    real_result = curs2.fetchone();

    if real_result == None:
        #新增小时表,流水表数据
        insert_sql='insert into `t_dm_xps_advertiser_consume_hour_crid_real_time` ( `click_amount`, `imp_amount`, `consume_amount`, `aid`, `campid`, `adgid`, `crid`, `hour`, `data_time`, `create_time`) values("%s","%s","%s","%s","%s","%s","%s","%s","%s","%s");' %(r[1],r[2],r[3],r[4],r[5],r[6],r[7],r[8],r[9],curr_timestamp);
        #print insert_sql
        f.write(insert_sql+ '\n')
        insert_real_stream_sql = 'INSERT INTO  t_dm_xps_advertiser_consume_stream_real_time(serial_num,consume_amount ,aid ,campid ,adgid ,crid ,`hour` ,data_time ,create_time ) values ("%s","%s","%s","%s","%s","%s","%s","%s","%s");' %(serial_num,r[3],r[4],r[5],r[6],r[7],r[8],r[9],curr_timestamp);
        #print insert_real_stream_sql
        f.write(insert_real_stream_sql+'\n')

    else:
        #实时表有数据,插入流水抵扣值
        diff_consume = r[3] - real_result[3]
        diff_imp =  r[2] - real_result[2]
        diff_click = r[1] - real_result[1]
        print 'diff_consume=',diff_consume
        #-- 更新小时,更新流水
        if diff_consume <> 0:
            update_sql = 'update t_dm_xps_advertiser_consume_hour_crid_real_time set `click_amount`="%s",`imp_amount`="%s",`consume_amount`="%s" WHERE data_time="%s" and `aid`= "%s" and `campid`= "%s" and `adgid`="%s" and `crid`="%s";' %(r[1],r[2],r[3],r[9],r[4],r[5],r[6],r[7]);
            #print update_sql
            f.write(update_sql+ '\n')

            insert_real_stream_sql = 'INSERT INTO  t_dm_xps_advertiser_consume_stream_real_time(serial_num,consume_amount ,aid ,campid ,adgid ,crid ,`hour` ,data_time ,create_time ) values ("%s","%s","%s","%s","%s","%s","%s","%s","%s");' %(serial_num,diff_consume,r[4],r[5],r[6],r[7],r[8],r[9],curr_timestamp);
            #print insert_real_stream_sql
            f.write(insert_real_stream_sql+ '\n')
        elif diff_click <> 0 or diff_imp <> 0:
            update_sql = 'update t_dm_xps_advertiser_consume_hour_crid_real_time set `click_amount`="%s",`imp_amount`="%s",`consume_amount`="%s" WHERE data_time="%s" and `aid`= "%s" and `campid`= "%s" and `adgid`="%s" and `crid`="%s";' %(r[1],r[2],r[3],r[9],r[4],r[5],r[6],r[7]);
            #print update_sql
            f.write(update_sql+ '\n')



#2.实时 差集,删除数据,新增抵扣流水
#                         0                 1               2               3                4          5           6          7       8            9                10
real_hour_crid_sql = 'select a.`id`, a.`click_amount`, a.`imp_amount`, a.`consume_amount`, a.`aid`, a.`campid`, a.`adgid`, a.`crid`, a.`hour`, a.`data_time`, a.`create_time` from t_dm_xps_advertiser_consume_hour_crid_real_time a left join t_dm_xps_advertiser_consume_hour_crid b  on a.aid = b.aid AND a.campid = b.campid AND a.adgid = b.adgid AND a.crid = b.crid AND a.data_time = b.data_time where b.id is NULL AND a.data_time="%s" limit 0,10000;' %(curr_time);
curs3 = conn.cursor();
diff_real_hour_list = curs3.execute(real_hour_crid_sql);
diff_results = curs3.fetchmany(diff_real_hour_list)
for o in diff_results:
    print '################diff start#######################'
    print "diff_real:",o[0],o[1],o[2],o[3],o[4],o[5],o[6],o[7],o[8],o[9],o[10]
    del_sql='delete from t_dm_xps_advertiser_consume_hour_crid_real_time WHERE data_time="%s" and `aid`= "%s" and `campid`= "%s" and `adgid`="%s" and `crid`="%s";' %(o[9],o[4],o[5],o[6],o[7]);
    f.write(del_sql+ '\n')
    insert_real_stream_sql = 'INSERT INTO  t_dm_xps_advertiser_consume_stream_real_time(serial_num,consume_amount ,aid ,campid ,adgid ,crid ,`hour` ,data_time ,create_time ) values ("%s","%s","%s","%s","%s","%s","%s","%s","%s");' %(serial_num,-o[3],o[4],o[5],o[6],o[7],o[8],o[9],curr_timestamp);
    f.write(insert_real_stream_sql+ '\n')


conn.commit()
curs.close()
curs2.close()
curs3.close()
back_curs.close()
curs_start.close()
f.close()
conn.close()
