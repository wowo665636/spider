#!/usr/bin/python
#coding=utf-8
from rediscluster import StrictRedisCluster
import sys
import string
import json
import datetime

def redis_cluster():
    redis_nodes = [{'host':'10.16.13.53','port':8000},
                   {'host':'10.16.13.53','port':8001},
                   {'host':'10.16.13.53','port':8002},
                   {'host':'10.16.13.53','port':8003},
                   {'host':'10.16.13.53','port':8004},
                   {'host':'10.16.13.53','port':8005},
                   {'host':'10.16.13.53','port':8006},
                   {'host':'10.16.13.53','port':8007},
                   {'host':'10.16.13.54','port':8000},
                   {'host':'10.16.13.54','port':8001},
                   {'host':'10.16.13.54','port':8002},
                   {'host':'10.16.13.54','port':8003},
                   {'host':'10.16.13.54','port':8004},
                   {'host':'10.16.13.54','port':8005},
                   {'host':'10.16.13.54','port':8006},
                   {'host':'10.16.13.54','port':8007}
                   ]
    try:
        redisconn = StrictRedisCluster(startup_nodes = redis_nodes,password='Ef4bE3H')
        return redisconn
    except Exception,e:
        print "connect error"
        sys.exit(1)



    #redisconn.set('name','kk')
def sum_key(day_key):
    redisconn = redis_cluster()
    c_id_dict = redisconn.hgetall(day_key)
    sum_v=0
    for key in c_id_dict:
        #if string.find(key,'aid') ==-1:
        #     redisconn.rename('bi_c_id_2018010518',)
        #     print key
        value = int(c_id_dict[key])

        sum_v +=value
    print day_key,":","{:,}".format(sum_v)


def sum_day_key(hkey,day,hour,include):
    redisconn = redis_cluster()
    dict = redisconn.hgetall(hkey+day+hour)
    sum=0
    for key in dict:
        value = 0
        if include !="":
            if string.find(key,include)!=-1:
                value = int(dict[key])
        else:
            value = int(dict[key])

        sum +=value
    print hkey+day+hour,":","{:,}".format(sum)
    return sum

def sum_day(hkey,day,include):
    now = datetime.datetime.now()
    time_format = now.strftime("%Y%m%d%H")
    curr_hour=int(time_format[8:10])
    sum_val=0
    while (curr_hour>-1):
        if curr_hour<10:
            search_time='0'+str(curr_hour)
        else:
            search_time=str(curr_hour)
        val = sum_day_key(hkey,day,search_time,include)
        sum_val +=val
        curr_hour=curr_hour-1

    print '总计:',"{:,}".format(sum_val)
    return sum_val




if __name__ == '__main__':
#    date = sys.argv[1]
    date='20180322'
    redisconn = redis_cluster()
    #redisconn.set("exp_test",1)
    #redisconn.expireat("exp_test",1523176820)
    # print redisconn.ttl("exp_test")
    # print redisconn.get("exp_test")
    prefix = 'bi_custom_'

    #serch 统计联盟空广告
    #redisconn.set('sql_rule_search','select count(1) from xpssearch where appid in ("union","wapunion") group by appid,adslotid,developerid,unionappid')
    #sum_key('bi_search_na_2018040316')
    # 宽表过滤规则 bi_custom_rule
    # redisconn.hset('bi_custom_rule','sql_fact','select ett,accounttype,appid,charge,bidtype,appdelaytrack,adslotid,reposition,lc,rr,isspam,timestamp from xpstrackingcharge where ett in ("na","v","av","click")   group by ett,accounttype,appid,charge,bidtype,appdelaytrack,adslotid,reposition,lc,rr,isspam,timestamp,developerid,unionappid')
    # print redisconn.hget('bi_custom_rule','sql_fact')

    # 分维度计算
    #print redisconn.hgetall('bi_dim_rule')
    #print redisconn.hset('bi_dim_rule','union','select appid,adslotid,developerid,unionappid,sum(charge),sum(v),sum(click),sum(av) from xpstrackingcharge where ett in ("na","v","av","click") and appid in ("union","wapunion") group by appid,adslotid,developerid,unionappid')
    # dau sql 模板
    # redisconn.set('bi_custom_count_uv','select sum(uv) from xpstrackingcharge where appid in ("news","newssdk") group by appid,os_rename,adslotid,appv')
    # print redisconn.hgetall('bi_custom_count_uv_'+'idfa_'+'20180408')
    # print redisconn.hgetall('bi_custom_count_uv_'+'imei_'+'20180408')
    # print redisconn.hgetall('bi_custom_count_uv_'+'cid_'+'20180408')
    #
    # print redisconn.hlen('bi_custom_count_uv_'+'idfa_'+'20180408')
    print redisconn.hgetall('t_dwd_alliance_spam_count')
    #print redisconn.hgetall(prefix+'union'+'_v_2018040817')
    # print redisconn.hlen(prefix+'sql_rule_appid'+'_v_2018040210')
   # print redisconn.hgetall(prefix+'union'+'_v_2018040211')









