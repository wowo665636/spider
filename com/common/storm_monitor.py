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
    #print redisconn.hgetall('bi_export_map')
    print redisconn.hgetall('bi_trend_naa_2018051611')
    # print redisconn.hgetall('bi_trend_naa_2018040210')
    # print redisconn.get("sql_rule_trend")
    # print redisconn.hgetall('t_dwd_alliance_spam_count')
    # sum_key('bi_trend_v_2018040415')
    # sum_key('bi_trend_av_2018040415')
    # sum_key('bi_trend_na_2018040415')
    # sum_key('bi_trend_consume_2018040415')

#print redisconn.hgetall('bi_custom_sql_rule_appid_v_2018032313')
    #print redisconn.hlen('bi_custom_sql_rule_appid_v_2018032312')
    #print redisconn.hgetall('bi_trend_v_2018032310')
    # and adslotid in ('12355','12224','13016')
    #---------redis dim key----------#
    #redisconn.hset("bi_custom_rule","sql_fact","select ett,accounttype,appid,charge,bidtype,appdelaytrack,adslotid,reposition,lc,rr,isspam,timestamp from xpstrackingcharge where ett in ('na','v','av','click')   group by ett,accounttype,appid,charge,bidtype,appdelaytrack,adslotid,reposition,lc,rr,isspam,timestamp")
    #redisconn.hset("bi_dim_rule","sql_rule_appid","select accounttype,appid,adslotid,sum(charge),sum(v),sum(click),sum(av) from xpstrackingcharge where   ett in('na','v','av','click') group by accounttype,appid,adslotid")

    #print redisconn.hgetall('bi_dim_rule')

    #print redisconn.get('sql_rule_trend')
    #---------redis dim key----------#
    # sum_day_key
    # sum_0=sum_day('bi_custom_sql_rule_appid_',date,'')


    #print "---合计:"+str(sum_0)
    #
    # sum_1=sum_day('bi_trend_av_',date,'')
    # print "---合计:"+str(sum_1)



    # sum_2=sum_day('bi_trend_v_',date,'')
    # print "---合计:"+str(sum_2)
    #
    # sum_3=sum_day('bi_trend_c_',date,'')
    # print "---合计:"+str(sum_3)
    #
    # sum_4=sum_day('bi_trend_na_',date,'')
    # print "---合计:"+str(sum_4)







