#!/usr/bin/python
#coding=utf-8
from rediscluster import StrictRedisCluster
import sys
import string
import json
import datetime

def redis_cluster():
    redis_nodes = [{'host':'10.16.13.53','port':9000},
                   {'host':'10.16.13.53','port':9001},
                   {'host':'10.16.13.54','port':9000},
                   {'host':'10.16.13.54','port':9001}
                  ]
    # redis_nodes = [{'host':'10.18.38.41','port':9000},
    #                 {'host':'10.18.38.41','port':9001},
    #                 {'host':'10.18.38.42','port':9000},
    #                 {'host':'10.18.38.42','port':9001}
    #                 ]
    try:
        redisconn = StrictRedisCluster(startup_nodes = redis_nodes,password='Ef4bE3H')
        return redisconn
    except Exception,e:
        print "connect error"
        sys.exit(1)

def redis_cluster2():
    redis_nodes = [{'host':'10.16.13.53','port':9000},
                   {'host':'10.16.13.53','port':9001},
                   {'host':'10.16.13.54','port':9000},
                   {'host':'10.16.13.54','port':9001}
                  ]

    try:
        redisconn = StrictRedisCluster(startup_nodes=redis_nodes,password='Ef4bE3H')
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
    while ((curr_hour-1) !=0):
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
    date='20180227'
    sum_0=sum_day('bi_trend_consume_',date,'accounttype=0&appid=newssdk&')
    sum_1 = sum_day('bi_trend_consume_',date,'accounttype=1&appid=newssdk&')

    print "---合计:"+str(sum_0+sum_1)


    #redisconn = redis_cluster()
   # print redisconn.hgetall("bi_trend_v_2018022715")
    # redisconn = redis_cluster2()

    # sum_key("bi_trend_v_"+date)
    #
    # consume_dict_new= redisconn1.hgetall("bi_trend_consume_"+date)
    # sum_v_new=0
    # for key in consume_dict_new:
    #     if string.find(key,'accounttype=1') !=-1:
    #         value = int(consume_dict_new[key])
    #         sum_v_new +=value
    #
    # sum_v_0=0
    # sum_v_1=0
    # sum_v_other=0;
    # v_dict_new=redisconn1.hgetall("bi_trend_v_"+date)
    # for key in v_dict_new:
    #     if string.find(key,'accounttype=0') !=-1:
    #         value =int(v_dict_new[key])
    #         sum_v_0+=value
    #     if string.find(key,'accounttype=0') ==-1 and  string.find(key,'accounttype=1')==-1:
    #         value =int(v_dict_new[key])
    #         sum_v_other+=value
    #     if string.find(key,'accounttype=1') !=-1:
    #         value =int(v_dict_new[key])
    #         sum_v_1+=value
    # print '长尾v:', '{:,}'.format(sum_v_1),'  AE:', '{:,}'.format(sum_v_0),'  其他:','{:,}'.format(sum_v_other)
    #
    # sum_key("bi_trend_av_"+date)
    # sum_key("bi_trend_c_"+date)
    # print "bi_trend_consume_"+date,":","{:,}".format(sum_v_new/100000)
    # na1 = redisconn1.hget("bi_trend_na_"+date,date[8:10])
    # print "na:","{:,}".format(int(na1))
    #
    # print "-------------------------"
    # v1 = redisconn.hget("bi_sstics_v_all_"+date[0:8],date[8:10])
    # print "v","{:,}".format(int(v1))
    # av1 = redisconn.hget("bi_sstics_av_all_"+date[0:8],date[8:10])
    # print "av","{:,}".format(int(av1))
    # c1 = redisconn.hget("bi_sstics_c_all_"+date[0:8],date[8:10])
    # print "c","{:,}".format(int(c1))
    # na2=redisconn.hget("bi_sstics_v_empty_"+date[0:8],date[8:10])
    #
    # consume_dict= redisconn.hgetall("bi_sstics_surplus_consume_"+date[0:8])
    #
    # sum_v1=0
    # for key in consume_dict:
    #     if string.find(key,date[8:10]) !=-1:
    #         value = int(consume_dict[key])
    #         sum_v1 +=value
    #
    # print "consume:","{:,}".format(sum_v1/100000)
    # print "na:","{:,}".format(int(na2))








