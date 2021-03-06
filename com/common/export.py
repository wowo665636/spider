#!/usr/bin/python
#coding=utf-8
from rediscluster import StrictRedisCluster

import string
import json
import datetime
import os

def redis_cluster():
    # redis_nodes = [{'host':'10.16.13.53','port':9000},
    #                {'host':'10.16.13.53','port':9001},
    #                {'host':'10.16.13.54','port':9000},
    #                {'host':'10.16.13.54','port':9001}
    #                ]
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



def sum_hour(hkey,day,include):
    redisconn = redis_cluster()
    dict =redisconn.hgetall(hkey+day)
    sum=0
    for key in sorted(dict.keys()):
        value=0
        if include !="":
            if string.find(key,include)!=-1:

                value = int(dict[key])
                print key,":",'{:,}'.format(value)
        else:
            value = int(dict[key])
            print key,":",'{:,}'.format(value)
        sum +=value
    print hkey+day,":","{:,}".format(sum)
    return sum

def get_dim(hkey,include):
    key_set=set()
    redisconn = redis_cluster()
    dict =redisconn.hgetall(hkey)
    for key in sorted(dict.keys()):
        if include !="":
            if string.find(key,include) !=-1:
                key_set.add(key)
        else:
            key_set.add(key)

    return key_set


def get_dim_sum(hkey,dim_set) :
    redisconn = redis_cluster()
    sum=0
    if dim_set:
        for field in dim_set:
            value =redisconn.hget(hkey,field)
            sum += int(value)


    return '{:,}'.format(sum)


def sum_day(hkey,day,include):
    now = datetime.datetime.now()
    time_format = now.strftime("%Y%m%d%H")
    curr_hour=int(time_format[8:10])
    sum_val=0
    while (curr_hour >=0):
        if curr_hour<10:
            search_time='0'+str(curr_hour)
        else:
            search_time=str(curr_hour)
        val = sum_day_key(hkey,day,search_time,include)
        sum_val +=val
        curr_hour=curr_hour-1

    print '总计:',"{:,}".format(sum_val)
    return sum_val

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

def sortedDictValues3(adict):
    keys = adict.keys()
    keys.sort()
    return map(adict.get, keys)

if __name__ == '__main__':

      redisconn = redis_cluster()
      #redisconn.set("sql_rule_trend","select accounttype,appid,appdelaytrack,sum(charge),sum(v),sum(click),sum(av) from xpstrackingcharge where   ett in('na','v','av','click') group by accounttype,appid,appdelaytrack")
      # redisconn.delete("bi_trend_na_plat_2018030206")
      print redisconn.hgetall('bi_trend_na_plat_2018030216')
      
      
      # hours=['00','01','02','03','04','05','06','07','08','09','10','11','12','13','14','15','16','17']
      # for hour in hours:
      #     redisconn.delete("bi_trend_na_plat_20180302"+hour)
      #     print hour
      #
      # f = open("/Users/wangdi/Documents/sohu_pro/spider/mmm.txt");
      # iter_f = iter(f);
      # dict1 = {}
      # for line in iter_f:
      #      value = str(line.strip())
      #      line_arr = value.split(',')
      #      hour=str(line_arr[3]).strip()
      #      if len(str(line_arr[3].strip()))==1:
      #          hour="0"+line_arr[3].strip()
      #      hkey="bi_trend_na_plat_20180302"+hour
      #      key = "appid="+str(line_arr[1]).strip()
      #      f_v= line_arr[0].strip()
      #      #redisconn.hset(hkey,key,long(f_v))
      #      #redisconn.expire(hkey,24*60*60)
      #      print hkey, key, long(f_v)
      #
           
          
      
      
      #print dict1
      
      # for key in sorted(dict1.keys()):
      #     print key,dict1[key]
      #
      # f.close()
      #
      
     #
     #  print conn.hgetall('bi_sstics_surplus_consume_20180227')
     #  sum=sum_hour('bi_sstics_v_','20180227','surplus_count')
     # print '加载总计:',sum
     # sum=sum_hour('bi_sstics_av_','20180227','surplus_count')
     # print '展示总计:',sum
     # sum=sum_hour('bi_sstics_surplus_consume_','20180227','surplus_01')
     # print '收入总计:',sum
     #
     # sum=sum_hour('bi_sstics_v_empty_','20180227','')
     # print '空广告总计:',sum
     # sum=sum_hour('bi_sstics_c_','20180227','surplus_count')
     # print '点击:',sum






      # dim_set = get_dim('bi_trend_v_2018030122','')
      #
      #
      # sum_count=get_dim_sum('bi_trend_v_2018030122',dim_set)
      # print '总和:'+sum_count



      # print redisconn.hgetall('bi_sstics_v_all_20180228')
      #
      # print '--------------------'
      # print redisconn.hgetall('bi_sstics_v_all_20180228')

      # sum=sum_hour('bi_sstics_v_','20180301','brand_count')
      # print '加载:','{:,}'.format(sum)


      # date = '20180228'
      # sum_v = sum_day('bi_trend_v_',date,'')
      # print sum_v
      























