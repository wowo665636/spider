#!/usr/bin/python
#coding=utf-8
from rediscluster import StrictRedisCluster
import sys
import string
import json

def redis_cluster():
    redis_nodes = [{'host':'10.16.13.53','port':9000},
                   {'host':'10.16.13.54','port':9000}
                  ]
    try:
        redisconn = StrictRedisCluster(startup_nodes = redis_nodes,password='Ef4bE3H')
        return redisconn
    except Exception,e:
        print "connect error"
        sys.exit(1)


    #redisconn.set('name','kk')

def sum_day_key(hkey,include):
    redisconn = redis_cluster()
    dict = redisconn.hgetall(hkey)
    sum=0
    for key in dict:
        value = 0
        if include !="":
            if string.find(key,include)!=-1:
                value = int(dict[key])
        else:
            value = int(dict[key])

        sum +=value
    print hkey,":","{:,}".format(sum)
    return sum

if __name__ == '__main__':
    redisconn =  redis_cluster()
    #redisconn.hgetall("bi_consume_amount_")
    #date = sys.argv[1]
    #print redisconn.hgetall('bi_consume_amount_2018040419')
    # c_id_dict =  redisconn.hgetall('bi_imp_amount_2018041216')
    # for key in c_id_dict:
    #     if string.find(key,'aid=10017654') !=-1:
    #         print key

    # hours=['00','01','02','03','04','05','06','07','08','09','10','11','12','13','14','15','16','17']
    # for hour in hours:
    #     print sum_day_key('bi_consume_amount_20180412'+hour,'')


    #print redisconn.hgetall('bi_consume_amount_2018031312')
    print redisconn.hgetall('bi_imp_amount_2018042619')
    #print redisconn.hgetall('bi_c_id_2018012208')

   # print redisconn.lrange('bi_consume_stream',0,-1)




   # print "c=".join(redisconn.get('c_id_'+'2017121520'))
    #print "v="+redisconn.get('v_id_'+date)
    #print "av="+redisconn.get('av_id'+date)

   # print "c="+json.dumps(redisconn.lrange('tc_repeat_c_key_'+'20180208224',0,-1))
    # print "v="+json.dumps(redisconn.lrange('tc_repeat_v_key_' +'2018022413',0,-1))
    #print "av="+json.dumps(redisconn.lrange('tc_repeat_av_key_'+'2018022413',0,-1))

    #redisconn.set("sql_rule_trend","select accounttype,appid,appdelaytrack,sum(charge),sum(v),sum(click),sum(av) from xpstrackingcharge where   ett in('na','v','av','click') group by accounttype,appid,appdelaytrack")

    #print redisconn.get("sql_rule_trend")
   #  print redisconn.get("sql_rule_trend")
   #  print redisconn.hgetall('bi_trend_v_2018022619')
   #  print redisconn.hgetall('bi_trend_av_2018022619')
   #  print redisconn.hgetall('bi_trend_c_2018022619')
   #  print redisconn.hgetall('bi_trend_consume_2018022619')
   #  print redisconn.hgetall('bi_trend_na_20180226')
   #  print redisconn.hgetall('bi_trend_naa_20180226')


    #print redisconn.hgetall('bi_c_id_2018012208')
