#!/usr/bin/python
#coding=utf-8
from rediscluster import StrictRedisCluster
import sys

def redis_cluster():
    redis_nodes = [{'host':'10.18.38.41','port':9000},
                   {'host':'10.18.38.41','port':9001},
                   {'host':'10.18.38.42','port':9000},
                   {'host':'10.18.38.42','port':9001}
                   ]

    # edis_nodes = [{'host':'10.16.13.53','port':9000},
    #               {'host':'10.16.13.54','port':9000}
    #               ]


    try:
        redisconn = StrictRedisCluster(startup_nodes = redis_nodes,password='Ef4bE3H')
        return redisconn
    except Exception,e:
        print "connect error"
        sys.exit(1)



if __name__ == '__main__':
    redisconn =  redis_cluster()
    #redisconn.hdel('real_hbase_rule','appid_dim')
    # redisconn.delete('rhr_mbbi:appid_dim_20180425')
    #redisconn.delete('rhr_mbbi:app_count_dim_20180426')
    # redisconn.delete('rhr_mbbi:union_dim_20180425')
    #redisconn.hdel('real_hbase_rule','adslotid_dim')
    #redisconn.hdel('real_hbase_rule','appid_dim')
    #print redisconn.hset('real_hbase_rule','union_dim','select appid,sum(charge),sum(v),sum(click),sum(av) from xpstrackingcharge where  appid in ("union","wapunion") group by appid')

    #print redisconn.hset('real_hbase_rule','app_count_dim','select appid,sum(charge),sum(v),sum(click),sum(av) from xpstrackingcharge where  appid in ("newssdk","news")  group by appid')

    #print redisconn.hset('real_hbase_rule','appid_dim','select sum(charge),sum(v),sum(click),sum(av) from xpstrackingcharge  group by appid,accounttype')
    #print redisconn.hset('real_hbase_rule','adslotid_dim',  'select sum(charge),sum(v),sum(click),sum(av) from xpstrackingcharge  group by appid,accounttype,os_rename,adslotid,newschn')
    print redisconn.hgetall('real_hbase_rule')
    #print redisconn.zrange('rhr_mbbi:appid_dim_20180425',0,-1,withscores=True)
    print redisconn.hgetall('rhr_mbbi:union_dim_201800502')

