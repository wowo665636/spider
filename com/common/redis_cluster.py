#!/usr/bin/python
#coding=utf-8
from rediscluster import StrictRedisCluster
import sys
import string
import json

def redis_cluster():
    redis_nodes = [{'host':'10.16.13.53','port':9000},
                   {'host':'10.16.13.53','port':9001},
                   {'host':'10.16.13.54','port':9000},
                   {'host':'10.16.13.54','port':9001}
                   ]
    # redis_nodes = [{'host':'10.18.70.40','port':9000},
    #                 {'host':'10.18.70.40','port':9001},
    #                 {'host':'10.18.70.41','port':9000},
    #                 {'host':'10.18.70.41','port':9001}
    #                 ]
    try:
        redisconn = StrictRedisCluster(startup_nodes = redis_nodes,password='Ef4bE3H')
        return redisconn
    except Exception,e:
        print "connect error"
        sys.exit(1)


    #redisconn.set('name','kk')


if __name__ == '__main__':
    redisconn =  redis_cluster()
    #date = sys.argv[1]
    # c_id_dict =  redisconn.hgetall('bi_c_id_2018010518')
    # for key in c_id_dict:
    #     if string.find(key,'aid') ==-1:
    #         redisconn.rename('bi_c_id_2018010518',)
    #
    #         print key
    #print redisconn.hgetall('bi_consume_amount_2018012208')
   # print redisconn.hgetall('bi_imp_amount_2018012208')
    #print redisconn.hgetall('bi_c_id_2018012208')

   # print redisconn.lrange('bi_consume_stream',0,-1)




   # print "c=".join(redisconn.get('c_id_'+'2017121520'))
    #print "v="+redisconn.get('v_id_'+date)
    #print "av="+redisconn.get('av_id'+date)

    print "c="+json.dumps(redisconn.lrange('tc_repeat_c_key_'+'2018020810',0,-1))
    print "v="+json.dumps(redisconn.lrange('tc_repeat_v_key_'+'2018020810',0,-1))
    print "av="+json.dumps(redisconn.lrange('tc_repeat_av_key_'+'2018020810',0,-1))

