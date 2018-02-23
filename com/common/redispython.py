#!/usr/bin/python
#coding=utf-8
import redis


class Database:
    def __init__(self,host,port,password):
         # self.host = 'm.redis.sohucs.com'
         # self.port = 22166
         # self.password = '8e79303e42627fe4a44353dd0d717402'

         self.host = host
         self.port = port
         self.password = password

    def write(self,key,value):  
        try:  
          
            val = value  
            #r = redis.StrictRedis(host=self.host,port=self.port,password=self.password)
            #r.set(key,val)
        except Exception, exception:  
            print exception  

    def read(self,key):
        try:
            r = redis.StrictRedis(host=self.host,port=self.port,password=self.password)
            value = r.get(key)
            # value = r.hgetall(key)
            print value
            return value  
        except Exception, exception:  
            print exception


if __name__ == '__main__':
    db = Database('10.16.13.54','9000','Ef4bE3H')
    r = redis.StrictRedis(host='10.16.13.53',port='9000',password='Ef4bE3H')
    #db.read('c_id_20171210')
    #db.read('v_id_20171210')
    #db.read('av_id_20171210')
    #db.read('bi_consume_amount_2017120515')
    #value = r.llen("bi_consume_stream")
    #,'aid=10000586-campid=20002990-adgid=40007443-crid=100013707'
    value=r.hgetall('bi_consume_amount_2017122209')
    print value
   # db.write('sppp','sppp')

    # db.read('bi_consume_amount_2017111019_aid=10002045-campid=20002606-adgid=40006408-crid=100011872')