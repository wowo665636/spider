#!/usr/bin/python
#coding=utf-8
from redispython import Database

class CheckCount(Database):

    def total_val_new(self,hkey):
        val = self.read(hkey)
        sum_val = 0
        for key in val:
            sum_val = sum_val+int(val[key])

        print("sum_val",sum_val)

    def total_val_old(self,hkey):
        val = self.read(hkey)
        sum_val = 0
        for key in val:
            sum_val = sum_val+int(val[key])

        print("sum_val",sum_val)



if __name__ == '__main__':
    redis_new =CheckCount('10.16.13.53','9000','Ef4bE3H')
    #redis_new.total_val_new('bi_consume_amount_2017112121')
    redis_new.total_val_new('bi_consume_stream')

    #redis_old =CheckCount('m.redis.sohucs.com',22166,'8e79303e42627fe4a44353dd0d717402')