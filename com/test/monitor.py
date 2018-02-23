# -*- coding: utf-8 -*-

import urllib
import json
#FILEDIR = '/data_b/userdata/mbbi/qgw/test.txt'
FILEDIR = '/Users/wangdi/Documents/upload/test.txt'

#王帝：18146556026
#祁更为：15600391747
#赵孟：18802716854
#艳蓉: 18612482498
#朱京昱: 18001285298
PHONENUMBERS = ['18146556026','15600391747','18802716854',
                '18612482498','18001285298']
def check():
    day = "*"
    hour = "*"
    fr = None
    try:
        fr = open(FILEDIR)
        lineNum=0
        today_hour_set_mysql=set()
        today_hour_set_hive=set()
        for line in fr.readlines():
            lineNum = lineNum + 1
            #mysql中小时表今天消费总数
            if lineNum == 20:
                mysql_today_all = line.split("\t")
                mysql_today_all_time = mysql_today_all[0]
                mysql_today_all_consume = mysql_today_all[1]
                continue
            #mysql中流水表今天消费总数
            if lineNum == 17:
                mysql_account = line.split("\t")
                mysql_account_consume = mysql_account[3]
                #                print mysql_account_consume
                continue
            #mysql中小时表前一小时点击总数、曝光总数、消费总数
            if lineNum == 3:
                mysql_today_1before = line.split("\t")
                mysql_today_1before_hour = mysql_today_1before[0].strip()
                mysql_today_1before_click = mysql_today_1before[1].strip()
                mysql_today_1before_imp = mysql_today_1before[2].strip()
                mysql_today_1before_consume = mysql_today_1before[3].strip()
                #print mysql_today_1before_hour,mysql_today_1before_click,mysql_today_1before_imp,mysql_today_1before_consume
                today_hour_set_mysql.add(mysql_today_1before_hour+"-"+mysql_today_1before_click+"-"+mysql_today_1before_imp+"-"+mysql_today_1before_consume)
                continue
            #mysql中小时表前两小时点击总数、曝光总数、消费总数
            if lineNum == 5:
                mysql_today_2before = line.split("\t")
                mysql_today_2before_hour = mysql_today_2before[0].strip()
                mysql_today_2before_click = mysql_today_2before[1].strip()
                mysql_today_2before_imp = mysql_today_2before[2].strip()
                mysql_today_2before_consume = mysql_today_2before[3].strip()
                #print mysql_today_2before_hour,mysql_today_2before_click,mysql_today_2before_imp,mysql_today_2before_consume
                today_hour_set_mysql.add(mysql_today_2before_hour+'-'+mysql_today_2before_click+"-"+mysql_today_2before_imp+"-"+mysql_today_2before_consume)
                continue
            #mysql中小时表前三小时点击总数、曝光总数、消费总数
            if lineNum == 7:
                mysql_today_3before = line.split("\t")
                mysql_today_3before_hour = mysql_today_3before[0].strip()
                mysql_today_3before_click = mysql_today_3before[1].strip()
                mysql_today_3before_imp = mysql_today_3before[2].strip()
                mysql_today_3before_consume = mysql_today_3before[3].strip()
                #print mysql_today_2before_hour,mysql_today_2before_click,mysql_today_2before_imp,mysql_today_2before_consume
                today_hour_set_mysql.add(mysql_today_3before_hour+'-'+mysql_today_3before_click+"-"+mysql_today_3before_imp+"-"+mysql_today_3before_consume)
                continue
                #hive中小时表向前一个小时点击总数、曝光总数、消费总数
            if lineNum == 37:
                hive_today_1before = line.split("|")
                hive_today_1before_hour = hive_today_1before[1].strip()
                hive_today_1before_click = hive_today_1before[2].strip()
                hive_today_1before_imp = hive_today_1before[3].strip()
                hive_today_1before_consume = hive_today_1before[4].strip()
                #                print mysql_today_1before_hour,mysql_today_1before_click,mysql_today_1before_imp,mysql_today_1before_consume
                today_hour_set_hive.add(hive_today_1before_hour+'-'+hive_today_1before_click+"-"+hive_today_1before_imp+"-"+hive_today_1before_consume)
                continue
            #hive中小时表向前两个小时点击总数、曝光总数、消费总数
            if lineNum == 42:
                hive_today_2before = line.split("|")
                hive_today_2before_hour = hive_today_2before[1].strip()
                hive_today_2before_click = hive_today_2before[2].strip()
                hive_today_2before_imp = hive_today_2before[3].strip()
                hive_today_2before_consume = hive_today_2before[4].strip()
                #print mysql_today_2before_hour,mysql_today_2before_click,mysql_today_2before_imp,mysql_today_2before_consume
                today_hour_set_hive.add(hive_today_2before_hour+'-'+hive_today_2before_click+"-"+hive_today_2before_imp+"-"+hive_today_2before_consume)
                continue
            #hive中小时表向前三个小时点击总数、曝光总数、消费总数
            if lineNum == 47:
                hive_today_3before = line.split("|")
                hive_today_3before_hour = hive_today_3before[1].strip()
                hive_today_3before_click = hive_today_3before[2].strip()
                hive_today_3before_imp = hive_today_3before[3].strip()
                hive_today_3before_consume = hive_today_3before[4].strip()
                #print mysql_today_2before_hour,mysql_today_2before_click,mysql_today_2before_imp,mysql_today_2before_consume
                today_hour_set_hive.add(hive_today_3before_hour+'-'+hive_today_3before_click+"-"+hive_today_3before_imp+"-"+hive_today_3before_consume)
                continue

                #mysql中天表统计昨天点击总数、曝光总数、消费总数
            if lineNum == 23:
                mysql_yesterday_now = line.split("\t")
                mysql_yesterday_now_hour = mysql_yesterday_now[0]
                mysql_yesterday_now_click = mysql_yesterday_now[1]
                mysql_yesterday_now_imp = mysql_yesterday_now[2]
                mysql_yesterday_now_consume = mysql_yesterday_now[3]
                # print mysql_yesterday_now_hour,mysql_yesterday_now_click,mysql_yesterday_now_imp,mysql_yesterday_now_consume
                mysql_yesterday_now_str=mysql_yesterday_now_hour+'-'+(mysql_yesterday_now_click+'-'+mysql_yesterday_now_imp+'-'+mysql_yesterday_now_consume)
                continue
            #hive中天表统计昨天的点击总数、曝光总数、消费总数
            if lineNum == 53:
                hive_yesterday_day = line.split("|")
                hive_yesterday_day_date = hive_yesterday_day[1].strip()
                hive_yesterday_day_click = hive_yesterday_day[2].strip()
                hive_yesterday_day_imp = hive_yesterday_day[3].strip()
                hive_yesterday_day_consume = hive_yesterday_day[4].strip()
                #print hive_day_date,hive_day_click,hive_day_imp,hive_day_consume
                hive_yesterday_day_str=hive_yesterday_day_date+'-'+hive_yesterday_day_click+'-'+hive_yesterday_day_imp+'-'+hive_yesterday_day_consume
                continue

        #mysql中实时流水今天消费总数
        #mysql中实时小时表统计截止目前今天点击总数、曝光总数、消费总数
        if(mysql_account_consume.strip() != mysql_today_all_consume.strip()):
            print "mysql中实时流水/小时表今天消费总数对比"+mysql_today_all_time
            return -1,day,'mysql中实时流水/小时表今天消费总数对比不一致,'

        #mysql中实时小时表向前三个小时点击总数、曝光总数、消费总数
        #hive中小时表向前三个小时点击总数、曝光总数、消费总数
        if(mysql_yesterday_now_str.strip() != hive_yesterday_day_str.strip()):
            print "mysql/hive中天表统计昨天数据不一致,mysql:"+mysql_yesterday_now_str+",hive:"+hive_yesterday_day_str
            return -2,day,"mysql/hive中天表统计昨天数据不一致,"

        #mysql中天表统计昨天点击总数、曝光总数、消费总数
        #hive中天表统计昨天的点击总数、曝光总数、消费总数
        print today_hour_set_mysql
        print today_hour_set_hive
        set_result = today_hour_set_mysql.difference(today_hour_set_hive)
        if any(set_result) == True:
            print day,hour,"mysql/hive中实时小时三小时数据对比不一致"
            return -3,day,'mysql/hive中实时小时三小时数据对比不一致,'

        print day,hour,"数据正常"
        return 0,day,hour
    except Exception, exception:
        print exception
        return -5,day,"邮件输出格式错误,"
    finally:
        if fr != None:
            fr.close()
def sendSms(day,hour):
    for phoneNumber in PHONENUMBERS:
        content = hour + "数据统计异常，详情请查看对应邮件"
        print content
       #url = "http://10.31.103.111:8088/smsServer/sms/send?destnumber="+phoneNumber+"&content="+content+"&application=bi&username=bi&type=1"
       # urllib.urlopen(url)
if __name__ == '__main__':
    result,day,hour = check()
    if result != 0:
        sendSms(day,hour)
