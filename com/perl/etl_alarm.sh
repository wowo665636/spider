#!/bin/sh
ls_date=`date +%Y-%m-%d`
exe_sql='/home/dwetl/wd/etl_job.sql'
echo "select rownum,RUNNINGSCRIPT,etl_job, last_jobstatus,LAST_STARTTIME,LAST_ENDTIME from etl_job  WHERE  last_jobstatus='Failed'  AND LAST_STARTTIME >'$ls_date' " > $exe_sql
perl /home/dwetl/wd/select_sql.pl $exe_sql >/home/dwetl/wd/etl_alarm.log
count=`cat /home/dwetl/wd/etl_alarm.log| grep -v '' |wc -l`

if [ $count -gt 0 ];then
   content=`cat /home/dwetl/wd/etl_alarm.log`
   wget -q -O /dev/null "zabbix.adrd.sohuno.com/sendweixin/sendweixinetl.php?name=@all&content='$content'"
   mv /home/dwetl/wd/etl_alarm.log /home/dwetl/wd/etl_alarm.log$ls_date
fi

