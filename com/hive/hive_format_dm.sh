#!/bin/sh
ls_date=`date +%Y%m%d`
ye_date=`date -d yesterday  +%Y%m%d` 
be_ye_date=`date -d "2 day ago" +%Y%m%d`

sourc_hql_dir=$1
target_hql_dir=`pwd`

sed 's/$b_yesterday/'"$be_ye_date"'/g'  $sourc_hql_dir > $target_hql_dir/result.hql

/usr/bin/beeline -u "jdbc:hive2://tcm.otocyon.com:10000/mbbi;principal=hive/tcm.otocyon.com@OTOCYON.COM"  --hiveconf mapreduce.job.queuename=media  -f $target_hql_dir/result.hql > $1".log"

