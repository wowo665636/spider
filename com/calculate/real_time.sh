#!/bin/sh

NUMS="00 01 02 03 04 05 06 07 08 09 10 11 12 13 14 15 16 17 18 19 20 21 22 23"
if [ x$1 == x ]; then
    echo "date is null"
    exit
fi
echo -e 'sum(click_amount)' '\t' 'sum(imp_amount)' '\t' 'sum(consume_amount)' '\t' 'data_time' >>real_time_$1.xls
for NUM in $NUMS
do
   mysql -h10.17.64.180 -udm_xps_rw -pb4ne8hIdaLMej3+ dm_xps --default-character-set=utf8 -e "select sum(click_amount),sum(imp_amount) ,sum(consume_amount) ,data_time from  t_dm_xps_advertiser_consume_hour_crid_real_time where data_time='2017-$1 $NUM:00:00' " | grep -v 'sum' >> real_time_$1.xls
done


