#!/bin/sh
curr_hour=`date +%m-%d%H`
NUMS="00 01 02 03 04 05 06 07 08 09 10 11 12 13 14 15 16 17 18 19 20 21 22 23"
#NUMS="06 07"
if [ x$1 == x ]; then
    echo "date is null"
    exit
fi

for NUM in $NUMS
do
	if [ $curr_hour == $1$NUM ]; then
    	echo '---exit---'
    	exit
	fi
	date_time='2017-'$1' '$NUM':00:00'	
	./real_add.py $date_time
	echo "time:" $date_time
  
done
