#!/bin/sh
if [ x$1 == x ]; then
    echo "project is null"
    exit
fi
file_name=$1
sed -e 's/[ ][ ]*//g' $file_name |awk 'BEGIN{ORS=","}{printf "%s",$1}' aa.txt |sed -e 's/,/\t/g' > ./hive_format_tmp.txt

