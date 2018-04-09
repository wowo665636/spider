# coding: utf-8

import os,sys

from pyspark.sql import SparkSession

#SPARK_HOME = os.environ["SPARK_HOME"]

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("beibei targeting ") \
        .config("mapreduce.job.queuename", "root.media") \
        .enableHiveSupport() \
        .getOrCreate()

    #sqlDF = spark.sql("SELECT hash_uid FROM mbadp.t_dw_third_beibei_shopping_201803 limit 10")
    #sqlDF = spark.read.text("viewfs://cluster11/user/mbadp/third_data/beibei_shopping/201803")
    sqlDF = spark.read.text("viewfs://cluster11/user/hive/warehouse/mbbi.db/t_md_sensitve_word")
    stringsDS = sqlDF.rdd.map(lambda r: r[0])

    for record in stringsDS.take(10):
        #print "%s-%s" % (record[0], record[1])
        print record
    spark.stop()