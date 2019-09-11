import os

from pyspark.sql import SparkSession
from pyspark.sql import Row,SQLContext
from pyspark.sql.functions import desc, size, max, abs, lit, expr, explode, to_date

import re
import random
from math import sqrt
from datetime import datetime
import time
# Dask imports
import dask.bag as db
import dask.dataframe as df
import numpy

from pyspark import SparkContext, SparkConf

os.environ["PYSPARK_PYTHON"]="/usr/local/bin/python3"
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages com.amazonaws:aws-java-sdk:1.7.4,org.apache.hadoop:hadoop-aws:2.7.3 pyspark-shell'

def init_spark():
    spark = SparkSession \
        .builder \
        .appName("Python Spark SQL basic example") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()
    return spark

def context_spark():
	conf = (SparkConf().setMaster("local[4]").set("spark.executor.extraJavaOptions","-Dcom.amazonaws.services.s3.enableV4=true").set("spark.driver.extraJavaOptions","-Dcom.amazonaws.services.s3.enableV4=true"))
	sc = SparkContext(conf=conf)
	sc.setSystemProperty("com.amazonaws.services.s3.enableV4", "true")
	
	sql = SQLContext(sc)
	
	hadoopConf = sc._jsc.hadoopConfiguration()
	hadoopConf.set("fs.s3a.awsAccessKeyId", "---")
	hadoopConf.set("fs.s3a.awsSecretAccessKey", "---")
	hadoopConf.set("fs.s3a.endpoint", "s3.us-east-1.amazonaws.com")
	hadoopConf.set("com.amazonaws.services.s3a.enableV4", "true")
	hadoopConf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
	print(sc._conf.getAll())
	return sql
	
date = "2011"
sql = context_spark()
spark = init_spark()

test = sql.read.parquet("file:///Users/Max/Desktop/Transaction-10:18/transaction_2010.snappy.parquet")
test = test.select("txID","blockID","Iaddr","Oaddr","year","month","day","Sum")
test.show()

