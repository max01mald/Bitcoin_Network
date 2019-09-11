import os

from pyspark.sql import SparkSession
from pyspark.sql import Row,SQLContext
from pyspark.sql.functions import desc, size, max, abs, lit, expr, explode, to_date

import re
import random
from math import sqrt, fabs
from datetime import datetime
import time
# Dask imports
import dask.bag as db
import dask.dataframe as df
import numpy

from pyspark import SparkContext, SparkConf

os.environ["PYSPARK_PYTHON"]="/usr/local/bin/python3"
#os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages com.amazonaws:aws-java-sdk-pom:1.11.83,org.apache.hadoop:hadoop-aws:2.7.3 pyspark-shell'
#os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages com.amazonaws:aws-java-sdk-pom:1.10.34,org.apache.hadoop:hadoop-aws-2.6.0 pyspark-shell'
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

def init_K(trans):
	s = random.randint(100,100000)

	sum = trans.select("Sum")
	sum = sum.filter(sum.Sum != "0.0").distinct()

	K = sum.sample(False, 0.1, seed=s).limit(5)

	return K.rdd.map(lambda p: (p[0])).collect()


def find_K(list,num):
	
	i = 0
	list2 = []
	n = 0
	while i < len(list):
		n = float(num) - float(list[i])
		list2.append(fabs(n))
		i += 1
	
	i = list2.index((min(list2)))
	
	return str(list[i])

def test_end(K,avg):
	i = 0
	
	while i < len(K):
		num = (float(K[i]) - float(avg[i]))**2
		
		if num > 3:
			return False

		i+=1
	
	return True


#def to_print()
SparkContext.setSystemProperty('spark.executor.memory','10g')
SparkContext.setSystemProperty('spark.driver.memory','10g')

#date = "2010"
sql = context_spark()
spark = init_spark()

trans = sql.read.parquet("s3a://---"+date)
trans = trans.select("txID","blockID","Iaddr","Oaddr","year","month","day","Sum","Price")
#trans.show()


max = 0
K =init_K(trans)


while 1:

	rdd = trans.rdd.map(lambda p: ([p[7]]))
	rdd = rdd.map(lambda p: (find_K(K,p[0]),(p[0])))

	i = 0
	sum_year = 0
	
	avg = []
	sum_trans = []
	num_trans = []
	
	while i < len(K):
		count = rdd.countByKey()
		diction = dict(count)
		sum = rdd.filter(lambda p: str(p[0]) == str(K[i])).reduceByKey(lambda a,b: float(a) + float(b))
		sum_trans.append(sum.collect()[0][1])
		sum_year += float(sum.collect()[0][1])
		num_trans.append(diction.get(str(K[i])))
		avg.append(float(sum.collect()[0][1]) / (diction.get(str(K[i]))))
		i+=1
	
	print(max)
	print("old: ")
	print(K)
	print("new: ")
	print(avg)
	
	if test_end(K,avg) or max == 33:
		break
	else:
		K = avg
		max += 1

rdd = trans.rdd.map(lambda p: ((p[0]),p[7]))
rdd = rdd.map(lambda p: (find_K(avg,p[1]),(p[0])))

rdd = rdd.map(lambda p: Row(K=(p[0]),txID=(p[1])))

frame = spark.createDataFrame(rdd)

frame = frame.join(trans,"txID","inner")
frame = frame.select("txID","blockID","Iaddr","Oaddr","year","month","day","Sum","K","Price")
frame.show()


frame.coalesce(1).write.parquet("s3a://---"+date)

'''
s_y = spark.sparkContext.parallelize([sum_year]).map(lambda p: (1,(p)))
s_t = spark.sparkContext.parallelize([sum_trans]).map(lambda p: (1,(p)))
n_t = spark.sparkContext.parallelize([num_trans]).map(lambda p: (1,(p)))
meta = s_y.map(lambda p: (p[0],p[1])).join(s_t.map(lambda p: (p[0],p[1]))).join(n_t.map(lambda p: (p[0],p[1])))
meta = meta.map(lambda p: ((p[1][0][0],p[1][0][1],p[1][1])))

meta = meta.map(lambda p: Row(sum_year=(p[0]),sum_trans=(p[1]),num_trans=(p[2])))
meta = spark.createDataFrame(meta)
meta.show()
'''
date = "2015"
trans = sql.read.parquet("s3a://---"+date)
trans.coalesce(1).parquet("s3a://---"+date)



