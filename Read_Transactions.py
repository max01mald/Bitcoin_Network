import os
from pyspark.sql import SparkSession
from pyspark.sql import Row,SQLContext
from pyspark.sql.functions import desc, size, max, abs, lit, expr, explode, to_date

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
	conf = (SparkConf().setMaster("local[4]").set("mapreduce.fileoutputcommitter.algorithm.version", "2").set("spark.executor.extraJavaOptions","-Dcom.amazonaws.services.s3.enableV4=true").set("spark.driver.extraJavaOptions","-Dcom.amazonaws.services.s3.enableV4=true"))
	sc = SparkContext(conf=conf)
	sc.setSystemProperty("com.amazonaws.services.s3.enableV4", "true")
	
	sql = SQLContext(sc)
	
	hadoopConf = sc._jsc.hadoopConfiguration()
	hadoopConf.set("fs.s3a.canned.acl", "BucketOwnerFullControl")
	hadoopConf.set("fs.s3a.awsAccessKeyId", "AKIAXCVNOCMJCZTV7IWP")
	hadoopConf.set("fs.s3a.awsSecretAccessKey", "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")
	hadoopConf.set("fs.s3a.endpoint", "s3.us-east-1.amazonaws.com")
	hadoopConf.set("com.amazonaws.services.s3a.enableV4", "true")
	hadoopConf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
	print(sc._conf.getAll())
	return sql

sql = context_spark()

def toCSVLineRDD(rdd):
    """This function is used by toCSVLine to convert an RDD into a CSV string

    """
    a = rdd.map(lambda row: ",".join([str(elt) for elt in row]))\
           .reduce(lambda x, y: "\n".join([x, y]))
    return a + "\n"


def toCSVLine(data):
    """This function convert an RDD or a DataFrame into a CSV string

    """
    if isinstance(data, RDD):
        return toCSVLineRDD(data)
    elif isinstance(data, DataFrame):
        return toCSVLineRDD(data.rdd)
    return None

	 
def prep_overview(sql):
	spark = init_spark()
	lines = sql.read.text("s3a://max01bb/trans_ovrw.dat").rdd
	#lines = spark.read.text("/Users/Max/Desktop/Bitcoin-Project/trans_ovrw.dat").rdd
	#lines = spark.read.text("./data/plants.data").rdd
	lines = lines.map(lambda row: row.value.split("\t"))
	
	lines = lines.map(lambda p: Row(txID=(p[0]),\
	blockID=(p[1]),n_inputs=(p[2]),n_outputs=(p[3])))
	
	frame = spark.createDataFrame(lines)
	frame = frame.select("txID","blockID","n_inputs","n_outputs")
	
	return frame

prep = prep_overview(sql)

def to_time(timestamp):
	
	list = []
	
	date = datetime.fromtimestamp(timestamp)
	
	string = str(date)
	
	list.append(string[0:4])
	list.append(string[5:7])
	list.append(string[8:10])
	
	return list


def prep_block(sql):
	spark = init_spark()
	lines = sql.read.text("s3a://max01bb/blocks.dat").rdd
	#lines = spark.read.text("./data/plants.data").rdd
	lines = lines.map(lambda row: row.value.split("\t"))
	
	lines = lines.map(lambda p: (p[0],p[1],p[2],to_time(int(p[2])),p[3]))
	lines = lines.map(lambda p: (p[0],p[1],p[2],p[3][0],p[3][1],p[3][2],p[4]))
	
	lines = lines.map(lambda p: Row(blockID=(p[0]),\
	hash=(p[1]),block_timestamp=(p[2]),year=(p[3]),month=(p[4]),day=(p[5]),n_txs=(p[6])))
	
	frame = spark.createDataFrame(lines)
	frame = frame.select("blockID","year","month","day")
	
	return frame

block = prep_block(sql)

def prep_inputs(sql,prep,block):
	spark = init_spark()
	lines = spark.read.text("s3a://max01bb/trans_in.dat").rdd
	#lines = spark.read.text("/Users/Max/Desktop/Bitcoin-Project/1M_trans_in.dat").rdd
	#lines = spark.read.text("./data/plants.data").rdd
	lines = lines.map(lambda row: row.value.split("\t"))
	
	lines = lines.map(lambda p: Row(txID=(p[0]),\
	input_seq=(p[1]),prev_txID=(p[2]),prev_output_seq=(p[3]),addrID=(p[4]),sum=(p[5])))
	
	frame = spark.createDataFrame(lines)
	frame = frame.select("txID","prev_txID","addrID","sum")
	
	prep = prep.join(frame,"txID","inner")
	
	prep = prep.join(block,"blockID","inner")
	
	prep = prep.select("txID","prev_txID","blockID","year","month","day","n_inputs","n_outputs","addrID","sum")
	
	return prep
    
input = prep_inputs(sql,prep,block)

def prep_outputs(prep,block):
	spark = init_spark()
	lines = spark.read.text("/Users/Max/Desktop/Bitcoin-Project/trans_out.dat").rdd
	#lines = spark.read.text("./data/plants.data").rdd
	lines = lines.map(lambda row: row.value.split("\t"))
	
	lines = lines.map(lambda p: Row(txID=(p[0]),\
	output_seq=(p[1]),addrID=(p[2]),sum=(p[3])))
	
	frame = spark.createDataFrame(lines)
	frame = frame.select("txID","output_seq","addrID","sum")
	
	frame = prep.join(frame,"txID","inner")
	
	frame = blcok.join(frame,"blockID","inner")
	
	return frame


SparkContext.setSystemProperty('spark.executor.memory','10g')
SparkContext.setSystemProperty('spark.driver.memory','10g')

date = "2016"

fi = input.filter( input.year == date)

fi.show()
fi.repartition(1).write.parquet("s3a://max01bb/input_modify_"+date)

SparkContext.stop(sc)
print(datetime.fromtimestamp(time.time()))

