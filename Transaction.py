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
	hadoopConf.set("fs.s3a.awsAccessKeyId", "AKIAXCVNOCMJCZTV7IWP")
	hadoopConf.set("fs.s3a.awsSecretAccessKey", "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")
	hadoopConf.set("fs.s3a.endpoint", "s3.us-east-1.amazonaws.com")
	hadoopConf.set("com.amazonaws.services.s3a.enableV4", "true")
	hadoopConf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
	print(sc._conf.getAll())
	return sql

def mk_list(list):
	i = 0
	j = 0
	
	internal = []
	external = []
	#print(list)
	
	while i < len(list):
		
		if j < 1:
			internal.append(list[i])
			j += 1
		else:
			internal.append(list[i])
			external.append(internal)
			internal = []
			j=0
			
		
		i += 1
		
	return external

def mk_sum(input,output):
	i = 0
	
	ver = 0
	sum = 0
	
	Iaddr = []
	Oaddr = []
	Excl = []
	
	while i < len(input):
		j = 0
		while j < len(output):	
			if  input[i][0] == output[j][0]:
				sum += int(input[i][1]) - int(output[j][1])
				Excl.append(str(output[j][0]))
			j += 1
		i += 1
	
	i = 0
	max = 0
	hold = 0
	
	while i < len(input):
		if str(input[i][0]) not in Excl:
			sum += int(input[i][1])
		
		if int(input[i][1]) > max:
			max = int(input[i][1])
			hold = i
		
		i += 1
	
	i = 0
	
	while i < len(output):
		if str(output[i][0]) not in Excl:
			Oaddr.append(str(output[i][0]))
			ver += int(output[i][1])
		i += 1
	
	
	return [str(input[hold][0]),",".join(Oaddr),sum]

def Year(string):
	
	return string[0:4]

def Month(string):
	
	return string[5:7]

def Day(string):
	
	return string[8:10]

def	conv_book():
	spark = init_spark()
	lines = spark.read.text("s3a://max01bb/book.csv").rdd
	#lines = spark.read.text("/Users/Max/Desktop/Bitcoin-Project/BTC-USD.csv").rdd
	#lines = spark.read.text("./data/plants.data").rdd
	lines = lines.map(lambda row: row.value.split(","))
	lines = lines.map(lambda p: ((p[0]),(p[5])))
	book = lines.collect()
	
	#lines = lines.map(lambda p: Row(Year=(Year((p[0]))),Month=(Month((p[0]))),Day=(Day((p[0]))),\
	#Open=(p[1]),High=(p[2]),Low=(p[3]),Close=(p[4]),Adj_Close=(p[5]),Volume=(p[6])))
	
	return book
	
def convert(year,month,day,num,book):
	
	srch = year+"-"+month+"-"+day
	if int(year) >= 2010 and int(month) > 7:	
		i = 0
		t = 0
		while book[i][0] != srch:
			i+=1
			if i == len(book):
				t = 1
				break
		
		if t == 1:
			i = 0
			while book[i][0][0:4] != year:
				i+=1
				if i == len(book):
					i -= 1
					break
		exc = str(book[i][1])
	else:
		exc = "0.049510"
	return int(num/100000000) * float(exc)
	
def price(year,month,day,book):
	
	srch = year+"-"+month+"-"+day
	if int(year) >= 2010 and int(month) > 7:	
		i = 0
		t = 0
		while book[i][0] != srch:
			i+=1
			if i == len(book):
				t = 1
				break
		
		if t == 1:
			i = 0
			while book[i][0][0:4] != year:
				i+=1
				if i == len(book):
					i -= 1
					break
		exc = str(book[i][1])
	else:
		exc = "0.049510"
	return float(exc)
	
	
	
SparkContext.setSystemProperty('spark.executor.memory','10g')
SparkContext.setSystemProperty('spark.driver.memory','10g')

date = "2015"
sql = context_spark()
spark = init_spark()


input = sql.read.parquet("s3a://max01bb/input_modify_"+date)
input = input.rdd.map(lambda p: ((p[0],p[2],p[3],p[4],p[5]),(p[8],p[9])))
#txID|prev_txID|blockID|year|month|day|n_inputs|n_outputs| addrID|        sum

input = input.reduceByKey(lambda a,b: a + b)
input = input.map(lambda p: ((p[0][0]),(p[0][1],p[0][2],p[0][3],p[0][4],mk_list(p[1]))))
#print(input.take(10))


output = sql.read.parquet("s3a://max01bb/output_modify_"+date)

output = output.rdd.map(lambda p: ((p[0]),(p[3],p[4])))
output = output.reduceByKey(lambda a,b : a + b)
output = output.map(lambda p: ((p[0]),mk_list((p[1]))))
#txID (Oaddr , Sum)
#print(output.take(10))

trans = input.map(lambda p: ((p[0],p[1]))).join(output.map(lambda p: (p[0],p[1])))
#print(trans.take(10))

trans = trans.map(lambda p: ((p[0],p[1][0][0],p[1][0][1],p[1][0][2],p[1][0][3]),(p[1][0][4],p[1][1])))
trans = trans.map(lambda p: (p[0],mk_sum(p[1][0],p[1][1])))


book = conv_book()

trans = trans.map(lambda p: Row(txID=(p[0][0]),blockID=(p[0][1]),year=(p[0][2]),month=(p[0][3]),\
day=(p[0][4]),Iaddr=(p[1][0]),Oaddr=(p[1][1]),\
Sum=(convert(p[0][2],p[0][3],p[0][4],p[1][2],book)),Price=(price(p[0][2],p[0][3],p[0][4],book))))

frame = spark.createDataFrame(trans)

frame = frame.select("txID","blockID","year","month","day","Iaddr","Oaddr","Sum","Price",)

frame.show()

frame.repartition(1).write.parquet("s3a://max01bb/transaction_modify_"+date)

print(datetime.fromtimestamp(time.time()))
