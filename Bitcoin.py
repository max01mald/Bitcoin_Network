import os
os.environ["PYSPARK_PYTHON"]="/usr/local/bin/python3"
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages com.amazonaws:aws-java-sdk-pom:1.11.83,org.apache.hadoop:hadoop-aws:2.7.3 pyspark-shell'
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages com.amazonaws:aws-java-sdk-pom:1.10.34,org.apache.hadoop:hadoop-aws-2.6.0 pyspark-shell'
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages com.amazonaws:aws-java-sdk:1.10.34,org.apache.hadoop:hadoop-aws:2.6.0 pyspark-shell'



from pyspark.sql import SparkSession
from pyspark.sql import Row
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

#os.environ["PYSPARK_PYTHON"]="/usr/local/bin/python3"

def init_spark():
    spark = SparkSession \
        .builder \
        .appName("Python Spark SQL basic example") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()
    return spark

def context_spark():
	conf = SparkConf()
	sc = SparkContext(conf=conf)
	return sc

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
    
def Year(string):
	
	return string[0:4]

def Month(string):
	
	return string[5:7]

def Day(string):
	
	return string[8:10]

def bit_to_usd_rdd():
	
	spark = init_spark()
	lines = spark.read.text("/Users/Max/Desktop/Bitcoin-Project/BTC-USD.csv").rdd
	#lines = spark.read.text("./data/plants.data").rdd
	lines = lines.map(lambda row: row.value.split(","))
	
	lines = lines.map(lambda p: Row(Year=(Year(p[0])),Month=(Month(p[0])),Day=(Day(p[0])),\
	Open=(p[1]),High=(p[2]),Low=(p[3]),Close=(p[4]),Adj_Close=(p[5]),Volume=(p[6])))
	
	frame = spark.createDataFrame(lines)
	frame = frame.select("Open","High","Low","Close","Adj_Close","Volume")
	
	return frame
	
#bit_to_usd_rdd()
	
	 
def prep_overview():
	spark = init_spark()
	'''lines = spark.read.text("/Users/Max/Desktop/Bitcoin-Project/trans_ovrw.dat").rdd
	#lines = spark.read.text("./data/plants.data").rdd
	lines = lines.map(lambda row: row.value.split("\t"))
	
	lines = lines.map(lambda p: Row(txID=(p[0]),\
	blockID=(p[1]),n_inputs=(p[2]),n_outputs=(p[3])))
	
	frame = spark.createDataFrame(lines)
	frame = frame.select("txID","blockID","n_inputs","n_outputs")
	
	frame.filter(frame.txID == "170").show()
	
	#return frame'''
	
	prep = ["150","1","3","2"]
	input = ["150","123","9","321","1111","600"]
	
	prep = spark.sparkContext.parallelize(prep)
	input = spark.sparkContext.parallelize(input)
	
	prep = prep.map(lambda p: Row(txID=(p[0]),\
	blockID=(p[1]),n_inputs=(p[2]),n_outputs=(p[3])))
	
	input = input.map(lambda p: Row(txID=(p[0]),\
	input_seq=(p[1]),prev_txID=(p[2]),prev_output_seq=(p[3]),addrID=(p[4]),sum=(p[5])))
	
	prep = spark.createDataFrame(prep)
	input = spark.createDataFrame(input)
	
	prep = prep.join(input,"inner")
	
	prep.show()
	
	'''rdd = frame.rdd.map(lambda row: (row[0],row[1],row[2],row[3])) 
	
	list = rdd.take(10)
		
	rdd2 = spark.sparkContext.parallelize(list)
	rdd2 = rdd2.map(lambda p: Row(txID=(p[0]),\
	blockID=(p[1]),n_inputs=(p[2]),n_outputs=(p[3])))
	
	frame2 = spark.createDataFrame(rdd2)
	frame2 = frame2.select("txID","blockID","n_inputs","n_outputs")
	
	return frame2'''

prep_overview()

def to_time(timestamp):
	
	list = []
	
	date = datetime.fromtimestamp(timestamp)
	
	string = str(date)
	
	list.append(string[0:4])
	list.append(string[5:7])
	list.append(string[8:10])
	
	return list



def prep_block():
	spark = init_spark()
	lines = spark.read.text("/Users/Max/Desktop/Bitcoin-Project/blocks.dat").rdd
	#lines = spark.read.text("./data/plants.data").rdd
	lines = lines.map(lambda row: row.value.split("\t"))
	
	lines = lines.map(lambda p: (p[0],p[1],p[2],to_time(int(p[2])),p[3]))
	lines = lines.map(lambda p: (p[0],p[1],p[2],p[3][0],p[3][1],p[3][2],p[4]))
	
	lines = lines.map(lambda p: Row(blockID=(p[0]),\
	hash=(p[1]),block_timestamp=(p[2]),year=(p[3]),month=(p[4]),day=(p[5]),n_txs=(p[6])))
	
	frame = spark.createDataFrame(lines)
	frame = frame.select("blockID","hash","block_timestamp","year","month","day","n_txs")
	
	return frame
	
	


def prep_inputs(prep,block):
	spark = init_spark()
	lines = spark.read.text("/Users/Max/Desktop/Bitcoin-Project/trans_in.dat").rdd
	#lines = spark.read.text("./data/plants.data").rdd
	lines = lines.map(lambda row: row.value.split("\t"))
	
	lines = lines.map(lambda p: Row(txID=(p[0]),\
	input_seq=(p[1]),prev_txID=(p[2]),prev_output_seq=(p[3]),addrID=(p[4]),sum=(p[5])))
	
	frame = spark.createDataFrame(lines)
	frame = frame.select("txID","input_seq","prev_txID","prev_output_seq","addrID","sum")
	
	frame.show()
	
	#frame = prep.join(frame,"txID","inner")
	
	#frame = block.join(frame,"blockID","inner")
	'''rdd = frame.rdd.map(lambda row: (row[0],row[1],row[2],row[3],row[4],row[5])) 
	
	list = rdd.take(10)
		
	rdd2 = spark.sparkContext.parallelize(list)
	rdd2 = rdd2.map(lambda p: Row(txID=(p[0]),\
	input_seq=(p[1]),prev_txID=(p[2]),prev_output_seq=(p[3]),addrID=(p[4]),sum=(p[5])))
	
	
	frame2 = spark.createDataFrame(rdd2)
	frame2 = frame2.select("txID","input_seq","prev_txID","prev_output_seq","addrID","sum")
	
	frame2 = prep.join(frame2,"txID","inner")
	frame2 = block.join(frame2,"blockID","inner")
	
	frame2 = frame2.select("txID","input_seq","prev_txID","prev_output_seq","addrID","sum","blockID","hash","block_timestamp","year","month","day","n_txs","n_inputs","n_outputs")
	'''
	#return frame
    
#prep_inputs("","")
  
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


def prep_transaction(input,output):
	
	transaction = input.join(output,"txID","inner")
	
	transaction.show()

'''#SparkContext.setSystemProperty('spark.executor.cores','5')
SparkContext.setSystemProperty('spark.executor.memory','10g')
SparkContext.setSystemProperty('spark.driver.memory','10g')
#SparkContext.setSystemProperty('spark.master','local[3]')
sc = SparkContext("local","App Name")
print(sc._conf.getAll())	


ovrw = prep_overview()
block = prep_block()
input = prep_inputs(ovrw,block) 
#output = prep_outputs() 

date = "2009"
input.persist()
while 1:

	fi = input.rdd.filter(lambda p: str(p[9]) == date)
	
	if int(date) > 2008:
	
		#list = fi.collect()
		string = ""
		print("Amount in " + str(date) + str(fi.count()) + "\n" )
		fi.map(lambda x: str(x[0]) + "," + str(x[1]) + "," + str(x[2]) + "," + str(x[3]) + "," + str(x[4]) + "," + str(x[5])+ "," + \
			str(x[6]) + "," + str(x[7]) + "," + str(x[8]) + "," + str(x[9]) + "," + str(x[10]) + "," + str(x[11]) + "," + str(x[12]) + "," + \
			str(x[13]) + "," + str(x[14])).coalesce(1,True).saveAsTextFile("file:///Users/Max/Desktop/Bitcoin-Project/input_modifications_"+date)
		
	else:
		if int(date) > 2018:
			break
		
		date = str( int(date) + 1)

SparkConext.stop(sc)'''
print(datetime.fromtimestamp(time.time()))

#transaction = prep_transaction(input,output)	

































    
