import os
import sys
from pyspark.rdd import RDD
from pyspark.sql import Row, SQLContext
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from pyspark.sql.functions import desc
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.recommendation import ALS
from pyspark.sql.functions import mean, min, max
from pyspark import SparkContext, SparkConf

import random

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

def basic_recommender():
    '''
    This function must print the RMSE of recommendations obtained
    through ALS collaborative filtering, similarly to the example at
    http://spark.apache.org/docs/latest/ml-collaborative-filtering.html
    The training ratio must be 80% and the test ratio must be 20%. The
    random seed used to sample the training and test sets (passed to
    ''DataFrame.randomSplit') is an argument of the function. The seed
    must also be used to initialize the ALS optimizer (use
    *ALS.setSeed()*). The following parameters must be used in the ALS
    optimizer:
    - maxIter: 5
    - rank: 70
    - regParam: 0.01
    - coldStartStrategy: 'drop'
    Test file: tests/test_basic_als.py
    '''
    

SparkContext.setSystemProperty('spark.executor.memory','10g')
SparkContext.setSystemProperty('spark.driver.memory','10g')

sql = context_spark()
spark = init_spark()

date = "2017"

input = sql.read.parquet("s3a://max01bb/K_"+date)
frame = input.select("txID","blockID","year","month","day","Iaddr","Oaddr","Sum","K","Price",)

K = frame.select("K",).rdd.map(lambda p: (p[0])).distinct().collect()

Num = []
Sum = []

i=0
while i < len(K):
	sl = frame.filter(frame.K == str(K[i]))
	Num.append(sl.count())
	Sum.append(sl.rdd.map(lambda p: (p[7])).reduce(lambda a,b: float(a) + float(b)))	
	i+=1


print(K)
print(Num)
print(Sum)





