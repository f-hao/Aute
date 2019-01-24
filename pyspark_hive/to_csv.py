
from pyspark import SparkConf, SparkContext
from pyspark.sql import HiveContext
import sys
import pandas as pd
sc = SparkContext()

conf = (SparkConf()
         .setMaster("spark://192.168.1.117:7077")
         .setAppName("My app")
         .set("spark.executor.memory", "1g"))
sqlContext = HiveContext(sc)
my_dataframe = sqlContext.sql("SELECT * FROM top_ip") 
my_dataframe.write.csv("/home/tank/newData/Fan_Hao/Data/top_ip_new1")

sc.stop()
