#--encoding=utf-8--
from pyspark import SparkConf, SparkContext
from pyspark.sql import HiveContext
import sys


if __name__== "__main__":
    conf = (SparkConf()
         .setMaster("spark://192.168.1.117:7077")
         .setAppName("out_ap_time")
         .set("spark.executor.memory", "1g"))
    sc = SparkContext(conf=conf)
    sc.setLogLevel("WARN")
    sqlContext = HiveContext(sc)

   # sqlContext.sql("use databases default")
#Extracting non-empty valid data from iplogs 
    sqlContext.sql(" SELECT int(ts_usec), ori_len,user_ip  FROM  PCAPTEST where (int(ts_usec) is not  NULL) AND (ori_len is not NULL ) AND (user_ip <> ' ') ").createOrReplaceTempView('tmp1')
    sqlContext.sql("select user_ip,sum(ori_len) as sum_len from tmp1 group by user_ip ").repartition(1).createOrReplaceTempView('tmp2')
   # sqlContext.sql("select * from tmp2 limit 10 ").show()
   # sqlContext.sql("select * from tmp2 order by sum_len ").repartition(1).createOrReplaceTempView('tmp3')
   # sqlContext.sql("select * from tmp3 order by sum_len ").createOrReplaceTempView('tmp4')
   # sqlContext.sql("select * from tmp4 limit 10 ").show() 
#1 null
   # sqlContext.sql(" create  table top_ip5 (ontm_userip string,sum_len bigint) ") 
   # sqlContext.sql(" insert overwrite table top_ip5 select * from tmp2 b order by b.sum_len desc")
#2  not order 
    df=sqlContext.sql("select * from tmp2 order by sum_len desc").repartition(1)
    df.write.saveAsTable('top_ip_info',mode='overwrite')
#3 error 
    # sqlContext.sql(" create table top_ip4 as select * from  tmp2 order by sum_len desc")
   # sqlContext.sql("select * from top_ip4 limit 10 ").show()
  
    sc.stop()
