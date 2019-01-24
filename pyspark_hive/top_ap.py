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

#my_dataframe = sqlContext.sql("SELECT int(ts_usec), ori_len, user_ip  FROM  PCAPTEST LIMIT 10000")
#my_dataframe.show()
   # sqlContext.sql("use databases default")
#Extracting non-empty valid data from iplogs 
    sqlContext.sql(" SELECT int(ts_usec), ori_len,user_ip  FROM  PCAPTEST where (int(ts_usec) is not  NULL) AND (ori_len is not NULL ) AND (user_ip <> ' ') ").createOrReplaceTempView('tmp1')
#Internal connection  the table ap-ip and  the table tmp1
   # sqlContext.sql("select * from tmp1 limit 10").show()
    sqlContext.sql("set hive.auto.convert.join=false")
   # sqlContext.sql(" SELECT * from ip_ap_new").createOrReplaceTempView('tmp2')
    #sqlContext.sql("select * from tmp2 limit 10").show()
    sqlContext.sql("select ts_usec,ori_len,ontm_userip,ontm_apname from tmp1 a join ip_ap_new b on a.user_ip=b.ontm_userip").createOrReplaceTempView('tmp3')
    #sqlContext.sql("select * from tmp3 limit 100").show()
#Statistics of transmission frequency and load per AP in seconds 
    #sqlContext.sql("drop table if exists out_ap_time")
   
    #sqlContext.sql("create table out_ap_time( ts_usec int,ap_name string,pk_num bigint,pk_size bigint) ")
    sqlContext.sql(" select ts_usec ,ontm_apname ,count(ori_len) as pk_num,sum(ori_len) as pk_size from tmp3 group by ts_usec,ontm_apname").createOrReplaceTempView('tmp4')
  # sqlContext.sql(" select ts_usec ,ontm_apname ,count(ori_len),sum(ori_len) from tmp3 group by ts_usec,ontm_apname order by ts_usec,ontm_apname desc").write.saveAsTable("out_ap_time3")
 
   # sqlContext.sql('insert overwrite table out_ap_time select * from tmp4')
   # sqlContext.sql("select * from out_ap_time limit 10 ").show()
    
    
    #df=sqlContext.sql("select * from tmp4 order by ts_usec,ontm_apname desc").repartition(1)
    df=sqlContext.sql("select * from tmp4 ").repartition(1)
    df.write.saveAsTable('out_ap_time_new1',mode='overwrite')
    sqlContext.sql("select * from out_ap_time_new1 limit 10 ").show()
    
   # filterDF=sqlContext.sql("SELECT * FROM out_ap_time  limit 100")
   # filterDF.show()
    sc.stop()
