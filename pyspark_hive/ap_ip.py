#--coding=utf-8--
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import HiveContext
import sys
sc = SparkContext()
sc.setLogLevel("WARN")
if __name__== "__main__":
    conf = (SparkConf()
         .setMaster("spark://192.168.1.117:7077")
         .setAppName("ip_ap")
         .set("spark.executor.memory", "10g"))
    sqlContext = HiveContext(sc)
    
    sqlContext.sql("set hive.merge.mapredfiles= true")
    sqlContext.sql( "set hive.merge.mapfiles = true")
#Extracting non-empty valid data from iplogs 
    sqlContext.sql(" select ontm_userip,ontm_apname from iplogs where(ontm_userip <> ' ') and (ontm_apname <> ' ') ").createOrReplaceTempView('tmp1')
    #sqlContext.sql("select * from tmp1 limit 10 ").show()

#Count the number of times an IP corresponds to each AP 
#(each IP may correspond to multiple aps) 
    sqlContext.sql("select ontm_userip,ontm_apname,count(*) as num from tmp1 group by ontm_userip,ontm_apname ").createOrReplaceTempView('tmp2')   
     #sqlContext.sql("select * from tmp2 limit 10 ").show()

#Select the AP with the highest number of occurrences for each IP 
    #sqlContext.sql("select ontm_userip,ontm_apname,num,row_number() over (partition by ontm_userip order by num desc) rank from tmp2 order by ontm_userip,num desc  ").createOrReplaceTempView('tmp3')
    sqlContext.sql("set hive.merge.mapredfiles= true")
    sqlContext.sql("select ontm_userip,ontm_apname,num,row_number() over (partition by ontm_userip order by num desc) rank from tmp2 order by ontm_userip,num desc  ").createOrReplaceTempView('tmp3')
    sqlContext.sql("select ontm_userip,ontm_apname,num from tmp3 a where a.rank = 1 ").createOrReplaceTempView('tmp4')   

    #sqlContext.sql(" select a.* from ( select ontm_userip,ontm_apname,num,row_number() over (partition by ontm_userip order by num desc) rank from tmp2 order by ontm_userip,num desc )a where a.rank<=1 ").createOrReplaceTempView('tmp3')   
    df=sqlContext.sql("select * from tmp4 ").repartition(1)    
    df.write.saveAsTable('ip_ap_new',mode='overwrite')
    #sqlContext.sql("drop table if exists ip_ap3")
   # sqlContext.sql(" create  table ip_ap3 (ontm_userip string,ontm_apname string,num bigint) ") 
   # sqlContext.sql('insert overwrite table ip_ap3 select * from tmp4')
    sqlContext.sql("select * from ip_ap_new limit 10 ").show()

    sc.stop()
