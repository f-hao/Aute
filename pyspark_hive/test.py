
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import HiveContext
import sys
if __name__== "__main__":
    conf = (SparkConf()
         .setMaster("spark://192.168.1.117:7077")
         .setAppName("My app")
         .set("spark.executor.memory", "1g"))
    sc = SparkContext(conf=conf)
    sc.setLogLevel("WARN")
    sqlContext = HiveContext(sc)
    #in_tname='iplogs'
    sqlContext.sql(" select ontm_userip,ontm_apname from iplogs where(ontm_userip <> ' ') and (ontm_apname <> ' ') ").createOrReplaceTempView('tmp1')
    sqlContext.sql("select ontm_userip,ontm_apname,count(*) as num from tmp1 group by ontm_userip,ontm_apname ").createOrReplaceTempView('tmp2')   
   # sqlContext.sql("select * from tmp2 limit 10 ").show()
   # sqlContext.sql("create table ip_ap_out_a as select a.* from ( select ontm_userip,ontm_apname,num,row_number() over (partition by ontm_userip order by num desc) rank from tmp2 order by ontm_userip,num desc )a where a.rank<=1 ") 
   #sql.Context.sql("set hive.auto.convert.join=false ")     
    sqlContext.sql("select ontm_userip,ontm_apname,num,row_number() over (partition by ontm_userip order by num desc) rank from tmp2 order by ontm_userip,num desc  ").createOrReplaceTempView('tmp3')
    #sqlContext.sql(" create  table ip_ap_out_r (ontm_userip string, ontm_apname string , num bigint,rank bigint) ")   
    df=sqlContext.sql("select * from tmp3 ")    
    df.write.saveAsTable('testtable',mode='overwrite')
    
    #sqlContext.sql("select * from tmp3 limit 10 ").show()
    # sqlContext.sql("desc tmp3  ").show()
    #out_tname='ip_ap_out_e'
    #sqlContext.sql(" create  table ip_ap_out_q (ontm_userip string, ontm_apname string , num bigint) ")           
    #sqlContext.sql(" insert  into table ip_ap_out_q select ontm_userip, ontm_apname, num from tmp3 a where a.rank = 1 ")           
    #sqlContext.sql("create table ip_ap_type_p as select a.* from ( select ontm_userip,ontm_apname,num,row_number() over (partition by ontm_userip order by num desc) rank from tmp2 order by ontm_userip,num desc )a where a.rank=1 ")

    #sqlContext.sql(" select ip_ap_out_k from %s limit 50 ").show()
  #  sqlContext.sql("desc from ip_ap_out_d  ").show()
    

    sc.stop()
  
