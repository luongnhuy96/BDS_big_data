import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

spark = SparkSession\
    .builder\
    .master('local[2]')\
    .appName('BDS_etl')\
    .config('spark.jars.packages','org.mongodb.spark:mongo-spark-connector_2.12:2.4.1')\
    .getOrCreate()

#Load the dataset
df_load=spark.read.csv(r"D:\Xu_ly_du_lieu_lon\data_bds_clean_2023_new_19_atm_Khongdau_new.csv", header=True)
# Read the Excel file using pandas

#df_load = df_load.withColumn('date', to_timestamp(col('date'), 'yyyy-MM-dd'))
df_load = df_load.withColumn('date', to_timestamp(col('date'), 'M/d/yyyy').cast(TimestampType()))
df_load = df_load.withColumn('price_per_m2', col('price_per_m2').cast(IntegerType()))


# build table into mongodb
df_load.write.format('mongo')\
    .mode('overwrite')\
    .option('spark.mongodb.output.uri','mongodb://127.0.0.1/BDS.bds').save()