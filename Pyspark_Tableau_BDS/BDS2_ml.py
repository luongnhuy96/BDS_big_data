import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.ml import Pipeline
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.feature import MinMaxScaler
from pyspark.ml.regression import DecisionTreeRegressor
from pyspark.ml.classification import DecisionTreeClassifier
import numpy as np

from pyspark.sql.functions import col, expr
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.ml.feature import StringIndexer
from pyspark.sql.functions import concat_ws


#Create spark session


#Create spark session


spark = SparkSession\
    .builder\
    .master('local[2]')\
    .appName('BDS_ml')\
    .config('spark.jars.packages','org.mongodb.spark:mongo-spark-connector_2.12:2.4.1')\
    .getOrCreate()

final_data=spark.read.format('mongo')\
    .option('spark.mongodb.input.uri','mongodb://127.0.0.1/BDS.bds').load()


# Select the "price_per_m2" column and divide it by 10^9
final_data = final_data.withColumn("price_per_m2", expr("price_per_m2 / 1000"))
#final_data = final_data.withColumn("date", F.to_date("date", "M/dd/YYYY"))
final_data = final_data.withColumn('month', month(col('date')))



# Create StringIndexer for 'district'
district_indexer = StringIndexer(inputCol="district", outputCol="districtIndex")

# Create StringIndexer for 'ward'
ward_indexer = StringIndexer(inputCol="ward", outputCol="wardIndex")

# Create StringIndexer for 'ward'
street_indexer = StringIndexer(inputCol="street", outputCol="streetIndex")

# Create a list of indexers
indexers = [district_indexer, ward_indexer,street_indexer]

# Use a Pipeline to process both indexers
pipeline = Pipeline(stages=indexers)


#final_data_indexer = StringIndexer(inputCol="district", outputCol="categoryIndex") 
final_data_indexed = pipeline.fit(final_data).transform(final_data) 

# Show the updated DataFrame
final_data_indexed.show()
'''
Data Processing
'''

df_train, df_test = final_data_indexed.randomSplit([0.99,0.01], seed=101)



df_test_clean=df_test['districtIndex','wardIndex','streetIndex','Bed_num', 'Bath_num', 'price_per_m2', 'length', 'width',
       'gross_floor_area(m2)', 'tag_atm_vcb', 'tag_branch_vcb', 'tag_atm_acb', 'tag_branch_acb']

df_test_clean=df_test_clean.withColumn('Bed_num',df_test_clean['Bed_num'].cast(DoubleType()))\
    .withColumn('Bath_num',df_test_clean['Bath_num'].cast(DoubleType()))\
    .withColumn('districtIndex',df_test_clean['districtIndex'].cast(DoubleType()))\
    .withColumn('wardIndex',df_test_clean['wardIndex'].cast(DoubleType()))\
    .withColumn('streetIndex',df_test_clean['streetIndex'].cast(DoubleType()))\
    .withColumn('price_per_m2',df_test_clean['price_per_m2'].cast(DoubleType()))\
    .withColumn('length',df_test_clean['length'].cast(DoubleType()))\
    .withColumn('width',df_test_clean['width'].cast(DoubleType()))\
    .withColumn('gross_floor_area(m2)',df_test_clean['gross_floor_area(m2)'].cast(DoubleType()))\
    .withColumn('tag_atm_vcb',df_test_clean['tag_atm_vcb'].cast(DoubleType()))\
    .withColumn('tag_branch_vcb',df_test_clean['tag_branch_vcb'].cast(DoubleType()))\
    .withColumn('tag_atm_acb',df_test_clean['tag_atm_acb'].cast(DoubleType()))\
    .withColumn('tag_branch_acb',df_test_clean['tag_branch_acb'].cast(DoubleType()))


df_train_clean=df_train['districtIndex','wardIndex','streetIndex','Bed_num', 'Bath_num', 'price_per_m2', 'length', 'width',
       'gross_floor_area(m2)', 'tag_atm_vcb', 'tag_branch_vcb', 'tag_atm_acb', 'tag_branch_acb']

df_train_clean=df_train_clean.withColumn('Bed_num',df_train_clean['Bed_num'].cast(DoubleType()))\
    .withColumn('Bath_num',df_train_clean['Bath_num'].cast(DoubleType()))\
    .withColumn('districtIndex',df_train_clean['districtIndex'].cast(DoubleType()))\
    .withColumn('wardIndex',df_train_clean['wardIndex'].cast(DoubleType()))\
    .withColumn('streetIndex',df_train_clean['streetIndex'].cast(DoubleType()))\
    .withColumn('price_per_m2',df_train_clean['price_per_m2'].cast(DoubleType()))\
    .withColumn('length',df_train_clean['length'].cast(DoubleType()))\
    .withColumn('width',df_train_clean['width'].cast(DoubleType()))\
    .withColumn('gross_floor_area(m2)',df_train_clean['gross_floor_area(m2)'].cast(DoubleType()))\
    .withColumn('tag_atm_vcb',df_train_clean['tag_atm_vcb'].cast(DoubleType()))\
    .withColumn('tag_branch_vcb',df_train_clean['tag_branch_vcb'].cast(DoubleType()))\
    .withColumn('tag_atm_acb',df_train_clean['tag_atm_acb'].cast(DoubleType()))\
    .withColumn('tag_branch_acb',df_train_clean['tag_branch_acb'].cast(DoubleType()))


df_testing=df_test_clean['districtIndex','wardIndex','streetIndex','Bed_num', 'Bath_num','price_per_m2', 'length', 'width',
       'gross_floor_area(m2)', 'tag_atm_vcb', 'tag_branch_vcb', 'tag_atm_acb', 'tag_branch_acb']

df_training=df_train_clean['districtIndex','wardIndex','streetIndex','Bed_num', 'Bath_num','price_per_m2', 'length', 'width',
       'gross_floor_area(m2)', 'tag_atm_vcb', 'tag_branch_vcb', 'tag_atm_acb', 'tag_branch_acb']


#remove nan
df_training=df_training.dropna()
df_testing=df_testing.dropna()

#Create Feature Vector
assembler =VectorAssembler(inputCols=['districtIndex','wardIndex','streetIndex','Bed_num', 'Bath_num',  'length', 'width',
        'gross_floor_area(m2)', 'tag_atm_vcb', 'tag_branch_vcb','tag_atm_acb', 'tag_branch_acb'], outputCol='features')

scaler = MinMaxScaler(inputCol="features", outputCol="scaledFeatures")

#Create the model
model_reg = RandomForestRegressor(featuresCol='scaledFeatures', labelCol='price_per_m2')

pipeline = Pipeline(stages=[assembler,scaler, model_reg])
#train the model
#model=pipeline.fit(df_training)
pipiline_model = pipeline.fit(df_training)
#train_transformed = pipiline_model.transform(df_training)
#make the prediction
pred_results = pipiline_model.transform(df_testing)



#Evaluate model
evaluator = RegressionEvaluator(labelCol='price_per_m2',predictionCol='prediction',metricName='rmse')
rmse = evaluator.evaluate(pred_results)


# MAE
mae_evaluator = RegressionEvaluator(labelCol='price_per_m2', predictionCol='prediction', metricName='mae')
mae = mae_evaluator.evaluate(pred_results)

# R-squared (R2)
r2_evaluator = RegressionEvaluator(labelCol='price_per_m2', predictionCol='prediction', metricName='r2')
r2 = r2_evaluator.evaluate(pred_results)

print("RMSE:", rmse)
print("MAE:", mae)
print("R2:", r2)


'''
Create the prediction result
'''
df_pred_results=pred_results['districtIndex','wardIndex','streetIndex','Bed_num', 'Bath_num',  'length', 'width',
        'gross_floor_area(m2)', 'tag_atm_vcb', 'tag_branch_vcb', 'tag_atm_acb', 'tag_branch_acb','price_per_m2','prediction']

#Rename the prediction field
df_pred_results= df_pred_results.withColumnRenamed('prediction','Pred_Price')

#add more column
df_pred_results=df_pred_results.withColumn('RMSE',lit(rmse))
df_pred_results=df_pred_results.withColumn('mae',lit(mae))
df_pred_results=df_pred_results.withColumn('r2',lit(r2))


#Load the dataset into mongodb
df_pred_results.write.format('mongo')\
    .mode('overwrite')\
    .option('spark.mongodb.output.uri','mongodb://127.0.0.1/BDS.pred_results').save()
print(df_pred_results.show(5))

print('Info: job ran successfully')