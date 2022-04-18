import csv
import json
import numpy as np
import matplotlib.pyplot as plt

import pandas as pd
pd.plotting.register_matplotlib_converters()

import sys
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.types import IntegerType

if __name__ == "__main__":

       #load data of three files 
       sc = pyspark.SparkContext.getOrCreate()
       spark = SparkSession(sc)
       df_security = spark.read.json('/user/sw5201/keyfood_nyc_stores.json')
       sample_df = spark.read.option("header","true").csv("/user/sw5201/keyfood_sample_items.csv")
       df = spark.read.option("header","true").csv("/tmp/bdm/keyfood_products.csv")
       security_Dict = { x:sample_df.select(x+'.foodInsecurity').first()['foodInsecurity'] for x in sample_df.columns}     

       #get the digits after '-' in UPC code in two file
       sample_df = sample_df.withColumn('code', F.split(F.col('UPC code'), "-").getItem(1))
       df = df.drop('size', 'department')
       df = df.withColumn('code', F.split(F.col('upc'), "-").getItem(1)).drop('upc')
       df = df.withColumn('price', F.regexp_extract(F.col('price'), "\d+\.\d{2}", 0))
       UDF_double = F.udf(lambda x: security_Dict[x], T.DoubleType())
       df = df.withColumn('foodInsecurity', UDF_double(F.col('store')) ) #join the security data to df

       #filter by UPC
       list_UPCcode = df_sample.select("code").rdd.flatMap(lambda x: x).collect()
       df = df.filter(F.col("code").isin(security_Dict))
       df = df.drop('store','code') 
       df = df.withColumn('foodInsecurity', F.col('foodInsecurity').cast('double')*100)
       df = df.withColumn('price', F.col('price').cast('double'))
       #rename
       df = df.withColumnRenamed('price', 'Price ($)').withColumnRenamed('foodInsecurity', '% Food Insecurity').withColumnRenamed('product', 'Item Name')
       outputTask1 = df.select(['Item Name', 'Price ($)', '% Food Insecurity'])
       outputTask1.rdd.saveAsTextFile(sys.argv[1])
