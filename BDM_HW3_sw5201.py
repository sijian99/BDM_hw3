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

if __name__ == "__main__":

       sc = pyspark.SparkContext.getOrCreate()
       spark = SparkSession(sc)

       df_sample = spark.read.option("header","true").csv("/user/sw5201/keyfood_sample_items.csv")
       df = spark.read.option("header","true").csv("/tmp/bdm/keyfood_products.csv")
       df_store = spark.read.json('/user/sw5201/keyfood_nyc_stores.json')

       # Prepare
       df_sample = df_sample.withColumn('code', F.split(F.col('UPC code'), "-").getItem(1))
       df = df.drop('size', 'department')
       df = df.withColumn('code', F.split(F.col('upc'), "-").getItem(1)).drop('upc')
       df = df.withColumn('price', F.regexp_extract(F.col('price'), "\d+\.\d{2}", 0))

       # Concat foodSecurity
       insecurityDict = { x:df_store.select(x+'.foodInsecurity').first()['foodInsecurity'] for x in df_store.columns}
       callnewColsUdf = F.udf(lambda x: insecurityDict[x], T.DoubleType())
       df = df.withColumn('foodInsecurity', callnewColsUdf(F.col('store')) )

       # Filter with UPC code
       codeList = df_sample.select("code").rdd.flatMap(lambda x: x).collect()
       df = df.filter(F.col("code").isin(codeList))

       # Correct format
       df = df.drop('store','code')
       df = df.withColumn('foodInsecurity', F.col('foodInsecurity').cast('double')*100)
       df = df.withColumn('price', F.col('price').cast('double'))
       df = df.withColumnRenamed('price', 'Price ($)')\
              .withColumnRenamed('foodInsecurity', '% Food Insecurity')\
              .withColumnRenamed('product', 'Item Name')
       outputTask1 = df.select(['Item Name', 'Price ($)', '% Food Insecurity'])
       outputTask1.rdd.saveAsTextFile(sys.argv[1])
