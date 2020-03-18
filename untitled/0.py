import os
from pyspark.sql import SparkSession

java8_location = '/Library/Java/JavaVirtualMachines/liberica-jdk-1.8.0_202/Contents/Home'  # Set your own
os.environ['JAVA_HOME'] = java8_location
spark = SparkSession\
    .builder\
    .appName("A1")\
    .getOrCreate()

a = spark.sparkContext.getConf().getAll()
print(a)