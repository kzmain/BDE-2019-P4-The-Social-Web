import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when
from SYS.CANDIDATES import candidates_analysis as candidates
import SYS.COL as COL
from scipy import stats

java8_location = '/Library/Java/JavaVirtualMachines/liberica-jdk-1.8.0_202/Contents/Home'  # Set your own
os.environ['JAVA_HOME'] = java8_location
spark = SparkSession\
    .builder\
    .appName("A1")\
    .getOrCreate()


# Prediction Compare
normal_df = spark.read.parquet("../data/result-sentiment/False.parquet.gzip")\
    .withColumn(COL.winner, when(col(COL.winner) == "T", lit(1)).otherwise(0))

top_df = spark.read.parquet("../data/result-sentiment/True.parquet.gzip")\
    .withColumn(COL.winner, when(col(COL.winner) == "T", lit(1)).otherwise(0))

normal_list = [row["winner"] for row in normal_df.select("winner").collect()]
top_list    = [row["winner"] for row in top_df   .select("winner").collect()]
t1, p1 = stats.ttest_ind(normal_list, top_list)
print("Prediction t = " + str(t1))
print("Prediction p = " + str(p1))

# Candidate sentiment analysis

for candidate in candidates:
    false_df = spark.read.parquet("../data/analysis-sentiment/False-{}.parquet.gzip".format(candidate[0]))\
        .filter(col(COL.senti) == "positive")\
        .sort(col(COL.date))\
        .select(col(COL.date), col(COL.ratio).alias("false"))
    true_df = spark.read.parquet("../data/analysis-sentiment/True-{}.parquet.gzip".format(candidate[0])) \
        .filter(col(COL.senti) == "positive") \
        .sort(col(COL.date)) \
        .select(col(COL.date), col(COL.ratio).alias("true"))
    df = false_df.join(true_df, on=COL.date, how="outer")
    df = df.withColumn("false", when(col("false").isNull(), col("true")).otherwise(col("false")))
    false_list = [row["false"] for row in df.select("false").collect()]
    true_list = [row["true"] for row in df.select("true").collect()]
    t2, p2 = stats.ttest_ind(false_list, true_list)
    print("{} sentiment analysis t = {}".format(candidate[0], t2))
    print("{} sentiment analysis p = {}".format(candidate[0], p2))

