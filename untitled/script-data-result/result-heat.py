import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from SYS.MODE import debug_mode as debug
from SYS.MODE import limit_mode as limit
from SYS.CANDIDATES import candidates_result as cands
from SYS.DATA_RESULT.FileProcessor import FileProcessor
import SYS.URI as DIR
import SYS.COL as COL
import SYS.DATE as DATE

java8_location = '/Library/Java/JavaVirtualMachines/liberica-jdk-1.8.0_202/Contents/Home'  # Set your own
os.environ['JAVA_HOME'] = java8_location
spark = SparkSession\
    .builder\
    .config("spark.master", "local[*]") \
    .config("spark.executor.memory", "4g")\
    .appName("A1")\
    .getOrCreate()


for isTop in (True, False):
    for candidate in cands:
        fn, fe = FileProcessor.in_parquet_uri(candidate, isTop, DIR.sen_data)

        if fe is False:
            continue

        df = spark.read.parquet(fn) \
            .filter(col(COL.date) < DATE.election_date) \
            .select(col(COL.date), col(COL.count))

        if debug:
            df = df.filter(col(COL.count).isNotNull())

        df = df.groupby(col(COL.date)).sum(COL.count).withColumnRenamed(COL.sum_count, COL.sum)

        if limit:
            df = df.limit(100)
            df.show()

        df.write.parquet(FileProcessor.out_parquet_uri(candidate, isTop, DIR.hea_resu), compression="gzip", mode="overwrite")
        df.show()
#
# spark.read.parquet("../data/analysis-sentiment/True-Donald-Trump.parquet.gzip").sort("ratio", ascending=False)\
#     .filter(col(COL.date) < datetime(2016, 11, 9))\
#     .filter(col(COL.senti) == "positive").show(30)
# spark.read.parquet("../data/analysis-sentiment/True-Donald-Trump.parquet.gzip").sort("ratio", ascending=True )\
#     .filter(col(COL.date) < datetime(2016, 11, 9))\
#     .filter(col(COL.senti) == "positive").show(30)
#
# spark.read.parquet("../data/analysis-sentiment/True-Hillary.parquet.gzip").sort("ratio", ascending=False)\
#     .filter(col(COL.date) < datetime(2016, 11, 9))\
#     .filter(col(COL.senti) == "positive").show(30)
# spark.read.parquet("../data/analysis-sentiment/True-Hillary.parquet.gzip").sort("ratio", ascending=True )\
#     .filter(col(COL.date) < datetime(2016, 11, 9))\
#     .filter(col(COL.senti) == "positive").show(30)
# # spark.read.parquet("../data/analysis-ner/True-Donald Trump.parquet.gzip")\
# #     .groupby(col("ner"))\
# #     .sum("count")\
# #     .sort("sum(count)", ascending=False)\
# #     .filter((~col("ner").contains("donald")) &
# #             (~col("ner").contains("trump")) &
# #             (~col("ner").contains("don")) &
# #             (~col("ner").contains("hillary")) &
# #             (~col("ner").contains("clinton")) &
# #             (~col("ner").contains("americ")) &
# #             (col("ner") != "2") &
# #             (col("ner") != "one") &
# #             (col("ner") != "today")
# #             )\
# #     .show(50)
