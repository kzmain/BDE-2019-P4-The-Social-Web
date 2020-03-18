import os
from pyspark.sql import SparkSession
from pyspark.sql.types import ArrayType, StringType
from pyspark.sql.functions import udf, col
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


def get_support(t_ratio, h_ratio):
    return ["T", t_ratio, t_ratio, "-"] if t_ratio >= h_ratio else ["H", h_ratio, "-", h_ratio]


def get_sentiment_df(t_filename, h_filename):
    t_df = spark.read.parquet(t_filename) \
        .filter(col(COL.senti) == COL.positive) \
        .filter(col(COL.date) < DATE.election_date) \
        .select(col(COL.date), col(COL.ratio).alias(COL.t_ratio))

    h_df = spark.read.parquet(h_filename) \
        .filter(col(COL.senti) == COL.positive) \
        .filter(col(COL.date) < DATE.election_date) \
        .select(col(COL.date), col(COL.ratio).alias(COL.h_ratio))

    s_df = t_df.join(h_df, on=[COL.date], how="full")
    return s_df


# for isTop in (True, False):
for isTop in [True]:
    t_fn, t_fe = FileProcessor.in_parquet_uri(cands[1], isTop, DIR.sen_data)
    h_fn, h_fe = FileProcessor.in_parquet_uri(cands[0], isTop, DIR.sen_data)

    if (t_fe is False) or (h_fe is False):
        continue

    support_df = get_sentiment_df(t_fn, h_fn)

    if debug:
        support_df = support_df.filter(col(COL.t_ratio).isNotNull() & col(COL.h_ratio).isNotNull()).sort(COL.date)

    if limit:
        support_df = support_df.limit(100)

    support_udf = udf(get_support, ArrayType(StringType()))
    support_df = support_df.withColumn(COL.winner, support_udf(col(COL.t_ratio), col(COL.h_ratio)))

    support_df = support_df.select(col(COL.date),
                                   col(COL.winner)[0].alias(COL.winner),
                                   col(COL.winner)[1].alias(COL.ratio),
                                   col(COL.winner)[2].alias(COL.trump),
                                   col(COL.winner)[3].alias(COL.hillary),
                                   )

    support_df.sort(col(COL.date))\
        .write \
        .parquet(FileProcessor.out_parquet_uri("", isTop, DIR.sen_resu), compression="gzip", mode="overwrite")
    support_df.show()
    if debug:
        support_df.show()
