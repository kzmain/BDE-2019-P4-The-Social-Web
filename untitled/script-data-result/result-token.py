import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, ArrayType
from pyspark.sql.functions import col, lit, udf
from SYS.MODE import debug_mode as debug
from SYS.MODE import limit_mode as limit
from SYS.CANDIDATES import candidates_result as cands
from SYS.DATA_RESULT.FileProcessor import FileProcessor
import SYS.URI as DIR
import SYS.COL as COL

java8_location = '/Library/Java/JavaVirtualMachines/liberica-jdk-1.8.0_202/Contents/Home'  # Set your own
os.environ['JAVA_HOME'] = java8_location
spark = SparkSession\
    .builder\
    .config("spark.master", "local[*]") \
    .config("spark.executor.memory", "4g")\
    .appName("A1")\
    .getOrCreate()


def update_token(token_txt):
    if 'Hillary' in token_txt \
            or 'hillary' in token_txt \
            or 'HILLARY' in token_txt \
            or 'Clinton' in token_txt \
            or 'Trump' in token_txt \
            or 'Donald' in token_txt \
            or 'don' in token_txt\
            or 'tonight' in token_txt \
            or 'day' in token_txt \
            or 'let' in token_txt \
            or 'get' in token_txt \
            or 'go' in token_txt \
            or 'want' in token_txt \
            or 'think' in token_txt \
            or 'need' in token_txt \
            or 'like' in token_txt:
        return
    if token_txt in ['look', 'come', 'talk', 'run', 'night', 'said', 'found','com', 'year', 'thing', 'word']:
        return
    if 'President' in token_txt:
        return 'president'
    if 'won' in token_txt:
        return 'win'
    if 'Debates2016' in token_txt:
        return 'debate'
    if 'Debate' in token_txt:
        return token_txt.lower()
    if 'supporters' in token_txt:
        return 'supporter'
    if 'Americans' in token_txt:
        return
    if 'voters' in token_txt:
        return 'voter'
    if 'StrongerTogether' in token_txt:
        return 'MAGA'
    return token_txt


for isTop in (True, False):
    for candidate in cands:
        fn, fe = FileProcessor.in_parquet_uri(candidate, isTop, DIR.tok_data)

        if fe is False:
            continue

        df = spark.read.parquet(fn)

        udf_token = udf(update_token, StringType())
        df = df.withColumn(COL.token, udf_token(col(COL.token)))

        if debug:
            df = df.filter(col(COL.token).isNotNull())

        df = df.filter(col(COL.token).isNotNull())
        # df = df.groupby(col(COL.date), col(COL.token)).sum().withColumnRenamed(COL.sum_count, COL.count)
        df = df.groupby(col(COL.token)).sum().withColumnRenamed(COL.sum_count, COL.count)
        df = df.filter(col(COL.count) > lit(5))

        df = df.sort(col(COL.count), ascending=False)

        if limit:
            df = df.limit(100)

        if debug:
            df.show()
        df.show(1000)
        # df.write.parquet(FileProcessor.out_parquet_uri(candidate, isTop, DIR.tok_resu), compression="gzip", mode="overwrite")
