import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, ArrayType
from pyspark.sql.functions import udf, col, lit
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


def update_tag(tag_txt):
    if 'debate' in tag_txt:
        return '#debate'
    if 'trump2016' in tag_txt:
        return '#trump'
    if 'election2016' in tag_txt:
        return '#election'
    if 'podestaemails' in tag_txt:
        return "#podestaemails"
    if 'hereiamwithher' in tag_txt:
        return "#imwithher"
    return tag_txt


# for isTop in (True, False):
for isTop in [True]:
    for candidate in cands:
        fn, fe = FileProcessor.in_parquet_uri(candidate, isTop, DIR.tag_data)

        if fe is False:
            continue

        df = spark.read.parquet(fn)

        udf_tag = udf(update_tag, StringType())
        df = df.withColumn(COL.tag, udf_tag(col(COL.tag)))

        if debug:
            df = df.filter(col(COL.count).isNotNull())
        df = df.groupby(col(COL.date), col(COL.tag)).sum().withColumnRenamed(COL.sum_count, COL.count)
        df = df.filter(col(COL.count) > lit(5))
        df = df.sort(col(COL.count), ascending=False)

        if limit:
            df = df.limit(100)

        if debug:
            df.show()
        df.write.parquet(FileProcessor.out_parquet_uri(candidate, isTop, DIR.tag_resu), compression="gzip", mode="overwrite")
