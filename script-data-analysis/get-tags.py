import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, ArrayType
from pyspark.sql.functions import udf, col, explode
from SYS.CANDIDATES import candidates_analysis as candidates
from SYS.DATA_ANALYSIS.FileProcessor import FileProcessor
from SYS.MODE import debug_mode as debug
from SYS.MODE import limit_mode as limit
from SYS.URI import tag_data
import SYS.COL as COL

java8_location = '/Library/Java/JavaVirtualMachines/liberica-jdk-1.8.0_202/Contents/Home'  # Set your own
os.environ['JAVA_HOME'] = java8_location
spark = SparkSession \
    .builder \
    .config("spark.master", "local[*]") \
    .config("spark.executor.memory", "2g") \
    .appName("A1") \
    .getOrCreate()


def split_hash_tags(in_hash_tag):
    return in_hash_tag.split(" ")


def process_tags(in_df):
    in_df = in_df.select(COL.date, COL.tags).filter(col(COL.tags) != "")
    if limit:
        in_df = in_df.limit(100)
    # Split TAGs into a list of TAGs
    udf_tag = udf(split_hash_tags, ArrayType(StringType()))
    in_df = in_df.withColumn(COL.tag, udf_tag(col(COL.tags)))

    # Split TAGs list into A tag in different rows
    in_df = in_df.withColumn(COL.tag, explode(col(COL.tag)))
    in_df = in_df.groupby(col(COL.date), col(COL.tag)).count()

    if debug:
        in_df = in_df.sort(COL.count, ascending=False)
        in_df.show(100)
    return in_df


# for isTop in (True, False):
for isTop in [True]:
    for candidate in candidates:
        parquet_uri0, has_folder0 = FileProcessor.in_parquet_uri(candidate[0], isTop)
        if has_folder0 is False:
            continue
        # parquet_uri1, has_folder1 = FileProcessor.in_parquet_uri(candidate[1], isTop)
        # if has_folder1 is False:
        #     continue

        # Read in
        df0 = spark.read.parquet(parquet_uri0)
        # df1 = spark.read.parquet(parquet_uri1)
        # df = df0.union(df1)
        df = df0

        # Process tags
        df = process_tags(df)
        # Write out
        parquet_uri = FileProcessor.out_parquet_uri(candidate[0], isTop, tag_data)
        df.write.parquet(parquet_uri, compression='gzip', mode="overwrite")
