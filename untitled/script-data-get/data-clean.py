import re
import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, DateType
from pyspark.sql.functions import udf, col
from SYS.CANDIDATES import candidates_raw
from SYS.DATA_GET.DATA_CLEAN.FileProcessor import FileProcessor
import SYS.COL as COL
from SYS.MODE import debug_mode as debug


def clean_tweet(tweet):
    """
    Utility function to clean tweet text by removing links, special characters
    using simple regex statements.
    :param tweet: a piece of tweet need to clean
    :return: cleaned tweet
    """
    return ' '.join(re.sub(r"(@[A-Za-z0-9]+)|([^0-9A-Za-z \t])|(\w+://\S+)", " ", tweet).split())


def update_tags(tag):
    return tag.lower()


def update_date(in_date):
    return in_date.date()


java8_location = '/Library/Java/JavaVirtualMachines/liberica-jdk-1.8.0_202/Contents/Home'  # Set your own
os.environ['JAVA_HOME'] = java8_location
spark = SparkSession \
    .builder \
    .config("spark.master", "local[*]") \
    .config("spark.executor.memory", "2g") \
    .appName("A1") \
    .getOrCreate()


# for isTop in (True, False):
for isTop in [True]:
    for candidate in candidates_raw:
        parquet_uri, has_folder = FileProcessor.in_parquet_uri(candidate, isTop)
        if has_folder is False:
            continue
        df = spark.read.parquet(parquet_uri)
        df = df.select(COL.id, COL.usr, COL.txt, COL.date, COL.rtw, COL.fav, COL.tags)

        udf_clean = udf(clean_tweet, StringType())
        df = df.withColumn(COL.txt, udf_clean(col(COL.txt)))

        udf_clean = udf(update_date, DateType())
        df = df.withColumn(COL.date, udf_clean(col(COL.date)))

        udf_clean = udf(update_tags, StringType())
        df = df.withColumn(COL.tags, udf_clean(col(COL.tags)))

        parquet_uri = FileProcessor.out_parquet_uri(candidate, isTop)
        df.write.parquet(parquet_uri, compression='gzip', mode="overwrite")

        if debug:
            df.show()
