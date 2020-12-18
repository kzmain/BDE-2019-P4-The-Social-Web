import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType
from pyspark.sql.functions import udf, col
from textblob import TextBlob
from SYS.CANDIDATES import candidates_analysis as candidates
from SYS.MODE import debug_mode as debug
from SYS.MODE import limit_mode as limit
from SYS.URI import sen_data
import SYS.COL as COL
from SYS.DATA_ANALYSIS.FileProcessor import FileProcessor


def get_tweet_sentiment(tweet):
    """
    Utility function to classify sentiment of passed tweet
    using textblob's sentiment method
    :param tweet: a piece of tweet need to analysis
    :return: sentiment of passed tweet
    """
    # create TextBlob object of passed tweet text

    analysis = TextBlob(tweet)

    # set sentiment
    if analysis.sentiment.polarity > 0:
        return "positive"
    elif analysis.sentiment.polarity == 0:
        return "neutral"
    else:
        return "negative"


def process_sentiment(in_df):
    in_df = in_df.select(COL.date, COL.txt).distinct()
    if limit:
        in_df = in_df.limit(100)

    senti_udf = udf(get_tweet_sentiment, StringType())
    in_df = in_df.withColumn(COL.senti, senti_udf(col(COL.txt)))

    in_df = in_df.groupby(col(COL.date), col(COL.senti)).count()

    pop_df = in_df.select(COL.date, COL.count)\
        .groupby(col(COL.date))\
        .sum()\
        .withColumnRenamed(COL.sum_count, COL.sum)

    in_df = in_df.join(pop_df, on=[COL.date], how="full")
    in_df = in_df.withColumn(COL.ratio, col(COL.count)/col(COL.sum)*100)

    if debug:
        in_df = in_df.sort(COL.count, ascending=False)
        in_df.show(100)
    return in_df


java8_location = '/Library/Java/JavaVirtualMachines/liberica-jdk-1.8.0_202/Contents/Home'  # Set your own
os.environ['JAVA_HOME'] = java8_location
spark = SparkSession\
    .builder\
    .appName("A1")\
    .getOrCreate()

# for isTop in (True, False):
for isTop in [False]:
    for candidate in candidates:
        parquet_uri0, has_folder0 = FileProcessor.in_parquet_uri(candidate[0], isTop)
        if has_folder0 is False:
            continue

        df0 = spark.read.parquet(parquet_uri0)
        df = df0

        df = process_sentiment(df)

        parquet_uri = FileProcessor.out_parquet_uri(candidate[0], isTop, sen_data)
        df.write.parquet(parquet_uri, compression='gzip', mode="overwrite")

        df.sort(col(COL.date)).show(500)
