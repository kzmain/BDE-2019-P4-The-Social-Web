import re
import os
import nltk
import spacy
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, ArrayType
from pyspark.sql.functions import udf, col, explode

from SYS.MODE import debug_mode as debug
from SYS.MODE import limit_mode as limit
from SYS.URI import tok_data
from SYS.CANDIDATES import candidates_analysis as candidates
from SYS.DATA_ANALYSIS.FileProcessor import FileProcessor
import SYS.COL as COL

nltk.download('stopwords')
os.system("python -m spacy download en_core_web_sm")
nlp = spacy.load("en_core_web_sm")


def get_token(tweet):
    doc = nlp(tweet)
    tokens = []
    for t in doc:
        if len(t) >= 2:
            if t.is_stop is False:
                if t.pos_ in ["VERB", "NOUN"]:
                    tokens.append(str(t.lemma_))
                if t.pos_ in ["NUM", "SYM", "ADP"]:
                    continue
                else:
                    tokens.append(str(t))
    return tokens


def process_token(in_df):
    in_df = in_df.select(COL.date, COL.txt).distinct()
    udf_token = udf(get_token, ArrayType(StringType()))

    in_df = in_df.withColumn(COL.token, udf_token(col(COL.txt)))

    in_df = in_df.withColumn(COL.token, explode(col(COL.token)))
    in_df = in_df.groupby(col(COL.date), col(COL.token)).count()

    return in_df


java8_location = '/Library/Java/JavaVirtualMachines/liberica-jdk-1.8.0_202/Contents/Home'  # Set your own
os.environ['JAVA_HOME'] = java8_location
spark = SparkSession\
    .builder\
    .config("spark.master", "local[*]") \
    .config("spark.executor.memory", "4g")\
    .appName("A1")\
    .getOrCreate()

# for isTop in (True, False):
for isTop in [True]:
    for candidate in candidates:
        parquet_uri0, has_folder0 = FileProcessor.in_parquet_uri(candidate[0], isTop)
        if has_folder0 is False:
            continue
        # parquet_uri1, has_folder1 = FileProcessor.in_parquet_uri(candidate[1], isTop)
        # if has_folder1 is False:
        #     continue

        df0 = spark.read.parquet(parquet_uri0)
        # df1 = spark.read.parquet(parquet_uri1)
        # df = df0.union(df1)
        df = df0

        if limit:
            df = df.limit(100)
            df.show()

        # Process tokens
        df = process_token(df)

        # Write out
        parquet_uri = FileProcessor.out_parquet_uri(candidate[0], isTop, tok_data)
        df.write.parquet(parquet_uri, compression='gzip', mode="overwrite")

        if debug:
            df = df.sort(COL.count, ascending=False)
            df.show(100)
