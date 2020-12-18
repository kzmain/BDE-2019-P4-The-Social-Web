import os
import nltk
import spacy
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, ArrayType
from pyspark.sql.functions import udf, col, explode
import SYS.COL as COL
from SYS.CANDIDATES import candidates_analysis as candidates
from SYS.DATA_ANALYSIS.FileProcessor import FileProcessor
from SYS.URI import ner_data
from SYS.MODE import debug_mode as debug
from SYS.MODE import limit_mode as limit

nltk.download('stopwords')
os.system("python -m spacy download en_core_web_sm")
nlp = spacy.load("en_core_web_sm")


def get_ner(tweet):
    doc = nlp(tweet)
    ners = []
    for ent in doc.ents:
        ners.append(ent.text)
    return ners


def process_ner(in_df):
    in_df = in_df.select(COL.date, COL.txt).distinct()
    if limit:
        in_df = in_df.limit(100)
        in_df.show()
    udf_ner = udf(get_ner, ArrayType(StringType()))
    in_df = in_df.withColumn(COL.ner, udf_ner(col(COL.txt)))

    in_df = in_df.withColumn(COL.ner, explode(col(COL.ner)))
    in_df = in_df.groupby(col(COL.date), col(COL.ner)).count()

    if debug:
        in_df = in_df.sort(COL.count, ascending=False)
        in_df.show()

    return in_df


java8_location = '/Library/Java/JavaVirtualMachines/liberica-jdk-1.8.0_202/Contents/Home'  # Set your own
os.environ['JAVA_HOME'] = java8_location
spark = SparkSession\
    .builder\
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

        df = process_ner(df)

        parquet_uri = FileProcessor.out_parquet_uri(candidate[0], isTop, ner_data)
        df.write.parquet(parquet_uri, compression='gzip', mode="overwrite")
