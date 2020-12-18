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


def update_ner(ner_txt):
    if 'Hillary' in ner_txt:
        return
    if 'hillary' in ner_txt:
        return
    if 'Clinton' in ner_txt:
        return
    if 'Trump' in ner_txt:
        return
    if 'Donald' in ner_txt:
        return
    if 'today' in ner_txt:
        return
    if 'Today' in ner_txt:
        return
    if 'this morning' in ner_txt:
        return
    if 'Tonight' in ner_txt:
        return
    if 'tonight' in ner_txt:
        return
    if 'last night' in ner_txt:
        return
    if 'Last night' in ner_txt:
        return
    if '11 years ago' in ner_txt:
        return
    if 'tomorrow' in ner_txt:
        return
    if 'one' in ner_txt:
        return
    if 'One' in ner_txt:
        return
    if 'two' in ner_txt:
        return
    if 'Two' in ner_txt:
        return
    if 'first' in ner_txt:
        return
    if '1st' in ner_txt:
        return
    if 'First' in ner_txt:
        return
    if 'second' in ner_txt:
        return
    if '2nd' in ner_txt:
        return
    if 'third' in ner_txt:
        return
    if 'Between Two' in ner_txt:
        return
    if 'hours' in ner_txt:
        return
    if ner_txt.isnumeric():
        return
    if 'Obama' in ner_txt:
        return 'Obama'

    if 'American' in ner_txt:
        return
    if 'America' in ner_txt:
        return
    if 'the United States' in ner_txt:
        return
    if 'US' in ner_txt:
        return

    if 'Russian' in ner_txt:
        return 'Russia'
    if 'Republic' in ner_txt:
        return 'Republican'
    return ner_txt


for isTop in (True, False):
    for candidate in cands:
        fn, fe = FileProcessor.in_parquet_uri(candidate, isTop, DIR.ner_data)

        if fe is False:
            continue

        df = spark.read.parquet(fn)

        udf_ner = udf(update_ner, StringType())
        df = df.withColumn(COL.ner, udf_ner(col(COL.ner)))

        if debug:
            df = df.filter(col(COL.ner).isNotNull())

        df = df.filter(col(COL.ner).isNotNull())
        df = df.groupby(col(COL.date), col(COL.ner)).sum().withColumnRenamed(COL.sum_count, COL.count)
        df = df.filter(col(COL.count) > lit(5))

        df = df.sort(col(COL.count), ascending=False)

        if limit:
            df = df.limit(100)

        if debug:
            df.show()
        df.write.parquet(FileProcessor.out_parquet_uri(candidate, isTop, DIR.ner_resu), compression="gzip", mode="overwrite")
