from pyspark.sql.types import StringType
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import col, lit
from SYS import URI, COL


class DataProcessor:
    def __init__(self):
        self.spark = self.__initial_spark()
        self.true_support_df = self.__get_true_data_estimation_df()
        self.false_support_df = self.__get_false_data_estimation_df()
        self.sentiment_trump_df = self.__get_true_data_sentiment_trump_df()
        self.sentiment_hillary_df = self.__get_true_data_sentiment_hillary_df()
        self.true_data_heat_trump_df = self.__get_true_data_heat_trump_df()
        self.true_data_heat_hillary_df = self.__get_true_data_heat_hillary_df()
        self.word_df = self.__get_true_data_word_cloud_df()

    @staticmethod
    def __initial_spark():
        spark = SparkSession \
            .builder \
            .config("spark.master", "local[*]") \
            .config("spark.executor.memory", "2g") \
            .appName("tsw") \
            .getOrCreate()
        return spark

    def get_true_x_axis_data(self):
        true_x_axis_data = self.true_support_df\
            .withColumn(COL.date, col(COL.date).cast(StringType()))\
            .select(col(COL.date))\
            .toPandas()[COL.date]\
            .to_json(orient='values')
        return true_x_axis_data

    def __get_true_data_estimation_df(self):
        true_support_df = self.spark.read.parquet(URI.show_supp_true).sort(col(COL.date))
        return true_support_df

    def __get_false_data_estimation_df(self):
        false_support_df = self.spark.read.parquet(URI.show_supp_false).sort(col(COL.date))
        return false_support_df

    def get_true_data_estimation_hillary(self):
        true_data_estimation_hillary = self.true_support_df\
            .select(col(COL.hillary))\
            .toPandas()[COL.hillary]\
            .to_json(orient='values')
        return true_data_estimation_hillary

    def get_true_data_estimation_trump(self):
        true_data_estimation_trump = self.true_support_df\
            .select(col(COL.trump))\
            .toPandas()[COL.trump]\
            .to_json(orient='values')
        return true_data_estimation_trump

    def get_false_data_estimation_hillary(self):
        false_data_estimation_hillary = self.false_support_df \
            .select(col(COL.hillary))\
            .toPandas()[COL.hillary]\
            .to_json(orient='values')
        return false_data_estimation_hillary

    def get_false_data_estimation_trump(self):
        false_data_estimation_trump = self.false_support_df\
            .select(col(COL.trump))\
            .toPandas()[COL.trump]\
            .to_json(orient='values')
        return false_data_estimation_trump

    def __get_true_data_sentiment_trump_df(self):
        sentiment_trump_df = self.spark.read.parquet(URI.show_senti_trump_true).sort(col(COL.date))
        return sentiment_trump_df

    def get_true_data_positive_trump(self):
        true_data_positive_trump = self.sentiment_trump_df.filter(col(COL.senti) == "positive")\
            .sort(col(COL.date)) \
            .select(col(COL.ratio)) \
            .toPandas()[COL.ratio] \
            .to_json(orient='values')
        return true_data_positive_trump

    def get_true_data_neutral_trump(self):
        true_data_neutral_trump = self.sentiment_trump_df.filter(col(COL.senti) == "neutral")\
            .sort(col(COL.date)) \
            .select(col(COL.ratio)) \
            .toPandas()[COL.ratio] \
            .to_json(orient='values')
        return true_data_neutral_trump

    def get_true_data_negative_trump(self):
        true_data_negative_trump = self.sentiment_trump_df.filter(col(COL.senti) == "negative")\
            .sort(col(COL.date)) \
            .select(col(COL.ratio)) \
            .toPandas()[COL.ratio] \
            .to_json(orient='values')
        return true_data_negative_trump

    def __get_true_data_sentiment_hillary_df(self):
        sentiment_hillary_df = self.spark.read.parquet(URI.show_senti_hilla_true).sort(col(COL.date))
        return sentiment_hillary_df

    def get_true_data_positive_hillary(self):
        true_data_positive_hillary = self.sentiment_hillary_df.filter(col(COL.senti) == "positive")\
            .sort(col(COL.date)) \
            .select(col(COL.ratio)) \
            .toPandas()[COL.ratio] \
            .to_json(orient='values')
        return true_data_positive_hillary

    def get_true_data_neutral_hillary(self):
        true_data_neutral_hillary = self.sentiment_hillary_df.filter(col(COL.senti) == "neutral")\
            .sort(col(COL.date)) \
            .select(col(COL.ratio)) \
            .toPandas()[COL.ratio] \
            .to_json(orient='values')
        return true_data_neutral_hillary

    def get_true_data_negative_hillary(self):
        true_data_negative_hillary = self.sentiment_hillary_df.filter(col(COL.senti) == "negative")\
            .sort(col(COL.date)) \
            .select(col(COL.ratio)) \
            .toPandas()[COL.ratio] \
            .to_json(orient='values')
        return true_data_negative_hillary

    def __get_true_data_heat_trump_df(self):
        true_data_heat_trump_df = self.spark.read.parquet(URI.show_heat_trump_true).sort(col(COL.date))
        return true_data_heat_trump_df

    def get_true_data_heat_trump(self):
        true_data_heat_trump = self.true_data_heat_trump_df \
            .select(col(COL.sum)) \
            .toPandas()[COL.sum] \
            .to_json(orient='values')
        return true_data_heat_trump

    def __get_true_data_heat_hillary_df(self):
        true_data_heat_hillary_df = self.spark.read.parquet(URI.show_heat_hillary_true).sort(col(COL.date))
        return true_data_heat_hillary_df

    def get_true_data_heat_hillary(self):
        true_data_heat_hillary = self.true_data_heat_hillary_df \
            .select(col(COL.sum)) \
            .toPandas()[COL.sum] \
            .to_json(orient='values')
        return true_data_heat_hillary

    def __get_true_data_word_cloud_df(self):
        t_df = self.spark.read.parquet(URI.show_ner_true)\
            .withColumnRenamed(COL.ner, COL.name)\
            .withColumnRenamed(COL.count, COL.value)
        n_df = self.spark.read.parquet(URI.show_token_true)\
            .withColumnRenamed(COL.ner, COL.name)\
            .withColumnRenamed(COL.count, COL.value)
        df = t_df.union(n_df).drop_duplicates([COL.date, COL.name])
        return df

    def get_true_data_word_cloud(self):
        return self.word_df\
            .drop(col(COL.date))\
            .groupby(col(COL.name)).sum(COL.value)\
            .withColumnRenamed(COL.sum_value, COL.value)\
            .sort(COL.value, ascending=False)\
            .limit(200).toPandas().to_json(orient='records')

    def get_new_true_data_word_cloud(self, start_date, end_date):
        return self.word_df\
            .filter((col(COL.date) >= start_date) & (col(COL.date) <= end_date))\
            .drop(col(COL.date))\
            .groupby(col(COL.name)).sum(COL.value)\
            .withColumnRenamed(COL.sum_value, COL.value)\
            .sort(COL.value, ascending=False)\
            .limit(200).toPandas().to_json(orient='records')
