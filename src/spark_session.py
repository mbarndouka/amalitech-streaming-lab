from pyspark.sql import SparkSession


def get_spark_session(app_name: str, jdbc_package: str) -> SparkSession:
    spark = SparkSession.builder.appName(app_name).config("spark.jars.packages", jdbc_package).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark
