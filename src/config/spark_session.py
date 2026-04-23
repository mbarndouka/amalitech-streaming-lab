from pyspark.sql import SparkSession


def get_spark_session(app_name: str, jdbc_package: str) -> SparkSession:
    """
    Initializes and returns a Apache Spark session with the required JDBC package.

    Args:
        app_name (str): The name of the Spark application.
        jdbc_package (str): The Maven coordinate for the required JDBC driver.

    Returns:
        SparkSession: The configured Spark session instance.
    """
    spark = SparkSession.builder.appName(app_name).config("spark.jars.packages", jdbc_package).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark
