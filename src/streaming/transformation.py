from pyspark.sql import DataFrame
from pyspark.sql.functions import col, to_timestamp, current_timestamp


def transform_events(df: DataFrame) -> DataFrame:
    """
    Cleans and transforms the raw events stream.

    This function drops rows missing an event_id, renames the 'products' column
    to 'product_id', casts the parsed event_timestamp string to a native timestamp,
    and adds a 'processed_at' column to record when the event was handled.

    Args:
        df (DataFrame): The incoming raw Spark DataFrame.

    Returns:
        DataFrame: The transformed DataFrame ready to be written.
    """
    return (
        df.dropna(subset=['event_id']).withColumnRenamed("products", "product_id").withColumn("event_timestamp", to_timestamp(col("event_timestamp"),
                                                                                  "yyyy-MM-dd'T'HH:mm:ss.SSSSSS'Z'")).withColumn(
            "processed_at", current_timestamp())
    )
