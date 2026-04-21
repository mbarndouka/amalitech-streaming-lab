from pyspark.sql import DataFrame
from pyspark.sql.functions import col, to_timestamp, current_timestamp


def transform_events(df: DataFrame) -> DataFrame:
    """Transforms the events."""
    return (
        df.dropna(subset=['event_id']).withColumnRenamed("products", "product_id").withColumn("event_timestamp", to_timestamp(col("event_timestamp"),
                                                                                  "yyyy-MM-dd'T'HH:mm:ss.SSSSSS'Z'")).withColumn(
            "processed_at", current_timestamp())
    )
