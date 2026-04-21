from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

def get_schema() -> StructType:
    """Returns the schema for the events."""
    return StructType([
        StructField("event_id", StringType(), False),
        StructField("user_id", IntegerType(), True),
        StructField("action", StringType(), True),
        StructField("products", StringType(), True),
        StructField("event_timestamp", StringType(), True)
    ])