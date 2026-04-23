from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

def get_schema() -> StructType:
    """
    Returns the expected schema for the incoming event stream.

    This function creates a PySpark StructType defining the structure sizes and types
    of the raw input CSV files. Defining a schema upfront improves read performance
    in PySpark streaming applications.

    Returns:
        StructType: The defined schema with columns: event_id, user_id, action, products, and event_timestamp.
    """
    return StructType([
        StructField("event_id", StringType(), False),
        StructField("user_id", IntegerType(), True),
        StructField("action", StringType(), True),
        StructField("products", StringType(), True),
        StructField("event_timestamp", StringType(), True)
    ])