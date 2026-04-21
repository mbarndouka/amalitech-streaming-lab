import os
import uuid
from datetime import datetime
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType

from src.config import config as get_config
from src.spark_session import get_spark_session
from src.write_to_postgres import write_to_postgres


def test_db_write():
    # 1. Load the configuration
    base_dir = os.path.dirname(os.path.abspath(__file__))
    conf = get_config(os.path.join(base_dir, "config.toml"))

    # 2. Initialize Spark with the JDBC driver
    spark = get_spark_session(
        app_name="TestWriteApp",
        jdbc_package=conf["spark"]["jdbc_package"]
    )

    print("Spark Session created. Building mock data...")

    # 3. Create 1 mock event matching your schema exactly
    now = datetime.now()
    mock_data = [{
        "event_id": str(uuid.uuid4()),
        "user_id": 9999,  # Easy number to spot
        "action": "test_action",
        "product_id": "test_product",
        "event_timestamp": now,
        "processed_at": now
    }]

    schema = StructType([
        StructField("event_id", StringType(), False),
        StructField("user_id", IntegerType(), False),
        StructField("action", StringType(), False),
        StructField("product_id", StringType(), False),
        StructField("event_timestamp", TimestampType(), False),
        StructField("processed_at", TimestampType(), False),
    ])

    df = spark.createDataFrame(mock_data, schema=schema)
    df.show() # Print out the dataframe we are trying to write

    # 4. Attempt to write to Postgres bypassing all streaming logic
    print("Writing to Postgres via JDBC...")
    try:
        # We pass 999 as a fake batch_id
        write_to_postgres(df, 999, conf)
        print("SUCCESS: Data successfully written to Postgres!")
    except Exception as e:
        print(f"FAILED: Could not write. Error: {e}")


if __name__ == "__main__":
    test_db_write()

