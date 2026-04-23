import pytest
import os
from pyspark.sql import SparkSession

from generator.io import write_to_csv
from src.generator.generator import generate_batch
from streaming.streaming import run_stream
from src.streaming.schema import get_schema
from src.generator import io

@pytest.fixture(scope="module")
def spark():
    spark = SparkSession.builder.appName("test_streaming").master("local[2]").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    yield spark
    spark.stop()

def test_detect_new_files(spark: SparkSession, tmp_path):
    """Tests that Spark Streaming correctly detects and reads new CSV
    files added to a directory while the stream is running."""
    output_dir = str(tmp_path / "streaming_output")
    os.makedirs(output_dir, exist_ok=True)
    actions = ["view", "add_to_cart", "purchase"]
    products = ["shoes", "socks", "t-shirt"]

    schema = get_schema()
    streaming_df = spark.readStream.format("csv").schema(schema).option("header","true").load(output_dir)

    query_name = "test_event_stream"
    query = streaming_df.writeStream.format("memory").queryName(query_name).outputMode("append").start()

    try:
        event_batch = generate_batch(10,actions, products)
        write_to_csv(event_batch, output_dir)

        query.processAllAvailable()

        result_df = spark.sql(f"SELECT * FROM {query_name}")
        row_count = result_df.count()

        assert row_count == 10, f"Expected 10 rows, but got {row_count}"

        event_batch_2 = generate_batch(5,actions, products)
        write_to_csv(event_batch_2, output_dir)

        query.processAllAvailable()

        result_df_2 = spark.sql(f"SELECT * FROM {query_name}")
        row_count_2 = result_df_2.count()

        assert row_count_2 == 15, f"Expected 15 rows, but got {row_count_2}"
    finally:
        query.stop()