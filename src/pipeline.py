import logging
from pyspark.sql import SparkSession
from typing import Dict, Any

from src.schema import get_schema
from src.transformation import transform_events
from src.write_to_postgres import write_to_postgres


def build_pipeline(spark: SparkSession, db_config: Dict[str, Any]) -> None:
    """Builds the pipeline."""

    storage_cfg = db_config["storage"]
    spark_cfg = db_config["spark"]
    db_cfg = db_config["database"]

    logging.info(f"Building pipeline with config: {storage_cfg['output_dir']}")
    raw_stream_df = spark.readStream.format("csv").schema(get_schema()).option("header", "true").option(
        "maxFilesPerTrigger", 1).load(storage_cfg["output_dir"])
    transformed_stream_df = transform_events(raw_stream_df)

    logging.info("Writing to PostgreSQL")
    query = transformed_stream_df.writeStream.foreachBatch(
        lambda df, batch_id: write_to_postgres(df, batch_id, db_config)).option("checkpointLocation",
                                                                                spark_cfg["checkpoint_dir"]).trigger(
        processingTime="5 seconds").start()
    query.awaitTermination()
