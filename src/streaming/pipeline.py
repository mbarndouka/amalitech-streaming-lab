import logging
from pyspark.sql import SparkSession
from typing import Dict, Any

from streaming.schema import get_schema
from streaming.transformation import transform_events
from streaming.write_to_postgres import write_to_postgres

from spark_listeners import create_performance_listener
from src.utils import logger as get_logger

pipline_logger = get_logger("Pipeline", "INFO")


def build_pipeline(spark: SparkSession, db_config: Dict[str, Any]) -> None:
    """Builds the pipeline."""

    storage_cfg = db_config["storage"]
    spark_cfg = db_config["spark"]
    db_cfg = db_config["database"]

    listener = create_performance_listener(pipline_logger)
    spark.streams.addListener(listener)

    pipline_logger.info(f"Building pipeline with config: {storage_cfg['output_dir']}")
    raw_stream_df = spark.readStream.format("csv").schema(get_schema()).option("header", "true").option(
        "maxFilesPerTrigger", 1).load(storage_cfg["output_dir"])
    transformed_stream_df = transform_events(raw_stream_df)

    pipline_logger.info("Writing to PostgreSQL")
    query = transformed_stream_df.writeStream.foreachBatch(
        lambda df, batch_id: write_to_postgres(df, batch_id, db_config)).option("checkpointLocation",
                                                                                spark_cfg["checkpoint_dir"]).trigger(
        processingTime="5 seconds").start()

    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        pipline_logger.info("Gracefully stopping the Spark streaming query...")
        query.stop()
        pipline_logger.info("Query stopped.")
