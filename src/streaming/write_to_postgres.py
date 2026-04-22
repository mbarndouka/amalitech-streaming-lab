from pyspark.sql import DataFrame
from typing import Dict, Any

from src.utils import logger as get_logger


def write_to_postgres(batch_df: DataFrame, batch_id: int, db_config: Dict[str, Any]) -> None:
    """
    Writes a single micro-batch DataFrame to PostgreSQL.
    This acts as a side-effect isolated to the micro-batch execution.
    """
    # Fix: Store the initialized logger into a variable to avoid missing arguments later
    batch_logger = get_logger("PostgresWriter", db_config["logging"]["level"])

    batch_logger.info(
        f"Writing micro-batch {batch_id} to PostgreSQL"
    )

    if batch_df.isEmpty():
        return

    try:
        # We need to tell the JDBC driver how to map strings to UUIDs
        # because PostgreSQL strictly rejects strings passed into UUID columns by default.
        string_type = "stringtype"
        jdbc_opts = db_config["jdbc_options"].copy()

        # This tells PostgreSQL JDBC to let the database handle UUID parsing implicitly.
        if string_type not in jdbc_opts:
            jdbc_opts[string_type] = "unspecified"

        safe_db = batch_df.select(
            "event_id",
            "user_id",
            "action",
            "product_id",
            "event_timestamp",
            "processed_at"
        )
        safe_db.write.format("jdbc").options(**jdbc_opts).mode("append").save()

        # Repaired calls to logger (using the variable initialized above)
        batch_logger.info(f"Micro-batch {batch_id} written to PostgreSQL")

    except Exception as e:
        # Repaired calls to logger
        batch_logger.error(f"Error writing micro-batch {batch_id} to PostgreSQL: {str(e)}")
        raise e