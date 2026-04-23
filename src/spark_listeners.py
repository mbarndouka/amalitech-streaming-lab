import logging
from typing import Dict, Any

from src.utils import logger as get_logger
from pyspark.sql.streaming import StreamingQueryListener

app_logger = get_logger(__name__, "INFO")

# ==========================================
# 1. PURE FUNCTIONS (Core Logic)
# ==========================================
def extract_metrics(progress_event: Any) -> Dict[str, Any]:
    """Purely extracts and maps PySpark progress data into an immutable dictionary."""
    return{
        "batch_id": getattr(progress_event, "batchId", -1),
        "input_rows": getattr(progress_event, "numInputRows", 0),
        "input_rate": getattr(progress_event, "inputRowsPerSecond", 0.0),
        "process_rate": getattr(progress_event, "processedRowsPerSecond", 0.0),
        "duration_ms": progress_event.durationMs.get("triggerExecution", 0) if hasattr(progress_event, "durationMs") else 0
    }

def format_metrics_log(metrics: Dict[str, Any]) -> str:
    """Pure function to format the metrics dictionary into a readable log string."""
    return (
        f"[METRICS] Batch {metrics['batch_id']} | "
        f"Rows: {metrics['input_rows']:02d} | "
        f"Latency: {metrics['duration_ms']} ms | "
        f"Input Rate: {metrics['input_rate']:.1f} rows/sec | "
        f"Process Rate: {metrics['process_rate']:.1f} rows/sec"
    )

def create_performance_listener(logger: logging.Logger) -> StreamingQueryListener:
    """
    Creates and returns a functional listener adapter for PySpark.

    This abstracts away the OOP requirements of PySpark's StreamingQueryListener,
    allowing us to supply a standard logger and handle stream progress events
    by extracting and recording meaningful performance metrics (latency, throughput).

    Args:
        logger (logging.Logger): The logger to use for reporting stream events.

    Returns:
        StreamingQueryListener: An initialized PySpark query listener.
    """
    class FunctionalListenerAdapter(StreamingQueryListener):
        def onQueryStarted(self, event: Any) -> None:
            logger.info(f"Stream [ID: {event.id}] started successfully.")

        def onQueryProgress(self, event: Any)-> None:
            # 1. Pure data extraction
            metrics = extract_metrics(event.progress)

            # 2. Side-effect (logging) applied conditionally
            if metrics["input_rows"] > 0:
                log_msg = format_metrics_log(metrics)
                logger.info(log_msg)

        def onQueryTerminated(self, event: Any)-> None:
            if event.exception:
                logger.error(f"Stream crashed! Exception: {event.exception}")
            else:
                logger.info("Stream terminated gracefully.")

    return FunctionalListenerAdapter()
