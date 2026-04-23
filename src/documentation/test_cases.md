# Test Plan & Execution Report

This project utilizes both automated unit testing (via `pytest`) and manual system testing to verify data integrity and streaming capabilities.

## Test Cases

| Test ID | Type | Component | Description | Expected Outcome | Actual Outcome | Status |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **TC-01** | Automated | Generator | Verify pure data generation logic (`test_generate_batch`). | Dictionary output contains exactly 5 correct keys and valid datatypes. | Pytest assertions passed. | ✅ PASS |
| **TC-02** | Automated | IO Edge | Verify file I/O operations (`tmp_path` fixture). | CSV file is created with correct rows, headers, and no data loss. | Pytest confirmed file creation and valid schema. | ✅ PASS |
| **TC-03** | Automated | Spark | Verify Spark detects new files dynamically (`memory` sink). | Spark processes initial files, then catches a subsequent file dropped mid-stream. | Pytest confirmed batch increments accurately. | ✅ PASS |
| **TC-04** | Manual | Spark | Verify Data Transformation (Null dropping). | Records missing `event_id` are removed before database insertion. | Monitored DB row count perfectly matches valid CSV rows. | ✅ PASS |
| **TC-05** | Manual | PostgreSQL | Verify JDBC Micro-batch insertion. | Transformed records appear in `ecommerce_events` table automatically. | `SELECT COUNT(*)` increments automatically every 5 seconds. | ✅ PASS |
| **TC-06** | Manual | Performance | Verify end-to-end processing speed and limits. | Streaming batch read, transform, and database write finish within the defined micro-batch interval. | Pipeline logs output processing metrics (e.g., Latency: 371 ms, Process Rate: 35.0 rows/sec). | ✅ PASS |
