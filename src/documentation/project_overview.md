# Real-Time E-Commerce Data Ingestion Pipeline
**Project Overview & Architecture**

## 1. System Purpose
This project simulates a real-time e-commerce tracking system. It continuously generates simulated user activity events (views, cart additions, purchases), streams them through an analytics engine, cleans the data, and writes it to a persistent database for downstream analytics.

## 2. Architectural Paradigm: Functional Programming
Unlike standard object-oriented scripts, this system is built using strict **Functional Programming** principles. It explicitly separates pure business logic from I/O side-effects, making the codebase highly testable, predictable, and robust.

## 3. Core Components
1. **The Data Generator (The Faucet)**
   * **Tech:** Python (Standard Library)
   * **Role:** Acts as the simulated e-commerce frontend. Generates batches of randomized user events and writes them to local CSV files. Uses pure functions for data generation and isolates the file-saving logic.

2. **The Stream Processor (The Pipes)**
   * **Tech:** Apache Spark Structured Streaming (PySpark)
   * **Role:** Continuously monitors the `input_data/` directory. Reads new CSVs using a strict predefined schema, drops invalid records, and converts string timestamps into native SQL timestamps without mutating state.

3. **The Data Storage (The Bucket)**
   * **Tech:** PostgreSQL
   * **Role:** Serves as the persistent analytical database. Spark writes data here in micro-batches using the JDBC driver via an impure `foreachBatch` adapter.

4. **The Enterprise Metric Listener**
   * **Role:** A custom adapter bridging Spark's OOP listener interface to our functional codebase, extracting real-time throughput and latency metrics natively.

## 4. Data Flow
`[Python Generator] -> (Local CSV Files) -> [Spark ReadStream] -> [Pure Transformations] -> [Spark foreachBatch] -> [PostgreSQL Table]`