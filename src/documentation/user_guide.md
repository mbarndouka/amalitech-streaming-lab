# Simple Guide: Running the Pipeline

## What You Need
1. **Docker & Docker Compose** (to run the database easily).
2. **Python 3.11+** (managed with `uv`).
3. **Java 8 or 11** (needed for Apache Spark).

## Steps to Run

### 1. Start the Database
Open your terminal and run Docker Compose to start the PostgreSQL database. It will automatically set up the tables using the SQL script:
```bash
docker-compose up -d
```

### 2. Run the Main Script
Install your dependencies and start the pipeline using `uv`:
```bash
uv sync
uv run main.py
```

### 3. Stop the Database (Optional)
When you are done, you can stop the database using:
```bash
docker-compose down
```
