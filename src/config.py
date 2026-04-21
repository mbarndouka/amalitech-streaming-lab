import os
import tomllib
from typing import Dict, Any
from dotenv import load_dotenv


def config(filepath: str) -> Dict[str, Any]:
    """Reads the config file and applies environment overrides."""
    # Load environment variables from .env
    load_dotenv()

    with open(filepath, "rb") as f:
        conf = tomllib.load(f)

    # Override database config with .env variables if they exist
    db_conf = conf.get("database", {})
    db_user = os.getenv("POSTGRES_USER", db_conf.get("user"))
    db_pass = os.getenv("POSTGRES_PASSWORD", db_conf.get("password"))
    db_name = os.getenv("POSTGRES_DB", db_conf.get("dbname"))
    db_host = os.getenv("POSTGRES_HOST", db_conf.get("host"))
    db_port = os.getenv("POSTGRES_PORT", db_conf.get("port"))

    # Construct the JDBC URL using the properties
    url = f"jdbc:postgresql://{db_host}:{db_port}/{db_name}"

    # Inject jdbc_options which is expected by src/write_to_postgres.py
    conf["jdbc_options"] = {
        "url": url,
        "dbtable": db_conf.get("table_name", "ecommerce_events"),
        "user": db_user,
        "password": db_pass,
        "driver": db_conf.get("driver", "org.postgresql.Driver")
    }

    return conf