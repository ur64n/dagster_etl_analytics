import os
from dagster import resource
from sqlalchemy import create_engine

@resource(
    config_schema={
        "username": str,
        "password": str,
        "hostname": str,
        "db_name": str,
    }
)
def postgres_db_resource(context):
    # Pobieranie zmiennych środowiskowych z os.environ
    username = os.getenv("PG_USERNAME") or context.resource_config.get("username")
    password = os.getenv("PG_PASSWORD") or context.resource_config.get("password")
    hostname = os.getenv("PG_HOSTNAME") or context.resource_config.get("hostname")
    db_name = os.getenv("PG_DB") or context.resource_config.get("db_name")

    # Tworzenie silnika i połączenia z bazą danych
    engine = create_engine(f"postgresql://{username}:{password}@{hostname}/{db_name}")
    return engine.connect()