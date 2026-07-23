import os
from dotenv import load_dotenv

load_dotenv()

PROJECT_ID = os.getenv("GCP_PROJECT_ID")
DATASET_ID = os.getenv("BQ_DATASET")

LOCATION = "southamerica-east1"


# Tablas BigQuery

BRONZE_TABLE = "capa_bronze_dolares"

SILVER_TABLE = "capa_silver_dolares"

GOLD_TABLE = "capa_gold_dolares_analisis"