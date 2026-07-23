import os
from datetime import datetime, timezone
from dotenv import load_dotenv
from google.cloud import bigquery
from zoneinfo import ZoneInfo
from google.api_core.exceptions import NotFound


load_dotenv()

PROJECT_ID = os.getenv("GCP_PROJECT_ID")
DATASET_ID = os.getenv("BQ_DATASET")
TABLE_ID = "capa_bronze_dolares"


SCHEMA = [
    bigquery.SchemaField("moneda", "STRING"),
    bigquery.SchemaField("casa", "STRING"),
    bigquery.SchemaField("nombre", "STRING"),
    bigquery.SchemaField("compra", "FLOAT"),
    bigquery.SchemaField("venta", "FLOAT"),
    bigquery.SchemaField("fechaActualizacion", "TIMESTAMP"),

    # Metadata
    bigquery.SchemaField("fecha_carga", "DATETIME"),
    bigquery.SchemaField("fuente", "STRING"),
]


def obtener_tabla_ref():
    """Devuelve la referencia completa de la tabla."""
    return f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"


def crear_dataset_si_no_existe(client):
    """Crea el dataset si no existe."""

    dataset_ref = f"{PROJECT_ID}.{DATASET_ID}"

    try:
        client.get_dataset(dataset_ref)
        print(f"El dataset {dataset_ref} ya existe.")

    except NotFound:
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = "southamerica-east1"

        client.create_dataset(dataset)

        print(f"Dataset {dataset_ref} creado correctamente.")


def crear_tabla_si_no_existe(client, tabla_ref):
    """Crea la tabla Bronze si todavía no existe."""

    try:
        client.get_table(tabla_ref)
        print(f"La tabla {tabla_ref} ya existe.")

    except NotFound:

        tabla = bigquery.Table(
            tabla_ref,
            schema=SCHEMA
        )

        tabla.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="fecha_carga"
        )

        client.create_table(tabla)

        print(f"Tabla {tabla_ref} creada correctamente.")


def cargar_datos(client, tabla_ref, data):
    """Carga los datos de la API en Bronze."""

    ahora_utc = datetime.now(timezone.utc)
    fecha_carga_ar = ahora_utc.astimezone(ZoneInfo("America/Argentina/Buenos_Aires")).replace(tzinfo=None, microsecond=0)
    fecha_carga_str = fecha_carga_ar.isoformat()  # "2026-07-23T18:30:00" -> válido para DATETIME

    for fila in data:
        fila["fecha_carga"] = fecha_carga_str
        fila["fuente"] = "dolarapi"

    job_config = bigquery.LoadJobConfig(
        schema=SCHEMA,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
    )

    job = client.load_table_from_json(
        data,
        tabla_ref,
        job_config=job_config,
    )

    job.result()

    print(f"Se cargaron {len(data)} filas en {tabla_ref}.")


def cargar_bronze(data):
    """Ejecuta la carga de la capa Bronze."""

    client = bigquery.Client(project=PROJECT_ID)

    crear_dataset_si_no_existe(client)

    tabla_ref = obtener_tabla_ref()

    crear_tabla_si_no_existe(client, tabla_ref)

    cargar_datos(client, tabla_ref, data)


if __name__ == "__main__":
    from extract.extract import extraer_datos
    datos = extraer_datos()
    cargar_bronze(datos)