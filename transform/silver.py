import os

from dotenv import load_dotenv
from google.cloud import bigquery
from google.api_core.exceptions import NotFound
import pandas as pd

load_dotenv()

PROJECT_ID = os.getenv("GCP_PROJECT_ID")
DATASET_ID = os.getenv("BQ_DATASET")
TABLE_ID = "capa_silver_dolares"


def silver_existe(client, tabla_ref):
    """Chequea si la tabla Silver ya existe."""
    try:
        client.get_table(tabla_ref)
        return True
    except NotFound:
        return False


def obtener_ultima_fecha_silver(client, tabla_ref):
    """Devuelve la última fecha de carga que ya tiene Silver, o None si está vacía."""

    query = f"""
    SELECT MAX(fecha_carga) AS ultima_fecha
    FROM `{tabla_ref}`
    """

    df = client.query(query).to_dataframe()

    ultima_fecha = df["ultima_fecha"][0]

    return ultima_fecha


def crear_silver():
    """Transforma los datos nuevos de Bronze y los agrega (append) a Silver."""

    client = bigquery.Client(project=PROJECT_ID)

    tabla_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

    # --- VERSIÓN : incremental basada en MAX(fecha_carga) de Silver ---
    if silver_existe(client, tabla_ref):
        # Silver ya existe: traer solo lo nuevo que no esté ya cargado
        ultima_fecha = obtener_ultima_fecha_silver(client, tabla_ref)

        if ultima_fecha is None:
            # Silver existe pero está vacía: mismo caso que "no existe"
            query = f"""
            SELECT a.*
            FROM `{PROJECT_ID}.{DATASET_ID}.capa_bronze_dolares` a
            """
        else:
            query = f"""
            SELECT a.*
            FROM `{PROJECT_ID}.{DATASET_ID}.capa_bronze_dolares` a
            WHERE DATE(a.fecha_carga) > DATE('{ultima_fecha}')
            AND NOT EXISTS (
                SELECT 1
                FROM `{tabla_ref}` b
                WHERE b.tipo_dolar = a.casa
                AND b.moneda = a.moneda
                AND b.fechaActualizacion = a.fechaActualizacion
            )
            """
    else:
        # Primera carga: Silver no existe todavía, no hay nada contra qué comparar
        query = f"""
        SELECT a.*
        FROM `{PROJECT_ID}.{DATASET_ID}.capa_bronze_dolares` a
        """

    df = client.query(query).to_dataframe()

    ultima_fecha = obtener_ultima_fecha_silver(client, tabla_ref)

    if df.empty:
        print("No hay registros nuevos para cargar en Silver.")
        return

    # Normalizar texto
    df["casa"] = (
        df["casa"]
        .str.lower()
        .str.strip()
    )

    df["moneda"] = (
        df["moneda"]
        .str.upper()
        .str.strip()
    )

    # Renombrar columnas
    df = df.rename(columns={"casa": "tipo_dolar"})

    # Eliminar columnas redundantes
    df = df.drop(columns=["nombre"])

    # Convertir tipos
    df["compra"] = pd.to_numeric(df["compra"], errors="coerce")
    df["venta"] = pd.to_numeric(df["venta"], errors="coerce")

    # Eliminar registros inválidos
    df = df.dropna(subset=["compra", "venta"])

    # Eliminar duplicados dentro del batch nuevo (defensa extra ante reintentos)
    df = df.drop_duplicates(subset=["tipo_dolar", "moneda", "fechaActualizacion"])

    # Crear columnas derivadas
    fecha = pd.to_datetime(df["fechaActualizacion"])

    df["fecha"] = fecha.dt.date
    df["hora"] = fecha.dt.time

    if df.empty:
        print("No quedaron registros válidos para cargar en Silver.")
        return

    # Configuración de carga: APPEND porque Silver acumula historial
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND
    )

    # Cargar Silver
    tabla_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

    job = client.load_table_from_dataframe(
        df,
        tabla_ref,
        job_config=job_config,
    )

    job.result()

    print(f"Silver actualizada correctamente. Se agregaron {len(df)} registros nuevos.")


if __name__ == "__main__":
    crear_silver()