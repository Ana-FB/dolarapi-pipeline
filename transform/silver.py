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


def obtener_ultima_fecha_carga(client, tabla_ref):
    """Devuelve el último fecha_carga que ya tiene Silver, o None si está vacía."""
    query = f"SELECT MAX(fecha_carga) AS ultima_fecha FROM `{tabla_ref}`"
    df = client.query(query).to_dataframe()
    return df["ultima_fecha"][0]


def crear_silver():
    """Transforma los datos nuevos de Bronze y los agrega (append) a Silver."""

    client = bigquery.Client(project=PROJECT_ID)

    tabla_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
    tabla_bronze = f"{PROJECT_ID}.{DATASET_ID}.capa_bronze_dolares"

    if silver_existe(client, tabla_ref):
        ultima_fecha = obtener_ultima_fecha_carga(client, tabla_ref)

        if ultima_fecha is None:
            # Silver existe pero está vacía: mismo caso que "no existe"
            where_filtro = ""
        else:
            # Filtro rápido: solo bronze cargado DESPUÉS de la última fecha_carga en silver
            where_filtro = f"WHERE a.fecha_carga > DATETIME('{ultima_fecha}')"

        query = f"""
        SELECT a.*
        FROM `{tabla_bronze}` a
        {where_filtro}
        """
    else:
        # Primera carga: Silver no existe todavía, no hay nada contra qué comparar
        query = f"""
        SELECT a.*
        FROM `{tabla_bronze}` a
        """

    df = client.query(query).to_dataframe()

    if df.empty:
        print("No hay registros nuevos para cargar en Silver.")
        return

    # Normalizar texto
    df["casa"] = df["casa"].str.lower().str.strip()
    df["moneda"] = df["moneda"].str.upper().str.strip()

    # Renombrar columnas
    df = df.rename(columns={"casa": "tipo_dolar"})

    # Renombrar valores de tipo_dolar según mapping
    mapping = {"contadoconliqui": "ccl"}
    df["tipo_dolar"] = df["tipo_dolar"].replace(mapping)

    # Eliminar columnas redundantes
    df = df.drop(columns=["nombre"])

    # Convertir tipos
    df["compra"] = pd.to_numeric(df["compra"], errors="coerce")
    df["venta"] = pd.to_numeric(df["venta"], errors="coerce")

    # Eliminar registros inválidos
    df = df.dropna(subset=["compra", "venta"])

    # Eliminar duplicados dentro del batch nuevo (defensa extra ante reintentos,
    # cubre el caso donde MAX(fecha_carga) solo no alcanza)
    df = df.drop_duplicates(subset=["tipo_dolar", "moneda", "fechaActualizacion"])

    # Crear columnas derivadas
    fecha = pd.to_datetime(df["fechaActualizacion"])
    df["fecha"] = fecha.dt.date
    df["hora"] = fecha.dt.time

    if df.empty:
        print("No quedaron registros válidos para cargar en Silver.")
        return

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND
    )

    job = client.load_table_from_dataframe(
        df,
        tabla_ref,
        job_config=job_config,
    )

    job.result()

    print(f"Silver actualizada correctamente. Se agregaron {len(df)} registros nuevos.")


if __name__ == "__main__":
    crear_silver()