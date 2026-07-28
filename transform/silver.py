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
    try:
        client.get_table(tabla_ref)
        return True
    except NotFound:
        return False


def obtener_ultima_fecha_carga(client, tabla_ref):
    query = f"SELECT MAX(fecha_carga) AS ultima_fecha FROM `{tabla_ref}`"
    df = client.query(query).to_dataframe()
    return df["ultima_fecha"][0]



# crea una tabla Silver en BigQuery a partir de la tabla Bronze, aplicando transformaciones y filtros

def obtener_ultimo_valor_del_dia(client, tabla_ref):
    """Devuelve compra/venta más reciente por tipo_dolar, solo del día de hoy (AR) en Silver."""
    query = f"""
    SELECT tipo_dolar, moneda, compra, venta
    FROM `{tabla_ref}`
    WHERE fecha = CURRENT_DATE("America/Argentina/Buenos_Aires")
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY tipo_dolar, moneda
        ORDER BY fechaActualizacion DESC
    ) = 1
    """
    return client.query(query).to_dataframe()


def crear_silver():
    client = bigquery.Client(project=PROJECT_ID)

    tabla_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
    tabla_bronze = f"{PROJECT_ID}.{DATASET_ID}.capa_bronze_dolares"

    silver_ya_existe = silver_existe(client, tabla_ref)

    if silver_ya_existe:
        ultima_fecha = obtener_ultima_fecha_carga(client, tabla_ref)
        where_filtro = "" if ultima_fecha is None else f"WHERE a.fecha_carga > DATETIME('{ultima_fecha}')"
        query = f"SELECT a.* FROM `{tabla_bronze}` a {where_filtro}"
    else:
        query = f"SELECT a.* FROM `{tabla_bronze}` a"

    df = client.query(query).to_dataframe()

    if df.empty:
        print("No hay registros nuevos para cargar en Silver.")
        return

    df["casa"] = df["casa"].str.lower().str.strip()
    df["moneda"] = df["moneda"].str.upper().str.strip()
    df = df.rename(columns={"casa": "tipo_dolar"})

    mapping = {"contadoconliqui": "ccl"}
    df["tipo_dolar"] = df["tipo_dolar"].replace(mapping)

    df = df.drop(columns=["nombre"])

    df["compra"] = pd.to_numeric(df["compra"], errors="coerce")
    df["venta"] = pd.to_numeric(df["venta"], errors="coerce")
    df = df.dropna(subset=["compra", "venta"])

    df = df.drop_duplicates(subset=["tipo_dolar", "moneda", "fechaActualizacion"])

    # fechaActualizacion viene en UTC. Se convierte a hora Argentina antes de
    # derivar fecha/hora, para que "fecha" sea consistente con el
    # CURRENT_DATE("America/Argentina/Buenos_Aires") que usa el filtro de
    # "mismo valor, mismo día" más abajo.
    fecha_ar = pd.to_datetime(df["fechaActualizacion"], utc=True).dt.tz_convert("America/Argentina/Buenos_Aires")
    df["fecha"] = fecha_ar.dt.date
    df["hora"] = fecha_ar.dt.time

    if df.empty:
        print("No quedaron registros válidos para cargar en Silver.")
        return

    # Descarta filas cuyo valor (compra/venta) es idéntico al último ya
    # cargado HOY (hora Argentina) en Silver para ese tipo_dolar -> evita
    # repetir el mismo valor varias veces cuando la API "actualiza" el
    # timestamp sin que la cotización real haya cambiado.
    if silver_ya_existe:
        ultimos_hoy = obtener_ultimo_valor_del_dia(client, tabla_ref)

        if not ultimos_hoy.empty:
            df = df.merge(
                ultimos_hoy,
                on=["tipo_dolar", "moneda"],
                how="left",
                suffixes=("", "_anterior"),
            )

            df = df[
                (df["compra"] != df["compra_anterior"])
                | (df["venta"] != df["venta_anterior"])
                | (df["compra_anterior"].isna())
            ]

            df = df.drop(columns=["compra_anterior", "venta_anterior"])

    if df.empty:
        print("No hay cambios de cotización nuevos para cargar en Silver.")
        return

    job_config = bigquery.LoadJobConfig(write_disposition=bigquery.WriteDisposition.WRITE_APPEND)
    job = client.load_table_from_dataframe(df, tabla_ref, job_config=job_config)
    job.result()

    print(f"Silver actualizada correctamente. Se agregaron {len(df)} registros nuevos.")


if __name__ == "__main__":
    crear_silver()