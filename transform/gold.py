import os
from dotenv import load_dotenv
from google.cloud import bigquery
import pandas as pd


load_dotenv()


PROJECT_ID = os.getenv("GCP_PROJECT_ID")
DATASET_ID = os.getenv("BQ_DATASET")
TABLE_ID = "capa_gold_dolares_analisis"


def crear_gold():

    client = bigquery.Client(project=PROJECT_ID)


    # 1. Leer Silver desde BigQuery

    query = f"""
    SELECT * EXCEPT(fecha_carga, fechaActualizacion) 
    FROM `{PROJECT_ID}.{DATASET_ID}.capa_silver_dolares`
"""

    df = client.query(query).to_dataframe()


    # 2. Transformaciones Gold


    df["spread"] = (
        df["venta"] - df["compra"]
    ).round(2)


    df["spread_porcentual"] = (
        (df["venta"] - df["compra"])
        /
        df["venta"]
        * 100
    ).round(2)


    # Orden necesario para cálculo histórico

    df = df.sort_values(
        ["tipo_dolar", "fecha"]
    )


    # LAG venta día anterior

    df["venta_dia_anterior"] = (
        df
        .groupby("tipo_dolar")["venta"]
        .shift(1)
    )


    df["variacion_absoluta"] = (
        df["venta"]
        -
        df["venta_dia_anterior"]
    ).round(2)


    df["variacion_porcentual"] = (
        (
            df["venta"]
            -
            df["venta_dia_anterior"]
        )
        /
        df["venta_dia_anterior"]
        * 100
    ).round(2)


    # Fecha para análisis visual

    df["fecha_formateada"] = (
        pd.to_datetime(df["fecha"])
        .dt
        .strftime("%d-%m-%Y")
    )


    # 3. Guardar Gold en BigQuery

    tabla_gold = (
        f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
    )


    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
    )


    job = client.load_table_from_dataframe(
        df,
        tabla_gold,
        job_config=job_config
    )


    job.result()
    print(f"Gold actualizada correctamente. Se generaron {len(df)} registros.")
    


if __name__ == "__main__":
    crear_gold()