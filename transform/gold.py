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
    ).round(2)


    # Orden necesario para calculo historico

    df = df.sort_values(
        ["tipo_dolar", "fecha", "hora"]
    )

    # LAG venta dia anterior

    df["venta_anterior"] = (
        df
        .groupby("tipo_dolar")["venta"]
        .shift(1)
    )


    df["variacion_absoluta"] = (
        df["venta"]
        -
        df["venta_anterior"]
    ).round(2)


    df["variacion_porcentual"] = (
        (
            df["venta"]
            -
            df["venta_anterior"]
        )
        /
        df["venta_anterior"]
    ).round(2)

    
    df["compra_anterior"] = (
        df
        .groupby("tipo_dolar")["compra"]
        .shift(1)
    )

    df["variacion_absoluta_compra"] = (
        df["compra"] 
        -
        df["compra_anterior"]
    ).round(2)

    df["variacion_porcentual_compra"] = (
        (
            df["compra"]
            -
            df["compra_anterior"]
        )
        /
        df["compra_anterior"]
    ).round(2)


    # Precio promedio entre compra y venta

    df["precio_promedio"] = (
        (
            df["compra"] +
            df["venta"]
        ) / 2
    ).round(2)


    # Tendencia del precio

    df["tendencia"] = "Sin cambios"

    df.loc[
        df["variacion_absoluta"] > 0,
        "tendencia"
    ] = "Sube"

    df.loc[
        df["variacion_absoluta"] < 0,
        "tendencia"
    ] = "Baja"

    # Calcular la brecha con el oficial (solo para los tipos de dolar que no son oficial)
    oficial = (
        df[df["tipo_dolar"] == "oficial"]
        .sort_values("hora")
        .groupby("fecha")
        .tail(1)
        [["fecha", "venta"]]
        .rename(columns={"venta": "venta_oficial"})
    )

    df = df.merge(
        oficial,
        on="fecha",
        how="left"
    )

   # Calcular la brecha con el oficial
    df["brecha_oficial"] = (
        (
            df["venta"] - df["venta_oficial"]
        )
        /
        df["venta_oficial"]
    ).round(2)

    df.drop(columns=["venta_oficial"], inplace=True)

    # Fecha para analisis visual

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