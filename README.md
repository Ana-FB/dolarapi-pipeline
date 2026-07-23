# dolarapi-pipeline

Pipeline de datos que extrae las cotizaciones del dólar publicadas por [DolarAPI](https://dolarapi.com/) y las carga en BigQuery siguiendo una arquitectura por capas **Bronze → Silver → Gold**.

## Arquitectura

```
DolarAPI  ──▶  Bronze  ──▶  Silver  ──▶  Gold
(extract)     (raw)        (limpia)     (análisis)
```

- **Extract** ([extract/extract.py](extract/extract.py)): llama a la API y devuelve el JSON crudo con las cotizaciones de todas las casas de cambio.
- **Bronze** ([load/bronze.py](load/bronze.py)): carga los datos crudos tal cual llegan de la API en `capa_bronze_dolares`, agregando metadata de carga (`fecha_carga`, `fuente`). Tabla particionada por día, con `WRITE_APPEND`.
- **Silver** ([transform/silver.py](transform/silver.py)): toma los registros nuevos del día desde Bronze, evitando duplicados ya existentes en Silver, y aplica limpieza:
  - normaliza texto (`casa` en minúsculas, `moneda` en mayúsculas)
  - convierte `compra`/`venta` a numérico y descarta filas inválidas
  - elimina duplicados dentro del mismo batch
  - deriva columnas `fecha` y `hora`
  - se guarda en `capa_silver_dolares` (`WRITE_APPEND`, acumula historial)
- **Gold** ([transform/gold.py](transform/gold.py)): lee todo Silver y calcula métricas de análisis:
  - `spread` y `spread_porcentual` (venta - compra)
  - `variacion_absoluta` y `variacion_porcentual` respecto al día anterior por casa (usando `LAG`)
  - `fecha_formateada` para reportes
  - se guarda en `capa_gold_dolares_analisis` (`WRITE_TRUNCATE`, siempre representa el estado más reciente)

El orquestador principal es [main.py](main.py), que corre las cuatro etapas en secuencia.

## Requisitos

- Python 3.13
- Una cuenta de servicio de Google Cloud con permisos sobre BigQuery (autenticación vía [Application Default Credentials](https://cloud.google.com/docs/authentication/provide-credentials-adc))
- Acceso a la API pública de [DolarAPI](https://dolarapi.com/)

## Instalación

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

## Configuración

Crear un archivo `.env` en la raíz del proyecto con las siguientes variables:

```bash
BASE_URL=https://dolarapi.com/v1/dolares
GCP_PROJECT_ID=tu-proyecto-gcp
BQ_DATASET=tu-dataset-bigquery
```

Estas variables son leídas por [config/config.py](config/config.py) y por cada módulo de `extract`, `load` y `transform`.

## Uso

Ejecutar el pipeline completo:

```bash
python main.py
```

También se puede correr cada etapa por separado:

```bash
python -m extract.extract
python -m load.bronze
python -m transform.silver
python -m transform.gold
```

## Estructura del proyecto

```
.
├── main.py                # Orquestador del pipeline
├── config/
│   └── config.py          # Variables de entorno y nombres de tablas
├── extract/
│   └── extract.py         # Extracción de datos desde DolarAPI
├── load/
│   └── bronze.py          # Carga de la capa Bronze en BigQuery
└── transform/
    ├── silver.py           # Limpieza y deduplicación → capa Silver
    └── gold.py             # Métricas de análisis → capa Gold
```