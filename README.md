# dolarapi-pipeline

Pipeline de datos que extrae las cotizaciones del dólar publicadas por [DolarAPI](https://dolarapi.com/) y las carga en BigQuery siguiendo una arquitectura por capas **Bronze → Silver → Gold**.

## Arquitectura

```
DolarAPI  ──▶  Bronze  ──▶  Silver  ──▶  Gold
(extract)     (raw)        (limpia)     (análisis)
```

- **Extract** ([extract/extract.py](extract/extract.py)): llama a la API y devuelve el JSON crudo con las cotizaciones de todas las casas de cambio.
- **Bronze** ([load/bronze.py](load/bronze.py)): carga los datos crudos tal cual llegan de la API en `capa_bronze_dolares`, agregando metadata de carga (`fecha_carga`, `fuente`). Tabla particionada por día, con `WRITE_APPEND`.
- **Silver** ([transform/silver.py](https://github.com/Ana-FB/dolarapi-pipeline/blob/main/transform/silver.py)): trae de Bronze todo lo posterior a la última `fecha_carga` que ya tiene Silver (o el histórico completo si está vacía), aplica limpieza (normaliza texto, renombra `casa`→`tipo_dolar`, descarta filas inválidas, convierte tipos) y deriva `fecha`/`hora` en horario Argentina. Antes de cargar, descarta valores repetidos dentro del mismo día por `tipo_dolar` (si `compra`/`venta` no cambiaron respecto al último registro cargado hoy, no se vuelve a guardar), y aplica `drop_duplicates` dentro del batch como defensa extra ante reintentos. Se guarda en `capa_silver_dolares` con `WRITE_APPEND`.
  > **Nota de diseño:** el patrón estándar de industria para este tipo de carga incremental sería un `MERGE` (upsert) con watermark, que resuelve inserción y deduplicación en una sola operación atómica. Como el proyecto corre en BigQuery Sandbox (sin billing habilitado), no hay acceso a DML (`MERGE`, `UPDATE`, `DELETE`), así que el mismo patrón se reconstruyó manualmente: filtro por `fecha_carga` como watermark + deduplicación en pandas. Con billing habilitado, este paso se migraría a un `MERGE` real.
- **Gold** ([transform/gold.py](https://github.com/Ana-FB/dolarapi-pipeline/blob/main/transform/gold.py)): lee toda la capa Silver y calcula métricas de análisis — `spread`/`spread_porcentual`, `variacion_absoluta`/`variacion_porcentual` (venta y compra, contra el registro anterior de cada `tipo_dolar` vía `groupby().shift(1)`, equivalente a un `LAG` de SQL), `precio_promedio`, `tendencia` (Sube/Baja/Sin cambios) y `brecha_oficial` (diferencia % contra el oficial de referencia del día). Descarta `fecha_carga` y `fechaActualizacion`, y se guarda en `capa_gold_dolares_analisis` con `WRITE_TRUNCATE` (recalcula toda la tabla en cada corrida, ya que las métricas dependen del historial completo).
  El orquestador principal es [main.py](main.py), que corre las cuatro etapas en secuencia.

## Dashboard

📊 [Ver dashboard interactivo en Looker Studio](https://datastudio.google.com/embed/reporting/520a9267-2a3b-4a4a-bb96-8b57d0087c81/page/sx14F)

Visualización de las cotizaciones del dólar, con evolución histórica, comparación entre casas de cambio y brecha respecto al oficial.

## Requisitos

- Python 3.13
- Un proyecto de Google Cloud con la API de BigQuery habilitada, y credenciales configuradas vía [Application Default Credentials](https://cloud.google.com/docs/authentication/provide-credentials-adc) (por ejemplo, corriendo `gcloud auth application-default login` con tu cuenta de usuario; no hace falta una service account dedicada para uso local)
- Acceso a la API pública de [DolarAPI](https://dolarapi.com/)

## Instalación

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

## Configuración

Copiar el archivo de ejemplo y completar tus valores:

```bash
cp .env.example .env
```

`.env.example`:

```bash
BASE_URL=https://dolarapi.com/v1/dolares
GCP_PROJECT_ID=
BQ_DATASET=
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
├── main.py                    # Orquestador del pipeline
├── requirements.txt           # Dependencias del proyecto
├── .env.example                # Plantilla de variables de entorno
├── .gitignore
├── config/
│   ├── __init__.py
│   └── config.py               # Variables de entorno y nombres de tablas
├── extract/
│   ├── __init__.py
│   └── extract.py              # Extracción de datos desde DolarAPI
├── load/
│   ├── __init__.py
│   └── bronze.py               # Carga de la capa Bronze en BigQuery
└── transform/
    ├── __init__.py
    ├── silver.py                # Limpieza y deduplicación → capa Silver
    └── gold.py                  # Métricas de análisis → capa Gold
```
