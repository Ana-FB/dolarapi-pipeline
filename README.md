# dolarapi-pipeline

Pipeline de datos que extrae las cotizaciones del dólar publicadas por [DolarAPI](https://dolarapi.com/) y las carga en BigQuery siguiendo una arquitectura por capas **Bronze → Silver → Gold**.

## Arquitectura

```
DolarAPI  ──▶  Bronze  ──▶  Silver  ──▶  Gold
(extract)     (raw)        (limpia)     (análisis)
```

- **Extract** ([extract/extract.py](extract/extract.py)): llama a la API y devuelve el JSON crudo con las cotizaciones de todas las casas de cambio.
- **Bronze** ([load/bronze.py](load/bronze.py)): carga los datos crudos tal cual llegan de la API en `capa_bronze_dolares`, agregando metadata de carga (`fecha_carga`, `fuente`). Tabla particionada por día, con `WRITE_APPEND`.
- **Silver** ([transform/silver.py](transform/silver.py)): trae de Bronze todo lo posterior a la última fecha de carga que ya tiene Silver (`MAX(fecha_carga)`), en vez de fijarse solo en "hoy" — así, si el pipeline no corrió algún día, esa carga se recupera sola en la próxima corrida en vez de perderse. Si Silver está vacía o no existe todavía, trae todo el histórico disponible en Bronze. Además, evita duplicados ya existentes en Silver (`NOT EXISTS`) y aplica limpieza:
  - normaliza el contenido de texto (valores de `casa` en minúsculas, valores de `moneda` en mayúsculas)
  - renombra la columna `casa` a `tipo_dolar` para mayor claridad semántica
  - elimina la columna `nombre` (redundante con `tipo_dolar`)
  - convierte `compra`/`venta` a numérico y descarta filas inválidas
  - elimina duplicados dentro del mismo batch
  - deriva columnas `fecha` y `hora`
  - se guarda en `capa_silver_dolares` (`WRITE_APPEND`, acumula historial)
- **Gold** ([transform/gold.py](transform/gold.py)): lee toda la capa Silver y calcula métricas de análisis:
  - `spread` (venta − compra) y `spread_porcentual`
  - `variacion_absoluta` y `variacion_porcentual` respecto al día anterior por casa de cambio (calculado en pandas con `groupby("casa")["venta"].shift(1)`, equivalente a un `LAG` de SQL)
  - `fecha_formateada` (`dd-mm-aaaa`) para reportes
  - descarta las columnas `fecha_carga` y `fechaActualizacion` del resultado final
  - se guarda en `capa_gold_dolares_analisis` (`WRITE_TRUNCATE`: cada corrida recalcula y reemplaza toda la tabla, ya que las métricas dependen del historial completo de Silver)

El orquestador principal es [main.py](main.py), que corre las cuatro etapas en secuencia.

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
