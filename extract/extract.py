import requests
import json
import os
from dotenv import load_dotenv

# Carga las variables del archivo .env (BASE_URL) al entorno del script
load_dotenv()


def obtener_url():
    """Lee la URL base de la API desde variables de entorno (no hardcodeada)."""
    return os.getenv("BASE_URL")


def extraer_datos():
    # Esta función hace la llamada a la API y devuelve los datos en formato JSON.
    try:
        # timeout=5: evita que el script quede colgado indefinidamente
        # si la API no responde
        response = requests.get(obtener_url(), timeout=5)
    except requests.exceptions.Timeout:
        # Se relanza (raise) para que el error no quede en silencio:

        print("La API tardó demasiado en responder")
        raise
    except requests.exceptions.ConnectionError:
        print("No se pudo establecer conexión con la API")
        raise
    except requests.exceptions.RequestException as e:
        # Captura cualquier otro error de requests no cubierto arriba
        print(f"Error inesperado: {e}")
        raise

    if response.status_code == 200:
        data = response.json()
    else:
        # La API respondió, pero con un código de error (ej. 404, 500)
        # Se distingue de las excepciones de arriba: acá SÍ hubo respuesta
        raise Exception(f"Error al obtener datos: {response.status_code}")

    return data


# Este bloque solo corre si ejecutás "python extract.py" directamente,
# no si otro archivo (ej. load.py) importa la función extraer_datos()
if __name__ == "__main__":
    resultado = extraer_datos()
    print(json.dumps(resultado, indent=4))
    print(f"Tipo de datos: {type(resultado)}")