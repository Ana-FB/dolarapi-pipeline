from extract.extract import extraer_datos
from load.bronze import cargar_bronze
from transform.silver import crear_silver
from transform.gold import crear_gold

def main():

    print("Iniciando pipeline")

    datos = extraer_datos()
    cargar_bronze(datos)
    crear_silver()
    crear_gold()
    print("Pipeline finalizado")

if __name__ == "__main__":
    main()