import concurrent.futures, multiprocessing, subprocess, sys
from separar_lotes import leer_lote_completo

# Función para procesar un lote de datos
def procesar_lote(lote):
    # Lógica de procesamiento intensiva en CPU
    print(f"Procesando lote {lote}")

    subprocess.run([sys.executable, "new_renaper.py","--lotes",lote]) #Llamada a new_renaper.py para ejecutar desde "main"
    return f"Resultado del lote {lote}"


if __name__ == '__main__':
    global cant_lotes
    cant_lotes = leer_lote_completo() #Separa el lote_completo.txt en lotes de 10K y devuelve la cantidad de lotes generados

    multiprocessing.freeze_support()  # "congela" el script
    print("cant lotes devuelto por separar_lotes",cant_lotes)

    try:
        # Verificar el intérprete de Python
        # print(f"Usando Python en: {sys.executable}")
    
        # Crear un pool de procesos, asegurándo que usa el mismo ejecutable de Python
        pool = multiprocessing.get_context('spawn').Pool()

        lotes = [f"{i}" for i in range(1, cant_lotes+1)]  # Lista de lotes

        # Usar ProcessPoolExecutor para procesamiento paralelo
        # with multiprocessing.Pool() as pool:
        #     resultados = pool.map(procesar_lote, lotes)

        # Usar ProcessPoolExecutor para procesamiento paralelo en múltiples procesos
        with concurrent.futures.ProcessPoolExecutor() as executor:
            pool.map(procesar_lote, lotes)

    except Exception as exception:
        print(exception)

"""
1) ejecuto separar_lotes.py // q devuelve cant de lotes creados a ejecutar
2) llamo desde el executor al new_renaper
3) ejecuto el acta de defuncion con el lote de fallecidos y cargo los datos en la tabla de blacklist
"""
