from time import gmtime, strftime

def crear_lote(lista_person, nro_lote):
    try:
        # with open(f"C:/Users/Alejandra/Documents/Desarrollo/Renaper/ArchivosEntradaPy/LOTE{nro_lote}.txt", 'w') as lote:          #Ubicación del lote de pruebas
        with open(f"C:/Users/Price03/Desktop/acceso_db_oracle/proy_access_db/LOTE{nro_lote}.txt", 'w') as lote:  # Crear o sobrescribir archivo de lote
            lote.writelines(lista_person)
            # print(f"SE CREÓ EL LOTE {nro_lote} CON {len(lista_person)} REGISTROS")
            print(f"SE CREO EL LOTE {nro_lote}")
    except Exception as error:
        timestamp = strftime("%a, %d %b %Y %H:%M:%S", gmtime())
        print(f"Error al crear el lote {nro_lote}: {error}")
        with open("exceptions.txt", 'a') as exceptions:
	# se crea archivo de log con excepciones con fecha y hora
            exceptions.write(f"ERROR {timestamp} \n {error} \n ERROR AL LEER LOTE_COMPLETO: {error}\n")

def leer_lote_completo():
    CORTE = 10000  # Cantidad de registros por lote # 10000
    lista_person = []
    nro_lote = 0
    pos_lote = 0

    try:
#         # with open('C:/Users/Alejandra/Documents/Desarrollo/ReNaPer/ArchivosEntradaPy/LOTE_COMPLETO.txt','r') as personas: #esta es la ruta del q esta hoy en la pc de alejandra
        with open('C:/Users/Price03/Desktop/acceso_db_oracle/proy_access_db/LOTE_COMPLETO.txt', 'r') as personas:
            arch_persons = personas.readlines()
            tamano_total = len(arch_persons)  # 138152 en este caso

            for persona in arch_persons:
                pos_lote += 1
                lista_person.append(persona)

                # Crear un lote cuando se alcanza el corte
                if pos_lote % CORTE == 0:
                    nro_lote += 1
                    crear_lote(lista_person, nro_lote)
                    lista_person.clear()

            # Crear el último lote si quedaron registros pendientes
            if lista_person:
                nro_lote += 1
                crear_lote(lista_person, nro_lote)

            print(f"Se procesaron {tamano_total} registros en {nro_lote} lotes.")

    except Exception as error:
        timestamp = strftime("%a, %d %b %Y %H:%M:%S", gmtime())
        print(f"Error al leer LOTE_COMPLETO.txt: {error}")
#        with open("errores.txt", 'a') as log_errores:
#            log_errores.write(f"ERROR AL LEER LOTE_COMPLETO: {e}\n")
        with open("exceptions.txt", 'a') as exceptions:
	# se crea archivo de log con excepciones con fecha y hora
            exceptions.write(f"ERROR {timestamp} \n {error} \n ERROR AL LEER LOTE_COMPLETO: {error}\n")
    finally:
        return nro_lote


# Llamada a la función
# leer_lote_completo()
