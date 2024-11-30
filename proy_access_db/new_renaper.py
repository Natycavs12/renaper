import os, requests, json, time
from cryptography.fernet import Fernet
from time import gmtime, strftime
from acta_defuncion import getCertificadoDefuncion #IMPORTACION DE METODO DE acta_defuncion.PY PARA PODER INVOCARLO
import argparse

#Tiempo de inicio del proceso
start_time = time.time()

# Configura la variable de entorno para el Oracle Instant Client
oracle_client_path = "C:\\oracle\\instantclient_11_2_x64"

# Establece la variable de entorno PATH temporalmente en el script de Python
os.environ["PATH"] = oracle_client_path + ";" + os.environ["PATH"]


def salida_txt(nombre,apellido,cuil,fechaNac,provincia,fechaFallec,dniNro, gender): # Crea archivo resumen con los datos obtenidos de cada difunto 
    try:
        vacio = os.stat("resumen_fallecidos.txt").st_size == 0

        with open("resumen_fallecidos.txt", 'a') as fallecidos:        # Abre el archivo y añade el contenido al final

            if vacio:
                fallecidos.write("Nombre/s;Apellido/s;CUIL;Fecha_Nacimiento;Cdad/Prov;Fecha_Defuncion\n") # Inserta los datos del difunto en el txt

            fallecidos.write(f"{nombre};{apellido};{cuil};{fechaNac};{provincia};{fechaFallec}\n") # Inserta los datos del difunto en el txt
    except Exception as error:
        timestamp = strftime("%a, %d %b %Y %H:%M:%S", gmtime())

        with open("exceptions.txt", 'a') as exceptions:
        # se crea archivo de log con excepciones con fecha y hora
            exceptions.write(f"ERROR {timestamp} en new_renaper.py \n {error} \n Ocurrió un error al leer/escribir el archivo de resumen \n")

    try:
        with open("C:/Users/Alejandra/Documents/Desarrollo/Renaper/ArchivosEntradaPy/LOTE_ACTA_DEF.txt", 'a') as acta:        # Abre el archivo y añade el contenido al final

            acta.write(f"{dniNro}\t{gender}\n") # Inserta los datos del difunto en el txt

    except Exception as error:
        timestamp = strftime("%a, %d %b %Y %H:%M:%S", gmtime())

        with open("exceptions.txt", 'a') as exceptions:
        # se crea archivo de log con excepciones con fecha y hora
            exceptions.write(f"ERROR {timestamp} en new_renaper.py \n {error} \n Ocurrió un error al leer/escribir el archivo para Acta_De_Defunción \n")


def procesar_rta(respDict,dniNro, gender):
    nombre = respDict['nombres']
    apellido = respDict['apellido']
    cuil = respDict['cuil']
    fechaNac = respDict['fechaNacimiento']
    provincia = respDict['provincia']
    fechaFallec = respDict['fechaf']
    salida_txt(nombre,apellido,cuil,fechaNac,provincia,fechaFallec,dniNro, gender) # pegamos los valores en el txt RESUMEN_FALLECIDOS para pasar a .csv 
    # salida_txt(nombre,apellido,tipoDNI,nroDNI,fechaNac,fechaDef,genero)


def validar_datos(response,dniNro, gender):
    code_status = response.status_code
    respDict = response.json()
    error_code_status = ""

    if code_status == 200:
        try:
            data_fallecido = respDict['mensaf']

            if data_fallecido == 'FALLECIDO': #Se procesa pues se obtuvieron datos de un fallecido
                procesar_rta(respDict,dniNro, gender)  # llama a metodo que genera la salida a txt
                #getCertificadoDefuncion(dni, sexo) # llama a metodo que realiza la consulta a API REGISTRO CIVIL e inserta el ACTA de DEFUNCION en la BBDD 
                return True
            elif data_fallecido == 'Sin Aviso de Fallecimiento':
                return False

        except Exception as error:
            timestamp = strftime("%a, %d %b %Y %H:%M:%S", gmtime())

            with open("exceptions.txt", 'a') as exceptions:
			# se crea archivo de log con excepciones con fecha y hora
                exceptions.write(f"ERROR {timestamp} en new_renaper.py \n {error} \n Sucedió al consultar a RENAPER con el DNI {dni} / {sexo}\n")

    else: # LOS STATUS_CODE VAN A UN ARCHIVO DE LOG con fecha y hora
        try:
            cod_error = respDict['code']
            if code_status == 400:
                error_code_status = f"RNP / STATUS CODE: {code_status}: Datos mal ingresados"
            if code_status == 401:
                error_code_status = f"RNP / STATUS CODE: {code_status} \n código de error {cod_error}: Unauthorized / No se envió el token"
            if code_status == 404:
                error_code_status = f"RNP / STATUS CODE: {code_status} \n código de error {cod_error}: Sin resultados"

            if code_status == 408:
                error_code_status = f"RNP / STATUS CODE: {code_status} \n código de error {cod_error}: Base de datos ocupada"

            if code_status == 409:
                # error_code_status = f"RNP / STATUS CODE: {code_status}: Error al obtener el dato desde RENAPER"

                if cod_error == '10001':
                    error_code_status = f"RNP / STATUS CODE: {code_status} \n código de error {cod_error}: Gender not provided / No se envió el parámetro gender"

                elif cod_error == '10002':
                    error_code_status = f"RNP / STATUS CODE: {code_status} \n código de error {cod_error}: El género debe ser uno de [F,M,X] / Se ingresó un género distinto a [F,M,X]"

                elif cod_error == '10004':
                    error_code_status = f"RNP / STATUS CODE: {code_status} \n código de error {cod_error}: Error obtenido dato de RENAPER / Se ingresó un dato de DNI que no está en login y RENAPER no respondió"

            if code_status == 500:
                if cod_error == '10003':
                    error_code_status = f"RNP / STATUS CODE: {code_status} \n código de error {cod_error}: El N° de documento debe ser un valor numérico / No se envió el parámetro document_number o se envió un dato NO numérico"

            timestamp = strftime("%a, %d %b %Y %H:%M:%S", gmtime())

            with open("codigos_error.txt", 'a') as log_errores:
            # se crea archivo de log de errores con fecha y hora
                log_errores.write(f"ERROR {timestamp} en new_renaper.py \n {error_code_status} \n Sucedió al consultar a RENAPER con el DNI {dni} / {sexo}\n")

            return None
        except Exception as error:
            timestamp = strftime("%a, %d %b %Y %H:%M:%S", gmtime())

            with open("exceptions.txt", 'a') as exceptions:
			# se crea archivo de log con excepciones con fecha y hora
                exceptions.write(f"ERROR {timestamp} en new_renaper.py \n {error} \n Sucedió al consultar a RENAPER con el DNI {dni} / {sexo}\n")


def open_config():
    # Cargar la clave de cifrado
    with open('clave.key', 'rb') as clave_file:
        key = clave_file.read()

    # Inicializa Fernet con la clave
    cipher_suite = Fernet(key)

    # Lee los datos cifrados
    with open('config.json.encrypted', 'rb') as encrypted_file:
        encrypted_data = encrypted_file.read()

    # Desencripta los datos
    decrypted_data = cipher_suite.decrypt(encrypted_data)

    # Configuración de la conexión a la base de datos Oracle
    config = json.loads(decrypted_data.decode('utf-8'))

    return config


def getDatosRenaper(dni, sexo):
    response = ""
    config = open_config()
    dniNro = str(dni)
    gender = str(sexo)

    # HEADERS Y PARAMETROS API RENAPER PRD
    apiHML = f"https://xroad-mhfgc.buenosaires.gob.ar/r1/edicaba/GOB/002/Prov-MIBA-Prod/getInfoRenaper?document_number={dniNro}&gender={gender}"
    headerKey = config['API_HEADERS_PRD']['headerKey']
    headerValue = config['API_HEADERS_PRD']['headerValue']
    client_id = config['CLIENT_PRD']['client_id']
    client_secret = config['CLIENT_PRD']['client_secret']

    headers = {
        'Content-Type': 'application/x-www-form-urlencoded',
        headerKey: headerValue,
        'client_id': client_id,
        'client_secret': client_secret
    }

    # Realiza la request a la API
    try:
        response = requests.request("GET", apiHML, headers=headers)

    except Exception as error:
        timestamp = strftime("%a, %d %b %Y %H:%M:%S", gmtime())

        with open("exceptions.txt", 'a') as exceptions:
        # se crea archivo de log con excepciones con fecha y hora
            exceptions.write(f"ERROR {timestamp} en new_renaper.py \n {error} Error al realizar la consulta a la API\n")

    finally:
        valido = validar_datos(response,dniNro, gender )
        if valido:
            return response
        else:
            return None


def open_personas(lote): # Abre cada mini-lote
    try:
        # personas = open(f"C:/Users/Alejandra/Documents/Desarrollo/Renaper/ArchivosEntradaPy/LOTE{lote}.txt")         #esta es la ruta de los mini-lotes (10k) en la pc de alejandra
        personas = open(f"C:/Users/Price03/Desktop/acceso_db_oracle/proy_access_db/LOTE{lote}.txt")     #Ubicación de los mini-lotes (1k) de pruebas 
        print(f"Se abrio el lote {lote}")
    except Exception as error:
        timestamp = strftime("%a, %d %b %Y %H:%M:%S", gmtime())

        with open("exceptions.txt", 'a') as exceptions:
        # se crea archivo de log con excepciones con fecha y hora
            exceptions.write(f"ERROR {timestamp} en new_renaper.py \n {error} \n No se pudo abrir el archivo LOTE{lote}.txt\n")
    finally:
        return personas


#Lee el txt LOTE_COMPLETO y envia los parametros a la API
def leer_txt(procesados, nro_lote):
	# print("argumentos: procesados",procesados,"nro_lote",nro_lote)
	personas = open_personas(nro_lote) # Abre el archivo LOTE_COMPLETO.txt

	#Declaramos las variables
	num = 0
	global dni
	global sexo

	#Recorremos el archivo de texto para leer los documentos y el sexo, y devuelvo la informacion solicitada
	for persona in personas.readlines():
		num += 1
		try:
			dni = int(persona.rstrip('\n').split()[0])
			sexo = persona.rstrip('\n').split()[1]
			if sexo == '':
				sexo = 'X'
		except Exception as error:
			timestamp = strftime("%a, %d %b %Y %H:%M:%S", gmtime())

			with open("exceptions.txt", 'a') as exceptions:
			# se crea archivo de log con excepciones con fecha y hora
				exceptions.write(f"ERROR {timestamp} en new_renaper.py \n {error} \n Error al leer el DNI/Sexo desde el .TXT en la posicion N° {num}\n")

		response = getDatosRenaper(dni, sexo) # VER SI EN PRD DEVUELVE EL DICCIONARIO DE DATOS

		if str(response) == '<Response [200]>':
			procesados =procesados+1
			print(f"Se insertaron los datos correctamente del agente {sexo} con DNI N° {dni}. Esto sucedió en la posición N° {num} del lote {nro_lote}")
		elif str(response) == '<Response [400]>' or str(response) == 'None':
			print(f"No se obtuvieron datos para el agente con DNI N° {dni} ({sexo}). Esto sucedió en la posición N° {num} del lote {nro_lote}")

	personas.close() #Cierro el archivo de texto "LOTE COMPLETO.txt"
	return procesados


if __name__ == "__main__":
    # Parsear los argumentos de la línea de comandos
    parser = argparse.ArgumentParser(description='Procesar lotes de datos.')
    parser.add_argument('--lotes', type=int, default=1, help='Cantidad de lotes a procesar', required=True)
    
    # Obtener los argumentos pasados
    args = parser.parse_args()
    nro_lote = args.lotes
    # print("cant_lotes en new_renaper:",nro_lote)
    cant_procesados = 0
    procesados = 0
    # nro_lote = 1

    # 1) y 2) Ejecutamos el metodo que lee los datos del TXT y obtenemos el response de los fallecidos
    # 3) procesar los datos obtenidos
    try:
        procesados = leer_txt(cant_procesados,nro_lote)
    except Exception as error:
        timestamp = strftime("%a, %d %b %Y %H:%M:%S", gmtime())

        with open("exceptions.txt", 'a') as exceptions:
        # se crea archivo de log con excepciones con fecha y hora
            exceptions.write(f"ERROR {timestamp} en new_renaper.py \n {error} \n error al ejecutar el new_renaper\n")

    #Cacula el tiempo que tardo en ejecutar el proceso
    time_exec = time.time() - start_time

    print("\nProceso finalizado.")
    print(f"El tiempo de ejecucion fue de {time_exec} segundos")
    print(f"Se procesaron {procesados} registros")

    """
    1) LEER TXT LOTE_COMPLETO
    2) OBTENER DATOS DE CADA FALLECIDO
        A) PROCESAR Y VALIDAR SI CORRESPONDE A UN FALLECIDO
    3) SALIDA A TXT
    4) LLAMADA A PROCESO DE ACTA_DE_DEFUNCION
	A) VALIDAR Y PROCESAR SI SE OBTIENE EL ACTA
	B) CONECTAR E INSERTAR LOS DATOS DEL ACTA EN LA BBDD
    """
