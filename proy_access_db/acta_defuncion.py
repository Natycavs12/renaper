import os, cx_Oracle, requests, json
from cryptography.fernet import Fernet
from time import gmtime, strftime


# Configura la variable de entorno para el Oracle Instant Client
oracle_client_path = "C:\\oracle\\instantclient_11_2_x64"

# Establece la variable de entorno PATH temporalmente en el script de Python
os.environ["PATH"] = oracle_client_path + ";" + os.environ["PATH"]


def conection_data():
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

    # return oracle_username,oracle_password,oracle_dsn
    return config

def validar_datos(response,dni,sexo):
    code_status = response.status_code
    if code_status == 200:
        respDict = response.json()

        dni_fallecido = respDict['materiaResponse']['registros'][0]['difunto']['numeroDocumento']
        dni_autorizante = respDict['materiaResponse']['registros'][0]
        autorizante = dni_autorizante.get("autorizante")

        if autorizante == 'None' or autorizante is None :
            procesar_rta(respDict) #Se procesa igual pues se obtuvo datos de un fallecido
            return response
        else:

            if (str(dni) != str(dni_autorizante) and str(dni) == str(dni_fallecido)) : # Se procesa porque el dni buscado es distinto al dni del autorizante, es decir, es del fallecido
                procesar_rta(respDict)
                return response
            else :
                # print("ES EL DNI DEL AUTORIZANTE, NO SE PROCESA") #EL DNI DEL AGENTE ES AUTORIZANTE , NO DEL FALLECIDO
                return False

    else:
        if code_status == 500:
            error_code_status = f"REG.CIVIL / STATUS CODE: {code_status}: No se pudo obtener acceso al servidor"

        if code_status == 404:
            error_code_status = f"REG.CIVIL / STATUS CODE: {code_status}: Sin resultados"

        if code_status == 400:
            error_code_status = f"REG.CIVIL / STATUS CODE: {code_status}: Datos mal ingresados"

        if code_status == 408:
            error_code_status = f"REG.CIVIL / STATUS CODE: {code_status}: Base de datos ocupada"

        timestamp = strftime("%a, %d %b %Y %H:%M:%S", gmtime())

        with open("logs.txt", 'a') as log_errores:
        # se crea archivo de log de errores con fecha y hora
            log_errores.write(f"ERROR {timestamp} en acta_defuncion.py \n {error_code_status} \n Sucedió al obtener el ACTA DE DEFUNCION del DNI {dni} / {sexo}\n")

        return None


def get_db_connection(oracle_username,oracle_password,oracle_dsn):
    #Función para obtener la conexión a la base de datos Oracle.
    try:
        connection = cx_Oracle.connect(user=oracle_username, password=oracle_password, dsn=oracle_dsn)
        return connection

    except cx_Oracle.DatabaseError as e:
        error, = e.args
        print(f"Error de conexión a Oracle: {error.message}")
        return None


def get_idhr(connection,nroDNI):
    sql = f"SELECT STD_ID_PERSON FROM STD_PERSON WHERE STD_SSN = '{nroDNI}'"
    if connection is None:
        print(({'error': 'No se pudo conectar a la base de datos Oracle'}), 500)

    try:
        cursor = connection.cursor()
        cursor.execute(sql)
        while True:
            id_hr, = cursor.fetchone()
            if id_hr is None:
                break
        cursor.close()

    except cx_Oracle.DatabaseError as e:
        error, = e.args
        return print(f"Error al ejecutar SELECT en PL/SQL: {error.message}", 500)

    finally:
        # connection.close()
        return id_hr


# def execute_plsql(nombre,apellido,tipoDNI,nroDNI,fechaNac,fechaDef,genero):
def execute_plsql(nroDNI,fechaDef,nro_acta_gedo,tomo,acta,anio):

    # Endpoint para ejecutar una consulta A PL/SQL.
    # oracle_username,oracle_password,oracle_dsn = conection_data()
    config = conection_data()
    oracle_dsn = config['DESARROLLO']['oracle_dsn']
    oracle_username = config['DESARROLLO']['oracle_username']
    oracle_password = config['DESARROLLO']['oracle_password']
    connection = get_db_connection(oracle_username,oracle_password,oracle_dsn)

    # QUERY con placeholders
    insert = "INSERT INTO GCABA.M4CAR_HR_BLACKLIST VALUES(:1, :2, :3, :4, :5, :6, :7, :8)"

    id_hr = get_idhr(connection,nroDNI)
    comment = f"Defunción día {fechaDef} s/{nro_acta_gedo} (tomo: {tomo} - acta: {acta} - año: {anio})"

    if connection is None:
        print(({'error': 'No se pudo conectar a la base de datos Oracle'}), 500)

    try:
        cursor2 = connection.cursor()
        cursor2.execute(insert,('0080',id_hr,'II',comment,None,'M4ADM','NBBARRON',None))
        connection.commit()
        cursor2.close()
        # print(({'message': 'Datos insertados correctamente'}), 201)

    except cx_Oracle.DatabaseError as e:
        error, = e.args
        return print(f"Error al ejecutar INSERT en PL/SQL: {error.message}", 500)
    finally:
        connection.close()


def procesar_rta(respDict):
    nroDNI = respDict['materiaResponse']['registros'][0]['difunto']['numeroDocumento']
    fechaDef = respDict['materiaResponse']['registros'][0]['datosDefuncion']['fechaDefuncion']
    nro_acta_gedo = respDict['materiaResponse']['registros'][0]['datosTopograficos']['numeroActaGedo']
    tomo = respDict['materiaResponse']['registros'][0]['datosTopograficos']['tomo']
    acta = respDict['materiaResponse']['registros'][0]['datosTopograficos']['acta']
    anio = respDict['materiaResponse']['registros'][0]['datosTopograficos']['anio']
    execute_plsql(nroDNI,fechaDef,nro_acta_gedo,tomo,acta,anio)


def getCertificadoDefuncion(dni, sexo):
    config = conection_data()
    dniNro = str(dni)
    dniTipo = "DNI"
    gender = sexo

    # HEADERS Y PARAMETROS API X-ROAD REG. CIVIL PRD
    apiHML = f"https://xroad-mhfgc.buenosaires.gob.ar/r1/edicaba/GOB/017/Prov-Partidas-Prod/Actasdefuncion?numeroDocumento={dniNro}&tipoDocumento={dniTipo}&sexoDescripcion={gender}"
    headerKey = config['API_HEADERS_PRD']['headerKey']
    headerValue = config['API_HEADERS_PRD']['headerValue']

    response = ""

    headers = {
        'Content-Type': 'application/x-www-form-urlencoded',
        headerKey: headerValue
    }

    try:
        response = requests.request("GET", apiHML, headers=headers)
        print(response.json())
    except Exception as error:
        timestamp = strftime("%a, %d %b %Y %H:%M:%S", gmtime())

        with open("exceptions.txt", 'a') as exceptions:
        # se crea archivo de log con excepciones con fecha y hora
            exceptions.write(f"ERROR {timestamp} en acta_defuncion.py \n {error} \n No se pudo realizar la solicitud a la API Externa\n")

    finally:
        valido = validar_datos(response,dni,sexo)
        if valido:
            return response
        # validar_datos(response,dni)
        # return response


def open_personas(): # Abre el archivo de los fallecidos (DNI/GENERO)
    try:
        # personas = open("C:/Users/Alejandra/Documents/Desarrollo/Renaper/ArchivosEntradaPy/LOTE_ACTA_DEF.txt")         #esta es la ruta de LOTE_ACTA_DEF en la pc de alejandra
        personas = open("C:/Users/Price03/Desktop/acceso_db_oracle/proy_access_db/LOTE_ACTA_DEF.txt")     #Ubicación de LOTE_ACTA_DEF de pruebas
    except Exception as error:
        timestamp = strftime("%a, %d %b %Y %H:%M:%S", gmtime())

        with open("exceptions.txt", 'a') as exceptions:
        # se crea archivo de log con excepciones con fecha y hora
            exceptions.write(f"ERROR {timestamp} en acta_defuncion.py \n {error} \n No se pudo abrir el archivo LOTE_ACTA_DEF.txt\n")
    finally:
        return personas


#Lee el txt LOTE_ACTA_DEF y envia los parametros a la API
def leer_txt(procesados):
	personas = open_personas() # Abre el archivo LOTE_COMPLETO.txt

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
			if sexo == 'F':
				sexo = 'Femenino'
			elif sexo == 'M':
				sexo = 'Masculino'

		except Exception as error:
			timestamp = strftime("%a, %d %b %Y %H:%M:%S ", gmtime())

			with open("exceptions.txt", 'a') as exceptions:
			# se crea archivo de log con excepciones con fecha y hora
				exceptions.write(f"ERROR {timestamp} en acta_defuncion.py \n {error} \n Error al leer el DNI/Sexo desde el .TXT en la posicion N° {num}\n")

		response = getCertificadoDefuncion(dni, sexo) # VER SI EN PRD DEVUELVE EL DICCIONARIO DE DATOS
		if str(response) == '<Response [200]>':
			procesados =procesados+1
			print(f"Se insertaron los datos correctamente del agente {sexo} con DNI N° {dni}. Esto sucedió en la posición N° {num}")
		elif str(response) == '<Response [400]>' or str(response) == 'None':
			print(f"No se obtuvieron datos para el agente con DNI N° {dni} ({sexo}). Esto sucedió en la posición N° {num}")

	personas.close() #Cierro el archivo de texto "LOTE COMPLETO.txt"
	return procesados


if __name__ == "__main__":
    """
    SE LLAMA DESDE NEW_RENAPER.PY AL MéTODO getCertificadoDefuncion()
    PARA QUE CONSULTE AL REGISTRO CIVIL POR EL ACTA DE DEFUNCION.
    SE ACCEDE A LA BBDD ORACLE EN execute_plsql() (conection_data() Y get_db_connection() )
    LOS DATOS QUE SE OBTIENEN, SE INSERTAN EN LA TABLA BLACKLIST
    /X/GCABA.DIFUNTOS_TEST (desarrollo)/X/
    """
    cant_procesados = 0

    try:
        procesados = leer_txt(cant_procesados)
    except Exception as error:
        timestamp = strftime("%a, %d %b %Y %H:%M:%S", gmtime())

        with open("exceptions.txt", 'a') as exceptions:
        # se crea archivo de log con excepciones con fecha y hora
            exceptions.write(f"ERROR {timestamp} en acta_defuncion.py \n {error} \n error al ejecutar el acta_defuncion.py\n")