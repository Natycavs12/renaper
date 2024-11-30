# Renaper
Este proyecto se basa en el consumo de un servicio externo a través de API para la obtención de datos de fallecidos y la inserción de los mismos en la Base de Datos local.

## Primeros pasos

1) Instalar: python ( y su gestor de librerias pip )

2) Crear carpeta del proyecto

3) Crear entorno virtual en la carpeta del proyecto desde CMD de windows:
python -m venv env

4) Activar el entorno virtual (en la carpeta Scripts de env):
activate.bat

5) Volver a la raíz del proyecto e instalar dependencias:
pip install flask
pip install oracledb
y las listadasa en el archivo requeriments.txt

## Notas
El proyecto incluye un script llamado **encrip_config.py** cuyo propósito es encriptar los datos de configuración necesarios para acceder a la API externa y la BBDD. Este script devuelve el archivo ´config.json.encrypted´ y la ´clave.key´ para luego poder desencriptarlo.

Para correr el proyecto se debe ejecutar el script **ejecutar_renaper.py**, el cual a su vez ejecutará **separar_lotes.py** (porque tratamos con archivos de 130K registros) y luego multiprocesará **new_renaper.py** el cual es el que realiza el procesado de archivos, las consultas pertinentes a la API y salidas de archivos finales.

El script **acta_defuncion.py** realiza la consulta a una API interna para obtener los certificados de defunción e inserta los datos en una tabla específica de una BBDD propia.
