from cryptography.fernet import Fernet

# Genera una clave de cifrado
key = Fernet.generate_key()

# Guarda la clave en un archivo seguro
with open('clave.key', 'wb') as clave_file:
    clave_file.write(key)

# Inicializa Fernet con la clave
cipher_suite = Fernet(key)

# Lee el contenido del archivo de configuración
with open('ej_config.json', 'rb') as file:
    config_data = file.read()

# Cifra los datos
encrypted_data = cipher_suite.encrypt(config_data)

# Escribe los datos cifrados en un archivo nuevo
with open('config.json.encrypted', 'wb') as encrypted_file:
    encrypted_file.write(encrypted_data)

print("Archivo cifrado con éxito.")
