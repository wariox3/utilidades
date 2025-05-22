from decouple import config
import mysql.connector
from b2sdk.v2 import InMemoryAccountInfo, B2Api
from io import BytesIO
import requests
import sys

def obtener_entero(mensaje):
    while True:
        try:
            return int(input(mensaje))
        except ValueError:
            print("Error: Debe ingresar un número entero válido.")

def main():

    print("\n=== Filtro de documentos por año y mes ===")
    año = obtener_entero("Ingrese el año (ej: 2023): ")
    mes = obtener_entero("Ingrese el mes (1-12): ")
        
    if mes < 1 or mes > 12:
        print("Error: El mes debe estar entre 1 y 12.")
        return
    print(f"Procesando documentos del año {año} y mes {mes}...")    
    
    parametros = {
        'user': config('DATABASE_USER'),
        'password': config('DATABASE_CLAVE'),
        'host': config('DATABASE_HOST'),
        'port': config('DATABASE_PORT'),
        'database': 'bdenergy'
    }
    COMPRESS_SERVICE_URL = "http://boro.semantica.com.co/comprimir"
    try:
        info = InMemoryAccountInfo()
        b2_api = B2Api(info)
        b2_api.authorize_account("production", config('B2_APPLICATION_KEY_ID'), config('B2_APPLICATION_KEY'))
        bucket = b2_api.get_bucket_by_name('semantica')
        
        conexion = mysql.connector.connect(**parametros)
        cursor = conexion.cursor(dictionary=True)
        query = """
            SELECT codigo_fichero_pk, extension 
            FROM doc_fichero 
            WHERE comprimido = false 
            AND codigo_modelo_fk='TteGuia' 
            AND extension='jpg' 
            AND codigo_fichero_tipo_fk = 'G'
            AND YEAR(fecha) = %s
            AND MONTH(fecha) = %s
            LIMIT 30000
        """
        cursor.execute(query, (año, mes))
        registros = cursor.fetchall()                      
        for registro in registros:
            original_name = f"{registro['codigo_fichero_pk']}.{registro['extension']}"
            file_name = f"energy/fichero/{registro['codigo_fichero_pk']}.{registro['extension']}"
            file_version = bucket.get_file_info_by_name(file_name)
            file_id = file_version.id_

            downloaded_file = bucket.download_file_by_id(file_id)
            original_content = BytesIO()
            downloaded_file.save(original_content)
            original_content.seek(0)
            files = {'file': (original_name, original_content, f'image/{registro["extension"]}')}
            
            response = requests.post(
                COMPRESS_SERVICE_URL,
                files=files,
                data={'quality': 10, 'optimize': True}
            )        
            if response.status_code == 200:
                bucket.delete_file_version(file_id=file_id, file_name=file_name)
                print(f"Eliminado original de B2: {file_name}")

                compressed_content = BytesIO(response.content)
                bucket.upload_bytes(
                    data_bytes=compressed_content.getvalue(),
                    file_name=file_name
                )
                print(f"Subido comprimido a B2: {file_name}")
                
                compressed_size = len(response.content)
                update_query = """
                    UPDATE doc_fichero 
                    SET comprimido = true, 
                        tamano = %s,
                        extension = 'jpg'
                    WHERE codigo_fichero_pk = %s
                """
                cursor.execute(update_query, (compressed_size, registro['codigo_fichero_pk']))
                conexion.commit()
                print(f"BD actualizada para {registro['codigo_fichero_pk']}")            
            else:
                print(f"Error en el servicio de compresión fichero {registro['codigo_fichero_pk']}: {response.text}")
    except Exception as e:
        print(f"Error: {e}")
        
    finally:
        if 'conexion' in locals() and conexion.is_connected():
            conexion.close()

if __name__ == "__main__":
    main()            