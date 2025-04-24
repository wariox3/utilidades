
from b2sdk.v2 import InMemoryAccountInfo, B2Api
from decouple import config
import os

directorio_destino = "/home/desarrollo/Escritorio/backup/"
cantidad = 500
def listar_archivos(bucket_name):    
    info = InMemoryAccountInfo()
    b2_api = B2Api(info)
    b2_api.authorize_account("production", config('B2_APPLICATION_KEY_ID'), config('B2_APPLICATION_KEY'))
    bucket = b2_api.get_bucket_by_name(bucket_name)
    archivos_descargados = 0
    for file_version, folder_name in bucket.ls():            
        file_id = file_version.id_
        file_name = file_version.file_name              
        ruta_destino = os.path.join(directorio_destino, file_name)
        print(f"Descargando: {file_name}")   
        try:
            with open(ruta_destino, 'wb') as archivo:
                bucket.download_file_by_id(file_id).save(archivo)
            print(f"Archivo guardado en: {ruta_destino}")
                        
            bucket.delete_file_version(file_id, file_name)
            print(f"Archivo eliminado de Backblaze: {file_name}")

            # Verificar si la eliminación fue exitosa
            try:
                # Intentar listar nuevamente el archivo
                bucket.get_file_info(file_id)
                print(f"Error: El archivo {file_name} sigue existiendo en Backblaze.")
            except Exception as e:
                print(f"El archivo {file_name} ha sido eliminado correctamente de Backblaze.")            
        except Exception as e:
            print(f"Error al descargar {file_name}: {e}")

        print(f"Archivo guardado en: {ruta_destino}")
        archivos_descargados += 1        
        if archivos_descargados >= cantidad:
            break
listar_archivos(config('B2_BUCKET_NAME'))