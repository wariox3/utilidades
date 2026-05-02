
from b2sdk.v2 import InMemoryAccountInfo, B2Api
from decouple import config
import os

directorio_destino = "/home/desarrollo/Escritorio/backup/"

def descargar_backup(anio, mes, cantidad=None):
    info = InMemoryAccountInfo()
    b2_api = B2Api(info)
    b2_api.authorize_account("production", config('B2_APPLICATION_KEY_ID'), config('B2_APPLICATION_KEY'))
    bucket = b2_api.get_bucket_by_name(config('B2_BUCKET_NAME'))

    prefijo = f"{anio}/{mes:02d}/"
    print(f"Listando archivos en: {prefijo}")

    archivos_descargados = 0
    for file_version, folder_name in bucket.ls(folder_to_list=prefijo, recursive=True):
        if folder_name is not None:
            continue
        file_id = file_version.id_
        file_name = file_version.file_name

        ruta_local = os.path.join(directorio_destino, file_name)
        os.makedirs(os.path.dirname(ruta_local), exist_ok=True)

        print(f"Descargando: {file_name}")
        try:
            with open(ruta_local, 'wb') as archivo:
                bucket.download_file_by_id(file_id).save(archivo)
            print(f"Archivo guardado en: {ruta_local}")

            bucket.delete_file_version(file_id, file_name)
            print(f"Archivo eliminado de Backblaze: {file_name}")

            try:
                bucket.get_file_info(file_id)
                print(f"Error: El archivo {file_name} sigue existiendo en Backblaze.")
            except Exception:
                print(f"El archivo {file_name} ha sido eliminado correctamente de Backblaze.")

            archivos_descargados += 1
            if cantidad is not None and archivos_descargados >= cantidad:
                break
        except Exception as e:
            print(f"Error al descargar {file_name}: {e}")

    print(f"\nTotal descargados: {archivos_descargados}")

def mostrar_menu():
    anio = int(input("Año (ej: 2026): ").strip())
    mes = int(input("Mes (ej: 1): ").strip())
    entrada = input("Límite de archivos (Enter para todos): ").strip()
    cantidad = int(entrada) if entrada else None
    descargar_backup(anio, mes, cantidad)

if __name__ == "__main__":
    mostrar_menu()
