import os
from datetime import datetime

from b2sdk.v2 import B2Api, InMemoryAccountInfo
from b2sdk.v2 import exception as b2_exception
from decouple import config

directorio_destino = config('B2_DIRECTORIO_DESTINO', default="/home/desarrollo/Escritorio/backup/")


def conectar_b2():
    info = InMemoryAccountInfo()
    b2_api = B2Api(info)
    b2_api.authorize_account(
        "production",
        config('B2_APPLICATION_KEY_ID'),
        config('B2_APPLICATION_KEY'),
    )
    return b2_api.get_bucket_by_name(config('B2_BUCKET_NAME'))


def formatear_tamano(num_bytes):
    for unidad in ("B", "KB", "MB", "GB"):
        if num_bytes < 1024:
            return f"{num_bytes:.1f} {unidad}"
        num_bytes /= 1024
    return f"{num_bytes:.1f} TB"


def descargar_archivo(bucket, file_version, ruta_local):
    """Descarga a un .part, valida el tamaño y renombra. Si algo falla no deja archivos a medias."""
    ruta_temporal = ruta_local + ".part"
    try:
        bucket.download_file_by_id(file_version.id_).save_to(ruta_temporal)
        tamano_local = os.path.getsize(ruta_temporal)
        if tamano_local != file_version.size:
            raise OSError(f"tamaño local {tamano_local} distinto al remoto {file_version.size}")
        os.replace(ruta_temporal, ruta_local)
    finally:
        if os.path.exists(ruta_temporal):
            os.remove(ruta_temporal)


def eliminar_de_b2(bucket, file_version):
    bucket.delete_file_version(file_version.id_, file_version.file_name)
    try:
        bucket.get_file_info_by_id(file_version.id_)
        return False
    except b2_exception.FileNotPresent:
        return True


def log(indice, total, mensaje):
    print(f"[{indice}/{total}] [{datetime.now():%Y-%m-%d %H:%M:%S}] {mensaje}")


def descargar_backup(anio, mes, cantidad=None):
    bucket = conectar_b2()

    prefijo = f"{anio}/{mes:02d}/"
    print(f"\n[{datetime.now():%Y-%m-%d %H:%M:%S}] Listando archivos en: {prefijo}")

    descargados = 0
    omitidos = 0
    eliminados = 0
    errores = []
    bytes_descargados = 0

    archivos = [
        file_version
        for file_version, folder_name in bucket.ls(folder_to_list=prefijo, recursive=True)
        if folder_name is None
    ]
    if cantidad is not None:
        archivos = archivos[:cantidad]
    total = len(archivos)
    print(f"[{datetime.now():%Y-%m-%d %H:%M:%S}] Archivos a procesar: {total}\n")

    for indice, file_version in enumerate(archivos, 1):
        file_name = file_version.file_name
        ruta_local = os.path.join(directorio_destino, file_name)
        os.makedirs(os.path.dirname(ruta_local), exist_ok=True)

        try:
            if os.path.exists(ruta_local) and os.path.getsize(ruta_local) == file_version.size:
                log(indice, total, f"Ya existe localmente, se omite descarga: {file_name}")
                omitidos += 1
            else:
                log(indice, total, f"Descargando: {file_name} ({formatear_tamano(file_version.size)})")
                descargar_archivo(bucket, file_version, ruta_local)
                log(indice, total, f"Guardado en: {ruta_local}")
                descargados += 1
                bytes_descargados += file_version.size

            if eliminar_de_b2(bucket, file_version):
                log(indice, total, f"Eliminado de Backblaze: {file_name}")
                eliminados += 1
            else:
                log(indice, total, f"Error: el archivo sigue existiendo en Backblaze: {file_name}")
                errores.append((file_name, "no se eliminó de Backblaze"))
        except Exception as e:
            log(indice, total, f"Error con {file_name}: {e}")
            errores.append((file_name, str(e)))

    print(f"\n{'=' * 50}")
    print(f"[{datetime.now():%Y-%m-%d %H:%M:%S}] Proceso finalizado")
    print(f"Descargados: {descargados} ({formatear_tamano(bytes_descargados)})")
    print(f"Omitidos (ya existían): {omitidos}")
    print(f"Eliminados de Backblaze: {eliminados}")
    print(f"Errores: {len(errores)}")
    for file_name, error in errores:
        print(f"  - {file_name}: {error}")


def pedir_entero(mensaje, minimo=None, maximo=None, opcional=False):
    while True:
        entrada = input(mensaje).strip()
        if not entrada and opcional:
            return None
        try:
            valor = int(entrada)
        except ValueError:
            print("Valor inválido, ingrese un número.")
            continue
        if (minimo is not None and valor < minimo) or (maximo is not None and valor > maximo):
            print(f"El valor debe estar entre {minimo} y {maximo}.")
            continue
        return valor


def mostrar_menu():
    print(f"Destino: {directorio_destino}")
    anio = pedir_entero("Año (ej: 2026): ", minimo=2000, maximo=2100)
    mes = pedir_entero("Mes (ej: 1): ", minimo=1, maximo=12)
    cantidad = pedir_entero("Límite de archivos (Enter para todos): ", minimo=1, opcional=True)
    descargar_backup(anio, mes, cantidad)


if __name__ == "__main__":
    mostrar_menu()
