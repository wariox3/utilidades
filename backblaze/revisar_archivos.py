from decouple import config
import mysql.connector
from b2sdk.v2 import InMemoryAccountInfo, B2Api, exception as b2_exception
from datetime import datetime
from io import BytesIO


def conectar_mysql():
    return mysql.connector.connect(
        user=config('DATABASE_USER'),
        password=config('DATABASE_CLAVE'),
        host=config('DATABASE_HOST'),
        port=config('DATABASE_PORT'),
        database=config('DATABASE_NAME'),
    )


def conectar_b2():
    info = InMemoryAccountInfo()
    b2_api = B2Api(info)
    b2_api.authorize_account(
        "production",
        config('B2_APPLICATION_KEY_ID'),
        config('B2_APPLICATION_KEY'),
    )
    return b2_api.get_bucket_by_name(config('B2_BUCKET_NAME'))


def construir_ruta(registro):
    directorio = config('B2_DIRECTORIO_ALMACENAMIENTO')
    pk = registro['codigo_archivo_pk']
    extension = registro.get('extension_original') or ''
    return f"{directorio}/archivo/{pk}.{extension}"


def revisar():
    conexion = None
    try:
        print(f"\n[{datetime.now():%Y-%m-%d %H:%M:%S}] Conectando a MySQL y Backblaze...")
        conexion = conectar_mysql()
        cursor = conexion.cursor(dictionary=True)
        bucket = conectar_b2()

        cursor.execute(
            "SELECT codigo_archivo_pk, directorio, nombre, extension_original, tamano, error_archivo "
            "FROM doc_archivo ORDER BY codigo_archivo_pk"
        )
        registros = cursor.fetchall()
        total = len(registros)
        print(f"Registros a procesar: {total}\n")

        encontrados = 0
        no_encontrados = 0
        inconsistentes = 0

        for i, reg in enumerate(registros, 1):
            pk = reg['codigo_archivo_pk']
            ruta = construir_ruta(reg)
            tamano_bd = reg.get('tamano') or 0.0

            try:
                file_version = bucket.get_file_info_by_name(ruta)
                diferencia = abs(file_version.size - tamano_bd)
                if diferencia > 0:
                    inconsistentes += 1
                    estado = f"INCONSISTENTE  bd={tamano_bd:.0f}B  b2={file_version.size}B  diff={diferencia:.0f}B"
                    cursor.execute(
                        "UPDATE doc_archivo SET error_archivo = true WHERE codigo_archivo_pk = %s",
                        (pk,)
                    )
                    conexion.commit()
                else:
                    encontrados += 1
                    estado = "OK"
            except b2_exception.FileNotPresent:
                no_encontrados += 1
                estado = "NO ENCONTRADO EN B2"
                cursor.execute(
                    "UPDATE doc_archivo SET error_archivo = true WHERE codigo_archivo_pk = %s",
                    (pk,)
                )
                conexion.commit()
            except KeyError:
                encontrados += 1
                estado = "OK (sin content-type)"
            except Exception as e:
                no_encontrados += 1
                estado = f"ERROR: {e}"

            print(f"[{i:>6}/{total}] pk={pk:<10} {estado:<50} {ruta}")

        print(f"\n{'='*60}")
        print(f"Total procesados : {total}")
        print(f"Correctos        : {encontrados}")
        print(f"No encontrados   : {no_encontrados}")
        print(f"Inconsistentes   : {inconsistentes}")
        print(f"{'='*60}")

    except Exception as e:
        print(f"Error general: {e}")
    finally:
        if conexion and conexion.is_connected():
            conexion.close()


def leer_directorio(directorio_principal):
    prefijo = f"{directorio_principal}/archivo/"
    nombre_archivo = f"backblaze/salida/{directorio_principal}_{datetime.now():%Y%m%d_%H%M%S}.txt"

    try:
        print(f"\n[{datetime.now():%Y-%m-%d %H:%M:%S}] Conectando a Backblaze...")
        bucket = conectar_b2()

        print(f"Leyendo prefijo: {prefijo}")
        print(f"Exportando a: {nombre_archivo}\n")

        total = 0
        with open(nombre_archivo, 'w', encoding='utf-8') as f:
            f.write(f"Directorio: {prefijo}\n")
            f.write(f"Generado: {datetime.now():%Y-%m-%d %H:%M:%S}\n")
            f.write("=" * 80 + "\n")
            for file_version, _ in bucket.ls(prefijo, recursive=True):
                total += 1
                linea = f"{file_version.file_name}  {file_version.size}B  {file_version.id_}\n"
                f.write(linea)
                print(f"[{total:>6}] {file_version.file_name}")
            f.write("=" * 80 + "\n")
            f.write(f"Total archivos: {total}\n")

        print(f"\nTotal archivos encontrados: {total}")
        print(f"Exportado a: {nombre_archivo}")

    except Exception as e:
        print(f"Error general: {e}")


def cruzar_directorios():
    origen = "eurovic/archivo/"
    destinos = ["eurovicmedellin/archivo/", "eurovicbogota/archivo/"]
    nombre_archivo = f"backblaze/salida/cruce_{datetime.now():%Y%m%d_%H%M%S}.txt"

    try:
        print(f"\n[{datetime.now():%Y-%m-%d %H:%M:%S}] Conectando a Backblaze...")
        bucket = conectar_b2()

        print(f"Leyendo archivos en: {origen}")
        archivos_origen = {
            fv.file_name.split('/')[-1]: fv
            for fv, _ in bucket.ls(origen, recursive=True)
        }
        total_origen = len(archivos_origen)
        print(f"Archivos encontrados en origen: {total_origen}\n")

        solo_origen = 0
        en_todos = 0
        parciales = 0

        with open(nombre_archivo, 'w', encoding='utf-8') as f:
            f.write(f"Cruce de directorios B2\n")
            f.write(f"Origen  : {origen}\n")
            for d in destinos:
                f.write(f"Destino : {d}\n")
            f.write(f"Generado: {datetime.now():%Y-%m-%d %H:%M:%S}\n")
            f.write("=" * 100 + "\n")
            encabezado = f"{'ARCHIVO':<40} {'ORIGEN':<8} {'MEDELLIN':<12} {'BOGOTA':<10}\n"
            f.write(encabezado)
            f.write("-" * 100 + "\n")

            for i, (nombre, fv_origen) in enumerate(archivos_origen.items(), 1):
                presencia = []
                for destino in destinos:
                    ruta_destino = f"{destino}{nombre}"
                    try:
                        bucket.get_file_info_by_name(ruta_destino)
                        presencia.append("SI")
                    except b2_exception.FileNotPresent:
                        presencia.append("NO")
                    except Exception as e:
                        presencia.append(f"ERR")

                col_origen = "SI"
                col_med = presencia[0]
                col_bog = presencia[1]

                if col_med == "SI" and col_bog == "SI":
                    en_todos += 1
                elif col_med == "NO" and col_bog == "NO":
                    solo_origen += 1
                else:
                    parciales += 1

                linea = f"{nombre:<40} {col_origen:<8} {col_med:<12} {col_bog:<10}\n"
                f.write(linea)
                print(f"[{i:>6}/{total_origen}] {nombre:<40} medellin={col_med}  bogota={col_bog}")

            f.write("=" * 100 + "\n")
            f.write(f"Total origen          : {total_origen}\n")
            f.write(f"En los tres           : {en_todos}\n")
            f.write(f"Solo en origen        : {solo_origen}\n")
            f.write(f"Parcial (uno de dos)  : {parciales}\n")

        print(f"\n{'='*60}")
        print(f"Total origen         : {total_origen}")
        print(f"En los tres          : {en_todos}")
        print(f"Solo en origen       : {solo_origen}")
        print(f"Parcial (uno de dos) : {parciales}")
        print(f"Exportado a          : {nombre_archivo}")
        print(f"{'='*60}")

    except Exception as e:
        print(f"Error general: {e}")


def replicar_faltantes():
    origen = "eurovic/archivo/"
    destinos = ["eurovicmedellin/archivo/", "eurovicbogota/archivo/"]
    nombre_archivo = f"backblaze/salida/replicar_{datetime.now():%Y%m%d_%H%M%S}.txt"

    try:
        print(f"\n[{datetime.now():%Y-%m-%d %H:%M:%S}] Conectando a Backblaze...")
        bucket = conectar_b2()

        print(f"Leyendo archivos en: {origen}")
        archivos_origen = {
            fv.file_name.split('/')[-1]: fv
            for fv, _ in bucket.ls(origen, recursive=True)
        }
        total_origen = len(archivos_origen)
        print(f"Archivos en origen: {total_origen}\n")

        copiados = 0
        ya_existian = 0
        errores = 0

        with open(nombre_archivo, 'w', encoding='utf-8') as f:
            f.write(f"Replicación eurovic → eurovicmedellin / eurovicbogota\n")
            f.write(f"Generado: {datetime.now():%Y-%m-%d %H:%M:%S}\n")
            f.write("=" * 100 + "\n")

            for i, (nombre, fv_origen) in enumerate(archivos_origen.items(), 1):
                for destino in destinos:
                    ruta_destino = f"{destino}{nombre}"
                    try:
                        bucket.get_file_info_by_name(ruta_destino)
                        ya_existian += 1
                        estado = f"YA EXISTE   {ruta_destino}"
                    except KeyError:
                        ya_existian += 1
                        estado = f"YA EXISTE   {ruta_destino} (sin content-type)"
                    except b2_exception.FileNotPresent:
                        try:
                            contenido = BytesIO()
                            bucket.download_file_by_id(fv_origen.id_).save(contenido)
                            bucket.upload_bytes(
                                data_bytes=contenido.getvalue(),
                                file_name=ruta_destino,
                            )
                            copiados += 1
                            estado = f"COPIADO     {ruta_destino}"
                        except KeyError:
                            errores += 1
                            estado = f"OMITIDO     {ruta_destino} (origen sin content-type)"
                        except Exception as e:
                            errores += 1
                            estado = f"ERROR COPIA {ruta_destino}: {e}"
                    except Exception as e:
                        errores += 1
                        estado = f"ERROR       {ruta_destino}: {e}"

                    f.write(f"{nombre:<40} {estado}\n")
                    print(f"[{i:>6}/{total_origen}] {nombre:<40} {estado}")

            f.write("=" * 100 + "\n")
            f.write(f"Total origen  : {total_origen}\n")
            f.write(f"Copiados      : {copiados}\n")
            f.write(f"Ya existían   : {ya_existian}\n")
            f.write(f"Errores       : {errores}\n")

        print(f"\n{'='*60}")
        print(f"Total origen  : {total_origen}")
        print(f"Copiados      : {copiados}")
        print(f"Ya existían   : {ya_existian}")
        print(f"Errores       : {errores}")
        print(f"Exportado a   : {nombre_archivo}")
        print(f"{'='*60}")

    except Exception as e:
        print(f"Error general: {e}")


def mostrar_menu():
    while True:
        print("\n=== Backblaze - Archivos ===")
        print("1. Revisar archivos (MySQL vs B2)")
        print("2. Cruzar eurovic vs eurovicmedellin y eurovicbogota")
        print("3. Leer directorio eurovic y exportar a txt")
        print("4. Leer directorio eurovicbogota y exportar a txt")
        print("5. Leer directorio eurovicmedellin y exportar a txt")
        print("6. Replicar faltantes de eurovic a eurovicmedellin y eurovicbogota")
        print("0. Salir")
        opcion = input("\nOpción: ").strip()

        if opcion == '1':
            revisar()
        elif opcion == '2':
            cruzar_directorios()
        elif opcion == '3':
            leer_directorio('eurovic')
        elif opcion == '4':
            leer_directorio('eurovicbogota')
        elif opcion == '5':
            leer_directorio('eurovicmedellin')
        elif opcion == '6':
            replicar_faltantes()
        elif opcion == '0':
            break
        else:
            print("Opción no válida.")


if __name__ == "__main__":
    mostrar_menu()
