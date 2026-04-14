import mysql.connector
import sys
from decouple import config
import os
from datetime import datetime

log_file = None


def log(mensaje=""):
    global log_file
    print(mensaje)
    if log_file:
        log_file.write(mensaje + "\n")
        log_file.flush()


def conectar(database):
    try:
        conn = mysql.connector.connect(
            host=config('DATABASE_HOST'),
            user=config('DATABASE_USER'),
            password=config('DATABASE_CLAVE'),
            database=database,
            port=config('DATABASE_PORT'),
            connection_timeout=5
        )
        return conn
    except Exception as e:
        log(f"[ERROR] No se pudo conectar a {database}: {e}")
        return None


def tabla_existe(cursor):
    cursor.execute("""
        SELECT COUNT(*)
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_NAME = 'rhu_solicitud_empleado_tipo'
          AND TABLE_SCHEMA = DATABASE()
    """)
    return cursor.fetchone()[0] > 0


def analizar_base(nombre_bd, registros_globales):
    """
    Extrae registros de rhu_solicitud_empleado_tipo en nombre_bd.
    Acumula en registros_globales (dict pk -> registro) sin repetir PK.
    Retorna (encontrados, nuevos, duplicados) o None si la tabla no existe / hay error.
    """
    log(f"\n========== {nombre_bd} ==========")

    conn = conectar(nombre_bd)
    if not conn:
        return None

    try:
        cursor = conn.cursor()

        if not tabla_existe(cursor):
            log("  [INFO] Tabla no existe en esta base")
            return None

        cursor.execute("""
            SELECT codigo_solicitud_empleado_tipo_pk, nombre, habilitado_portal
            FROM rhu_solicitud_empleado_tipo
        """)
        filas = cursor.fetchall()

        encontrados = len(filas)
        nuevos = 0
        duplicados = 0

        for fila in filas:
            pk = fila[0]
            if pk not in registros_globales:
                registros_globales[pk] = (nombre_bd, fila)
                nuevos += 1
            else:
                duplicados += 1

        log(f"  Registros     : {encontrados}")
        log(f"  Nuevos únicos : {nuevos}")
        log(f"  Duplicados PK : {duplicados}")

        return encontrados, nuevos, duplicados

    except Exception as e:
        log(f"  [ERROR] En {nombre_bd}: {e}")
        return None
    finally:
        conn.close()


def main():
    global log_file

    BASE_DIR = os.path.dirname(os.path.abspath(__file__))

    fecha = datetime.now().strftime("%Y%m%d%H%M")
    ARCHIVO_RESULTADO = os.path.join(BASE_DIR, f"resultado_rhu_solicitud_empleado_tipo{fecha}.txt")

    log_file = open(ARCHIVO_RESULTADO, "w", encoding="utf-8")

    log("===============================================")
    log("ANALISIS: rhu_solicitud_empleado_tipo")
    log(f"Fecha ejecucion: {datetime.now()}")
    log("===============================================")

    try:
        ARCHIVO_BASES = os.path.join(BASE_DIR, "bases.txt")
        with open(ARCHIVO_BASES, "r", encoding="utf-8") as f:
            bases = [line.strip() for line in f if line.strip()]
    except Exception as e:
        log(f"No se pudo leer bases.txt: {e}")
        log_file.close()
        sys.exit(1)

    # pk -> (bd_origen, (codigo, nombre, habilitado_portal))
    registros_globales = {}

    total_bases = 0
    bases_con_tabla = 0
    bases_sin_tabla = 0

    for bd in bases:
        total_bases += 1
        resultado = analizar_base(bd, registros_globales)
        if resultado is None:
            bases_sin_tabla += 1
        else:
            bases_con_tabla += 1

    # --- Registros únicos ---
    log("\n\n================ REGISTROS ÚNICOS ================")
    log(f"{'CODIGO':<12} {'NOMBRE':<82} {'PORTAL':<8} BD ORIGEN")
    log("-" * 115)
    for codigo, (bd_origen, fila) in sorted(registros_globales.items()):
        _, nombre, habilitado_portal = fila
        portal = "Sí" if habilitado_portal else "No"
        log(f"{str(codigo):<12} {str(nombre or ''):<82} {portal:<8} {bd_origen}")

    log("\n\n================ RESUMEN ================")
    log(f"Bases analizadas        : {total_bases}")
    log(f"Bases con tabla         : {bases_con_tabla}")
    log(f"Bases sin tabla / error : {bases_sin_tabla}")
    log(f"Registros únicos        : {len(registros_globales)}")
    log("=========================================")
    log("\nArchivo generado en:")
    log(ARCHIVO_RESULTADO)

    log_file.close()


if __name__ == "__main__":
    main()
