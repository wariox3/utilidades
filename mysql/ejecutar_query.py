import os
import sys
import mysql.connector
from decouple import config
from datetime import datetime

# ============================================================
# DEFINE AQUI LA QUERY A EJECUTAR EN TODAS LAS BASES DEL TXT
# Puede contener varias sentencias separadas por ';'
# ============================================================
QUERY = """
DROP TABLE IF EXISTS fin_saldo;
"""

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ARCHIVO_BASES = os.path.join(BASE_DIR, "bases.txt")

# manejador global del archivo de resultado
log_file = None


def log(mensaje=""):
    """Escribe en consola y en archivo de resultado"""
    print(mensaje)
    if log_file:
        log_file.write(mensaje + "\n")
        log_file.flush()


def conectar(database):
    try:
        return mysql.connector.connect(
            host=config('DATABASE_HOST'),
            user=config('DATABASE_USER'),
            password=config('DATABASE_CLAVE'),
            database=database,
            port=config('DATABASE_PORT'),
            connection_timeout=5
        )
    except Exception as e:
        log(f"[ERROR] No se pudo conectar a {database}: {e}")
        return None


def ejecutar_en_base(nombre_bd, sentencias):
    """Ejecuta las sentencias en una base. Devuelve True si todas corrieron."""
    conn = conectar(nombre_bd)
    if conn is None:
        return False

    try:
        cursor = conn.cursor()
        for sentencia in sentencias:
            cursor.execute(sentencia)
        conn.commit()
        cursor.close()
        log(f"[OK]    {nombre_bd}")
        return True
    except Exception as e:
        conn.rollback()
        log(f"[FALLO] {nombre_bd} -> {e}")
        return False
    finally:
        conn.close()


def main():
    global log_file

    # separar la query en sentencias individuales
    sentencias = [s.strip() for s in QUERY.split(";") if s.strip()]
    if not sentencias:
        print("[AVISO] La constante QUERY esta vacia. Define la query y vuelve a ejecutar.")
        sys.exit(0)

    # leer bases
    try:
        with open(ARCHIVO_BASES, "r", encoding="utf-8") as f:
            bases = [line.strip() for line in f if line.strip()]
    except FileNotFoundError:
        print(f"[ERROR] No existe {ARCHIVO_BASES}. Ejecuta primero exportar_bases.py")
        sys.exit(1)

    if not bases:
        print(f"[AVISO] {ARCHIVO_BASES} no tiene bases.")
        sys.exit(0)

    # confirmacion antes de tocar las bases
    print("===============================================")
    print(f"Servidor : {config('DATABASE_HOST')}:{config('DATABASE_PORT')}")
    print(f"Bases    : {len(bases)}")
    print("Sentencias a ejecutar:")
    for s in sentencias:
        print(f"  - {s};")
    print("===============================================")
    respuesta = input("Escribe SI para ejecutar en TODAS las bases: ").strip()
    if respuesta != "SI":
        print("Cancelado.")
        sys.exit(0)

    # abrir archivo de resultado
    fecha = datetime.now().strftime("%Y%m%d%H%M")
    archivo_resultado = os.path.join(BASE_DIR, f"resultado_query_{fecha}.txt")
    log_file = open(archivo_resultado, "w", encoding="utf-8")

    log("===============================================")
    log("EJECUCION DE QUERY EN MULTIPLES BASES")
    log(f"Fecha ejecucion: {datetime.now()}")
    log(f"Servidor: {config('DATABASE_HOST')}:{config('DATABASE_PORT')}")
    for s in sentencias:
        log(f"Sentencia: {s};")
    log("===============================================")

    exitosas = []
    fallidas = []
    for nombre_bd in bases:
        if ejecutar_en_base(nombre_bd, sentencias):
            exitosas.append(nombre_bd)
        else:
            fallidas.append(nombre_bd)

    # resumen
    log("\n===============================================")
    log("RESUMEN")
    log(f"  Corrio OK en  : {len(exitosas)} bases")
    log(f"  Fallo en      : {len(fallidas)} bases")
    if fallidas:
        log("  Bases con fallo:")
        for b in fallidas:
            log(f"    - {b}")
    log("===============================================")
    log(f"Detalle guardado en: {archivo_resultado}")

    log_file.close()


if __name__ == "__main__":
    main()
