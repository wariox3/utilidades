import os
import sys
import mysql.connector
from decouple import config

# bases del sistema que se excluyen del listado
BASES_SISTEMA = {"information_schema", "mysql", "performance_schema", "sys"}

# archivo de salida (mismo directorio del script)
ARCHIVO_SALIDA = os.path.join(os.path.dirname(os.path.abspath(__file__)), "bases.txt")


def conectar():
    try:
        return mysql.connector.connect(
            host=config('DATABASE_HOST'),
            user=config('DATABASE_USER'),
            password=config('DATABASE_CLAVE'),
            port=config('DATABASE_PORT'),
            connection_timeout=5
        )
    except Exception as e:
        print(f"[ERROR] No se pudo conectar al servidor: {e}")
        return None


def obtener_bases(conn):
    cursor = conn.cursor()
    cursor.execute("SHOW DATABASES")
    bases = [fila[0] for fila in cursor.fetchall()]
    cursor.close()
    # se descartan las bases del sistema y se ordenan alfabeticamente
    return sorted(b for b in bases if b not in BASES_SISTEMA)


def exportar(bases):
    with open(ARCHIVO_SALIDA, "w", encoding="utf-8") as f:
        for base in bases:
            f.write(base + "\n")
    print(f"[OK] {len(bases)} bases exportadas a {ARCHIVO_SALIDA}")


if __name__ == "__main__":
    conn = conectar()
    if conn is None:
        sys.exit(1)

    bases = obtener_bases(conn)
    conn.close()

    if not bases:
        print("[AVISO] El servidor no tiene bases de datos (aparte de las del sistema)")
        sys.exit(0)

    exportar(bases)
