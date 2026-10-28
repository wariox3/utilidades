"""Restaura un dump (pg_dump -Fc) del escritorio en la base PG_BACKUP.

Uso:
    python3 restaurar_dump.py bdtorio_20261028073325.dump
"""
import os
import subprocess
import sys

import psycopg2
from decouple import config

ESCRITORIO = os.path.expanduser('~/Escritorio')
SCHEMAS_SISTEMA = ('pg_catalog', 'information_schema', 'pg_toast')


def crear_conexion():
    return psycopg2.connect(
        host=config('PG_BACKUP_DATABASE_HOST'),
        port=config('PG_BACKUP_DATABASE_PORT'),
        user=config('PG_BACKUP_DATABASE_USER'),
        password=config('PG_BACKUP_DATABASE_CLAVE'),
        dbname=config('PG_BACKUP_DATABASE_NAME'),
    )


def validar_bd_limpia():
    """Devuelve (limpia, detalle). Limpia = sin schemas de usuario ni objetos en public."""
    conexion = crear_conexion()
    try:
        with conexion.cursor() as cursor:
            cursor.execute("""
                SELECT nspname FROM pg_namespace
                WHERE nspname NOT IN %s
                  AND nspname NOT LIKE 'pg_temp_%%'
                  AND nspname NOT LIKE 'pg_toast_temp_%%'
                  AND nspname <> 'public'
                ORDER BY nspname
            """, (SCHEMAS_SISTEMA,))
            schemas = [fila[0] for fila in cursor.fetchall()]

            cursor.execute("""
                SELECT count(*) FROM pg_class c
                JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE n.nspname = 'public'
            """)
            objetos_public = cursor.fetchone()[0]
    finally:
        conexion.close()

    detalle = []
    if schemas:
        muestra = ', '.join(schemas[:10]) + (' ...' if len(schemas) > 10 else '')
        detalle.append(f"{len(schemas)} schemas de usuario: {muestra}")
    if objetos_public:
        detalle.append(f"{objetos_public} objetos en public")
    return not detalle, detalle


def restaurar(ruta_dump):
    entorno = os.environ.copy()
    entorno['PGPASSWORD'] = config('PG_BACKUP_DATABASE_CLAVE')
    comando = [
        'pg_restore',
        '--host', config('PG_BACKUP_DATABASE_HOST'),
        '--port', config('PG_BACKUP_DATABASE_PORT'),
        '--username', config('PG_BACKUP_DATABASE_USER'),
        '--dbname', config('PG_BACKUP_DATABASE_NAME'),
        '--no-owner',
        '--no-acl',
        '--verbose',
        ruta_dump,
    ]
    resultado = subprocess.run(comando, env=entorno)
    return resultado.returncode == 0


def main():
    if len(sys.argv) != 2:
        print("Uso: python3 restaurar_dump.py <nombre_archivo.dump>")
        sys.exit(1)

    ruta_dump = os.path.join(ESCRITORIO, sys.argv[1])
    if not os.path.isfile(ruta_dump):
        print(f"❌ No existe el archivo: {ruta_dump}")
        sys.exit(1)

    base = config('PG_BACKUP_DATABASE_NAME')
    host = config('PG_BACKUP_DATABASE_HOST')
    print(f"🔍 Validando que {base} en {host} esté limpia...")
    try:
        limpia, detalle = validar_bd_limpia()
    except psycopg2.OperationalError as e:
        print(f"❌ No se pudo conectar a {base}: {e}")
        sys.exit(1)
    if not limpia:
        print(f"❌ La base {base} no está limpia:")
        for linea in detalle:
            print(f"   - {linea}")
        sys.exit(1)
    print("✅ Base limpia")

    print(f"📦 Restaurando {ruta_dump} en {base}...")
    if restaurar(ruta_dump):
        print(f"✅ Restauración exitosa en {base}")
    else:
        print("⚠️ pg_restore terminó con errores, revisa la salida")
        sys.exit(1)


if __name__ == "__main__":
    main()
