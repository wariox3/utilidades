"""Recorre los contenedores (tenants) de itrio y marca electronicos=true en
public.cnt_contenedor cuando el schema tiene documentos electronicos.

Un documento es electronico si estado_electronico = true o tiene electronico_id.
Solo marca en true; nunca pone false.

Uso:
    python3 marcar_electronicos.py            # simulacion: solo reporta
    python3 marcar_electronicos.py --aplicar  # actualiza cnt_contenedor
"""
import sys
from datetime import datetime
from pathlib import Path

import psycopg2
from psycopg2 import sql
from decouple import config

SQL_CONTENEDORES = """
SELECT id, schema_name, nombre, electronicos
FROM public.cnt_contenedor
WHERE schema_name <> 'public'
ORDER BY schema_name"""

SQL_EXISTE_TABLA = """
SELECT EXISTS (SELECT 1 FROM information_schema.tables
               WHERE table_schema = %s AND table_name = 'gen_documento')"""

SQL_CONTAR = """
SELECT count(*) FROM {}.gen_documento
WHERE estado_electronico OR electronico_id IS NOT NULL"""


def conectar():
    datos = {}
    for sufijo, clave in (("HOST", "host"), ("USER", "user"), ("CLAVE", "password"),
                          ("PORT", "port"), ("NAME", "dbname")):
        datos[clave] = config(f"PG_ORIGEN_DATABASE_{sufijo}")
    return psycopg2.connect(**datos)


def main():
    aplicar = "--aplicar" in sys.argv
    marca = datetime.now().strftime("%Y%m%d%H%M%S")
    salida = Path(__file__).parent / f"marcar_electronicos_{marca}.txt"
    lineas = []

    def log(texto=""):
        print(texto)
        lineas.append(texto)

    conexion = conectar()
    conexion.autocommit = True
    with conexion.cursor() as cursor:
        cursor.execute(SQL_CONTENEDORES)
        contenedores = cursor.fetchall()

    log(f"Modo: {'APLICAR' if aplicar else 'SIMULACION'} - {len(contenedores)} contenedores")
    log(f"{'schema':<34}{'electronicos':>13}{'antes':>7}  accion")

    por_marcar = []
    for id_, schema, nombre, electronicos in contenedores:
        with conexion.cursor() as cursor:
            cursor.execute(SQL_EXISTE_TABLA, (schema,))
            if not cursor.fetchone()[0]:
                log(f"{schema:<34}{'-':>13}{str(electronicos):>7}  sin gen_documento")
                continue
            cursor.execute(sql.SQL(SQL_CONTAR).format(sql.Identifier(schema)))
            cantidad = cursor.fetchone()[0]
        if cantidad and not electronicos:
            accion = "marcar"
            por_marcar.append(id_)
        elif cantidad:
            accion = "ya marcado"
        else:
            accion = "sin electronicos" + (" (marcado en true)" if electronicos else "")
        log(f"{schema:<34}{cantidad:>13}{str(electronicos):>7}  {accion}")

    log()
    log(f"Contenedores a marcar: {len(por_marcar)}")
    if aplicar and por_marcar:
        with conexion.cursor() as cursor:
            cursor.execute("UPDATE public.cnt_contenedor SET electronicos = true WHERE id = ANY(%s)",
                           (por_marcar,))
            log(f"Actualizados: {cursor.rowcount}")
    elif por_marcar:
        log("Simulacion: no se actualizo nada. Use --aplicar para guardar.")
    conexion.close()
    salida.write_text("\n".join(lineas) + "\n", encoding="utf-8")
    print(f"\nResultado en {salida}")


if __name__ == "__main__":
    main()
