"""
migrar.py

Migra los datos de un esquema de la base de origen (PG_ORIGEN) al esquema del
mismo nombre en la base de destino (PG_DESTINO), copiando todo lo que sea
compatible: solo las tablas que existen en ambos lados y, dentro de ellas,
solo las columnas comunes.

Al arrancar muestra un menu:
  1. Migrar usuarios: pasa todos los usuarios de itrio (public.seguridad_user)
     a torio (public.seg_usuario), en una sola transaccion: o todos o ninguno.
  2. Migrar tenant: se escoge el tenant de itrio y se pregunta si se migran sus
     archivos adjuntos; si no existe en torio se crea
     ahi como lo hace la aplicacion (crear_tenant_torio.py) y despues se migran
     sus datos, tabla por tabla. Necesita que los usuarios ya esten (opcion 1).

Uso:
  python3 migrar.py                 # menu
  python3 migrar.py -n              # ensayo: informe, no escribe (ambas opciones)
  python3 migrar.py -n              # ensayo: informe por tabla, no escribe
  python3 migrar.py -t gen_contacto # solo esa tabla (repetible)
  python3 migrar.py -m reemplazar   # vacia las tablas destino antes
  python3 migrar.py -F              # ignora las FK (mas datos, menos integridad)
  python3 migrar.py -x              # sin rescate fila a fila

Las opciones -t, -m, -F y -x solo aplican a la opcion 2. Con -n la opcion 2 no
crea el tenant: solo puede ensayar uno que ya exista en torio.

Modos (-m):
  completar  (por defecto) conserva lo que ya hay en destino e inserta lo que
             no colisione, con ON CONFLICT DO NOTHING.
  reemplazar vacia con TRUNCATE ... CASCADE las tablas a migrar y carga desde cero.

Modelos ignorados (lista IGNORADAS, arriba en el script): catalogos que no se
migran nunca, salvo que se nombren de forma explicita con -t.

Si la carga masiva de una tabla falla, se reintenta fila a fila para salvar
todo lo que sea insertable (desactivable con -x).

Con -n la carga se ensaya de verdad en el destino, dentro de una transaccion
que se deshace con ROLLBACK al final: el informe dice tabla por tabla si migra
bien, si migra a medias o si falla, y por que.

Variables requeridas en .env (raiz del proyecto):
  PG_ORIGEN_DATABASE_HOST/USER/CLAVE/PORT/NAME
  PG_DESTINO_DATABASE_HOST/USER/CLAVE/PORT/NAME
Para los archivos adjuntos (gen_archivo), las claves de Backblaze B2 de cada
lado; sin ellas los datos se migran igual y los archivos se saltan con aviso:
  ITRIO_B2_KEY_ID/APP_KEY/BUCKET   bucket de itrio (basta solo lectura)
  TORIO_B2_KEY_ID/APP_KEY/BUCKET   bucket privado de torio (B2_BUCKET_PRIVADO)
  TORIO_B2_CDN_URL                 opcional, como B2_CDN_URL_PUBLICO de torio
Para crear tenants se usa el codigo de torio (TORIO_DIR, con su entorno
TORIO_PYTHON; por defecto /home/desarrollo/proyectos/torio y ~/.venvs/torio),
pero no su .env: la base es siempre la de PG_DESTINO y el dominio el de
  TORIO_TENANT_BASE_DOMAIN         el TENANT_BASE_DOMAIN del torio de destino
                                   (localhost en desarrollo)
El codigo de TORIO_DIR tiene que estar en la misma version que el torio de
destino: sus migraciones son las que se aplican al schema nuevo.
"""

import argparse
import base64
import datetime
import io
import json
import os
import re
import subprocess
import sys
import tempfile
import uuid as uuid_lib
from pathlib import Path

import psycopg2
import psycopg2.extras
import requests
from b2sdk.v2 import B2Api, InMemoryAccountInfo
from PIL import Image, ImageOps
from psycopg2 import sql
from decouple import config

DIR_SCRIPT = Path(__file__).resolve().parent
TORIO_DIR = config("TORIO_DIR", default="/home/desarrollo/proyectos/torio")
TORIO_PYTHON = config("TORIO_PYTHON", default=os.path.expanduser("~/.venvs/torio/bin/python"))

# Tablas de control de Django: migrarlas romperia el estado de migraciones.
EXCLUIDAS = ["django_migrations", "django_content_type", "django_session"]
# Modelos ignorados a proposito. gen_pais, gen_estado, gen_ciudad y
# gen_identificacion son catalogos generales que el destino ya trae cargados y
# cuyos ids ademas cambiaron de texto a bigint. gen_archivo cambio de forma en
# reddoc2 (la referencia generica modelo/documento_id se normalizo en el par
# modelo_id/objeto_id) y el archivo fisico cambia de bucket y de ruta: lo migra
# Migrador.migrar_archivos al final de la opcion 2.
IGNORADAS = ["gen_pais", "gen_estado", "gen_ciudad", "gen_identificacion", "gen_archivo"]

# Modelos que cambiaron de nombre entre las dos bases: tabla del origen ->
# tabla del destino. A partir de aqui el script trabaja siempre con el nombre
# del destino, y solo vuelve al del origen para leer los datos.
EQUIVALENTES = {
    # El id 1 choca: ADMINISTRATIVO en el origen y General en el destino son el
    # mismo centro de costo, asi que el ON CONFLICT DO NOTHING conserva el del
    # destino a proposito y solo entran los ids 2 y 3.
    "con_grupo": "con_centro_costo",
}

# Reglas por modelo: expresion SQL para las columnas que el origen no puede dar
# tal cual. La clave es tabla.columna del destino y el valor una expresion
# sobre la fila del origen, donde cada columna del origen se escribe o."columna"
# y llega siempre como texto. Sirven para dos cosas:
#   - convertir un valor cuyo tipo cambio entre las dos bases
#   - rellenar una columna del destino que no existe en el origen
# Las columnas del origen que use la expresion se traen solas, aunque no sean
# comunes, y las tablas del esquema se pueden nombrar sin calificar.
# Por ejemplo, para una columna que cambio de numeric(20,6) a bigint y para
# otra que el destino exige y el origen no tiene:
#   "gen_archivo.tamano": 'o."tamano"::numeric::bigint',
#   "gen_archivo.modelo_id": "10002",
REGLAS = {
    # El rename de con_grupo a con_centro_costo se llevo consigo la columna que
    # lo referencia: grupo_id paso a llamarse centro_costo_id.
    "con_activo.centro_costo_id": 'o."grupo_id"::bigint',
    "con_movimiento.centro_costo_id": 'o."grupo_id"::bigint',
    "gen_sede.centro_costo_id": 'o."grupo_id"::bigint',
    "gen_documento.centro_costo_id": 'o."grupo_contabilidad_id"::bigint',
    "gen_documento_detalle.centro_costo_id": 'o."grupo_id"::bigint',
    # En hum_contrato grupo_id sigue siendo el grupo de nomina; el centro de
    # costo venia en grupo_contabilidad_id.
    "hum_contrato.centro_costo_id": 'o."grupo_contabilidad_id"::bigint',
    # electronico_id paso de integer a uuid y los ids viejos no tienen
    # equivalente: el documento migra sin el vinculo electronico.
    "gen_documento.electronico_id": "NULL",
}

# Columnas que se copian del origen aunque la fila ya exista en el destino. El
# ON CONFLICT DO NOTHING conserva la fila del destino entera; estas columnas se
# sobrescriben despues, por id, con el valor del origen tal cual.
COPIAR_SIEMPRE = {
    # El destino trae los tipos de documento precargados con consecutivo 1:
    # sin esto la numeracion arrancaria de nuevo y repetiria documentos.
    "gen_documento_tipo": ["consecutivo"],
}

# gen_empresa desaparecio en reddoc2: sus datos viven en las columnas
# gen_empresa_* de gen_configuracion. Se leen de la empresa enlazada en
# gen_configuracion.empresa_id y se escriben en la configuracion del mismo id.
# La ciudad y el tipo de identificacion se buscan en el destino por codigo
# porque esos catalogos no se migran (IGNORADAS). El origen no tiene razon
# social: nombre_corto ya guarda el nombre legal completo.
# El logo no se copia como ruta: ver LOGOS_ITRIO mas abajo.
SQL_EMPRESA_ORIGEN = """
SELECT c.id,
       e.numero_identificacion, e.digito_verificacion, e.nombre_corto, e.direccion,
       e.telefono, e.correo, e.imagen, e.tipo_persona_id,
       ci.codigo AS ciudad_codigo, i.codigo AS identificacion_codigo
FROM gen_configuracion c
JOIN gen_empresa e ON e.id = c.empresa_id
LEFT JOIN gen_ciudad ci ON ci.id = e.ciudad_id
LEFT JOIN gen_identificacion i ON i.id = e.identificacion_id"""

SQL_EMPRESA_DESTINO = """
UPDATE gen_configuracion
   SET gen_empresa_numero_identificacion = %(numero_identificacion)s,
       gen_empresa_digito_verificacion   = %(digito_verificacion)s,
       gen_empresa_nombre_corto          = %(nombre_corto)s,
       gen_empresa_razon_social          = %(nombre_corto)s,
       gen_empresa_direccion             = %(direccion)s,
       gen_empresa_telefono              = %(telefono)s,
       gen_empresa_correo                = %(correo)s,
       gen_empresa_logotipo              = %(logotipo)s,
       gen_empresa_tipo_persona_id       = %(tipo_persona_id)s,
       gen_empresa_ciudad_id             = (SELECT id FROM gen_ciudad WHERE codigo = %(ciudad_codigo)s),
       gen_empresa_identificacion_id     = (SELECT id FROM gen_identificacion WHERE codigo = %(identificacion_codigo)s)
 WHERE id = %(id)s
RETURNING gen_empresa_ciudad_id, gen_empresa_identificacion_id"""

# En itrio el logo es un archivo en el bucket publico de DigitalOcean Spaces y
# gen_empresa.imagen guarda su ruta (itrio/prod/empresa/logo_52_1.jpg). En torio
# vive en gen_configuracion.gen_empresa_logotipo como PNG en base64, sin prefijo
# data:, y siempre normalizado igual (general/servicios/logotipo.py de torio):
# derecho segun el EXIF, transparencia sobre blanco, a lo sumo 400 px de lado.
# El logo por defecto de itrio no se migra: sin logotipo, torio deja el recuadro
# vacio en vez de imprimir el generico.
LOGOS_ITRIO = "https://semantica.sfo3.digitaloceanspaces.com/"
LOGO_DEFECTO_ITRIO = "logo_defecto"
LADO_MAXIMO_LOGOTIPO = 400

# Archivos adjuntos. En itrio el archivo esta en su bucket B2 con la ruta
# <schema>/<uuid>_<nombre> y gen_archivo.almacenamiento_id guarda el file id de
# B2; la fila apunta a un documento (documento_id) o, con modelo + codigo, a otro
# registro. En torio esta en el bucket privado con la ruta
# <cliente_id>/archivos/<modelo_id>/<anio>/<mes>/<uuid>.<ext> (la de
# general/servicios/archivo.py), almacenamiento_id guarda esa ruta y la fila
# apunta a gen_modelo + objeto_id. Se conservan el uuid y la fecha de subida.
SQL_ARCHIVOS_ORIGEN = """
SELECT id, fecha, archivo_tipo_id, nombre, tipo, tamano, almacenamiento_id, uuid,
       codigo, modelo, documento_id
FROM gen_archivo
ORDER BY id"""

# modelo de itrio -> (tabla del destino, clase de gen_modelo). Sin modelo, el
# archivo es de un documento.
MODELOS_ARCHIVO = {
    None: ("gen_documento", "GenDocumento"),
    "contacto": ("gen_contacto", "GenContacto"),
}

SQL_ARCHIVO_DESTINO = """
INSERT INTO gen_archivo
       (fecha, archivo_tipo_id, modelo_id, objeto_id, nombre, tipo, tamano, almacenamiento_id, uuid, url)
VALUES (%(fecha)s, %(archivo_tipo_id)s, %(modelo_id)s, %(objeto_id)s, %(nombre)s, %(tipo)s,
        %(tamano)s, %(key)s, %(uuid)s, %(url)s)"""


def conectar_b2(prefijo):
    """Bucket de B2 con las claves PREFIJO_B2_*, o None si faltan."""
    claves = [config(f"{prefijo}_B2_{n}", default="").strip() for n in ("KEY_ID", "APP_KEY", "BUCKET")]
    if not all(claves):
        return None
    api = B2Api(InMemoryAccountInfo())
    api.authorize_account("production", claves[0], claves[1])
    return api.get_bucket_by_name(claves[2])


# ---------------------------------------------------------------- utilidades

def info(mensaje):
    print(f"🔄 {mensaje}")


def ok(mensaje):
    print(f"✅ {mensaje}")


def aviso(mensaje):
    print(f"⚠️  {mensaje}")


def error(mensaje):
    sys.stdout.flush()
    print(f"❌ {mensaje}", file=sys.stderr)


def morir(mensaje):
    error(mensaje)
    sys.exit(1)


def ident(nombre):
    """Cita un identificador para pegarlo en SQL construido como texto."""
    return '"' + nombre.replace('"', '""') + '"'


def mensaje_error(e):
    texto = " ".join(str(getattr(e, "pgerror", None) or e).split())
    return texto.removeprefix("ERROR: ")


# ---------------------------------------------------------------- conexiones

def leer_conexion(prefijo):
    datos = {}
    for sufijo, clave in (("HOST", "host"), ("USER", "user"), ("CLAVE", "password"),
                          ("PORT", "port"), ("NAME", "dbname")):
        valor = config(f"{prefijo}_DATABASE_{sufijo}", default="").strip()
        if not valor:
            morir(f"Falta una variable de conexion en .env ({prefijo}_DATABASE_{sufijo})")
        datos[clave] = valor
    return datos


def conectar(datos, nombre):
    try:
        return psycopg2.connect(**datos)
    except psycopg2.Error as e:
        morir(f"No se pudo conectar al {nombre}: {mensaje_error(e)}")


def valor(conexion, consulta, parametros=None):
    with conexion.cursor() as cursor:
        cursor.execute(consulta, parametros)
        return cursor.fetchone()[0]


def contar(conexion, esquema, tabla):
    return valor(conexion, sql.SQL("SELECT count(*) FROM {}.{}").format(
        sql.Identifier(esquema), sql.Identifier(tabla)))


# ------------------------------------------------------- catalogo de columnas

# Columnas reales de cada tabla (sin las generadas, que no admiten INSERT).
SQL_COLUMNAS = """
SELECT c.relname, a.attname, format_type(a.atttypid, a.atttypmod)
FROM pg_attribute a
JOIN pg_class c ON c.oid = a.attrelid
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = %s AND c.relkind = 'r'
  AND a.attnum > 0 AND NOT a.attisdropped AND a.attgenerated = ''
ORDER BY c.relname, a.attnum"""

# Orden de carga: primero las tablas sin FK, luego las que dependen de ellas.
SQL_ORDEN = """
WITH RECURSIVE fk AS (
    SELECT c.conrelid AS hijo, c.confrelid AS padre
    FROM pg_constraint c
    JOIN pg_class r ON r.oid = c.conrelid
    JOIN pg_namespace n ON n.oid = r.relnamespace
    WHERE c.contype = 'f' AND n.nspname = %(esquema)s AND c.conrelid <> c.confrelid
), nivel AS (
    SELECT t.oid AS tabla, 0 AS lvl
    FROM pg_class t JOIN pg_namespace n ON n.oid = t.relnamespace
    WHERE n.nspname = %(esquema)s AND t.relkind = 'r'
      AND NOT EXISTS (SELECT 1 FROM fk WHERE fk.hijo = t.oid)
    UNION ALL
    SELECT fk.hijo, nivel.lvl + 1 FROM fk JOIN nivel ON nivel.tabla = fk.padre
    WHERE nivel.lvl < 15
)
SELECT c.relname FROM (SELECT tabla, max(lvl) AS lvl FROM nivel GROUP BY tabla) x
JOIN pg_class c ON c.oid = x.tabla ORDER BY x.lvl, c.relname"""


def leer_catalogo(conexion, esquema):
    """{tabla: {columna: tipo}} conservando el orden de las columnas."""
    catalogo = {}
    with conexion.cursor() as cursor:
        cursor.execute(SQL_COLUMNAS, (esquema,))
        for tabla, columna, tipo in cursor.fetchall():
            catalogo.setdefault(tabla, {})[columna] = tipo
    return catalogo


class Columnas:
    """Listas de columnas que necesita el SQL de una tabla, ya con las reglas
    del modelo aplicadas:
      copia    "a", "b"            -> lo que se trae del origen
      texto    "a" text, "b" text  -> las mismas, en la tabla temporal
      destino  "a", "b"            -> lo que recibe el INSERT
      masiva   d."a"::int, <regla> -> valores para la carga en bloque
      fila     r."a"::int, <regla> -> los mismos, para el rescate
    Los dos juegos de expresiones solo cambian en el alias porque el record del
    bucle plpgsql (r) no puede llamarse igual que el alias de la tabla temporal."""

    def __init__(self, tabla, origen, destino):
        cols_origen = origen.get(tabla, {})
        cols_destino = destino.get(tabla, {})
        expresion = {nombre: f"o.{ident(nombre)}::{tipo}"
                     for nombre, tipo in cols_destino.items() if nombre in cols_origen}
        copia = list(expresion)

        # Las reglas sustituyen la conversion por defecto o anaden una columna
        # que el origen no tiene.
        if expresion:
            for clave, regla in REGLAS.items():
                tabla_regla, columna = clave.split(".", 1)
                if tabla_regla != tabla:
                    continue
                if columna not in cols_destino:
                    morir(f"Regla {clave}: la columna no existe en el destino")
                expresion[columna] = regla

        # Columnas del origen que usan las reglas y que no viajaban en el volcado.
        usadas = sorted({m for e in expresion.values() for m in re.findall(r'o\."([^"]+)"', e)})
        for extra in usadas:
            if extra in copia:
                continue
            if extra not in cols_origen:
                morir(f'Las reglas de {tabla} usan o."{extra}", que no existe en el origen')
            copia.append(extra)

        self.cantidad = len(expresion)
        self.copia = ", ".join(ident(c) for c in copia)
        self.texto = ", ".join(f"{ident(c)} text" for c in copia)
        self.destino = ", ".join(ident(c) for c in expresion)
        self.masiva = ", ".join(e.replace('o."', 'd."') for e in expresion.values())
        self.fila = ", ".join(e.replace('o."', 'r."') for e in expresion.values())


# ------------------------------------------------------------------ migrador

class Migrador:
    def __init__(self, args, esquema):
        self.args = args
        self.datos_origen = leer_conexion("PG_ORIGEN")
        self.datos_destino = leer_conexion("PG_DESTINO")
        o, d = self.datos_origen, self.datos_destino
        if (o["host"], o["port"], o["dbname"]) == (d["host"], d["port"], d["dbname"]):
            morir(f"Origen y destino son la misma base: {o['host']}:{o['port']}/{o['dbname']}")

        self.origen = conectar(o, "origen")
        self.origen.set_session(readonly=True, autocommit=True)
        self.destino = conectar(d, "destino")
        self.errores = []
        self.esquema = esquema

        if args.sin_fk:
            with self.destino.cursor() as cursor:
                cursor.execute("SET session_replication_role = replica")
            self.destino.commit()

        self.planificar()

    # --------------------------------------------------------------- plan

    def planificar(self):
        self.catalogo_origen = leer_catalogo(self.origen, self.esquema)
        self.catalogo_destino = leer_catalogo(self.destino, self.esquema)
        self.total_origen = len(self.catalogo_origen)

        # El catalogo del origen pasa a hablar en nombres del destino, asi las
        # tablas renombradas quedan emparejadas como cualquier otra.
        self.nombre_en_origen = {}
        self.equivalentes_usadas = []
        for tabla_o, tabla_d in EQUIVALENTES.items():
            if tabla_o in self.catalogo_origen:
                self.catalogo_origen[tabla_d] = self.catalogo_origen.pop(tabla_o)
                self.nombre_en_origen[tabla_d] = tabla_o
                self.equivalentes_usadas.append(f"{tabla_o} -> {tabla_d}")

        comunes = (set(self.catalogo_origen) & set(self.catalogo_destino)) - set(EXCLUIDAS)

        # Los modelos ignorados solo se saltan cuando no se piden tablas con -t:
        # nombrar una tabla de forma explicita manda sobre la lista.
        self.ignoradas_aplicadas = []
        if not self.args.tablas:
            self.ignoradas_aplicadas = [t for t in IGNORADAS if t in comunes]
            comunes -= set(self.ignoradas_aplicadas)
        else:
            comunes &= set(self.args.tablas)
            if not comunes:
                morir(f"Ninguna de las tablas indicadas existe en ambos lados: {' '.join(self.args.tablas)}")

        if not comunes:
            morir(f"No hay tablas comunes entre ambos esquemas {self.esquema}")

        with self.destino.cursor() as cursor:
            cursor.execute(SQL_ORDEN, {"esquema": self.esquema})
            orden = [fila[0] for fila in cursor.fetchall()]
        self.destino.commit()
        # El orden manda, pero ninguna tabla comun puede quedarse fuera (ciclos de FK).
        self.plan = [t for t in orden if t in comunes]
        self.plan += sorted(comunes - set(self.plan))

    def columnas(self, tabla):
        return Columnas(tabla, self.catalogo_origen, self.catalogo_destino)

    # ---------------------------------------------------------- transporte

    def exportar(self, tabla, columnas, archivo):
        """Vuelca las columnas del origen en formato COPY de texto; devuelve las filas."""
        tabla_origen = self.nombre_en_origen.get(tabla, tabla)
        consulta = (f"COPY (SELECT {columnas.copia} FROM {ident(self.esquema)}.{ident(tabla_origen)}) "
                    "TO STDOUT")
        archivo.seek(0)
        archivo.truncate()
        with self.origen.cursor() as cursor:
            cursor.copy_expert(consulta, archivo)
        archivo.seek(0)
        filas = sum(1 for _ in archivo)
        archivo.seek(0)
        return filas

    def cargar_temporal(self, cursor, columnas, archivo, al_confirmar=""):
        # Los datos llegan como texto a una tabla temporal y cada columna se
        # convierte con su expresion (la de la regla, si la tiene).
        cursor.execute(f"CREATE TEMP TABLE _dat ({columnas.texto}) {al_confirmar}")
        archivo.seek(0)
        cursor.copy_expert(f"COPY _dat ({columnas.copia}) FROM STDIN", archivo)

    def sql_masiva(self, tabla, columnas):
        return (f"INSERT INTO {ident(self.esquema)}.{ident(tabla)} ({columnas.destino}) "
                f"SELECT {columnas.masiva} FROM _dat d ON CONFLICT DO NOTHING")

    def sql_rescate(self, tabla, columnas):
        # Fila a fila, saltando las que no entran.
        return f"""
DO $rescate$
DECLARE r record;
BEGIN
    FOR r IN SELECT * FROM _dat LOOP
        BEGIN
            INSERT INTO {ident(self.esquema)}.{ident(tabla)} ({columnas.destino})
                VALUES ({columnas.fila}) ON CONFLICT DO NOTHING;
        EXCEPTION WHEN others THEN NULL;
        END;
    END LOOP;
END
$rescate$"""

    def preparar_sesion(self, cursor):
        cursor.execute(sql.SQL("SET search_path TO {}, public").format(sql.Identifier(self.esquema)))

    # --------------------------------------------------------------- informe

    def encabezado(self):
        o, d = self.datos_origen, self.datos_destino
        print(f"Origen : {o['user']}@{o['host']}:{o['port']}/{o['dbname']}  esquema {self.esquema}")
        print(f"Destino: {d['user']}@{d['host']}:{d['port']}/{d['dbname']}  esquema {self.esquema}")
        print(f"Modo   : {self.args.modo}{' (sin comprobar FK)' if self.args.sin_fk else ''}")
        print(f"Tablas : {len(self.plan)} comunes de {self.total_origen} en origen "
              f"y {len(self.catalogo_destino)} en destino")
        if self.ignoradas_aplicadas:
            print(f"Ignora : {' '.join(self.ignoradas_aplicadas)}")
        if self.equivalentes_usadas:
            print(f"Renombra: {' '.join(self.equivalentes_usadas)}")
        print()

    # ------------------------------------------------------------ simulacion

    def simular(self):
        """Ensayo real: se carga todo dentro de una unica transaccion en el
        destino y se deshace con ROLLBACK al final. Al ir en una sola
        transaccion y en el orden del plan, cada tabla ve las filas de sus
        padres y las FK se comprueban de verdad. SET CONSTRAINTS ALL IMMEDIATE
        fuerza la verificacion de las FK diferidas en el mismo punto en que la
        haria el COMMIT de la migracion real."""
        info("Ensayando la carga en el destino (todo se deshace al terminar)...")
        resultados = []

        with tempfile.TemporaryFile("w+", encoding="utf-8") as archivo, self.destino.cursor() as cursor:
            cursor.execute("SET client_min_messages = warning")
            self.preparar_sesion(cursor)

            for tabla in self.plan:
                columnas = self.columnas(tabla)
                if not columnas.copia:
                    continue
                fila = {"tabla": tabla, "cols": columnas.cantidad, "origen": "?",
                        "destino": contar(self.destino, self.esquema, tabla),
                        "insertadas": 0, "estado": "", "motivo": ""}
                resultados.append(fila)

                try:
                    fila["origen"] = self.exportar(tabla, columnas, archivo)
                except psycopg2.Error as e:
                    fila["estado"], fila["motivo"] = "lectura", mensaje_error(e)
                    continue
                if fila["origen"] == 0:
                    fila["estado"] = "vacia"
                    continue

                fila["estado"], fila["motivo"] = self.ensayar_tabla(cursor, tabla, columnas, archivo)
                if fila["estado"] != "falla":
                    fila["insertadas"] = contar(self.destino, self.esquema, tabla) - fila["destino"]
                cursor.execute("DROP TABLE IF EXISTS _dat")

        self.destino.rollback()
        self.informe_simulacion(resultados)
        archivos = self.migrar_archivos(simulacion=True) if self.args.archivos else {}
        if archivos:
            info("Archivos adjuntos: " + ", ".join(f"{e} {c}" for e, c in archivos.items()))

    def ensayar_tabla(self, cursor, tabla, columnas, archivo):
        """Devuelve (estado, motivo) del ensayo de una tabla. Cada intento va en
        su propio SAVEPOINT para que un fallo no arrastre al resto del ensayo."""
        try:
            cursor.execute("SAVEPOINT temporal")
            self.cargar_temporal(cursor, columnas, archivo)
            cursor.execute("RELEASE SAVEPOINT temporal")
        except psycopg2.Error as e:
            cursor.execute("ROLLBACK TO SAVEPOINT temporal")
            return "falla", mensaje_error(e)

        try:
            cursor.execute("SAVEPOINT carga")
            cursor.execute(self.sql_masiva(tabla, columnas))
            cursor.execute("SET CONSTRAINTS ALL IMMEDIATE")
            cursor.execute("SET CONSTRAINTS ALL DEFERRED")
            cursor.execute("RELEASE SAVEPOINT carga")
            return "ok", ""
        except psycopg2.Error as e:
            cursor.execute("ROLLBACK TO SAVEPOINT carga")
            motivo = mensaje_error(e)

        if self.args.sin_rescate:
            return "falla", motivo

        try:
            cursor.execute("SAVEPOINT rescate")
            cursor.execute(self.sql_rescate(tabla, columnas))
            cursor.execute("SET CONSTRAINTS ALL IMMEDIATE")
            cursor.execute("SET CONSTRAINTS ALL DEFERRED")
            cursor.execute("RELEASE SAVEPOINT rescate")
            return "rescate", motivo
        except psycopg2.Error as e:
            cursor.execute("ROLLBACK TO SAVEPOINT rescate")
            return "falla", mensaje_error(e)

    def informe_simulacion(self, resultados):
        etiquetas = {
            "ok": "✅ migra bien",
            "rescate": "⚠️  migra a medias (rescate fila a fila)",
            "falla": "❌ falla",
            "vacia": "· vacia en origen",
            "lectura": "❌ falla: no se pudo leer el origen",
        }
        print()
        print(f"{'TABLA':<34} {'COLS':>6} {'ORIGEN':>10} {'DESTINO':>10} {'INSERTARIA':>12}  RESULTADO")
        for f in resultados:
            print(f"{f['tabla']:<34} {f['cols']:>6} {f['origen']:>10} {f['destino']:>10} "
                  f"{f['insertadas']:>12}  {etiquetas.get(f['estado'], '❓ sin diagnostico')}")

        cuenta = lambda *estados: sum(1 for f in resultados if f["estado"] in estados)
        print()
        ok(f"Tablas que migran bien: {cuenta('ok')}")
        if cuenta("rescate"):
            aviso(f"Tablas que migran a medias: {cuenta('rescate')}")
        if cuenta("vacia"):
            info(f"Tablas vacias en origen: {cuenta('vacia')}")
        if cuenta("falla", "lectura"):
            error(f"Tablas que fallan: {cuenta('falla', 'lectura')}")
        ok(f"Filas que se insertarian: {sum(f['insertadas'] for f in resultados)}")

        motivos = [f for f in resultados if f["motivo"]]
        if motivos:
            print("\nMotivos:")
            for f in motivos:
                print(f"  {f['tabla']}: {f['motivo']}")

        print()
        aviso("Simulacion: no se escribio nada en el destino.")

    # ------------------------------------------------------------- migracion

    def vaciar_destino(self):
        aviso(f"Se vaciaran {len(self.plan)} tablas de {self.datos_destino['dbname']}.{self.esquema} "
              "(TRUNCATE CASCADE).")
        if input("Escriba 'reemplazar' para confirmar: ").strip() != "reemplazar":
            morir("Confirmacion incorrecta, se cancela la migracion.")
        info("Vaciando tablas del destino...")
        tablas = sql.SQL(", ").join(sql.Identifier(self.esquema, t) for t in self.plan)
        with self.destino.cursor() as cursor:
            cursor.execute(sql.SQL("TRUNCATE {} CASCADE").format(tablas))
        self.destino.commit()

    def migrar_tabla(self, tabla, columnas, archivo):
        """Carga una tabla en su propia transaccion; devuelve el estado."""
        try:
            with self.destino.cursor() as cursor:
                self.preparar_sesion(cursor)
                self.cargar_temporal(cursor, columnas, archivo, "ON COMMIT DROP")
                cursor.execute(self.sql_masiva(tabla, columnas))
            self.destino.commit()
            return "✅ ok"
        except psycopg2.Error as e:
            self.destino.rollback()
            self.errores.append(f"[{tabla}] carga masiva: {mensaje_error(e)}")
        if self.args.sin_rescate:
            return "❌ fallo la carga"

        try:
            with self.destino.cursor() as cursor:
                self.preparar_sesion(cursor)
                self.cargar_temporal(cursor, columnas, archivo, "ON COMMIT DROP")
                cursor.execute(self.sql_rescate(tabla, columnas))
            self.destino.commit()
            return "⚠️  rescate fila a fila"
        except psycopg2.Error as e:
            self.destino.rollback()
            self.errores.append(f"[{tabla}] rescate: {mensaje_error(e)}")
            return "❌ fallo la carga"

    def copiar_columnas(self):
        """Sobrescribe en el destino las columnas de COPIAR_SIEMPRE con el
        valor del origen, por id; devuelve las filas actualizadas por tabla."""
        actualizadas = {}
        for tabla, columnas in COPIAR_SIEMPRE.items():
            if tabla not in self.plan:
                continue
            tabla_origen = self.nombre_en_origen.get(tabla, tabla)
            for columna in columnas:
                tipo = self.catalogo_destino[tabla][columna]
                with self.origen.cursor() as cursor:
                    cursor.execute(sql.SQL("SELECT id::text, {}::text FROM {}.{}").format(
                        sql.Identifier(columna), sql.Identifier(self.esquema), sql.Identifier(tabla_origen)))
                    filas = cursor.fetchall()
                if not filas:
                    continue
                ids, valores = zip(*filas)
                with self.destino.cursor() as cursor:
                    cursor.execute(sql.SQL("""
                        UPDATE {esquema}.{tabla} t
                           SET {columna} = v.valor::{tipo}
                          FROM unnest(%s::text[], %s::text[]) AS v(id, valor)
                         WHERE t.id::text = v.id
                           AND t.{columna} IS DISTINCT FROM v.valor::{tipo}""").format(
                        esquema=sql.Identifier(self.esquema), tabla=sql.Identifier(tabla),
                        columna=sql.Identifier(columna), tipo=sql.SQL(tipo)),
                        (list(ids), list(valores)))
                    actualizadas[f"{tabla}.{columna}"] = cursor.rowcount
                self.destino.commit()
        return actualizadas

    @staticmethod
    def logotipo(ruta):
        """El logo de itrio como lo guarda torio, o None si no hay o no se pudo
        leer. Un logo que falta no frena la migracion: se avisa y sigue."""
        if not ruta or LOGO_DEFECTO_ITRIO in ruta:
            return None
        url = LOGOS_ITRIO + ruta
        try:
            respuesta = requests.get(url, timeout=30)
            respuesta.raise_for_status()
            imagen = ImageOps.exif_transpose(Image.open(io.BytesIO(respuesta.content)))
            if imagen.mode == "P":
                imagen = imagen.convert("RGBA")
            if imagen.mode in ("RGBA", "LA"):
                fondo = Image.new("RGB", imagen.size, (255, 255, 255))
                fondo.paste(imagen, mask=imagen.getchannel("A"))
                imagen = fondo
            else:
                imagen = imagen.convert("RGB")
            imagen.thumbnail((LADO_MAXIMO_LOGOTIPO, LADO_MAXIMO_LOGOTIPO), Image.LANCZOS)
            buffer = io.BytesIO()
            imagen.save(buffer, format="PNG", optimize=True)
        except (requests.RequestException, OSError, ValueError) as e:
            aviso(f"Empresa: no se pudo leer el logo {url}: {e}")
            return None
        return base64.b64encode(buffer.getvalue()).decode("ascii")

    def copiar_empresa(self):
        """Pasa los datos de gen_empresa del origen a gen_configuracion del
        destino; devuelve cuantas configuraciones se actualizaron."""
        if "gen_configuracion" not in self.plan or "gen_empresa" in self.catalogo_destino:
            return 0
        if "gen_empresa" not in self.catalogo_origen \
                or "empresa_id" not in self.catalogo_origen.get("gen_configuracion", {}):
            return 0

        with self.origen.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
            self.preparar_sesion(cursor)
            cursor.execute(SQL_EMPRESA_ORIGEN)
            empresas = cursor.fetchall()

        actualizadas = 0
        with self.destino.cursor() as cursor:
            self.preparar_sesion(cursor)
            for empresa in empresas:
                empresa["logotipo"] = self.logotipo(empresa["imagen"])
                cursor.execute(SQL_EMPRESA_DESTINO, empresa)
                fila = cursor.fetchone()
                if fila is None:
                    aviso(f"gen_configuracion {empresa['id']} no existe en el destino: empresa sin copiar")
                    continue
                actualizadas += 1
                ciudad_id, identificacion_id = fila
                if empresa["ciudad_codigo"] and ciudad_id is None:
                    aviso(f"Empresa: la ciudad {empresa['ciudad_codigo']} no existe en el destino")
                if empresa["identificacion_codigo"] and identificacion_id is None:
                    aviso(f"Empresa: la identificacion {empresa['identificacion_codigo']} no existe en el destino")
        self.destino.commit()
        return actualizadas

    def leer_archivos(self):
        """Archivos del origen, ya resueltos contra el destino: cada uno con
        su modelo_id, objeto_id y la ruta que tendra en torio, o con el motivo
        por el que no se migra."""
        if "gen_archivo" not in self.catalogo_origen or "gen_archivo" not in self.catalogo_destino:
            return []
        with self.origen.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
            self.preparar_sesion(cursor)
            cursor.execute(SQL_ARCHIVOS_ORIGEN)
            archivos = cursor.fetchall()
        if not archivos:
            return []

        with self.destino.cursor() as cursor:
            self.preparar_sesion(cursor)
            cursor.execute("SELECT id FROM public.ctn_cliente WHERE schema_name = %s", (self.esquema,))
            cliente = cursor.fetchone()
            cursor.execute("SELECT clase, id FROM gen_modelo")
            modelos = dict(cursor.fetchall())
            cursor.execute("SELECT uuid::text FROM gen_archivo")
            ya_estan = {fila[0] for fila in cursor.fetchall()}
            existentes = {}
            for tabla, _ in MODELOS_ARCHIVO.values():
                cursor.execute(sql.SQL("SELECT id::text FROM {}").format(sql.Identifier(tabla)))
                existentes[tabla] = {fila[0] for fila in cursor.fetchall()}
        self.destino.commit()
        if cliente is None:
            morir(f"El tenant {self.esquema} no esta en public.ctn_cliente del destino")

        url_cdn = config("TORIO_B2_CDN_URL", default="").strip().rstrip("/")
        for a in archivos:
            a["motivo"] = None
            destino = MODELOS_ARCHIVO.get(a["modelo"])
            objeto = a["documento_id"] if a["modelo"] is None else a["codigo"]
            try:
                a["uuid"] = str(uuid_lib.UUID(a["uuid"]))
            except (TypeError, ValueError):
                a["motivo"] = f"uuid invalido: {a['uuid']}"
                continue
            if destino is None:
                a["motivo"] = f"modelo de itrio sin equivalente: {a['modelo']}"
            elif destino[1] not in modelos:
                a["motivo"] = f"{destino[1]} no esta en gen_modelo del destino"
            elif objeto is None or str(objeto) not in existentes[destino[0]]:
                a["motivo"] = f"{destino[0]} {objeto} no existe en el destino"
            elif a["uuid"] in ya_estan:
                a["motivo"] = "ya migrado"
            if a["motivo"]:
                continue
            a["modelo_id"] = modelos[destino[1]]
            a["objeto_id"] = str(objeto)
            a["tamano"] = int(a["tamano"])
            extension = os.path.splitext(a["nombre"])[1].lower().lstrip(".")
            nombre = f"{a['uuid']}.{extension}" if extension else a["uuid"]
            a["key"] = f"{cliente[0]}/archivos/{a['modelo_id']}/{a['fecha']:%Y/%m}/{nombre}"
            a["url"] = f"{url_cdn}/{a['key']}" if url_cdn else None
        return archivos

    def migrar_archivos(self, simulacion=False):
        """Copia los adjuntos del bucket de itrio al de torio y crea sus filas.
        Un archivo que falla se avisa y se salta: no frena al resto. Devuelve
        {estado: cantidad} para el informe."""
        archivos = self.leer_archivos()
        if not archivos:
            return {}
        conteo = {}

        def contar_estado(estado):
            conteo[estado] = conteo.get(estado, 0) + 1

        pendientes = []
        for a in archivos:
            if a["motivo"] == "ya migrado":
                contar_estado("ya migrados")
            elif a["motivo"]:
                contar_estado("omitidos")
                self.errores.append(f"[gen_archivo {a['id']}] {a['nombre']}: {a['motivo']}")
            else:
                pendientes.append(a)
        if not pendientes:
            return conteo

        try:
            origen_b2 = conectar_b2("ITRIO")
            destino_b2 = None if simulacion else conectar_b2("TORIO")
        except Exception as e:
            aviso(f"Archivos: no se pudo conectar a Backblaze: {e}")
            return conteo | {"sin migrar (B2)": len(pendientes)}
        if origen_b2 is None or (destino_b2 is None and not simulacion):
            aviso("Archivos: faltan las claves ITRIO_B2_* o TORIO_B2_* en .env; se saltan "
                  f"{len(pendientes)} archivos")
            return conteo | {"sin migrar (B2)": len(pendientes)}

        info(f"{'Revisando' if simulacion else 'Copiando'} {len(pendientes)} archivos en Backblaze...")
        for numero, a in enumerate(pendientes, start=1):
            try:
                if simulacion:
                    # Con -n solo se comprueba que el archivo exista en itrio.
                    origen_b2.api.get_file_info(a["almacenamiento_id"])
                    contar_estado("se copiarian")
                    continue
                contenido = io.BytesIO()
                origen_b2.download_file_by_id(a["almacenamiento_id"]).save(contenido)
                subido = destino_b2.upload_bytes(contenido.getvalue(), a["key"], content_type=a["tipo"])
            except Exception as e:
                contar_estado("fallidos")
                self.errores.append(f"[gen_archivo {a['id']}] {a['nombre']}: B2: {e}")
                continue
            try:
                with self.destino.cursor() as cursor:
                    self.preparar_sesion(cursor)
                    cursor.execute(SQL_ARCHIVO_DESTINO, a)
                self.destino.commit()
                contar_estado("copiados")
            except psycopg2.Error as e:
                self.destino.rollback()
                # Sin fila que lo referencie el objeto quedaria huerfano en B2.
                try:
                    destino_b2.delete_file_version(subido.id_, a["key"])
                except Exception:
                    self.errores.append(f"[gen_archivo {a['id']}] quedo huerfano en B2: {a['key']}")
                contar_estado("fallidos")
                self.errores.append(f"[gen_archivo {a['id']}] {a['nombre']}: {mensaje_error(e)}")
            if numero % 100 == 0:
                info(f"  {numero} de {len(pendientes)}")
        return conteo

    def ajustar_secuencias(self):
        # Las secuencias quedan atras si se insertaron ids explicitos.
        info(f"Ajustando secuencias del esquema {self.esquema}...")
        consulta = sql.SQL("""
DO $secuencias$
DECLARE r record; maximo bigint;
BEGIN
    FOR r IN
        SELECT c.relname AS tabla, a.attname AS columna,
               pg_get_serial_sequence(quote_ident({esquema}) || '.' || quote_ident(c.relname), a.attname) AS secuencia
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum > 0 AND NOT a.attisdropped
        WHERE n.nspname = {esquema} AND c.relkind = 'r'
          AND pg_get_serial_sequence(quote_ident({esquema}) || '.' || quote_ident(c.relname), a.attname) IS NOT NULL
    LOOP
        EXECUTE format('SELECT coalesce(max(%I), 0) FROM %I.%I', r.columna, {esquema}, r.tabla) INTO maximo;
        PERFORM setval(r.secuencia, GREATEST(maximo, 1), maximo > 0);
    END LOOP;
END
$secuencias$""").format(esquema=sql.Literal(self.esquema))
        with self.destino.cursor() as cursor:
            cursor.execute(consulta)
        self.destino.commit()

    def migrar(self):
        marca = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
        registro = DIR_SCRIPT / f"migracion_{self.esquema}_{marca}.txt"
        o, d = self.datos_origen["dbname"], self.datos_destino["dbname"]

        if self.args.modo == "reemplazar":
            self.vaciar_destino()

        migradas = parciales = fallidas = vacias = total_insertadas = 0
        print(f"{'TABLA':<34} {'ORIGEN':>8} {'INSERTADAS':>10} {'OMITIDAS':>10}  ESTADO")

        with open(registro, "w", encoding="utf-8") as log, \
                tempfile.TemporaryFile("w+", encoding="utf-8") as archivo:
            fecha = datetime.datetime.now().strftime("%c")
            log.write(f"Migracion {o}.{self.esquema} -> {d}.{self.esquema} | modo={self.args.modo} | {fecha}\n")

            for tabla in self.plan:
                columnas = self.columnas(tabla)
                if not columnas.copia:
                    continue

                try:
                    filas_origen = self.exportar(tabla, columnas, archivo)
                except psycopg2.Error as e:
                    self.errores.append(f"[{tabla}] exportar: {mensaje_error(e)}")
                    print(f"{tabla:<34} {'?':>8} {'0':>10} {'-':>10}  ❌ error al leer origen")
                    log.write(f"[{tabla}] error al exportar del origen\n")
                    fallidas += 1
                    continue

                if filas_origen == 0:
                    print(f"{tabla:<34} {'0':>8} {'0':>10} {'0':>10}  · vacia en origen")
                    vacias += 1
                    continue

                antes = contar(self.destino, self.esquema, tabla)
                self.destino.commit()
                estado = self.migrar_tabla(tabla, columnas, archivo)
                insertadas = contar(self.destino, self.esquema, tabla) - antes
                self.destino.commit()
                omitidas = filas_origen - insertadas
                total_insertadas += insertadas

                if estado.startswith("✅"):
                    migradas += 1
                elif estado.startswith("⚠️"):
                    parciales += 1
                else:
                    fallidas += 1

                print(f"{tabla:<34} {filas_origen:>8} {insertadas:>10} {omitidas:>10}  {estado}")
                log.write(f"[{tabla}] origen={filas_origen} insertadas={insertadas} omitidas={omitidas} {estado}\n")

            for clave, cantidad in self.copiar_columnas().items():
                info(f"Copiado del origen {clave}: {cantidad} filas actualizadas")
                log.write(f"[{clave}] copiado del origen: {cantidad} filas actualizadas\n")
            empresas = self.copiar_empresa()
            if empresas:
                info(f"Datos de gen_empresa copiados a gen_configuracion: {empresas}")
                log.write(f"[gen_configuracion] datos de gen_empresa copiados: {empresas}\n")
            self.ajustar_secuencias()
            if not self.args.archivos:
                log.write("[gen_archivo] no se migraron: se escogio no migrar archivos\n")
            archivos = self.migrar_archivos() if self.args.archivos else {}
            if archivos:
                resumen = ", ".join(f"{estado} {cantidad}" for estado, cantidad in archivos.items())
                info(f"Archivos adjuntos: {resumen}")
                log.write(f"[gen_archivo] {resumen}\n")

            print()
            ok(f"Tablas migradas por completo: {migradas}")
            if parciales:
                aviso(f"Tablas con rescate fila a fila: {parciales}")
            if vacias:
                info(f"Tablas vacias en origen: {vacias}")
            if fallidas:
                error(f"Tablas fallidas: {fallidas}")
            ok(f"Filas insertadas en total: {total_insertadas}")

            log.write("---\n")
            log.write(f"migradas={migradas} parciales={parciales} vacias={vacias} "
                      f"fallidas={fallidas} filas={total_insertadas}\n")

        if self.errores:
            archivo_errores = registro.with_name(f"{registro.stem}_errores.txt")
            archivo_errores.write_text("\n".join(self.errores) + "\n", encoding="utf-8")
            aviso(f"Detalle de errores: {archivo_errores}")
        ok(f"Registro: {registro}")


# ------------------------------------------------------------------ usuarios

# Usuarios de itrio tal como los necesita torio. El correo pasa en minusculas:
# torio los guarda asi al registrar y el login compara con `=`. Todos pasan
# verificados: itrio no exigia verificar el correo para entrar y torio si, asi
# que migrar `verificado` tal cual dejaria fuera a usuarios que hoy entran.
SQL_USUARIOS_ORIGEN = """
SELECT id, password, last_login, lower(username) AS email, username AS email_original,
       is_active, true AS is_verified,
       coalesce(nombre_corto, nullif(trim(concat_ws(' ', nombre, apellido)), '')) AS nombre_corto,
       numero_identificacion, telefono AS celular, idioma, fecha_creacion,
       vr_saldo AS saldo_pendiente
FROM public.seguridad_user
ORDER BY id"""

SQL_USUARIOS_REFERENCIADOS = """
SELECT usuario_id FROM public.cnt_usuario_contenedor
UNION SELECT usuario_id FROM public.cnt_contenedor WHERE usuario_id IS NOT NULL"""

# Un usuario de torio es solo su fila en seg_usuario: el registro de torio
# (seguridad/serializers/usuario.py) no lo vincula a ningun cliente, ni siquiera
# a public. Las membresias y los permisos llegan al crear o migrar cada tenant.
SQL_USUARIO_DESTINO = """
INSERT INTO public.seg_usuario
       (id, password, last_login, email, is_active, is_verified, nombre_corto,
        numero_identificacion, celular, idioma, fecha_creacion, saldo_pendiente)
VALUES (%(id)s, %(password)s, %(last_login)s, %(email)s, %(is_active)s, %(is_verified)s,
        %(nombre_corto)s, %(numero_identificacion)s, %(celular)s, %(idioma)s,
        %(fecha_creacion)s, %(saldo_pendiente)s)"""

def elegir_usuarios(usuarios):
    """Uno por correo. Itrio distinguia mayusculas y torio no: entre los que
    comparten correo gana el ultimo que inicio sesion, luego el que ya estaba
    escrito en minusculas y luego el id mas bajo. Devuelve (elegidos, omitidos)."""
    por_correo = {}
    for usuario in usuarios:
        por_correo.setdefault(usuario["email"], []).append(usuario)
    elegidos, omitidos = [], []
    minimo = datetime.datetime.min.replace(tzinfo=datetime.timezone.utc)
    for grupo in por_correo.values():
        grupo.sort(key=lambda u: (u["last_login"] or minimo,
                                  u["email_original"] == u["email"], -u["id"]), reverse=True)
        elegidos.append(grupo[0])
        omitidos += [(u, grupo[0]) for u in grupo[1:]]
    elegidos.sort(key=lambda u: u["id"])
    return elegidos, omitidos


class MigradorUsuarios:
    """Opcion 1: todos los usuarios de itrio a torio, en una sola transaccion.
    Los usuarios conservan su id (asi todo lo que los referencia en los
    esquemas sigue valiendo) y su clave, porque las dos aplicaciones guardan el
    mismo hash de Django. Si un usuario ya esta en torio con el mismo id y el
    mismo correo se salta; cualquier otro choque aborta sin escribir nada."""

    def __init__(self, args):
        self.args = args
        self.datos_origen = leer_conexion("PG_ORIGEN")
        self.datos_destino = leer_conexion("PG_DESTINO")
        self.origen = conectar(self.datos_origen, "origen")
        self.origen.set_session(readonly=True, autocommit=True)
        self.destino = conectar(self.datos_destino, "destino")

    def leer_origen(self):
        with self.origen.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
            cursor.execute(SQL_USUARIOS_ORIGEN)
            usuarios = cursor.fetchall()
            cursor.execute(SQL_USUARIOS_REFERENCIADOS)
            referenciados = {fila["usuario_id"] for fila in cursor.fetchall()}
        return usuarios, referenciados

    def leer_destino(self):
        with self.destino.cursor() as cursor:
            cursor.execute("SELECT to_regclass('public.seg_usuario') IS NOT NULL")
            if not cursor.fetchone()[0]:
                morir("El destino no tiene public.seg_usuario: no es una base de torio")
            cursor.execute("SELECT id, email FROM public.seg_usuario")
            existentes = dict(cursor.fetchall())
        self.destino.commit()
        return existentes

    def migrar(self):
        o, d = self.datos_origen, self.datos_destino
        print(f"Origen : {o['user']}@{o['host']}:{o['port']}/{o['dbname']}  public.seguridad_user")
        print(f"Destino: {d['user']}@{d['host']}:{d['port']}/{d['dbname']}  public.seg_usuario\n")

        usuarios, referenciados = self.leer_origen()
        elegidos, omitidos = elegir_usuarios(usuarios)
        existentes = self.leer_destino()
        por_correo = {email.lower(): id_ for id_, email in existentes.items()}

        nuevos, ya_estaban, choques = [], [], []
        for u in elegidos:
            if existentes.get(u["id"], "").lower() == u["email"]:
                ya_estaban.append(u)
            elif u["id"] in existentes:
                choques.append(f"id {u['id']}: en torio es {existentes[u['id']]}, en itrio {u['email']}")
            elif u["email"] in por_correo:
                choques.append(f"{u['email']}: en torio tiene id {por_correo[u['email']]}, en itrio {u['id']}")
            else:
                nuevos.append(u)

        info(f"Usuarios en itrio: {len(usuarios)}")
        if omitidos:
            aviso(f"Correos repetidos (solo cambian mayusculas): se omiten {len(omitidos)}")
            for omitido, elegido in omitidos:
                marca = "  ⚠️  tiene tenants en itrio" if omitido["id"] in referenciados else ""
                print(f"     {omitido['id']:>5} {omitido['email_original']:<45} -> queda {elegido['id']}{marca}")
        if ya_estaban:
            info(f"Ya estaban en torio (mismo id y correo): {len(ya_estaban)}")
        if choques:
            error(f"Choques con usuarios de torio: {len(choques)}")
            for choque in choques:
                print(f"     {choque}")
            morir("No se migro ningun usuario: resuelva los choques y vuelva a correr")
        if not nuevos:
            ok("No hay usuarios nuevos que migrar")
            return

        ids = [u["id"] for u in nuevos]
        try:
            with self.destino.cursor() as cursor:
                psycopg2.extras.execute_batch(cursor, SQL_USUARIO_DESTINO, nuevos, page_size=500)
                # Los ids explicitos dejan atras la identidad de seg_usuario.
                cursor.execute("""SELECT setval(pg_get_serial_sequence('public.seg_usuario', 'id'),
                                                (SELECT max(id) FROM public.seg_usuario))""")
                cursor.execute("SET CONSTRAINTS ALL IMMEDIATE")
        except psycopg2.Error as e:
            self.destino.rollback()
            morir(f"No se migro ningun usuario: {mensaje_error(e)}")

        if self.args.simulacion:
            self.destino.rollback()
        else:
            self.destino.commit()

        verbo = "Se migrarian" if self.args.simulacion else "Usuarios migrados"
        ok(f"{verbo}: {len(nuevos)}")
        inactivos = sum(1 for u in nuevos if not u["is_active"])
        if inactivos:
            info(f"Inactivos: {inactivos}")
        if self.args.simulacion:
            print()
            aviso("Simulacion: no se escribio nada en el destino.")
        else:
            marca = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
            registro = DIR_SCRIPT / f"migracion_usuarios_{marca}.txt"
            with open(registro, "w", encoding="utf-8") as log:
                log.write(f"Migracion usuarios {o['dbname']} -> {d['dbname']} | {datetime.datetime.now():%c}\n")
                log.write(f"origen={len(usuarios)} migrados={len(nuevos)} ya_estaban={len(ya_estaban)} "
                          f"omitidos={len(omitidos)}\n")
                for omitido, elegido in omitidos:
                    log.write(f"[omitido] {omitido['id']} {omitido['email_original']} -> {elegido['id']}\n")
            ok(f"Registro: {registro}")


# ------------------------------------------------------------------- tenants

SQL_TENANTS_ORIGEN = """
SELECT c.id, c.schema_name, c.nombre
FROM public.cnt_contenedor c
WHERE c.schema_name <> 'public'
ORDER BY c.schema_name"""

# El cliente de torio toma correo y celular del dueno, como cuando el dueno lo
# crea desde la aplicacion.
SQL_TENANT_ORIGEN = """
SELECT c.id, c.schema_name, c.nombre, c.usuario_id AS owner_id, c.fecha AS fecha_creacion,
       c.fecha_ultima_conexion, lower(u.username) AS correo, coalesce(u.telefono, '') AS celular,
       p.venta, p.compra, p.tesoreria, p.cartera, p.inventario, p.humano, p.contabilidad
FROM public.cnt_contenedor c
JOIN public.seguridad_user u ON u.id = c.usuario_id
LEFT JOIN public.cnt_plan p ON p.id = c.plan_id
WHERE c.schema_name = %s"""

SQL_MIEMBROS_ORIGEN = """
SELECT usuario_id, rol FROM public.cnt_usuario_contenedor WHERE contenedor_id = %s ORDER BY usuario_id"""

# Modulos del plan de itrio -> flags acceso_* de torio. turno no existia en itrio.
MODULOS_PLAN = ("venta", "compra", "tesoreria", "cartera", "inventario", "humano", "contabilidad")


def elegir_tenant(args):
    """Lista los tenants de itrio, marcando los que ya existen en torio, y
    devuelve el schema escogido."""
    origen = conectar(leer_conexion("PG_ORIGEN"), "origen")
    destino = conectar(leer_conexion("PG_DESTINO"), "destino")
    with origen.cursor() as cursor:
        cursor.execute(SQL_TENANTS_ORIGEN)
        tenants = cursor.fetchall()
    with destino.cursor() as cursor:
        cursor.execute("SELECT schema_name FROM public.ctn_cliente")
        en_torio = {fila[0] for fila in cursor.fetchall()}
    origen.close()
    destino.close()

    print("\n=== Tenants de itrio (✓ = ya existe en torio) ===")
    for indice, (_, schema, nombre) in enumerate(tenants, start=1):
        marca = "✓" if schema in en_torio else " "
        print(f"  {indice:>3}. {marca} {schema:<32} {nombre or ''}")
    print("    q. Salir")

    schemas = [t[1] for t in tenants]
    while True:
        opcion = input("\nTenant a migrar (numero o schema): ").strip()
        if opcion.lower() == "q":
            print("Operacion cancelada.")
            sys.exit(0)
        if opcion.isdigit() and 1 <= int(opcion) <= len(schemas):
            schema = schemas[int(opcion) - 1]
        elif opcion in schemas:
            schema = opcion
        else:
            error("Opcion no valida.")
            continue
        if args.simulacion and schema not in en_torio:
            error(f"{schema} no existe en torio y con -n no se crea: escoja uno marcado con ✓")
            continue
        print()
        return schema, schema in en_torio


def preguntar_archivos(schema):
    """Pregunta si se migran los archivos adjuntos del tenant, contando antes
    cuantos tiene en itrio. Copiarlos es lo lento: un archivo por segundo."""
    origen = conectar(leer_conexion("PG_ORIGEN"), "origen")
    with origen.cursor() as cursor:
        cursor.execute("SELECT to_regclass(%s) IS NOT NULL", (f'"{schema}".gen_archivo',))
        cantidad, tamano = 0, 0
        if cursor.fetchone()[0]:
            cursor.execute(sql.SQL("SELECT count(*), coalesce(sum(tamano), 0) FROM {}.gen_archivo").format(
                sql.Identifier(schema)))
            cantidad, tamano = cursor.fetchone()
    origen.close()
    if not cantidad:
        return False

    print(f"El tenant tiene {cantidad} archivos adjuntos ({float(tamano) / 1024 / 1024:.1f} MB) en Backblaze.")
    while True:
        respuesta = input("¿Desea migrar los archivos? (s/n): ").strip().lower()
        if respuesta in ("s", "si", "sí"):
            print()
            return True
        if respuesta in ("n", "no"):
            print()
            return False
        error("Responda s o n.")


def crear_tenant(schema):
    """Crea el tenant en torio con crear_tenant_torio.py, dentro del entorno de
    torio. El dueno es propietario con todos los modulos; control e invitado
    entran como superusuarios con los modulos que incluia su plan en itrio."""
    origen = conectar(leer_conexion("PG_ORIGEN"), "origen")
    with origen.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
        cursor.execute(SQL_TENANT_ORIGEN, (schema,))
        tenant = cursor.fetchone()
        cursor.execute(SQL_MIEMBROS_ORIGEN, (tenant["id"],))
        miembros = cursor.fetchall()
    origen.close()

    # Sin plan (no deberia pasar fuera de public) se dan todos los modulos.
    con_plan = tenant["venta"] is not None
    accesos_plan = {f"acceso_{m}": (tenant[m] if con_plan else True) for m in MODULOS_PLAN}
    accesos_plan["acceso_turno"] = False
    entrada = {
        "base": {k: leer_conexion("PG_DESTINO")[k] for k in ("dbname", "host", "port")},
        "cliente": {k: tenant[k] for k in ("id", "schema_name", "owner_id", "fecha_creacion",
                                           "fecha_ultima_conexion", "correo", "celular")}
                   | {"nombre": tenant["nombre"] or schema},
        "miembros": [{"usuario_id": m["usuario_id"], "propietario": m["rol"] == "propietario",
                      "is_superuser": True, "accesos": accesos_plan} for m in miembros],
    }

    # Torio se ejecuta con su codigo pero contra el destino de migrar.py: las
    # variables de entorno mandan sobre el .env de torio (python-decouple).
    destino = leer_conexion("PG_DESTINO")
    dominio = config("TORIO_TENANT_BASE_DOMAIN", default="").strip()
    if not dominio:
        morir("Falta TORIO_TENANT_BASE_DOMAIN en .env: el dominio del tenant sera <schema>.<ese valor>")
    entorno = os.environ | {
        "DATABASE_HOST": destino["host"], "DATABASE_PORT": destino["port"],
        "DATABASE_NAME": destino["dbname"], "DATABASE_USER": destino["user"],
        "DATABASE_CLAVE": destino["password"], "TENANT_BASE_DOMAIN": dominio,
    }
    info(f"Creando el tenant {schema} en {destino['user']}@{destino['host']}:{destino['port']}/"
         f"{destino['dbname']}, dominio {schema}.{dominio} (migraciones y catalogos, puede tardar)...")
    try:
        proceso = subprocess.run(
            [TORIO_PYTHON, str(DIR_SCRIPT / "crear_tenant_torio.py")],
            cwd=TORIO_DIR, env=entorno, input=json.dumps(entrada, default=str),
            capture_output=True, text=True, check=False)
    except OSError as e:
        morir(f"No se pudo ejecutar torio ({TORIO_PYTHON}): {e}")
    lineas = proceso.stdout.strip().splitlines()
    try:
        resultado = json.loads(lineas[-1])
    except (IndexError, json.JSONDecodeError):
        morir(f"crear_tenant_torio.py no devolvio resultado:\n{proceso.stdout}{proceso.stderr}")
    if resultado["estado"] == "error":
        morir(f"No se creo el tenant {schema}:\n{resultado.get('detalle')}")
    if resultado["estado"] == "existe":
        info(f"El tenant {schema} ya existia en torio")
    else:
        ok(f"Tenant {schema} creado en torio (id {resultado['id']}, {resultado['miembros']} miembros, "
           "suscripcion de prueba)")
    print()


# ------------------------------------------------------------------ ejecucion

def leer_argumentos():
    parser = argparse.ArgumentParser(
        description="Migra de itrio (PG_ORIGEN) a torio (PG_DESTINO): los usuarios o los datos "
                    "de un tenant, segun la opcion que se escoja en el menu.")
    parser.add_argument("-m", dest="modo", default="completar", choices=("completar", "reemplazar"),
                        help="completar conserva lo del destino; reemplazar lo vacia antes")
    parser.add_argument("-t", dest="tablas", action="append", default=[],
                        help="migra solo esa tabla (repetible)")
    parser.add_argument("-n", dest="simulacion", action="store_true",
                        help="ensayo: informe por tabla, no escribe")
    parser.add_argument("-F", dest="sin_fk", action="store_true",
                        help="ignora las FK (mas datos, menos integridad)")
    parser.add_argument("-x", dest="sin_rescate", action="store_true", help="sin rescate fila a fila")
    return parser.parse_args()


def mostrar_menu():
    print("\n=== Migracion itrio -> torio ===")
    print("  1. Migrar usuarios")
    print("  2. Migrar tenant")
    print("  q. Salir")
    while True:
        opcion = input("\nSeleccione una opcion: ").strip().lower()
        if opcion in ("1", "2"):
            return opcion
        if opcion == "q":
            print("Operacion cancelada.")
            sys.exit(0)
        error("Opcion no valida.")


def main():
    args = leer_argumentos()
    if mostrar_menu() == "1":
        MigradorUsuarios(args).migrar()
        return
    schema, existe = elegir_tenant(args)
    args.archivos = preguntar_archivos(schema)
    if not existe:
        crear_tenant(schema)
    migrador = Migrador(args, schema)
    migrador.encabezado()
    if args.simulacion:
        migrador.simular()
    else:
        migrador.migrar()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nOperacion cancelada.")
        sys.exit(130)
