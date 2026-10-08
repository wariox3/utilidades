"""
migrar.py

Migra los datos de un esquema de la base de origen (PG_ORIGEN) al esquema del
mismo nombre en la base de destino (PG_DESTINO), copiando todo lo que sea
compatible: solo las tablas que existen en ambos lados y, dentro de ellas,
solo las columnas comunes.

Uso:
  python3 migrar.py                 # pregunta el esquema y lo migra
  python3 migrar.py -n              # ensayo: informe por tabla, no escribe
  python3 migrar.py -t gen_contacto # solo esa tabla (repetible)
  python3 migrar.py -m reemplazar   # vacia las tablas destino antes
  python3 migrar.py -F              # ignora las FK (mas datos, menos integridad)
  python3 migrar.py -x              # sin rescate fila a fila

El esquema se pide siempre al arrancar, escogiendolo entre los que existen
en ambas bases.

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
"""

import argparse
import datetime
import re
import sys
import tempfile
from pathlib import Path

import psycopg2
from psycopg2 import sql
from decouple import config

DIR_SCRIPT = Path(__file__).resolve().parent

# Tablas de control de Django: migrarlas romperia el estado de migraciones.
EXCLUIDAS = ["django_migrations", "django_content_type", "django_session"]
# Modelos ignorados a proposito. gen_pais, gen_estado, gen_ciudad y
# gen_identificacion son catalogos generales que el destino ya trae cargados y
# cuyos ids ademas cambiaron de texto a bigint. gen_archivo cambio de forma en
# reddoc2 (la referencia generica modelo/documento_id se normalizo en el par
# modelo_id/objeto_id), asi que necesita una migracion propia.
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
    # electronico_id paso de integer a uuid y los ids viejos no tienen
    # equivalente: el documento migra sin el vinculo electronico.
    "gen_documento.electronico_id": "NULL",
}


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
    def __init__(self, args):
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
        self.esquema = self.pedir_esquema()

        if args.sin_fk:
            with self.destino.cursor() as cursor:
                cursor.execute("SET session_replication_role = replica")
            self.destino.commit()

        self.planificar()

    # ------------------------------------------------------------- esquema

    def pedir_esquema(self):
        """Muestra los esquemas que existen en ambas bases y pide uno."""
        consulta = """SELECT nspname FROM pg_namespace
                       WHERE nspname NOT LIKE 'pg\\_%' AND nspname <> 'information_schema'"""
        esquemas = {}
        for conexion, nombre in ((self.origen, "origen"), (self.destino, "destino")):
            with conexion.cursor() as cursor:
                cursor.execute(consulta)
                esquemas[nombre] = {fila[0] for fila in cursor.fetchall()}
        self.destino.commit()

        comunes = sorted(esquemas["origen"] & esquemas["destino"])
        if not comunes:
            morir("No hay esquemas con el mismo nombre en origen y destino")

        o, d = self.datos_origen["dbname"], self.datos_destino["dbname"]
        print(f"\n=== Esquemas presentes en {o} y {d} ===")
        for indice, esquema in enumerate(comunes, start=1):
            print(f"  {indice:>3}. {esquema}")
        print("    q. Salir")

        while True:
            opcion = input("\nEsquema a migrar (numero o nombre): ").strip()
            if opcion.lower() == "q":
                print("Operacion cancelada.")
                sys.exit(0)
            if opcion.isdigit() and 1 <= int(opcion) <= len(comunes):
                esquema = comunes[int(opcion) - 1]
            elif opcion in comunes:
                esquema = opcion
            else:
                faltan = [n for n in ("origen", "destino")
                          if opcion and not opcion.isdigit() and opcion not in esquemas[n]]
                error(f"El esquema {opcion} no existe en el {' ni en el '.join(faltan)}" if faltan
                      else "Opcion no valida.")
                continue
            print()
            return esquema

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

            self.ajustar_secuencias()

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


# ------------------------------------------------------------------ ejecucion

def leer_argumentos():
    parser = argparse.ArgumentParser(
        description="Migra un esquema de PG_ORIGEN a PG_DESTINO copiando tablas y columnas comunes. "
                    "El esquema se pide siempre al arrancar.")
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


def main():
    args = leer_argumentos()
    migrador = Migrador(args)
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
