"""
backup_reddoc.py

Crea/restaura una base de datos PostgreSQL local (PG_DESTINO) a partir de un
archivo de dump ya existente. No se conecta a ningun servidor de origen.

Uso:
  python3 backup_reddoc.py                       # menu para escoger el archivo
  python3 backup_reddoc.py archivo.dump          # restaura el archivo indicado
  python3 backup_reddoc.py -n bdotranombre arch  # usa otro nombre en vez del del dump
  python3 backup_reddoc.py -j 4 archivo          # jobs paralelos (solo formato custom)
  python3 backup_reddoc.py -k archivo            # conserva la BD existente, no la recrea
  python3 backup_reddoc.py -D archivo            # restaura sin renombrar los dominios
  python3 backup_reddoc.py -m                    # solo renombra los dominios

Al terminar la restauracion los dominios de los tenants se renombran a
localhost, tomando el schema de cada uno: el schema 'public' queda como
'localhost' y el resto como '<schema>.localhost'.

La base destino se llama igual que la base de origen del dump (dbname en el
formato custom, CREATE DATABASE o \\connect en el SQL plano). Si el dump no
lo indica, se pregunta. Con -n se usa otro nombre.

Formatos aceptados: custom de pg_dump (-Fc), SQL plano y SQL plano comprimido
con gzip. El formato se detecta por contenido, no por la extension.

Variables requeridas en .env (raiz del proyecto):
  PG_DESTINO_DATABASE_HOST/USER/CLAVE/PORT
  PG_DESTINO_DATABASE_NAME solo se usa con -m (si falta, se pregunta).
"""

import argparse
import datetime
import gzip
import re
import os
import shutil
import subprocess
import sys
from pathlib import Path

import psycopg2
from psycopg2 import sql
from decouple import config

PREFIJO_ENV = "PG_DESTINO_DATABASE"
DIR_BACKUP = Path(os.environ.get("DIR_BACKUP", "/home/desarrollo/Escritorio/backup"))
DIRS_BUSQUEDA = [DIR_BACKUP, Path("/home/desarrollo/Escritorio")]
EXTENSIONES = (".sql", ".backup", ".dump", ".sql.gz")
MAX_CANDIDATOS = 20


# ---------------------------------------------------------------- utilidades

def info(mensaje):
    print(f"🔄 {mensaje}")


def ok(mensaje):
    print(f"✅ {mensaje}")


def aviso(mensaje):
    print(f"⚠️  {mensaje}")


def error(mensaje):
    print(f"❌ {mensaje}", file=sys.stderr)


def morir(mensaje):
    error(mensaje)
    sys.exit(1)


def tamano_legible(bytes_):
    for unidad in ("B", "K", "M", "G", "T"):
        if bytes_ < 1024 or unidad == "T":
            return f"{bytes_:.1f}{unidad}" if unidad != "B" else f"{bytes_}{unidad}"
        bytes_ /= 1024


def verificar_binarios():
    faltantes = [b for b in ("pg_restore", "psql") if shutil.which(b) is None]
    if faltantes:
        morir(f"Faltan binarios de PostgreSQL: {' '.join(faltantes)}")


def resolver_bin():
    """Usa los binarios mas nuevos instalados: pg_restore lee archivos de
    versiones anteriores, pero nunca posteriores (un dump de un servidor 17
    necesita un pg_restore >= 17)."""
    salida = subprocess.run(["psql", "--version"], capture_output=True, text=True).stdout
    instalada = int("".join(c if c.isdigit() else " " for c in salida).split()[0])
    base = Path("/usr/lib/postgresql")
    versiones = sorted((int(d.name) for d in base.iterdir() if d.name.isdigit()), reverse=True) if base.is_dir() else []
    for candidata in versiones:
        if candidata > instalada and os.access(base / str(candidata) / "bin" / "pg_restore", os.X_OK):
            info(f"Usando binarios PostgreSQL {candidata}")
            return f"{base / str(candidata) / 'bin'}/"
    return ""


def detectar_formato(archivo):
    """Devuelve custom | gzip | plano segun los primeros bytes del archivo."""
    with open(archivo, "rb") as f:
        cabecera = f.read(5)
    if cabecera == b"PGDMP":
        return "custom"
    if cabecera[:2] == b"\x1f\x8b":
        return "gzip"
    return "plano"


def nombre_base_dump(archivo, pg_bin=""):
    """Nombre de la base de origen guardado en el dump, o None si no consta."""
    formato = detectar_formato(archivo)
    if formato == "custom":
        resultado = subprocess.run([f"{pg_bin}pg_restore", "--list", str(archivo)],
                                   capture_output=True, text=True, check=False)
        coincidencia = re.search(r"^;\s+dbname:\s*(\S+)", resultado.stdout, re.MULTILINE)
        return coincidencia.group(1) if coincidencia else None

    # SQL plano: solo trae el nombre si se genero con pg_dump --create.
    # pg_dump escribe: CREATE DATABASE bd ...  /  \connect -reuse-previous=on "dbname='bd'"
    patron = re.compile(r"""^(?:CREATE DATABASE\s+"?|\\connect\s+(?:-reuse-previous=on\s+"dbname='|"?))([^"'\s;]+)""")
    abrir = gzip.open if formato == "gzip" else open
    with abrir(archivo, "rt", encoding="utf-8", errors="replace") as f:
        for numero, linea in enumerate(f):
            coincidencia = patron.match(linea)
            if coincidencia:
                return coincidencia.group(1)
            if numero > 500:
                break
    return None


# ------------------------------------------------------ seleccion de archivo

def listar_candidatos():
    """Archivos de dump no vacios en los directorios de busqueda, del mas
    reciente al mas antiguo."""
    vistos = set()
    candidatos = []
    for directorio in DIRS_BUSQUEDA:
        if not directorio.is_dir():
            continue
        for archivo in directorio.iterdir():
            if not archivo.is_file() or not archivo.name.endswith(EXTENSIONES):
                continue
            real = archivo.resolve()
            if real in vistos:
                continue
            estado = archivo.stat()
            if estado.st_size == 0:
                continue
            vistos.add(real)
            candidatos.append((estado.st_mtime, estado.st_size, archivo))
    candidatos.sort(reverse=True)
    return candidatos[:MAX_CANDIDATOS]


def seleccionar_archivo():
    candidatos = listar_candidatos()
    if not candidatos:
        error(f"No se encontraron archivos de dump en: {', '.join(str(d) for d in DIRS_BUSQUEDA)}")

    print("\n=== Archivos disponibles para restaurar ===")
    for indice, (mtime, tamano, archivo) in enumerate(candidatos, start=1):
        fecha = datetime.datetime.fromtimestamp(mtime).strftime("%Y-%m-%d %H:%M")
        print(f"  {indice:>2}. {fecha}  {tamano_legible(tamano):>8}  {archivo}")
    print("   r. Escribir otra ruta")
    print("   q. Salir")

    while True:
        opcion = input("\nSeleccione el archivo a restaurar: ").strip()
        if opcion.lower() == "q":
            print("Operacion cancelada.")
            sys.exit(0)
        if opcion.lower() == "r":
            ruta = Path(input("Ruta del archivo: ").strip()).expanduser()
            if ruta.is_file() and ruta.stat().st_size > 0:
                return ruta
            error(f"El archivo no existe o esta vacio: {ruta}")
            continue
        if opcion.isdigit() and 1 <= int(opcion) <= len(candidatos):
            return candidatos[int(opcion) - 1][2]
        error("Opcion no valida.")


# ------------------------------------------------------------------ restaurador

class Restaurador:
    def __init__(self, args):
        self.args = args
        self.host = self._leer_env("HOST")
        self.usuario = self._leer_env("USER")
        self.clave = self._leer_env("CLAVE")
        self.puerto = self._leer_env("PORT")
        self.nombre = args.nombre
        self.pg_bin = ""
        self.tabla_dominios = None
        self.tabla_tenants = None

    @staticmethod
    def _leer_env(sufijo, requerido=True):
        variable = f"{PREFIJO_ENV}_{sufijo}"
        valor = config(variable, default="").strip()
        if not valor and requerido:
            morir(f"Variable vacia o ausente en .env: {variable}")
        return valor

    @staticmethod
    def _pedir_nombre(motivo):
        nombre = input(f"Nombre de la base destino ({motivo}): ").strip()
        if not nombre:
            morir("No se indico el nombre de la base destino.")
        return nombre

    def resolver_nombre(self, archivo=None):
        """-n tiene prioridad; si no, el nombre sale del dump (o del .env con -m)."""
        if self.nombre:
            return
        if archivo is None:
            self.nombre = (self._leer_env("NAME", requerido=False)
                           or self._pedir_nombre(f"{PREFIJO_ENV}_NAME no definida en .env"))
            return
        self.nombre = nombre_base_dump(archivo, self.pg_bin)
        if self.nombre:
            info(f"Nombre de la base tomado del dump: {self.nombre}")
        else:
            self.nombre = self._pedir_nombre("el dump no indica el nombre de la base")

    def _entorno(self):
        return {**os.environ, "PGPASSWORD": self.clave}

    def _conectar(self, base):
        return psycopg2.connect(host=self.host, port=self.puerto, user=self.usuario,
                                password=self.clave, dbname=base)

    def _consultar_valor(self, consulta, parametros=None):
        with self._conectar(self.nombre) as conexion, conexion.cursor() as cursor:
            cursor.execute(consulta, parametros)
            fila = cursor.fetchone()
        conexion.close()
        return fila[0] if fila else None

    # ---------------------------------------------------------- base destino

    def crear_base_destino(self):
        conexion = self._conectar("postgres")
        conexion.autocommit = True
        try:
            with conexion.cursor() as cursor:
                cursor.execute("SELECT 1 FROM pg_database WHERE datname = %s", (self.nombre,))
                if cursor.fetchone():
                    if self.args.conservar:
                        info(f"La base {self.nombre} ya existe y se conserva (-k).")
                        return
                    aviso(f"La base {self.nombre} ya existe en {self.host}:{self.puerto} y sera ELIMINADA.")
                    confirmacion = input("Escriba el nombre de la base para confirmar: ").strip()
                    if confirmacion != self.nombre:
                        morir("Confirmacion incorrecta, se cancela la operacion.")

                    info(f"Cerrando conexiones activas sobre {self.nombre}...")
                    cursor.execute("""SELECT pg_terminate_backend(pid)
                                        FROM pg_stat_activity
                                       WHERE datname = %s AND pid <> pg_backend_pid()""", (self.nombre,))

                    info(f"Eliminando base {self.nombre}...")
                    cursor.execute(sql.SQL("DROP DATABASE {}").format(sql.Identifier(self.nombre)))

                info(f"Creando base {self.nombre}...")
                cursor.execute(sql.SQL("CREATE DATABASE {} WITH OWNER {} ENCODING 'UTF8'").format(
                    sql.Identifier(self.nombre), sql.Identifier(self.usuario)))
                ok(f"Base {self.nombre} creada")
        finally:
            conexion.close()

    # ----------------------------------------------------------- restauracion

    def _argumentos_conexion(self):
        return [f"--host={self.host}", f"--port={self.puerto}",
                f"--username={self.usuario}", f"--dbname={self.nombre}"]

    def restaurar_custom(self, archivo):
        # pg_restore devuelve codigo != 0 por avisos no fatales (roles,
        # extensiones); no se aborta, se informa al final.
        comando = [f"{self.pg_bin}pg_restore", *self._argumentos_conexion(),
                   f"--jobs={self.args.jobs}", "--no-owner", "--no-acl", "--verbose", str(archivo)]
        return subprocess.run(comando, env=self._entorno()).returncode

    def restaurar_plano(self, archivo, formato):
        comando = [f"{self.pg_bin}psql", *self._argumentos_conexion(),
                   "--no-psqlrc", "--echo-errors", "--set=ON_ERROR_STOP=0"]
        abrir = gzip.open if formato == "gzip" else open
        with abrir(archivo, "rb") as entrada:
            proceso = subprocess.Popen(comando, stdin=subprocess.PIPE, env=self._entorno())
            try:
                shutil.copyfileobj(entrada, proceso.stdin)
            except BrokenPipeError:
                pass
            finally:
                proceso.stdin.close()
            return proceso.wait()

    def restaurar_backup(self, archivo):
        formato = detectar_formato(archivo)
        print(f"Archivo: {archivo} ({tamano_legible(archivo.stat().st_size)}, formato {formato})\n")

        self.crear_base_destino()

        info(f"Restaurando (formato {formato}) en {self.host}:{self.puerto}/{self.nombre}...")
        if formato == "custom":
            codigo = self.restaurar_custom(archivo)
        else:
            codigo = self.restaurar_plano(archivo, formato)

        if codigo == 0:
            ok(f"Restauracion completada en {self.nombre}")
        else:
            aviso(f"La restauracion termino con codigo {codigo}: revise los mensajes anteriores.")
            aviso("Suele deberse a objetos de rol/extension que no existen en local.")

        tablas = self._consultar_valor(
            "SELECT count(*) FROM information_schema.tables "
            "WHERE table_schema NOT IN ('pg_catalog', 'information_schema')")
        ok(f"Tablas en {self.nombre}: {tablas}")

        if self.args.sin_dominios:
            info("Los dominios se dejan como estan (-D).")
        else:
            self.renombrar_dominios()

    # --------------------------------------------------------------- dominios

    def resolver_tablas_dominio(self):
        """Localiza la tabla de dominios y, por su clave foranea, la tabla de
        tenants que guarda el schema. Los nombres cambian entre bases
        (cnt_dominio/cnt_contenedor en reddoc, ctn_dominio/ctn_cliente en
        otras), por eso no se codifican."""
        for candidata in ("public.cnt_dominio", "public.ctn_dominio"):
            if self._consultar_valor("SELECT to_regclass(%s) IS NOT NULL", (candidata,)):
                self.tabla_dominios = candidata
                break
        if not self.tabla_dominios:
            return False

        self.tabla_tenants = self._consultar_valor("""
            SELECT c.confrelid::regclass::text
              FROM pg_constraint c
             WHERE c.conrelid = %s::regclass
               AND c.contype = 'f'
               AND EXISTS (SELECT 1
                             FROM pg_attribute a
                            WHERE a.attrelid = c.confrelid
                              AND a.attname = 'schema_name'
                              AND a.attnum > 0
                              AND NOT a.attisdropped)
             LIMIT 1""", (self.tabla_dominios,))
        return True

    def renombrar_dominios(self):
        """Renombra los dominios de la copia local a localhost para que la
        aplicacion resuelva los tenants en la maquina de desarrollo. Es
        idempotente: solo se actualizan las filas cuyo dominio no coincide ya
        con el valor calculado."""
        if not self.resolver_tablas_dominio():
            aviso(f"No se encontro la tabla de dominios en {self.nombre}: no se renombra nada.")
            return

        # Los nombres vienen de to_regclass/regclass::text, ya citados por PostgreSQL.
        dominios = sql.SQL(self.tabla_dominios)
        if self.tabla_tenants:
            # El dominio se deriva del schema del tenant: el schema 'public' es
            # el dominio principal y cada tenant queda en <schema>.localhost.
            # schema_name es unico, asi que no choca con el UNIQUE de domain.
            info(f"Dominios: {self.tabla_dominios} segun el schema en {self.tabla_tenants}")
            sql_nuevos = sql.SQL("""
                SELECT d.id,
                       CASE WHEN t.schema_name = 'public' THEN 'localhost'
                            ELSE t.schema_name || '.localhost'
                       END AS nuevo
                  FROM {dominios} d
                  JOIN {tenants} t ON t.id = d.tenant_id""").format(
                dominios=dominios, tenants=sql.SQL(self.tabla_tenants))
        else:
            # Sin tabla de tenants: se conserva la primera etiqueta del dominio.
            aviso("No se encontro la tabla de tenants: se usa la primera etiqueta del dominio.")
            sql_nuevos = sql.SQL("""
                SELECT id,
                       CASE WHEN id = 1 THEN 'localhost'
                            WHEN domain LIKE '%.%' THEN split_part(domain, '.', 1) || '.localhost'
                            ELSE domain
                       END AS nuevo
                  FROM {dominios}""").format(dominios=dominios)

        consulta = sql.SQL("""
            WITH nuevos AS ({nuevos}),
                 cambio AS (
                     UPDATE {dominios} d
                        SET domain = n.nuevo
                       FROM nuevos n
                      WHERE n.id = d.id AND d.domain <> n.nuevo
                     RETURNING 1
                 )
            SELECT count(*) FROM cambio""").format(nuevos=sql_nuevos, dominios=dominios)

        try:
            renombrados = self._consultar_valor(consulta)
        except psycopg2.Error as e:
            aviso(f"No se pudieron renombrar los dominios: {e}")
            return

        total = self._consultar_valor(sql.SQL("SELECT count(*) FROM {}").format(dominios))
        ok(f"Dominios renombrados: {renombrados} de {total}")

        with self._conectar(self.nombre) as conexion, conexion.cursor() as cursor:
            cursor.execute(sql.SQL("SELECT id, domain FROM {} ORDER BY id LIMIT 10").format(dominios))
            filas = cursor.fetchall()
        conexion.close()
        print(f"\n  {'id':>6} | domain")
        print(f"  {'-' * 6}-+-{'-' * 40}")
        for id_, dominio in filas:
            print(f"  {id_:>6} | {dominio}")
        print()
        if total > 10:
            info(f"Se muestran los 10 primeros de {total}.")


# ------------------------------------------------------------------ ejecucion

def leer_argumentos():
    parser = argparse.ArgumentParser(
        description="Restaura un dump de PostgreSQL en la base local (PG_DESTINO_DATABASE_*).")
    parser.add_argument("archivo", nargs="?", type=Path,
                        help="archivo de dump; si se omite se muestra un menu para escogerlo")
    parser.add_argument("-n", dest="nombre", help="nombre de la base destino (por defecto, el del dump)")
    parser.add_argument("-j", dest="jobs", type=int, default=1, help="jobs paralelos (solo formato custom)")
    parser.add_argument("-k", dest="conservar", action="store_true", help="conserva la BD existente, no la recrea")
    parser.add_argument("-D", dest="sin_dominios", action="store_true", help="no renombra los dominios")
    parser.add_argument("-m", dest="solo_dominios", action="store_true", help="solo renombra los dominios")
    return parser.parse_args()


def main():
    args = leer_argumentos()
    verificar_binarios()
    restaurador = Restaurador(args)
    restaurador.pg_bin = resolver_bin()

    if args.solo_dominios:
        restaurador.resolver_nombre()
        print(f"Destino: {restaurador.usuario}@{restaurador.host}:{restaurador.puerto}/{restaurador.nombre}\n")
        restaurador.renombrar_dominios()
    else:
        archivo = args.archivo.expanduser() if args.archivo else seleccionar_archivo()
        if not archivo.is_file():
            morir(f"El archivo no existe: {archivo}")
        if archivo.stat().st_size == 0:
            morir(f"El archivo esta vacio (0 bytes): {archivo}")
        restaurador.resolver_nombre(archivo)
        print(f"Destino: {restaurador.usuario}@{restaurador.host}:{restaurador.puerto}/{restaurador.nombre}")
        restaurador.restaurar_backup(archivo)

    ok("Proceso finalizado")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nOperacion cancelada.")
        sys.exit(130)
