#!/usr/bin/env bash
#
# backup_reddoc.sh
#
# Crea/restaura una base de datos PostgreSQL local (PG_BACKUP) a partir de un
# archivo de dump ya existente. No se conecta a ningun servidor de origen.
#
# Uso:
#   ./backup_reddoc.sh archivo.sql              # crea la BD destino y restaura
#   ./backup_reddoc.sh -n bdotranombre arch.sql # sobreescribe el nombre de la BD
#   ./backup_reddoc.sh -j 4 archivo.sql         # jobs paralelos (solo formato custom)
#   ./backup_reddoc.sh -k archivo.sql           # conserva la BD existente, no la recrea
#   ./backup_reddoc.sh -D archivo.sql           # restaura sin renombrar los dominios
#   ./backup_reddoc.sh -m                       # solo renombra los dominios
#
# Al terminar la restauracion los dominios de los tenants se renombran a
# localhost, tomando el schema de cada uno: el schema 'public' queda como
# 'localhost' y el resto como '<schema>.localhost'.
#
# Formatos aceptados: custom de pg_dump (-Fc), SQL plano y SQL plano comprimido
# con gzip. El formato se detecta por contenido, no por la extension.
#
# Variables requeridas en .env (raiz del proyecto):
#   PG_BACKUP_DATABASE_HOST/USER/CLAVE/PORT/NAME

set -euo pipefail

DIR_SCRIPT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ARCHIVO_ENV="${DIR_SCRIPT}/.env"
PREFIJO_ENV="PG_BACKUP_DATABASE"
DIR_BACKUP="${DIR_BACKUP:-/home/desarrollo/Escritorio/backup}"

ARCHIVO_DUMP=""
NOMBRE_DESTINO=""
JOBS=1
CONSERVAR_BD=0
RENOMBRAR_DOMINIOS=1
SOLO_DOMINIOS=0
PG_BIN=""
TABLA_DOMINIOS=""
TABLA_TENANTS=""

# ---------------------------------------------------------------- utilidades

info()  { echo -e "🔄 $*"; }
ok()    { echo -e "✅ $*"; }
aviso() { echo -e "⚠️  $*"; }
error() { echo -e "❌ $*" >&2; }

morir() { error "$*"; exit 1; }

mostrar_ayuda() {
    sed -n '3,23p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'
    exit 0
}

# Lee una variable del .env, quitando comillas simples/dobles y espacios.
leer_env() {
    local clave="$1"
    local valor
    valor="$(grep -E "^[[:space:]]*${clave}[[:space:]]*=" "$ARCHIVO_ENV" | tail -n 1 | cut -d '=' -f 2-)" || true
    valor="${valor#"${valor%%[![:space:]]*}"}"
    valor="${valor%"${valor##*[![:space:]]}"}"
    valor="${valor%\'}"; valor="${valor#\'}"
    valor="${valor%\"}"; valor="${valor#\"}"
    printf '%s' "$valor"
}

verificar_binarios() {
    local faltantes=()
    for binario in pg_restore psql; do
        command -v "$binario" >/dev/null 2>&1 || faltantes+=("$binario")
    done
    [ ${#faltantes[@]} -eq 0 ] || morir "Faltan binarios de PostgreSQL: ${faltantes[*]}"
}

# Usa los binarios mas nuevos instalados: pg_restore lee archivos de versiones
# anteriores, pero nunca posteriores (un dump de un servidor 17 necesita un
# pg_restore >= 17).
resolver_bin() {
    local instalada candidata
    instalada="$(psql --version | grep -oE '[0-9]+' | head -n 1)"
    for candidata in $(ls -1 /usr/lib/postgresql 2>/dev/null | sort -rn); do
        if [ "$candidata" -gt "$instalada" ] && [ -x "/usr/lib/postgresql/${candidata}/bin/pg_restore" ]; then
            PG_BIN="/usr/lib/postgresql/${candidata}/bin/"
            info "Usando binarios PostgreSQL ${candidata}"
            return
        fi
    done
    PG_BIN=""
}

# Devuelve custom | gzip | plano segun los primeros bytes del archivo.
detectar_formato() {
    local archivo="$1"
    if [ "$(head -c 5 "$archivo")" = "PGDMP" ]; then
        echo "custom"
    elif [ "$(head -c 2 "$archivo" | od -An -tx1 | tr -d ' ')" = "1f8b" ]; then
        echo "gzip"
    else
        echo "plano"
    fi
}

listar_candidatos() {
    local encontrados
    encontrados="$(find "$DIR_BACKUP" /home/desarrollo/Escritorio -maxdepth 1 \
        \( -name '*.sql' -o -name '*.backup' -o -name '*.dump' -o -name '*.sql.gz' \) \
        -size +0 -printf '%TY-%Tm-%Td %TH:%TM  %10s  %p\n' 2>/dev/null | sort -r | head -10)"
    if [ -n "$encontrados" ]; then
        error ""
        error "Archivos disponibles:"
        echo "$encontrados" >&2
    fi
}

# ------------------------------------------------------------------ opciones

while getopts ":n:j:kDmh" opcion; do
    case "$opcion" in
        n) NOMBRE_DESTINO="$OPTARG" ;;
        j) JOBS="$OPTARG" ;;
        k) CONSERVAR_BD=1 ;;
        D) RENOMBRAR_DOMINIOS=0 ;;
        m) SOLO_DOMINIOS=1 ;;
        h) mostrar_ayuda ;;
        \?) morir "Opcion no valida: -$OPTARG" ;;
        :)  morir "La opcion -$OPTARG requiere un argumento" ;;
    esac
done
shift $((OPTIND - 1))

if [ "$SOLO_DOMINIOS" -eq 0 ]; then
    [ $# -gt 0 ] || { error "Falta el archivo de dump a restaurar."; error "Uso: $(basename "$0") [-n nombre_bd] [-j jobs] [-k] [-D] archivo"; listar_candidatos; exit 1; }
    ARCHIVO_DUMP="$1"
fi

# ---------------------------------------------------------------- validacion

verificar_binarios
[ -f "$ARCHIVO_ENV" ] || morir "No se encontro el archivo .env en ${DIR_SCRIPT}"
if [ "$SOLO_DOMINIOS" -eq 0 ]; then
    [ -f "$ARCHIVO_DUMP" ] || { error "El archivo no existe: ${ARCHIVO_DUMP}"; listar_candidatos; exit 1; }
    [ -s "$ARCHIVO_DUMP" ] || { error "El archivo esta vacio (0 bytes): ${ARCHIVO_DUMP}"; listar_candidatos; exit 1; }
fi

DESTINO_HOST="$(leer_env "${PREFIJO_ENV}_HOST")"
DESTINO_USER="$(leer_env "${PREFIJO_ENV}_USER")"
DESTINO_CLAVE="$(leer_env "${PREFIJO_ENV}_CLAVE")"
DESTINO_PORT="$(leer_env "${PREFIJO_ENV}_PORT")"
DESTINO_NAME="${NOMBRE_DESTINO:-$(leer_env "${PREFIJO_ENV}_NAME")}"

for sufijo in HOST USER CLAVE PORT NAME; do
    variable="DESTINO_${sufijo}"
    [ -n "${!variable}" ] || morir "Variable vacia o ausente en .env: ${PREFIJO_ENV}_${sufijo}"
done

resolver_bin
FORMATO=""
[ "$SOLO_DOMINIOS" -eq 1 ] || FORMATO="$(detectar_formato "$ARCHIVO_DUMP")"

# --------------------------------------------------------------- operaciones

# Ejecuta psql contra la base de mantenimiento 'postgres' del destino.
psql_destino_admin() {
    PGPASSWORD="$DESTINO_CLAVE" "${PG_BIN}psql" \
        --host="$DESTINO_HOST" \
        --port="$DESTINO_PORT" \
        --username="$DESTINO_USER" \
        --dbname=postgres \
        --no-psqlrc \
        --quiet \
        --tuples-only \
        --set=ON_ERROR_STOP=1 \
        --command="$1"
}

# Ejecuta psql contra la base restaurada y devuelve un valor escalar.
psql_destino_valor() {
    PGPASSWORD="$DESTINO_CLAVE" "${PG_BIN}psql" \
        --host="$DESTINO_HOST" \
        --port="$DESTINO_PORT" \
        --username="$DESTINO_USER" \
        --dbname="$DESTINO_NAME" \
        --no-psqlrc \
        --quiet \
        --tuples-only \
        --no-align \
        --set=ON_ERROR_STOP=1 \
        --command="$1"
}

# Igual que psql_destino_valor, pero con la salida tabular de psql.
psql_destino_tabla() {
    PGPASSWORD="$DESTINO_CLAVE" "${PG_BIN}psql" \
        --host="$DESTINO_HOST" \
        --port="$DESTINO_PORT" \
        --username="$DESTINO_USER" \
        --dbname="$DESTINO_NAME" \
        --no-psqlrc \
        --set=ON_ERROR_STOP=1 \
        --command="$1"
}

crear_base_destino() {
    local existe
    existe="$(psql_destino_admin "SELECT 1 FROM pg_database WHERE datname = '${DESTINO_NAME}';" | tr -d '[:space:]')"

    if [ "$existe" = "1" ]; then
        if [ "$CONSERVAR_BD" -eq 1 ]; then
            info "La base ${DESTINO_NAME} ya existe y se conserva (-k)."
            return
        fi
        aviso "La base ${DESTINO_NAME} ya existe en ${DESTINO_HOST}:${DESTINO_PORT} y sera ELIMINADA."
        read -r -p "Escriba el nombre de la base para confirmar: " confirmacion
        [ "$confirmacion" = "$DESTINO_NAME" ] || morir "Confirmacion incorrecta, se cancela la operacion."

        info "Cerrando conexiones activas sobre ${DESTINO_NAME}..."
        psql_destino_admin "SELECT pg_terminate_backend(pid)
                            FROM pg_stat_activity
                            WHERE datname = '${DESTINO_NAME}' AND pid <> pg_backend_pid();" >/dev/null

        info "Eliminando base ${DESTINO_NAME}..."
        psql_destino_admin "DROP DATABASE \"${DESTINO_NAME}\";" >/dev/null
    fi

    info "Creando base ${DESTINO_NAME}..."
    psql_destino_admin "CREATE DATABASE \"${DESTINO_NAME}\" WITH OWNER \"${DESTINO_USER}\" ENCODING 'UTF8';" >/dev/null
    ok "Base ${DESTINO_NAME} creada"
}

restaurar_custom() {
    # pg_restore devuelve codigo != 0 por avisos no fatales (roles, extensiones);
    # no se aborta el script, se informa al final.
    local codigo=0
    PGPASSWORD="$DESTINO_CLAVE" "${PG_BIN}pg_restore" \
        --host="$DESTINO_HOST" \
        --port="$DESTINO_PORT" \
        --username="$DESTINO_USER" \
        --dbname="$DESTINO_NAME" \
        --jobs="$JOBS" \
        --no-owner \
        --no-acl \
        --verbose \
        "$ARCHIVO_DUMP" || codigo=$?
    return "$codigo"
}

restaurar_plano() {
    local codigo=0
    local lector=(cat "$ARCHIVO_DUMP")
    [ "$FORMATO" = "gzip" ] && lector=(gunzip -c "$ARCHIVO_DUMP")

    "${lector[@]}" | PGPASSWORD="$DESTINO_CLAVE" "${PG_BIN}psql" \
        --host="$DESTINO_HOST" \
        --port="$DESTINO_PORT" \
        --username="$DESTINO_USER" \
        --dbname="$DESTINO_NAME" \
        --no-psqlrc \
        --echo-errors \
        --set=ON_ERROR_STOP=0 || codigo=$?
    return "$codigo"
}

# ------------------------------------------------------------------ dominios

existe_tabla() {
    [ "$(psql_destino_valor "SELECT to_regclass('$1') IS NOT NULL;" | tr -d '[:space:]')" = "t" ]
}

# Localiza la tabla de dominios y, por su clave foranea, la tabla de tenants que
# guarda el schema. Los nombres cambian entre bases (cnt_dominio/cnt_contenedor
# en reddoc, ctn_dominio/ctn_cliente en otras), por eso no se codifican.
resolver_tablas_dominio() {
    local candidata
    for candidata in public.cnt_dominio public.ctn_dominio; do
        if existe_tabla "$candidata"; then
            TABLA_DOMINIOS="$candidata"
            break
        fi
    done
    [ -n "$TABLA_DOMINIOS" ] || return 1

    TABLA_TENANTS="$(psql_destino_valor "
        SELECT c.confrelid::regclass::text
          FROM pg_constraint c
         WHERE c.conrelid = '${TABLA_DOMINIOS}'::regclass
           AND c.contype = 'f'
           AND EXISTS (SELECT 1
                         FROM pg_attribute a
                        WHERE a.attrelid = c.confrelid
                          AND a.attname = 'schema_name'
                          AND a.attnum > 0
                          AND NOT a.attisdropped)
         LIMIT 1;" | tr -d '[:space:]')"
}

# Renombra los dominios de la copia local a localhost para que la aplicacion
# resuelva los tenants en la maquina de desarrollo. Es idempotente: solo se
# actualizan las filas cuyo dominio no coincide ya con el valor calculado.
renombrar_dominios() {
    if ! resolver_tablas_dominio; then
        aviso "No se encontro la tabla de dominios en ${DESTINO_NAME}: no se renombra nada."
        return
    fi

    local sql_nuevos
    if [ -n "$TABLA_TENANTS" ]; then
        # El dominio se deriva del schema del tenant: el schema 'public' es el
        # dominio principal y cada tenant queda en <schema>.localhost. schema_name
        # es unico, asi que no puede chocar con la restriccion UNIQUE de domain.
        info "Dominios: ${TABLA_DOMINIOS} segun el schema en ${TABLA_TENANTS}"
        sql_nuevos="SELECT d.id,
                           CASE WHEN t.schema_name = 'public' THEN 'localhost'
                                ELSE t.schema_name || '.localhost'
                           END AS nuevo
                      FROM ${TABLA_DOMINIOS} d
                      JOIN ${TABLA_TENANTS} t ON t.id = d.tenant_id"
    else
        # Sin tabla de tenants: se conserva la primera etiqueta del dominio.
        aviso "No se encontro la tabla de tenants: se usa la primera etiqueta del dominio."
        sql_nuevos="SELECT id,
                           CASE WHEN id = 1 THEN 'localhost'
                                WHEN domain LIKE '%.%' THEN split_part(domain, '.', 1) || '.localhost'
                                ELSE domain
                           END AS nuevo
                      FROM ${TABLA_DOMINIOS}"
    fi

    local codigo=0 renombrados=""
    renombrados="$(psql_destino_valor "
        WITH nuevos AS (${sql_nuevos}),
             cambio AS (
                 UPDATE ${TABLA_DOMINIOS} d
                    SET domain = n.nuevo
                   FROM nuevos n
                  WHERE n.id = d.id AND d.domain <> n.nuevo
                 RETURNING 1
             )
        SELECT count(*) FROM cambio;" | tr -d '[:space:]')" || codigo=$?

    if [ "$codigo" -ne 0 ]; then
        aviso "No se pudieron renombrar los dominios (codigo ${codigo}): revise el error anterior."
        return
    fi

    local total
    total="$(psql_destino_valor "SELECT count(*) FROM ${TABLA_DOMINIOS};" | tr -d '[:space:]')"
    ok "Dominios renombrados: ${renombrados} de ${total}"
    psql_destino_tabla "SELECT id, domain FROM ${TABLA_DOMINIOS} ORDER BY id LIMIT 10;"
    if [ "$total" -gt 10 ]; then
        info "Se muestran los 10 primeros de ${total}."
    fi
}

# --------------------------------------------------------------- restauracion

restaurar_backup() {
    crear_base_destino

    info "Restaurando (formato ${FORMATO}) en ${DESTINO_HOST}:${DESTINO_PORT}/${DESTINO_NAME}..."
    local codigo=0
    if [ "$FORMATO" = "custom" ]; then
        restaurar_custom || codigo=$?
    else
        restaurar_plano || codigo=$?
    fi

    if [ "$codigo" -eq 0 ]; then
        ok "Restauracion completada en ${DESTINO_NAME}"
    else
        aviso "La restauracion termino con codigo ${codigo}: revise los mensajes anteriores."
        aviso "Suele deberse a objetos de rol/extension que no existen en local."
    fi

    local tablas
    tablas="$(psql_destino_valor "SELECT count(*) FROM information_schema.tables WHERE table_schema NOT IN ('pg_catalog','information_schema');" | tr -d '[:space:]')"
    ok "Tablas en ${DESTINO_NAME}: ${tablas}"

    if [ "$RENOMBRAR_DOMINIOS" -eq 1 ]; then
        renombrar_dominios
    else
        info "Los dominios se dejan como estan (-D)."
    fi
}

# ------------------------------------------------------------------ ejecucion

echo "Destino: ${DESTINO_USER}@${DESTINO_HOST}:${DESTINO_PORT}/${DESTINO_NAME}"
if [ "$SOLO_DOMINIOS" -eq 1 ]; then
    echo
    renombrar_dominios
else
    echo "Archivo: ${ARCHIVO_DUMP} ($(du -h "$ARCHIVO_DUMP" | cut -f1), formato ${FORMATO})"
    echo

    restaurar_backup
fi

ok "Proceso finalizado"
