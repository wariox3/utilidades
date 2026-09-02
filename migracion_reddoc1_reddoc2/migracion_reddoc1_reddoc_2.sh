#!/usr/bin/env bash
#
# migracion_reddoc1_reddoc_2.sh
#
# Migra los datos de un esquema de la base de origen (PG_ORIGEN) al esquema del
# mismo nombre en la base de destino (PG_DESTINO), copiando todo lo que sea
# compatible: solo las tablas que existen en ambos lados y, dentro de ellas,
# solo las columnas comunes.
#
# Uso:
#   ./migracion_reddoc1_reddoc_2.sh                 # migra el esquema semantica
#   ./migracion_reddoc1_reddoc_2.sh -n              # simulacion: informe, no escribe
#   ./migracion_reddoc1_reddoc_2.sh -s otro_esquema # otro esquema
#   ./migracion_reddoc1_reddoc_2.sh -t gen_contacto # solo esa tabla (repetible)
#   ./migracion_reddoc1_reddoc_2.sh -m reemplazar   # vacia las tablas destino antes
#   ./migracion_reddoc1_reddoc_2.sh -F              # ignora las FK (mas datos, menos integridad)
#   ./migracion_reddoc1_reddoc_2.sh -x              # sin rescate fila a fila
#
# Modos (-m):
#   completar  (por defecto) conserva lo que ya hay en destino e inserta lo que
#              no colisione, con ON CONFLICT DO NOTHING.
#   reemplazar vacia con TRUNCATE ... CASCADE las tablas a migrar y carga desde cero.
#
# Si la carga masiva de una tabla falla, se reintenta fila a fila para salvar
# todo lo que sea insertable (desactivable con -x).
#
# Variables requeridas en .env:
#   PG_ORIGEN_DATABASE_HOST/USER/CLAVE/PORT/NAME
#   PG_DESTINO_DATABASE_HOST/USER/CLAVE/PORT/NAME

set -euo pipefail

DIR_SCRIPT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# El .env vive en la raiz del proyecto; se acepta tambien junto al script.
if [ -f "${DIR_SCRIPT}/.env" ]; then
    ARCHIVO_ENV="${DIR_SCRIPT}/.env"
else
    ARCHIVO_ENV="$(dirname "$DIR_SCRIPT")/.env"
fi

ESQUEMA="semantica"
MODO="completar"
SIMULACION=0
SIN_FK=0
SIN_RESCATE=0
TABLAS_PEDIDAS=()
# Tablas de control de Django: migrarlas romperia el estado de migraciones.
EXCLUIDAS="django_migrations django_content_type django_session"
PG_BIN=""

TIMESTAMP="$(date +%Y%m%d%H%M%S)"
DIR_TMP="$(mktemp -d)"
trap 'rm -rf "$DIR_TMP"' EXIT

# ---------------------------------------------------------------- utilidades

info()  { echo -e "🔄 $*"; }
ok()    { echo -e "✅ $*"; }
aviso() { echo -e "⚠️  $*"; }
error() { echo -e "❌ $*" >&2; }
morir() { error "$*"; exit 1; }

mostrar_ayuda() { sed -n '3,30p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'; exit 0; }

leer_env() {
    local clave="$1" valor
    valor="$(grep -E "^[[:space:]]*${clave}[[:space:]]*=" "$ARCHIVO_ENV" | tail -n 1 | cut -d '=' -f 2-)" || true
    valor="${valor#"${valor%%[![:space:]]*}"}"
    valor="${valor%"${valor##*[![:space:]]}"}"
    valor="${valor%\'}"; valor="${valor#\'}"
    valor="${valor%\"}"; valor="${valor#\"}"
    printf '%s' "$valor"
}

# psql mas nuevo instalado: conecta igual a servidores mayores y menores.
resolver_bin() {
    local instalada candidata
    instalada="$(psql --version | grep -oE '[0-9]+' | head -n 1)"
    for candidata in $(ls -1 /usr/lib/postgresql 2>/dev/null | sort -rn); do
        if [ "$candidata" -gt "$instalada" ] && [ -x "/usr/lib/postgresql/${candidata}/bin/psql" ]; then
            PG_BIN="/usr/lib/postgresql/${candidata}/bin/"
            return
        fi
    done
}

# ------------------------------------------------------------------ opciones

while getopts ":s:m:t:nFxh" opcion; do
    case "$opcion" in
        s) ESQUEMA="$OPTARG" ;;
        m) MODO="$OPTARG" ;;
        t) TABLAS_PEDIDAS+=("$OPTARG") ;;
        n) SIMULACION=1 ;;
        F) SIN_FK=1 ;;
        x) SIN_RESCATE=1 ;;
        h) mostrar_ayuda ;;
        \?) morir "Opcion no valida: -$OPTARG" ;;
        :)  morir "La opcion -$OPTARG requiere un argumento" ;;
    esac
done

case "$MODO" in
    completar|reemplazar) ;;
    *) morir "Modo no valido: ${MODO} (use 'completar' o 'reemplazar')" ;;
esac

# ---------------------------------------------------------------- conexiones

[ -f "$ARCHIVO_ENV" ] || morir "No se encontro el archivo .env (buscado en ${DIR_SCRIPT} y en su directorio padre)"
resolver_bin

O_HOST="$(leer_env PG_ORIGEN_DATABASE_HOST)";  O_USER="$(leer_env PG_ORIGEN_DATABASE_USER)"
O_CLAVE="$(leer_env PG_ORIGEN_DATABASE_CLAVE)"; O_PORT="$(leer_env PG_ORIGEN_DATABASE_PORT)"
O_NAME="$(leer_env PG_ORIGEN_DATABASE_NAME)"
D_HOST="$(leer_env PG_DESTINO_DATABASE_HOST)"; D_USER="$(leer_env PG_DESTINO_DATABASE_USER)"
D_CLAVE="$(leer_env PG_DESTINO_DATABASE_CLAVE)"; D_PORT="$(leer_env PG_DESTINO_DATABASE_PORT)"
D_NAME="$(leer_env PG_DESTINO_DATABASE_NAME)"

for v in O_HOST O_USER O_CLAVE O_PORT O_NAME D_HOST D_USER D_CLAVE D_PORT D_NAME; do
    [ -n "${!v}" ] || morir "Falta una variable de conexion en .env (${v})"
done

if [ "$O_HOST" = "$D_HOST" ] && [ "$O_PORT" = "$D_PORT" ] && [ "$O_NAME" = "$D_NAME" ]; then
    morir "Origen y destino son la misma base: ${O_HOST}:${O_PORT}/${O_NAME}"
fi

psql_origen()  { PGPASSWORD="$O_CLAVE" "${PG_BIN}psql" -h "$O_HOST" -p "$O_PORT" -U "$O_USER" -d "$O_NAME" --no-psqlrc -v ON_ERROR_STOP=1 "$@"; }
psql_destino() { PGPASSWORD="$D_CLAVE" "${PG_BIN}psql" -h "$D_HOST" -p "$D_PORT" -U "$D_USER" -d "$D_NAME" --no-psqlrc -v ON_ERROR_STOP=1 "$@"; }

psql_origen  -tAc "select 1" >/dev/null || morir "No se pudo conectar al origen"
psql_destino -tAc "select 1" >/dev/null || morir "No se pudo conectar al destino"

psql_origen  -tAc "select 1 from pg_namespace where nspname='${ESQUEMA}'" | grep -q 1 \
    || morir "El esquema ${ESQUEMA} no existe en el origen"
psql_destino -tAc "select 1 from pg_namespace where nspname='${ESQUEMA}'" | grep -q 1 \
    || morir "El esquema ${ESQUEMA} no existe en el destino"

# ------------------------------------------------------- catalogo de columnas

# Columnas reales de cada tabla (sin las generadas, que no admiten INSERT).
SQL_COLUMNAS="
SELECT c.relname || '|' || a.attname || '|' || format_type(a.atttypid, a.atttypmod)
FROM pg_attribute a
JOIN pg_class c ON c.oid = a.attrelid
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = '${ESQUEMA}' AND c.relkind = 'r'
  AND a.attnum > 0 AND NOT a.attisdropped AND a.attgenerated = ''
ORDER BY c.relname, a.attnum;"

psql_origen  -tAc "$SQL_COLUMNAS" > "$DIR_TMP/origen.txt"
psql_destino -tAc "$SQL_COLUMNAS" > "$DIR_TMP/destino.txt"

cut -d'|' -f1 "$DIR_TMP/origen.txt"  | sort -u > "$DIR_TMP/t_origen.txt"
cut -d'|' -f1 "$DIR_TMP/destino.txt" | sort -u > "$DIR_TMP/t_destino.txt"
comm -12 "$DIR_TMP/t_origen.txt" "$DIR_TMP/t_destino.txt" > "$DIR_TMP/t_comunes.txt"

for excluida in $EXCLUIDAS; do
    grep -vx "$excluida" "$DIR_TMP/t_comunes.txt" > "$DIR_TMP/t_tmp.txt" || true
    mv "$DIR_TMP/t_tmp.txt" "$DIR_TMP/t_comunes.txt"
done

if [ ${#TABLAS_PEDIDAS[@]} -gt 0 ]; then
    printf '%s\n' "${TABLAS_PEDIDAS[@]}" | sort -u > "$DIR_TMP/t_pedidas.txt"
    comm -12 "$DIR_TMP/t_comunes.txt" "$DIR_TMP/t_pedidas.txt" > "$DIR_TMP/t_tmp.txt"
    if [ ! -s "$DIR_TMP/t_tmp.txt" ]; then
        morir "Ninguna de las tablas indicadas existe en ambos lados: ${TABLAS_PEDIDAS[*]}"
    fi
    mv "$DIR_TMP/t_tmp.txt" "$DIR_TMP/t_comunes.txt"
fi

[ -s "$DIR_TMP/t_comunes.txt" ] || morir "No hay tablas comunes entre ambos esquemas ${ESQUEMA}"

# Orden de carga: primero las tablas sin FK, luego las que dependen de ellas.
SQL_ORDEN="
WITH RECURSIVE fk AS (
    SELECT c.conrelid AS hijo, c.confrelid AS padre
    FROM pg_constraint c
    JOIN pg_class r ON r.oid = c.conrelid
    JOIN pg_namespace n ON n.oid = r.relnamespace
    WHERE c.contype = 'f' AND n.nspname = '${ESQUEMA}' AND c.conrelid <> c.confrelid
), nivel AS (
    SELECT t.oid AS tabla, 0 AS lvl
    FROM pg_class t JOIN pg_namespace n ON n.oid = t.relnamespace
    WHERE n.nspname = '${ESQUEMA}' AND t.relkind = 'r'
      AND NOT EXISTS (SELECT 1 FROM fk WHERE fk.hijo = t.oid)
    UNION ALL
    SELECT fk.hijo, nivel.lvl + 1 FROM fk JOIN nivel ON nivel.tabla = fk.padre
    WHERE nivel.lvl < 15
)
SELECT c.relname FROM (SELECT tabla, max(lvl) AS lvl FROM nivel GROUP BY tabla) x
JOIN pg_class c ON c.oid = x.tabla ORDER BY x.lvl, c.relname;"

psql_destino -tAc "$SQL_ORDEN" > "$DIR_TMP/orden.txt"
# El orden manda, pero ninguna tabla comun puede quedarse fuera (ciclos de FK).
grep -xF -f "$DIR_TMP/t_comunes.txt" "$DIR_TMP/orden.txt" > "$DIR_TMP/plan.txt" || true
grep -vxF -f "$DIR_TMP/plan.txt" "$DIR_TMP/t_comunes.txt" >> "$DIR_TMP/plan.txt" || true

TOTAL_TABLAS=$(wc -l < "$DIR_TMP/plan.txt")

# Devuelve las columnas comunes de una tabla, en el orden del destino.
columnas_comunes() {
    local tabla="$1"
    awk -F'|' -v t="$tabla" 'NR==FNR { if ($1==t) o[$2]=1; next }
        $1==t && ($2 in o) { print $2 "|" $3 }' "$DIR_TMP/origen.txt" "$DIR_TMP/destino.txt"
}

# --------------------------------------------------------------- informe base

echo "Origen : ${O_USER}@${O_HOST}:${O_PORT}/${O_NAME}  esquema ${ESQUEMA}"
echo "Destino: ${D_USER}@${D_HOST}:${D_PORT}/${D_NAME}  esquema ${ESQUEMA}"
echo "Modo   : ${MODO}$([ "$SIN_FK" -eq 1 ] && echo ' (sin comprobar FK)')"
echo "Tablas : ${TOTAL_TABLAS} comunes de $(wc -l < "$DIR_TMP/t_origen.txt") en origen y $(wc -l < "$DIR_TMP/t_destino.txt") en destino"
echo

if [ "$SIMULACION" -eq 1 ]; then
    printf "%-34s %8s %8s %8s\n" "TABLA" "COLS" "ORIGEN" "DESTINO"
    while read -r tabla; do
        ncols=$(columnas_comunes "$tabla" | wc -l)
        forigen=$(psql_origen  -tAc "select count(*) from \"${ESQUEMA}\".\"${tabla}\"")
        fdestino=$(psql_destino -tAc "select count(*) from \"${ESQUEMA}\".\"${tabla}\"")
        printf "%-34s %8s %8s %8s\n" "$tabla" "$ncols" "$forigen" "$fdestino"
    done < "$DIR_TMP/plan.txt"
    echo
    aviso "Simulacion: no se escribio nada en el destino."
    exit 0
fi

# ------------------------------------------------------------------ migracion

REGISTRO="${DIR_SCRIPT}/migracion_${ESQUEMA}_${TIMESTAMP}.txt"
exec 3>"$REGISTRO"
echo "Migracion ${O_NAME}.${ESQUEMA} -> ${D_NAME}.${ESQUEMA} | modo=${MODO} | $(date)" >&3

if [ "$MODO" = "reemplazar" ]; then
    LISTA_TRUNCATE=$(awk -v e="$ESQUEMA" '{printf "%s\"%s\".\"%s\"", (NR>1?",":""), e, $1}' "$DIR_TMP/plan.txt")
    aviso "Se vaciaran ${TOTAL_TABLAS} tablas de ${D_NAME}.${ESQUEMA} (TRUNCATE CASCADE)."
    read -r -p "Escriba 'reemplazar' para confirmar: " confirmacion
    [ "$confirmacion" = "reemplazar" ] || morir "Confirmacion incorrecta, se cancela la migracion."
    info "Vaciando tablas del destino..."
    psql_destino -q -c "TRUNCATE ${LISTA_TRUNCATE} CASCADE;"
fi

migradas=0; parciales=0; fallidas=0; vacias=0; total_insertadas=0
printf "%-34s %8s %10s %10s  %s\n" "TABLA" "ORIGEN" "INSERTADAS" "OMITIDAS" "ESTADO"

while read -r tabla; do
    mapfile -t definicion < <(columnas_comunes "$tabla")
    [ ${#definicion[@]} -gt 0 ] || continue

    cols_lista=""; cols_texto=""; cols_cast=""
    for entrada in "${definicion[@]}"; do
        nombre="${entrada%%|*}"; tipo="${entrada#*|}"
        cols_lista+="${cols_lista:+, }\"${nombre}\""
        cols_texto+="${cols_texto:+, }\"${nombre}\" text"
        cols_cast+="${cols_cast:+, }r.\"${nombre}\"::${tipo}"
    done

    archivo="${DIR_TMP}/${tabla}.dat"
    if ! psql_origen -q -c "\copy (SELECT ${cols_lista} FROM \"${ESQUEMA}\".\"${tabla}\") TO '${archivo}'" 2>>"$DIR_TMP/errores.txt"; then
        printf "%-34s %8s %10s %10s  %s\n" "$tabla" "?" "0" "-" "❌ error al leer origen"
        echo "[${tabla}] error al exportar del origen" >&3
        fallidas=$((fallidas + 1)); continue
    fi

    filas_origen=$(wc -l < "$archivo")
    if [ "$filas_origen" -eq 0 ]; then
        printf "%-34s %8s %10s %10s  %s\n" "$tabla" "0" "0" "0" "· vacia en origen"
        vacias=$((vacias + 1)); continue
    fi

    antes=$(psql_destino -tAc "select count(*) from \"${ESQUEMA}\".\"${tabla}\"")

    # Carga masiva: tabla temporal con los tipos del destino y un unico INSERT.
    guardia=""
    [ "$SIN_FK" -eq 1 ] && guardia="SET session_replication_role = replica;"
    if psql_destino -q >>"$DIR_TMP/errores.txt" 2>&1 <<SQL
BEGIN;
${guardia}
CREATE TEMP TABLE _mig AS SELECT ${cols_lista} FROM "${ESQUEMA}"."${tabla}" WITH NO DATA;
\\copy _mig (${cols_lista}) FROM '${archivo}'
INSERT INTO "${ESQUEMA}"."${tabla}" (${cols_lista}) SELECT ${cols_lista} FROM _mig ON CONFLICT DO NOTHING;
COMMIT;
SQL
    then
        estado="✅ ok"
    elif [ "$SIN_RESCATE" -eq 1 ]; then
        estado="❌ fallo la carga"
    else
        # Rescate: todo como texto y fila a fila, saltando las que no entran.
        if psql_destino -q >>"$DIR_TMP/errores.txt" 2>&1 <<SQL
BEGIN;
${guardia}
CREATE TEMP TABLE _mig_txt (${cols_texto});
\\copy _mig_txt (${cols_lista}) FROM '${archivo}'
DO \$rescate\$
DECLARE r record;
BEGIN
    FOR r IN SELECT * FROM _mig_txt LOOP
        BEGIN
            INSERT INTO "${ESQUEMA}"."${tabla}" (${cols_lista}) VALUES (${cols_cast}) ON CONFLICT DO NOTHING;
        EXCEPTION WHEN others THEN NULL;
        END;
    END LOOP;
END
\$rescate\$;
COMMIT;
SQL
        then
            estado="⚠️  rescate fila a fila"
        else
            estado="❌ fallo la carga"
        fi
    fi

    despues=$(psql_destino -tAc "select count(*) from \"${ESQUEMA}\".\"${tabla}\"")
    insertadas=$((despues - antes))
    omitidas=$((filas_origen - insertadas))
    total_insertadas=$((total_insertadas + insertadas))

    case "$estado" in
        "✅ ok")   migradas=$((migradas + 1)) ;;
        "⚠️ "*)    parciales=$((parciales + 1)) ;;
        *)          fallidas=$((fallidas + 1)) ;;
    esac

    printf "%-34s %8s %10s %10s  %s\n" "$tabla" "$filas_origen" "$insertadas" "$omitidas" "$estado"
    echo "[${tabla}] origen=${filas_origen} insertadas=${insertadas} omitidas=${omitidas} ${estado}" >&3
done < "$DIR_TMP/plan.txt"

# Las secuencias quedan atras si se insertaron ids explicitos.
info "Ajustando secuencias del esquema ${ESQUEMA}..."
psql_destino -q -c "
DO \$secuencias\$
DECLARE r record; maximo bigint;
BEGIN
    FOR r IN
        SELECT c.relname AS tabla, a.attname AS columna,
               pg_get_serial_sequence(quote_ident('${ESQUEMA}') || '.' || quote_ident(c.relname), a.attname) AS secuencia
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum > 0 AND NOT a.attisdropped
        WHERE n.nspname = '${ESQUEMA}' AND c.relkind = 'r'
          AND pg_get_serial_sequence(quote_ident('${ESQUEMA}') || '.' || quote_ident(c.relname), a.attname) IS NOT NULL
    LOOP
        EXECUTE format('SELECT coalesce(max(%I), 0) FROM %I.%I', r.columna, '${ESQUEMA}', r.tabla) INTO maximo;
        PERFORM setval(r.secuencia, GREATEST(maximo, 1), maximo > 0);
    END LOOP;
END
\$secuencias\$;"

echo
ok "Tablas migradas por completo: ${migradas}"
[ "$parciales" -gt 0 ] && aviso "Tablas con rescate fila a fila: ${parciales}"
[ "$vacias" -gt 0 ]    && info  "Tablas vacias en origen: ${vacias}"
[ "$fallidas" -gt 0 ]  && error "Tablas fallidas: ${fallidas}"
ok "Filas insertadas en total: ${total_insertadas}"

{ echo "---"; echo "migradas=${migradas} parciales=${parciales} vacias=${vacias} fallidas=${fallidas} filas=${total_insertadas}"; } >&3
exec 3>&-
if [ -s "$DIR_TMP/errores.txt" ]; then
    cp "$DIR_TMP/errores.txt" "${REGISTRO%.txt}_errores.txt"
    aviso "Detalle de errores: ${REGISTRO%.txt}_errores.txt"
fi
ok "Registro: ${REGISTRO}"
