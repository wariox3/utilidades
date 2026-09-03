#!/usr/bin/env bash
#
# migrar.sh
#
# Migra los datos de un esquema de la base de origen (PG_ORIGEN) al esquema del
# mismo nombre en la base de destino (PG_DESTINO), copiando todo lo que sea
# compatible: solo las tablas que existen en ambos lados y, dentro de ellas,
# solo las columnas comunes.
#
# Uso:
#   ./migrar.sh                 # migra el esquema semantica
#   ./migrar.sh -n              # ensayo: informe por tabla, no escribe
#   ./migrar.sh -s otro_esquema # otro esquema
#   ./migrar.sh -t gen_contacto # solo esa tabla (repetible)
#   ./migrar.sh -m reemplazar   # vacia las tablas destino antes
#   ./migrar.sh -F              # ignora las FK (mas datos, menos integridad)
#   ./migrar.sh -x              # sin rescate fila a fila
#
# Modos (-m):
#   completar  (por defecto) conserva lo que ya hay en destino e inserta lo que
#              no colisione, con ON CONFLICT DO NOTHING.
#   reemplazar vacia con TRUNCATE ... CASCADE las tablas a migrar y carga desde cero.
#
# Modelos ignorados (arreglo IGNORADAS, arriba en el script): catalogos que no
# se migran nunca, salvo que se nombren de forma explicita con -t.
#
# Si la carga masiva de una tabla falla, se reintenta fila a fila para salvar
# todo lo que sea insertable (desactivable con -x).
#
# Con -n la carga se ensaya de verdad en el destino, dentro de una transaccion
# que se deshace con ROLLBACK al final: el informe dice tabla por tabla si
# migra bien, si migra a medias o si falla, y por que.
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
EXCLUIDAS=(django_migrations django_content_type django_session)
# Modelos ignorados a proposito. gen_pais, gen_estado, gen_ciudad y
# gen_identificacion son catalogos generales que el destino ya trae cargados y
# cuyos ids ademas cambiaron de texto a bigint. gen_archivo cambio de forma en
# reddoc2 (la referencia generica modelo/documento_id se normalizo en el par
# modelo_id/objeto_id), asi que necesita una migracion propia.
IGNORADAS=(gen_pais gen_estado gen_ciudad gen_identificacion gen_archivo)

# Modelos que cambiaron de nombre entre las dos bases: tabla del origen ->
# tabla del destino. A partir de aqui el script trabaja siempre con el nombre
# del destino, y solo vuelve al del origen para leer los datos.
declare -A EQUIVALENTES=(
    # El id 1 choca: ADMINISTRATIVO en el origen y General en el destino son el
    # mismo centro de costo, asi que el ON CONFLICT DO NOTHING conserva el del
    # destino a proposito y solo entran los ids 2 y 3.
    [con_grupo]=con_centro_costo
)

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
#   [gen_archivo.tamano]="o.\"tamano\"::numeric::bigint"
#   [gen_archivo.modelo_id]="10002"
declare -A REGLAS=(
    # El rename de con_grupo a con_centro_costo se llevo consigo la columna que
    # lo referencia: grupo_id paso a llamarse centro_costo_id.
    [con_activo.centro_costo_id]="o.\"grupo_id\"::bigint"
    [con_movimiento.centro_costo_id]="o.\"grupo_id\"::bigint"
    [gen_sede.centro_costo_id]="o.\"grupo_id\"::bigint"
    [gen_documento.centro_costo_id]="o.\"grupo_contabilidad_id\"::bigint"
    [gen_documento_detalle.centro_costo_id]="o.\"grupo_id\"::bigint"
)
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

mostrar_ayuda() { sed -n '3,36p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'; exit 0; }

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

# El catalogo del origen pasa a hablar en nombres del destino, asi las tablas
# renombradas quedan emparejadas como cualquier otra.
EQUIVALENTES_USADAS=()
for tabla_o in "${!EQUIVALENTES[@]}"; do
    if grep -q "^${tabla_o}|" "$DIR_TMP/origen.txt"; then
        EQUIVALENTES_USADAS+=("${tabla_o} -> ${EQUIVALENTES[$tabla_o]}")
        sed -i "s/^${tabla_o}|/${EQUIVALENTES[$tabla_o]}|/" "$DIR_TMP/origen.txt"
    fi
done

cut -d'|' -f1 "$DIR_TMP/origen.txt"  | sort -u > "$DIR_TMP/t_origen.txt"
cut -d'|' -f1 "$DIR_TMP/destino.txt" | sort -u > "$DIR_TMP/t_destino.txt"
comm -12 "$DIR_TMP/t_origen.txt" "$DIR_TMP/t_destino.txt" > "$DIR_TMP/t_comunes.txt"

quitar_de_comunes() {
    grep -vx "$1" "$DIR_TMP/t_comunes.txt" > "$DIR_TMP/t_tmp.txt" || true
    mv "$DIR_TMP/t_tmp.txt" "$DIR_TMP/t_comunes.txt"
}

for excluida in "${EXCLUIDAS[@]}"; do
    quitar_de_comunes "$excluida"
done

# Los modelos ignorados solo se saltan cuando no se piden tablas con -t:
# nombrar una tabla de forma explicita manda sobre la lista.
IGNORADAS_APLICADAS=()
if [ ${#TABLAS_PEDIDAS[@]} -eq 0 ]; then
    for ignorada in "${IGNORADAS[@]}"; do
        if grep -qx "$ignorada" "$DIR_TMP/t_comunes.txt"; then
            IGNORADAS_APLICADAS+=("$ignorada")
            quitar_de_comunes "$ignorada"
        fi
    done
fi

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

# Nombre que tiene en el origen una tabla nombrada como en el destino.
tabla_en_origen() {
    local destino="$1" origen
    for origen in "${!EQUIVALENTES[@]}"; do
        if [ "${EQUIVALENTES[$origen]}" = "$destino" ]; then printf '%s' "$origen"; return; fi
    done
    printf '%s' "$destino"
}

# Verdadero si el catalogo de columnas indicado tiene esa tabla.columna.
columna_existe_en() {
    awk -F'|' -v t="$1" -v c="$2" '$1==t && $2==c { hallada=1 } END { exit !hallada }' "$3"
}

en_lista() {
    local buscada="$1" elemento; shift
    for elemento in "$@"; do [ "$elemento" = "$buscada" ] && return 0; done
    return 1
}

# Prepara las listas de columnas que necesita el SQL de una tabla, ya con las
# reglas del modelo aplicadas:
#   COLS_COPIA    "a", "b"                 -> lo que se trae del origen
#   COLS_TEXTO    "a" text, "b" text       -> las mismas, en la tabla temporal
#   COLS_DESTINO  "a", "b"                 -> lo que recibe el INSERT
#   EXPR_MASIVA   d."a"::int, <regla>      -> valores para la carga en bloque
#   EXPR_FILA     r."a"::int, <regla>      -> los mismos, para el rescate
# Los dos juegos de expresiones solo cambian en el alias porque el record del
# bucle plpgsql (r) no puede llamarse igual que el alias de la tabla temporal.
preparar_columnas() {
    local tabla="$1" entrada nombre tipo clave columna extra
    local -a orden=() copia=()
    local -A expresion=()

    mapfile -t DEFINICION < <(columnas_comunes "$tabla")
    COLS_COPIA=""; COLS_TEXTO=""; COLS_DESTINO=""; EXPR_MASIVA=""; EXPR_FILA=""
    NUM_COLS=0
    [ ${#DEFINICION[@]} -gt 0 ] || return 0

    for entrada in "${DEFINICION[@]}"; do
        nombre="${entrada%%|*}"; tipo="${entrada#*|}"
        copia+=("$nombre"); orden+=("$nombre")
        expresion[$nombre]="o.\"${nombre}\"::${tipo}"
    done

    # Las reglas sustituyen la conversion por defecto o anaden una columna que
    # el origen no tiene.
    for clave in "${!REGLAS[@]}"; do
        [ "${clave%%.*}" = "$tabla" ] || continue
        columna="${clave#*.}"
        columna_existe_en "$tabla" "$columna" "$DIR_TMP/destino.txt" \
            || morir "Regla ${clave}: la columna no existe en el destino"
        [ -n "${expresion[$columna]:-}" ] || orden+=("$columna")
        expresion[$columna]="${REGLAS[$clave]}"
    done

    # Columnas del origen que usan las reglas y que no viajaban en el volcado.
    for extra in $(printf '%s\n' "${expresion[@]}" | grep -oE 'o\."[^"]+"' | sed 's/^o\."//; s/"$//' | sort -u); do
        en_lista "$extra" "${copia[@]}" && continue
        columna_existe_en "$tabla" "$extra" "$DIR_TMP/origen.txt" \
            || morir "Las reglas de ${tabla} usan o.\"${extra}\", que no existe en el origen"
        copia+=("$extra")
    done

    for nombre in "${copia[@]}"; do
        COLS_COPIA+="${COLS_COPIA:+, }\"${nombre}\""
        COLS_TEXTO+="${COLS_TEXTO:+, }\"${nombre}\" text"
    done
    for nombre in "${orden[@]}"; do
        COLS_DESTINO+="${COLS_DESTINO:+, }\"${nombre}\""
        EXPR_MASIVA+="${EXPR_MASIVA:+, }${expresion[$nombre]//o.\"/d.\"}"
        EXPR_FILA+="${EXPR_FILA:+, }${expresion[$nombre]//o.\"/r.\"}"
    done
    NUM_COLS=${#orden[@]}
}

# --------------------------------------------------------------- informe base

echo "Origen : ${O_USER}@${O_HOST}:${O_PORT}/${O_NAME}  esquema ${ESQUEMA}"
echo "Destino: ${D_USER}@${D_HOST}:${D_PORT}/${D_NAME}  esquema ${ESQUEMA}"
echo "Modo   : ${MODO}$([ "$SIN_FK" -eq 1 ] && echo ' (sin comprobar FK)')"
echo "Tablas : ${TOTAL_TABLAS} comunes de $(wc -l < "$DIR_TMP/t_origen.txt") en origen y $(wc -l < "$DIR_TMP/t_destino.txt") en destino"
[ ${#IGNORADAS_APLICADAS[@]} -gt 0 ] && echo "Ignora : ${IGNORADAS_APLICADAS[*]}"
[ ${#EQUIVALENTES_USADAS[@]} -gt 0 ] && echo "Renombra: ${EQUIVALENTES_USADAS[*]}"
echo

if [ "$SIMULACION" -eq 1 ]; then
    # Ensayo real: se carga todo dentro de una unica transaccion en el destino y
    # se deshace con ROLLBACK al final. Al ir en una sola transaccion y en el
    # orden del plan, cada tabla ve las filas de sus padres y las FK se
    # comprueban de verdad. SET CONSTRAINTS ALL IMMEDIATE fuerza la
    # verificacion de las FK diferidas en el mismo punto en que la haria el
    # COMMIT de la migracion real.
    ARCHIVO_SQL="${DIR_TMP}/simulacion.sql"
    ARCHIVO_RES="${DIR_TMP}/resultados.txt"

    guardia=""
    [ "$SIN_FK" -eq 1 ] && guardia="SET session_replication_role = replica;"

    {
        echo "SET client_min_messages = warning;"
        echo "BEGIN;"
        echo "SET search_path TO \"${ESQUEMA}\", public;"
        [ -n "$guardia" ] && echo "$guardia"
        echo "CREATE TEMP TABLE _sim (tabla text, insertadas bigint, estado text, motivo text);"
    } > "$ARCHIVO_SQL"

    declare -A SIM_COLS SIM_ORIGEN SIM_DESTINO SIM_INSERTADAS SIM_ESTADO SIM_MOTIVO
    ensayadas=0

    info "Ensayando la carga en el destino (todo se deshace al terminar)..."

    while read -r tabla; do
        preparar_columnas "$tabla"
        [ -n "$COLS_COPIA" ] || continue

        SIM_COLS[$tabla]=$NUM_COLS
        SIM_DESTINO[$tabla]=$(psql_destino -tAc "select count(*) from \"${ESQUEMA}\".\"${tabla}\"")
        SIM_INSERTADAS[$tabla]=0
        SIM_MOTIVO[$tabla]=""

        archivo="${DIR_TMP}/${tabla}.dat"
        if ! psql_origen -q -c "\copy (SELECT ${COLS_COPIA} FROM \"${ESQUEMA}\".\"$(tabla_en_origen "$tabla")\") TO '${archivo}'" 2>>"$DIR_TMP/errores.txt"; then
            SIM_ORIGEN[$tabla]="?"
            SIM_ESTADO[$tabla]="lectura"
            continue
        fi

        SIM_ORIGEN[$tabla]=$(wc -l < "$archivo")
        if [ "${SIM_ORIGEN[$tabla]}" -eq 0 ]; then
            SIM_ESTADO[$tabla]="vacia"
            continue
        fi

        SIM_ESTADO[$tabla]="sin_ensayo"
        ensayadas=$((ensayadas + 1))

        cat >> "$ARCHIVO_SQL" <<SQL
CREATE TEMP TABLE _dat (${COLS_TEXTO});
\copy _dat (${COLS_COPIA}) FROM '${archivo}'
DO \$sim\$
DECLARE
    r record; n bigint := 0; k bigint; motivo text := ''; resultado text := 'ok';
BEGIN
    BEGIN
        INSERT INTO "${ESQUEMA}"."${tabla}" (${COLS_DESTINO})
            SELECT ${EXPR_MASIVA} FROM _dat d ON CONFLICT DO NOTHING;
        GET DIAGNOSTICS n = ROW_COUNT;
        SET CONSTRAINTS ALL IMMEDIATE;
        SET CONSTRAINTS ALL DEFERRED;
    EXCEPTION WHEN others THEN
        motivo := SQLERRM; n := 0; resultado := 'falla';
SQL
        if [ "$SIN_RESCATE" -eq 0 ]; then
            cat >> "$ARCHIVO_SQL" <<SQL
        BEGIN
            FOR r IN SELECT * FROM _dat LOOP
                BEGIN
                    INSERT INTO "${ESQUEMA}"."${tabla}" (${COLS_DESTINO}) VALUES (${EXPR_FILA}) ON CONFLICT DO NOTHING;
                    GET DIAGNOSTICS k = ROW_COUNT; n := n + k;
                EXCEPTION WHEN others THEN NULL;
                END;
            END LOOP;
            SET CONSTRAINTS ALL IMMEDIATE;
            SET CONSTRAINTS ALL DEFERRED;
            resultado := 'rescate';
        EXCEPTION WHEN others THEN
            motivo := SQLERRM; n := 0; resultado := 'falla';
        END;
SQL
        fi
        cat >> "$ARCHIVO_SQL" <<SQL
    END;
    INSERT INTO _sim VALUES ('${tabla}', n, resultado, motivo);
END
\$sim\$;
DROP TABLE _dat;
SQL
    done < "$DIR_TMP/plan.txt"

    if [ "$ensayadas" -gt 0 ]; then
        echo "\copy (SELECT tabla || '|' || insertadas || '|' || estado || '|' || replace(motivo, E'\n', ' ') FROM _sim) TO '${ARCHIVO_RES}'" >> "$ARCHIVO_SQL"
    fi
    echo "ROLLBACK;" >> "$ARCHIVO_SQL"

    if psql_destino -q -f "$ARCHIVO_SQL" >>"$DIR_TMP/errores.txt" 2>&1 && [ -f "$ARCHIVO_RES" ]; then
        while IFS='|' read -r t ins est mot; do
            SIM_INSERTADAS[$t]="$ins"; SIM_ESTADO[$t]="$est"; SIM_MOTIVO[$t]="$mot"
        done < "$ARCHIVO_RES"
    elif [ "$ensayadas" -gt 0 ]; then
        aviso "El ensayo se interrumpio; algunas tablas quedan sin diagnostico."
    fi

    echo
    printf "%-34s %6s %10s %10s %12s  %s\n" "TABLA" "COLS" "ORIGEN" "DESTINO" "INSERTARIA" "RESULTADO"

    bien=0; parciales=0; fallidas=0; vacias=0; total_insertadas=0
    : > "$DIR_TMP/motivos.txt"

    while read -r tabla; do
        [ -n "${SIM_ESTADO[$tabla]:-}" ] || continue
        motivo="${SIM_MOTIVO[$tabla]:-}"
        insertadas="${SIM_INSERTADAS[$tabla]:-0}"
        case "${SIM_ESTADO[$tabla]}" in
            ok)      resultado="✅ migra bien";            bien=$((bien + 1)) ;;
            rescate) resultado="⚠️  migra a medias (rescate fila a fila)"; parciales=$((parciales + 1)) ;;
            falla)   resultado="❌ falla";                 fallidas=$((fallidas + 1)) ;;
            vacia)   resultado="· vacia en origen";       vacias=$((vacias + 1)) ;;
            lectura) resultado="❌ falla: no se pudo leer el origen"; fallidas=$((fallidas + 1)) ;;
            *)       resultado="❓ sin diagnostico" ;;
        esac
        [ -n "$motivo" ] && echo "  ${tabla}: ${motivo}" >> "$DIR_TMP/motivos.txt"
        total_insertadas=$((total_insertadas + insertadas))
        printf "%-34s %6s %10s %10s %12s  %s\n" "$tabla" "${SIM_COLS[$tabla]}" \
            "${SIM_ORIGEN[$tabla]}" "${SIM_DESTINO[$tabla]}" "$insertadas" "$resultado"
    done < "$DIR_TMP/plan.txt"

    echo
    ok    "Tablas que migran bien: ${bien}"
    [ "$parciales" -gt 0 ] && aviso "Tablas que migran a medias: ${parciales}"
    [ "$vacias" -gt 0 ]    && info  "Tablas vacias en origen: ${vacias}"
    [ "$fallidas" -gt 0 ]  && error "Tablas que fallan: ${fallidas}"
    ok    "Filas que se insertarian: ${total_insertadas}"

    if [ -s "$DIR_TMP/motivos.txt" ]; then
        echo
        echo "Motivos:"
        cat "$DIR_TMP/motivos.txt"
    fi

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
    preparar_columnas "$tabla"
    [ -n "$COLS_COPIA" ] || continue

    archivo="${DIR_TMP}/${tabla}.dat"
    if ! psql_origen -q -c "\copy (SELECT ${COLS_COPIA} FROM \"${ESQUEMA}\".\"$(tabla_en_origen "$tabla")\") TO '${archivo}'" 2>>"$DIR_TMP/errores.txt"; then
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

    # Carga masiva: los datos llegan como texto a una tabla temporal y cada
    # columna se convierte con su expresion (la de la regla, si la tiene).
    guardia=""
    [ "$SIN_FK" -eq 1 ] && guardia="SET session_replication_role = replica;"
    if psql_destino -q >>"$DIR_TMP/errores.txt" 2>&1 <<SQL
BEGIN;
SET search_path TO "${ESQUEMA}", public;
${guardia}
CREATE TEMP TABLE _dat (${COLS_TEXTO});
\\copy _dat (${COLS_COPIA}) FROM '${archivo}'
INSERT INTO "${ESQUEMA}"."${tabla}" (${COLS_DESTINO}) SELECT ${EXPR_MASIVA} FROM _dat d ON CONFLICT DO NOTHING;
COMMIT;
SQL
    then
        estado="✅ ok"
    elif [ "$SIN_RESCATE" -eq 1 ]; then
        estado="❌ fallo la carga"
    else
        # Rescate: fila a fila, saltando las que no entran.
        if psql_destino -q >>"$DIR_TMP/errores.txt" 2>&1 <<SQL
BEGIN;
SET search_path TO "${ESQUEMA}", public;
${guardia}
CREATE TEMP TABLE _dat (${COLS_TEXTO});
\\copy _dat (${COLS_COPIA}) FROM '${archivo}'
DO \$rescate\$
DECLARE r record;
BEGIN
    FOR r IN SELECT * FROM _dat LOOP
        BEGIN
            INSERT INTO "${ESQUEMA}"."${tabla}" (${COLS_DESTINO}) VALUES (${EXPR_FILA}) ON CONFLICT DO NOTHING;
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
