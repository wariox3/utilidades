from decouple import config
import mysql.connector
import psycopg2
import sys
import traceback
import datetime

# IDs por defecto para las FK (PROTECT) cuando el código del sistema viejo
# no se puede traducir directamente al ID del nuevo sistema. Ajustar según
# las tablas de referencia de reddoc.
ID_IDENTIFICACION_DEFECTO = 6
ID_CIUDAD_DEFECTO = 1
ID_TIPO_PERSONA_DEFECTO = 1


def crear_conexiones():
    parametros = {
        'user': config('DATABASE_USER'),
        'password': config('DATABASE_CLAVE'),
        'host': config('DATABASE_HOST'),
        'port': config('DATABASE_PORT'),
        'database': config('DATABASE_NAME')
    }

    postgres_parametros = {
        'user': config('PG_DATABASE_USER'),
        'password': config('PG_DATABASE_CLAVE'),
        'host': config('PG_DATABASE_HOST'),
        'port': config('PG_DATABASE_PORT'),
        'database': config('PG_DATABASE_NAME')
    }

    try:
        conexion = mysql.connector.connect(**parametros)
        cursor = conexion.cursor(dictionary=True)

        pg_conn = psycopg2.connect(**postgres_parametros)
        pg_cursor = pg_conn.cursor()

        return conexion, cursor, pg_conn, pg_cursor

    except Exception as e:
        print(f"Error al conectar a las bases de datos: {e}")
        sys.exit(1)


def cerrar_conexiones(conexion, pg_conn):
    try:
        if conexion and conexion.is_connected():
            conexion.close()
        if pg_conn and not pg_conn.closed:
            pg_conn.close()
        print("Conexiones cerradas.")
    except Exception as e:
        print(f"Error al cerrar conexiones: {e}")


def limpiar(valor, longitud=None):
    """Normaliza un valor de texto: None/blanco -> None, recorta longitud."""
    if valor is None:
        return None
    valor = str(valor).strip()
    if valor == "":
        return None
    if longitud:
        valor = valor[:longitud]
    return valor


def migrar_tabla(nombre, sql_seleccionar, sql_insertar, transformar,
                 tabla_origen=None, sql_contar=None, batch_size=1000,
                 tabla_destino=None, pk_origen=None):
    """Motor genérico de migración MySQL -> PostgreSQL por lotes.

    Parámetros:
        nombre:        etiqueta para los mensajes de log.
        sql_seleccionar: SELECT con marcadores {limit} y {offset}.
        sql_insertar:  INSERT con marcador {schema} y sus %s.
        transformar:   función registro(dict) -> tupla de valores del INSERT
                       (devolver None para omitir el registro).
        tabla_origen:  nombre de la tabla origen (para el COUNT por defecto).
        sql_contar:    SELECT COUNT alterno; si se omite usa tabla_origen.
        batch_size:    tamaño de lote.
        tabla_destino: si se indica junto con pk_origen, precarga los id ya
                       presentes en esa tabla y omite (no actualiza) los
                       registros cuyo registro[pk_origen] ya exista. Evita
                       reenviar a la BD lo ya migrado en corridas posteriores.
        pk_origen:     campo del registro origen que corresponde al id destino.
    """
    pg_schema = config('PG_SCHEMA_NAME', default='')
    conexion, cursor, pg_conn, pg_cursor = crear_conexiones()
    offset = 0

    existentes_destino = set()
    if tabla_destino and pk_origen:
        existentes_destino = _set_existentes(tabla_destino)
        print(f"[{nombre}] {len(existentes_destino)} registros ya existen en "
              f"destino; se omitirán.")

    try:
        if pg_schema:
            pg_cursor.execute(f"SET search_path TO {pg_schema}")

        if not sql_contar:
            sql_contar = f"SELECT COUNT(*) AS total FROM {tabla_origen}"
        cursor.execute(sql_contar)
        total_records = cursor.fetchone()['total']
        print(f"[{nombre}] Iniciando migración de {total_records} registros...")

        insert_query = sql_insertar.format(schema=pg_schema)

        while offset < total_records:
            cursor.execute(sql_seleccionar.format(limit=batch_size, offset=offset))
            registros = cursor.fetchall()
            if not registros:
                break

            values_batch = []
            for registro in registros:
                if pk_origen and registro[pk_origen] in existentes_destino:
                    continue
                valores = transformar(registro)
                if valores is not None:
                    values_batch.append(valores)

            try:
                if values_batch:
                    pg_cursor.executemany(insert_query, values_batch)
                    pg_conn.commit()
                ahora = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                print(f"[{ahora}] [{nombre}] Lote {offset // batch_size + 1} completado "
                      f"({offset + len(registros)}/{total_records} registros)")
            except Exception as e:
                print(f"[{nombre}] Error en lote {offset // batch_size + 1}: {e}")
                pg_conn.rollback()
                sys.exit(1)

            offset += batch_size

        print(f"[{nombre}] Migración finalizada.")

    except Exception as e:
        print(f"\n[{nombre}] ERROR CRÍTICO durante el procesamiento: {e}")
        print(f"[{nombre}] Último offset procesado: {offset}")
        traceback.print_exc()
        sys.exit(1)

    finally:
        cerrar_conexiones(conexion, pg_conn)


# ---------------------------------------------------------------------------
# Definiciones por tabla
# ---------------------------------------------------------------------------

def _transformar_contacto(registro):
    cliente = bool(registro['cliente'])
    proveedor = bool(registro['proveedor'])
    empleado = bool(registro['empleado'])

    # Campos obligatorios (NOT NULL en gen_contacto)
    numero_identificacion = limpiar(registro['numero_identificacion'], 20) or "1"
    nombre_corto = limpiar(registro['nombre_corto'], 200) or "Contacto"
    direccion = limpiar(registro['direccion'], 100) or "Conocida"

    telefono = limpiar(registro['telefono'], 50)
    if not telefono:
        telefono = limpiar(registro['celular'], 50)
    if not telefono:
        telefono = "1"

    celular = limpiar(registro['celular'], 50) or ""

    correo = limpiar(registro['correo'], 255)
    if not correo or '@' not in correo:
        correo = "correo@cliente.com"

    # Campos opcionales (NULL permitido)
    digito_verificacion = limpiar(registro['digito_verificacion'], 1)
    nombre1 = limpiar(registro['nombre1'], 50)
    nombre2 = limpiar(registro['nombre2'], 50)
    apellido1 = limpiar(registro['apellido1'], 50)
    apellido2 = limpiar(registro['apellido2'], 50)
    barrio = limpiar(registro['barrio'], 200)
    codigo_ciuu = limpiar(registro['codigo_ciuu'], 200)
    codigo_postal = limpiar(registro['codigo_postal'], 20)

    correo_facturacion = limpiar(registro['correo_factura_electronica'], 255)
    if correo_facturacion and '@' not in correo_facturacion:
        correo_facturacion = None

    return (
        registro['codigo_tercero_pk'],
        numero_identificacion,
        digito_verificacion,
        nombre_corto,
        nombre1,
        nombre2,
        apellido1,
        apellido2,
        direccion,
        barrio,
        codigo_ciuu,
        codigo_postal,
        telefono,
        celular,
        correo,
        correo_facturacion,
        cliente,
        proveedor,
        empleado,
        False,
        ID_IDENTIFICACION_DEFECTO,
        ID_CIUDAD_DEFECTO,
        ID_TIPO_PERSONA_DEFECTO,
    )


def procesar_contactos():
    migrar_tabla(
        nombre="gen_contacto",
        tabla_origen="gen_tercero",
        tabla_destino="gen_contacto",
        pk_origen="codigo_tercero_pk",
        sql_seleccionar=(
            "SELECT * FROM gen_tercero "
            "ORDER BY codigo_tercero_pk LIMIT {limit} OFFSET {offset}"
        ),
        sql_insertar="""
            INSERT INTO {schema}.gen_contacto
            (id, numero_identificacion, digito_verificacion, nombre_corto,
            nombre1, nombre2, apellido1, apellido2, direccion, barrio,
            codigo_ciuu, codigo_postal, telefono, celular, correo,
            correo_facturacion_electronica, cliente, proveedor, empleado,
            conductor, identificacion_id, ciudad_id, tipo_persona_id)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (id) DO NOTHING;
        """,
        transformar=_transformar_contacto,
        batch_size=1000,
    )


def _pg_conn():
    return psycopg2.connect(
        user=config('PG_DATABASE_USER'), password=config('PG_DATABASE_CLAVE'),
        host=config('PG_DATABASE_HOST'), port=config('PG_DATABASE_PORT'),
        database=config('PG_DATABASE_NAME'))


def _my_conn():
    return mysql.connector.connect(
        user=config('DATABASE_USER'), password=config('DATABASE_CLAVE'),
        host=config('DATABASE_HOST'), port=config('DATABASE_PORT'),
        database=config('DATABASE_NAME'))


def _calificar(tabla):
    """Antepone el schema configurado al nombre de la tabla destino."""
    pg_schema = config('PG_SCHEMA_NAME', default='')
    return f"{pg_schema}.{tabla}" if pg_schema else tabla


# --- Validación de FK: patrón uniforme de "precarga una vez, valida en memoria".
# Toda FK que se asigna en los procesar_* se comprueba contra uno de estos dos
# precargados, evitando una consulta por registro.

def _set_existentes(tabla, columna="id"):
    """Conjunto de valores presentes en una columna de la tabla destino.

    Para validar existencia de una FK: `valor in _set_existentes(tabla)`.
    """
    pg = _pg_conn()
    try:
        c = pg.cursor()
        c.execute(f"SELECT {columna} FROM {_calificar(tabla)} "
                  f"WHERE {columna} IS NOT NULL")
        return {fila[0] for fila in c.fetchall()}
    finally:
        pg.close()


def _mapa_codigo_id(tabla, columna_codigo="codigo"):
    """Mapa {codigo (texto) -> id} de la tabla destino.

    Para FK que en el origen llegan como código natural y en destino son id.
    Las claves se normalizan con strip().
    """
    pg = _pg_conn()
    try:
        c = pg.cursor()
        c.execute(f"SELECT {columna_codigo}, id FROM {_calificar(tabla)} "
                  f"WHERE {columna_codigo} IS NOT NULL")
        return {(cod or '').strip(): i for cod, i in c.fetchall()}
    finally:
        pg.close()


def procesar_centros_costo():
    # con_centro_costo.id lo asigna la secuencia; la clave natural para
    # detectar duplicados es el código. Precargamos los códigos existentes
    # y descartamos los repetidos (incluidos los que colisionan dentro de
    # esta misma corrida tras recortar espacios).
    existentes = _set_existentes("con_centro_costo", "codigo")
    vistos = set()

    def transformar(registro):
        codigo = limpiar(registro['codigo_centro_costo_pk'], 20)
        if codigo is None or codigo in existentes or codigo in vistos:
            return None
        vistos.add(codigo)
        nombre = limpiar(registro['nombre'], 100) or "Sin nombre"
        return (codigo, nombre)

    migrar_tabla(
        nombre="con_centro_costo",
        tabla_origen="fin_centro_costo",
        sql_seleccionar=(
            "SELECT * FROM fin_centro_costo "
            "ORDER BY codigo_centro_costo_pk LIMIT {limit} OFFSET {offset}"
        ),
        sql_insertar="""
            INSERT INTO {schema}.con_centro_costo (codigo, nombre)
            VALUES (%s, %s);
        """,
        transformar=transformar,
        batch_size=1000,
    )


def _coordenada(valor, limite):
    """Valida una coordenada geográfica; fuera de rango o vacía -> None.

    El destino usa numeric(11,8) (|valor| < 1000) y el origen trae datos
    corruptos sin punto decimal (p. ej. 4894286 en vez de 4.894286).
    """
    if valor is None:
        return None
    try:
        valor = float(valor)
    except (TypeError, ValueError):
        return None
    if valor == 0 or abs(valor) > limite:
        return None
    return valor


def procesar_programadores():
    # codigo_programador_pk es entero -> se conserva como id.
    def transformar(registro):
        return (
            registro['codigo_programador_pk'],
            limpiar(registro['nombre'], 100) or "Programador",
            bool(registro['estado_inactivo']),
        )

    migrar_tabla(
        nombre="tur_programador",
        tabla_origen="tur_programador",
        tabla_destino="tur_programador",
        pk_origen="codigo_programador_pk",
        sql_seleccionar=(
            "SELECT * FROM tur_programador "
            "ORDER BY codigo_programador_pk LIMIT {limit} OFFSET {offset}"
        ),
        sql_insertar="""
            INSERT INTO {schema}.tur_programador (id, nombre, estado_inactivo)
            VALUES (%s, %s, %s)
            ON CONFLICT (id) DO NOTHING;
        """,
        transformar=transformar,
        batch_size=1000,
    )


def _mapas_puesto():
    """Construye los mapas para resolver las FK de tur_puesto.

    Devuelve:
        centro_costo: {codigo (origen) -> con_centro_costo.id}
        ciudad:       {codigo_ciudad_pk (origen) -> gen_ciudad.id (destino)},
                      enlazado por código DANE.
        contactos:    set de gen_contacto.id existentes.
        programadores: set de tur_programador.id existentes.
    """
    # La ciudad requiere un salto extra que vive en el origen (pk -> DANE);
    # el resto sale de los helpers de validación uniformes.
    my = _my_conn()
    try:
        mc = my.cursor(dictionary=True)
        mc.execute("SELECT codigo_ciudad_pk, codigo_dane_completo FROM gen_ciudad")
        dane_por_pk = {r['codigo_ciudad_pk']: (r['codigo_dane_completo'] or '').strip()
                       for r in mc.fetchall()}
    finally:
        my.close()

    id_por_dane = _mapa_codigo_id("gen_ciudad")  # DANE -> gen_ciudad.id destino
    ciudad = {pk: id_por_dane[dane] for pk, dane in dane_por_pk.items()
              if dane in id_por_dane}

    return (
        _mapa_codigo_id("con_centro_costo"),
        ciudad,
        _set_existentes("gen_contacto"),
        _set_existentes("tur_programador"),
    )


def procesar_puestos():
    # codigo_puesto_pk es entero -> se conserva como id (ON CONFLICT lo salta).
    # FKs: centro_costo y ciudad se resuelven por código/DANE; contacto y
    # programador mapean directo (conservan el id), validando existencia.
    centro_costo, ciudad, contactos, programadores = _mapas_puesto()

    def transformar(registro):
        nombre = limpiar(registro['nombre'], 200) or "Puesto"
        tercero = registro['codigo_tercero_fk']
        programador = registro['codigo_programador_fk']
        return (
            registro['codigo_puesto_pk'],
            nombre,
            limpiar(registro['direccion'], 100),
            limpiar(registro['celular'], 50),
            _coordenada(registro['latitud'], 90),
            _coordenada(registro['longitud'], 180),
            limpiar(registro['comentario']),
            bool(registro['estado_inactivo']),
            centro_costo.get(limpiar(registro['codigo_centro_costo_fk'], 20)),
            ciudad.get(registro['codigo_ciudad_fk']),
            tercero if tercero in contactos else None,
            programador if programador in programadores else None,
        )

    migrar_tabla(
        nombre="tur_puesto",
        tabla_origen="tur_puesto",
        tabla_destino="tur_puesto",
        pk_origen="codigo_puesto_pk",
        sql_seleccionar=(
            "SELECT * FROM tur_puesto "
            "ORDER BY codigo_puesto_pk LIMIT {limit} OFFSET {offset}"
        ),
        sql_insertar="""
            INSERT INTO {schema}.tur_puesto
            (id, nombre, direccion, celular, latitud, longitud, comentario,
            estado_inactivo, centro_costo_id, ciudad_id, contacto_id, programador_id)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (id) DO NOTHING;
        """,
        transformar=transformar,
        batch_size=1000,
    )


def procesar_items():
    # tur_item -> gen_item. Solo id y nombre son obligatorios en destino; el
    # resto de columnas tienen default. Las cuentas (cuenta_*_id -> con_cuenta)
    # quedan en null porque con_cuenta no tiene datos cargados. Los registros
    # de tur_item son servicios (servicio_aiu), por eso servicio/venta = True.
    def transformar(registro):
        return (
            registro['codigo_item_pk'],
            limpiar(registro['nombre'], 200) or "Item",
            limpiar(registro['codigo_interface'], 100),
            True,   # servicio
            True,   # venta
        )

    migrar_tabla(
        nombre="gen_item",
        tabla_origen="tur_item",
        tabla_destino="gen_item",
        pk_origen="codigo_item_pk",
        sql_seleccionar=(
            "SELECT * FROM tur_item "
            "ORDER BY codigo_item_pk LIMIT {limit} OFFSET {offset}"
        ),
        sql_insertar="""
            INSERT INTO {schema}.gen_item (id, nombre, codigo, servicio, venta)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (id) DO NOTHING;
        """,
        transformar=transformar,
        batch_size=1000,
    )


def _mapa_sector_por_codigo():
    """codigo_sector_pk (origen) -> gen_sector.id (destino), enlazado por nombre.

    gen_sector no tiene código natural, así que se correlaciona por el nombre
    del sector. Solo quedan los códigos cuyo nombre coincide exactamente en
    gen_sector (el resto del contrato dejará sector_id en null).
    """
    my = _my_conn()
    try:
        mc = my.cursor(dictionary=True)
        mc.execute("SELECT codigo_sector_pk, nombre FROM tur_sector")
        nombre_por_codigo = {r['codigo_sector_pk']: (r['nombre'] or '').strip()
                             for r in mc.fetchall()}
    finally:
        my.close()

    id_por_nombre = _mapa_codigo_id("gen_sector", "nombre")
    return {cod: id_por_nombre[nom] for cod, nom in nombre_por_codigo.items()
            if nom in id_por_nombre}


def procesar_contratos():
    # tur_contrato -> gen_documento (documento_tipo_id fijo = 34, "CONTRATO SERVICIO").
    # codigo_contrato_pk es entero -> se conserva como id (ON CONFLICT lo salta).
    # contacto_id se resuelve desde codigo_tercero_fk validando que el tercero
    # exista en gen_contacto (por eso contactos debe migrarse antes).
    # sector_id se mapea por nombre contra gen_sector; los que no coinciden
    # quedan en null. Los montos NOT NULL del origen llegan siempre con valor.
    DOCUMENTO_TIPO_CONTRATO = 34
    contactos = _set_existentes("gen_contacto")
    sectores = _mapa_sector_por_codigo()

    def transformar(registro):
        tercero = registro['codigo_tercero_fk']
        return (
            registro['codigo_contrato_pk'],
            DOCUMENTO_TIPO_CONTRATO,
            registro['fecha_generacion'],
            registro['fecha_cierre'],
            registro['vr_subtotal'],
            registro['vr_iva'],
            registro['vr_base_aiu'],
            registro['vr_salario_base'],
            registro['vr_total'],
            registro['horas'],
            registro['horas_diurnas'],
            registro['horas_nocturnas'],
            registro['estrato'],
            limpiar(registro['soporte'], 100),
            limpiar(registro['comentarios'], 500),
            bool(registro['estado_aprobado']),
            bool(registro['estado_anulado']),
            tercero if tercero in contactos else None,
            sectores.get(limpiar(registro['codigo_sector_fk'], 10)),
        )

    migrar_tabla(
        nombre="gen_documento (contratos)",
        tabla_origen="tur_contrato",
        tabla_destino="gen_documento",
        pk_origen="codigo_contrato_pk",
        sql_contar="SELECT COUNT(*) AS total FROM tur_contrato WHERE estado_terminado = 0",
        sql_seleccionar=(
            "SELECT * FROM tur_contrato WHERE estado_terminado = 0 "
            "ORDER BY codigo_contrato_pk LIMIT {limit} OFFSET {offset}"
        ),
        sql_insertar="""
            INSERT INTO {schema}.gen_documento
            (id, documento_tipo_id, fecha, fecha_vence, subtotal, impuesto,
            base_impuesto, salario, total, horas, horas_diurnas, horas_nocturnas,
            estrato, soporte, comentario, estado_aprobado, estado_anulado,
            contacto_id, sector_id)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (id) DO NOTHING;
        """,
        transformar=transformar,
        batch_size=1000,
    )


def _a_time(valor):
    """Convierte el TIME de MySQL (timedelta) a datetime.time para PostgreSQL.

    El conector MySQL devuelve las columnas TIME como timedelta; psycopg2 no las
    adapta a una columna `time`. Se normaliza dentro de las 24h.
    """
    if valor is None:
        return None
    if isinstance(valor, datetime.time):
        return valor
    if isinstance(valor, datetime.timedelta):
        total = int(valor.total_seconds()) % 86400
        return datetime.time(total // 3600, (total % 3600) // 60, total % 60)
    return None


def _mapas_contrato_detalle():
    """Mapas y conjuntos para resolver las FK de gen_documento_detalle.

    Devuelve:
        documentos:   set de gen_documento.id existentes (FK obligatoria).
        items:        set de gen_item.id existentes.
        puestos:      set de tur_puesto.id existentes (destino).
        modalidades:  {codigo (origen) -> gen_modalidad.id}.
    """
    return (
        _set_existentes("gen_documento"),
        _set_existentes("gen_item"),
        _set_existentes("tur_puesto"),
        _mapa_codigo_id("gen_modalidad"),
    )


def procesar_contrato_detalles():
    # tur_contrato_detalle -> gen_documento_detalle.
    # documento_id (FK a gen_documento) es obligatorio: si el contrato origen es
    # nulo o no se migró, el registro se omite. item, puesto y modalidad se
    # validan/mapean; contacto_id y cuenta_id quedan en null (sin origen / vacío).
    documentos, items, puestos, modalidades = _mapas_contrato_detalle()

    def transformar(registro):
        contrato = registro['codigo_contrato_fk']
        if contrato is None or contrato not in documentos:
            return None
        item = registro['codigo_item_fk']
        puesto = registro['codigo_puesto_fk']
        # En los detalles compuestos el precio de venta real es el mínimo:
        # se asigna vr_precio_minimo al campo precio en lugar de vr_precio.
        compuesto = bool(registro['compuesto'])
        precio = registro['vr_precio_minimo'] if compuesto else registro['vr_precio']
        return (
            registro['codigo_contrato_detalle_pk'],
            contrato,
            item if item in items else None,
            puesto if puesto in puestos else None,
            modalidades.get(limpiar(registro['codigo_modalidad_fk'], 10)),
            registro['cantidad'],
            precio,
            registro['vr_precio_minimo'],
            registro['vr_subtotal'],
            registro['vr_base_aiu'],
            registro['vr_iva'],
            registro['vr_total_detalle'],
            registro['horas'],
            registro['horas_diurnas'],
            registro['horas_nocturnas'],
            registro['dias'],
            registro['porcentaje_iva'] or 0,
            registro['fecha_desde'],
            registro['fecha_hasta'],
            _a_time(registro['hora_desde']),
            _a_time(registro['hora_hasta']),
            bool(registro['lunes']),
            bool(registro['martes']),
            bool(registro['miercoles']),
            bool(registro['jueves']),
            bool(registro['viernes']),
            bool(registro['sabado']),
            bool(registro['domingo']),
            bool(registro['festivo']),
            compuesto,
            bool(registro['cortesia']),
            bool(registro['programar']),
            limpiar(registro['detalle'], 150),
        )

    migrar_tabla(
        nombre="gen_documento_detalle",
        tabla_origen="tur_contrato_detalle",
        tabla_destino="gen_documento_detalle",
        pk_origen="codigo_contrato_detalle_pk",
        sql_contar="SELECT COUNT(*) AS total FROM tur_contrato_detalle WHERE estado_terminado = 0",
        sql_seleccionar=(
            "SELECT * FROM tur_contrato_detalle WHERE estado_terminado = 0 "
            "ORDER BY codigo_contrato_detalle_pk LIMIT {limit} OFFSET {offset}"
        ),
        sql_insertar="""
            INSERT INTO {schema}.gen_documento_detalle
            (id, documento_id, item_id, puesto_id, modalidad_id, cantidad, precio,
            precio_minimo, subtotal, base_impuesto, impuesto, total, horas,
            horas_diurnas, horas_nocturnas, dias, porcentaje, fecha_desde,
            fecha_hasta, hora_desde, hora_hasta, lunes, martes, miercoles, jueves,
            viernes, sabado, domingo, festivo, compuesto, cortesia, programar,
            detalle)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (id) DO NOTHING;
        """,
        transformar=transformar,
        batch_size=1000,
    )


def procesar_contrato_detalle_impuestos():
    # tur_contrato_detalle -> gen_documento_impuesto (los impuestos son N:1 con
    # el detalle). Solo los detalles con porcentaje_iva = 19 reciben el impuesto
    # 5 ("IVA 19% AIU"). La base es vr_base_aiu y el total el vr_iva ya calculado.
    # El id lo asigna la secuencia; para idempotencia se omiten los detalles que
    # ya tengan impuesto y se validan contra gen_documento_detalle (FK).
    IMPUESTO_IVA_19_AIU = 5
    detalles = _set_existentes("gen_documento_detalle")
    ya_con_impuesto = _set_existentes("gen_documento_impuesto", "documento_detalle_id")

    def transformar(registro):
        detalle = registro['codigo_contrato_detalle_pk']
        if detalle not in detalles or detalle in ya_con_impuesto:
            return None
        return (
            registro['vr_base_aiu'],
            registro['porcentaje_iva'],
            registro['porcentaje_base_iva'] or 0,
            registro['vr_iva'],
            detalle,
            IMPUESTO_IVA_19_AIU,
        )

    migrar_tabla(
        nombre="gen_documento_impuesto",
        tabla_origen="tur_contrato_detalle",
        sql_contar=(
            "SELECT COUNT(*) AS total FROM tur_contrato_detalle "
            "WHERE estado_terminado = 0 AND porcentaje_iva = 19"
        ),
        sql_seleccionar=(
            "SELECT * FROM tur_contrato_detalle "
            "WHERE estado_terminado = 0 AND porcentaje_iva = 19 "
            "ORDER BY codigo_contrato_detalle_pk LIMIT {limit} OFFSET {offset}"
        ),
        sql_insertar="""
            INSERT INTO {schema}.gen_documento_impuesto
            (base, porcentaje, porcentaje_base, total, documento_detalle_id, impuesto_id)
            VALUES (%s, %s, %s, %s, %s, %s);
        """,
        transformar=transformar,
        batch_size=1000,
    )


# Registro ordenado de migraciones disponibles: (descripción, función).
# El orden importa: respeta las dependencias de FK entre tablas.
# Para agregar una tabla nueva: define su _transformar_X() y procesar_X(),
# y añade una entrada aquí en el orden correcto.
#
# Se migran primero los MAESTROS (catálogos: contacto, centro de costo,
# programador, puesto, item) — se migran completos y omiten los existentes por
# id — y luego los DOCUMENTOS (contrato, detalle, impuesto), que van filtrados
# y dependen de los maestros ya cargados.
MIGRACIONES = [
    # Maestros
    ("Contactos (gen_tercero -> gen_contacto)", procesar_contactos),
    ("Centros de costo (fin_centro_costo -> con_centro_costo)", procesar_centros_costo),
    ("Programadores (tur_programador -> tur_programador)", procesar_programadores),
    ("Puestos (tur_puesto -> tur_puesto)", procesar_puestos),
    ("Items (tur_item -> gen_item)", procesar_items),
    # Documentos
    ("Contratos (tur_contrato -> gen_documento)", procesar_contratos),
    ("Detalle contratos (tur_contrato_detalle -> gen_documento_detalle)", procesar_contrato_detalles),
    ("Impuestos detalle (tur_contrato_detalle -> gen_documento_impuesto)", procesar_contrato_detalle_impuestos),
]


def migrar_todo():
    total = len(MIGRACIONES)
    print(f"\n=== Iniciando migración: {total} tabla(s) ===")
    for indice, (descripcion, funcion) in enumerate(MIGRACIONES, start=1):
        print(f"\n--- [{indice}/{total}] {descripcion} ---")
        funcion()
    print(f"\n=== Migración completada: {total}/{total} tabla(s) ===")


def mostrar_menu():
    print("\nSeleccione una opción:")
    print("m - Migrar")
    print("s - Salir")

    opcion = input("Opción: ").lower().strip()
    if opcion == 's':
        print("Saliendo del programa...")
        sys.exit(0)
    elif opcion == 'm':
        migrar_todo()
        mostrar_menu()
    else:
        print("Opción no válida. Intente nuevamente.")
        mostrar_menu()


if __name__ == "__main__":
    mostrar_menu()
