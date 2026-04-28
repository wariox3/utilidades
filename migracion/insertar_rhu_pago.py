from decouple import config
import mysql.connector
import pandas as pd
import sys
import math


def crear_conexion():
    try:
        conexion = mysql.connector.connect(
            user=config('DATABASE_USER'),
            password=config('DATABASE_CLAVE'),
            host=config('DATABASE_HOST'),
            port=config('DATABASE_PORT'),
            database=config('DATABASE_NAME')
        )
        return conexion
    except Exception as e:
        print(f"Error al conectar a la base de datos: {e}")
        sys.exit(1)


def limpiar_valor(valor, tipo='texto'):
    """Convierte NaN/NaT de pandas a None para MySQL."""
    if valor is None:
        return None
    if isinstance(valor, float) and math.isnan(valor):
        return None
    if pd.isna(valor):
        return None
    if tipo == 'entero':
        return int(valor)
    if tipo == 'flotante':
        return float(valor)
    if tipo == 'booleano':
        return bool(valor)
    if tipo == 'fecha':
        try:
            return pd.to_datetime(valor).date()
        except Exception:
            return None
    if tipo == 'fecha_hora':
        try:
            return pd.to_datetime(valor).to_pydatetime()
        except Exception:
            return None
    if isinstance(valor, float) and valor.is_integer():
        return str(int(valor))
    return str(valor)


def insertar_rhu_pago():
    ruta_excel = 'migracion/rhu_pago_migracion_v2.xlsx'
    batch_size = 1000

    print(f"Leyendo archivo: {ruta_excel}")
    df = pd.read_excel(ruta_excel, dtype={'codigo_grupo_fk': str})
    total = len(df)
    print(f"Total de registros a insertar: {total}")

    conexion = crear_conexion()
    cursor = conexion.cursor()

    insert_query = """
        INSERT INTO rhu_pago (
            codigo_pago_pk,
            codigo_pago_tipo_fk,
            codigo_entidad_salud_fk,
            codigo_entidad_pension_fk,
            codigo_entidad_caja_fk,
            codigo_periodo_fk,
            codigo_banco_fk,
            codigo_empleado_fk,
            codigo_contrato_fk,
            codigo_grupo_fk,
            codigo_programacion_detalle_fk,
            codigo_programacion_fk,
            codigo_vacacion_fk,
            codigo_liquidacion_fk,
            codigo_tiempo_fk,
            codigo_electronico_detalle_fk,
            codigo_empresa_fk,
            codigo_cargo_fk,
            cuenta,
            cuenta_tipo,
            numero,
            fecha,
            fecha_desde,
            fecha_hasta,
            fecha_desde_contrato,
            fecha_hasta_contrato,
            vr_salario_contrato,
            dias,
            vr_devengado,
            vr_deduccion,
            vr_neto,
            vr_cesantia,
            vr_interes,
            vr_prima,
            vr_vacacion,
            vr_indemnizacion,
            vr_ingreso_base_cotizacion,
            vr_ingreso_base_prestacion,
            vr_ingreso_base_prestacion_vacacion,
            vr_ingreso_base_prestacion_indemnizacion,
            vr_salario,
            vr_auxilio_transporte,
            vr_extra,
            vr_recargo_nocturno,
            vr_devengado_prestacional,
            vr_devengado_no_prestacional,
            vr_salud,
            vr_pension,
            vr_anticipo,
            vr_deduccion_otro,
            vr_incapacidad_empresa,
            vr_incapacidad_entidad,
            vr_embargo,
            vr_fondo_solidaridad,
            vr_retencion_fuente,
            vr_credito,
            dias_ausentismo,
            estado_autorizado,
            estado_aprobado,
            estado_anulado,
            estado_contabilizado,
            estado_egreso,
            habilitado_portal,
            comentario,
            usuario,
            codigo_soporte_contrato_fk,
            estado_electronico,
            cune,
            codigo_externo,
            cadena_codigo_qr,
            fecha_electronico,
            numero_electronico,
            migracion,
            vr_devengado_deducible
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s
        )
        ON DUPLICATE KEY UPDATE codigo_pago_pk = codigo_pago_pk
    """

    insertados = 0
    errores = 0

    for inicio in range(0, total, batch_size):
        lote = df.iloc[inicio:inicio + batch_size]
        valores_lote = []

        for _, fila in lote.iterrows():
            valores = (
                limpiar_valor(fila['codigo_pago_pk'], 'entero'),
                limpiar_valor(fila['codigo_pago_tipo_fk']),
                limpiar_valor(fila['codigo_entidad_salud_fk'], 'entero'),
                limpiar_valor(fila['codigo_entidad_pension_fk'], 'entero'),
                limpiar_valor(fila['codigo_entidad_caja_fk'], 'entero'),
                limpiar_valor(fila['codigo_periodo_fk']),
                limpiar_valor(fila['codigo_banco_fk']),
                limpiar_valor(fila['codigo_empleado_fk'], 'entero'),
                limpiar_valor(fila['codigo_contrato_fk'], 'entero'),
                limpiar_valor(fila['codigo_grupo_fk']),
                limpiar_valor(fila['codigo_programacion_detalle_fk'], 'entero'),
                limpiar_valor(fila['codigo_programacion_fk'], 'entero'),
                limpiar_valor(fila['codigo_vacacion_fk'], 'entero'),
                limpiar_valor(fila['codigo_liquidacion_fk'], 'entero'),
                limpiar_valor(fila['codigo_tiempo_fk']),
                limpiar_valor(fila['codigo_electronico_detalle_fk'], 'entero'),
                limpiar_valor(fila['codigo_empresa_fk'], 'entero') or 1,
                limpiar_valor(fila['codigo_cargo_fk']),
                limpiar_valor(fila['cuenta banco ']),
                limpiar_valor(fila['cuenta_tipo']),
                limpiar_valor(fila['numero'], 'entero'),
                limpiar_valor(fila['fecha'], 'fecha'),
                limpiar_valor(fila['fecha_desde'], 'fecha'),
                limpiar_valor(fila['fecha_hasta'], 'fecha'),
                limpiar_valor(fila['fecha_desde'], 'fecha'),
                limpiar_valor(fila['fecha_hasta'], 'fecha'),
                limpiar_valor(fila['vr_salario_contrato'], 'flotante'),
                limpiar_valor(fila['dias'], 'entero'),
                limpiar_valor(fila['vr_devengado'], 'flotante'),
                limpiar_valor(fila['vr_deduccion'], 'flotante'),
                limpiar_valor(fila['vr_neto'], 'flotante'),
                limpiar_valor(fila['vr_cesantia'], 'flotante'),
                limpiar_valor(fila['vr_interes'], 'flotante'),
                limpiar_valor(fila['vr_prima'], 'flotante'),
                limpiar_valor(fila['vr_vacacion'], 'flotante'),
                limpiar_valor(fila['vr_indemnizacion'], 'flotante'),
                limpiar_valor(fila['vr_ingreso_base_cotizacion'], 'flotante'),
                limpiar_valor(fila['vr_ingreso_base_prestacion'], 'flotante'),
                limpiar_valor(fila['vr_ingreso_base_prestacion_vacacion'], 'flotante'),
                limpiar_valor(fila['vr_ingreso_base_prestacion_indemnizacion'], 'flotante'),
                limpiar_valor(fila['vr_salario'], 'flotante'),
                limpiar_valor(fila['vr_auxilio_transporte'], 'flotante'),
                limpiar_valor(fila['vr_extra'], 'flotante'),
                limpiar_valor(fila['vr_recargo_nocturno'], 'flotante'),
                limpiar_valor(fila['vr_devengado_prestacional'], 'flotante'),
                limpiar_valor(fila['vr_devengado_no_prestacional'], 'flotante'),
                limpiar_valor(fila['vr_salud'], 'flotante'),
                limpiar_valor(fila['vr_pension'], 'flotante'),
                limpiar_valor(fila['vr_anticipo'], 'flotante'),
                limpiar_valor(fila['vr_deduccion_otro'], 'flotante'),
                limpiar_valor(fila['vr_incapacidad_empresa'], 'flotante'),
                limpiar_valor(fila['vr_incapacidad_entidad'], 'flotante'),
                limpiar_valor(fila['vr_embargo'], 'flotante'),
                limpiar_valor(fila['vr_fondo_solidaridad'], 'flotante'),
                limpiar_valor(fila['vr_retencion_fuente'], 'flotante'),
                limpiar_valor(fila['vr_credito'], 'flotante'),
                limpiar_valor(fila['dias_ausentismo'], 'entero'),
                limpiar_valor(fila['estado_autorizado'], 'booleano'),
                limpiar_valor(fila['estado_aprobado'], 'booleano'),
                limpiar_valor(fila['estado_anulado'], 'booleano'),
                limpiar_valor(fila['estado_contabilizado'], 'booleano'),
                limpiar_valor(fila['estado_egreso'], 'booleano'),
                limpiar_valor(fila['habilitado_portal'], 'booleano'),
                limpiar_valor(fila['comentario']),
                limpiar_valor(fila['usuario']),
                limpiar_valor(fila['codigo_soporte_contrato_fk'], 'entero'),
                limpiar_valor(fila['estado_electronico'], 'booleano'),
                limpiar_valor(fila['cune']),
                limpiar_valor(fila['codigo_externo']),
                limpiar_valor(fila['cadena_codigo_qr']),
                limpiar_valor(fila['fecha_electronico'], 'fecha_hora'),
                limpiar_valor(fila['numero_electronico'], 'entero'),
                1,   # migracion = True
                0.0  # vr_devengado_deducible = 0
            )
            valores_lote.append(valores)

        try:
            cursor.executemany(insert_query, valores_lote)
            conexion.commit()
            insertados += len(valores_lote)
            lote_num = inicio // batch_size + 1
            print(f"Lote {lote_num} completado ({insertados}/{total} registros)")
        except Exception as e:
            conexion.rollback()
            errores += len(valores_lote)
            lote_num = inicio // batch_size + 1
            print(f"Error en lote {lote_num} (filas {inicio}-{inicio + len(valores_lote) - 1}): {e}")
            print(f"Primer registro del lote: {valores_lote[0]}")
            sys.exit(1)

    cursor.close()
    conexion.close()
    print(f"\nMigración finalizada: {insertados} insertados, {errores} errores.")


if __name__ == "__main__":
    insertar_rhu_pago()
