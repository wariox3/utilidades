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


def insertar_rhu_pago_detalle():
    ruta_excel = 'migracion/rhu_pago_detalle_migración_v2.xlsx'
    batch_size = 1000

    print(f"Leyendo archivo: {ruta_excel}")
    df = pd.read_excel(ruta_excel, dtype={'codigo_concepto_fk': str})
    total = len(df)
    print(f"Total de registros a insertar: {total}")

    conexion = crear_conexion()
    cursor = conexion.cursor()

    insert_query = """
        INSERT INTO rhu_pago_detalle (
            codigo_pago_detalle_pk,
            codigo_pago_fk,
            codigo_concepto_fk,
            codigo_licencia_fk,
            codigo_incapacidad_fk,
            codigo_credito_fk,
            codigo_vacacion_fk,
            vr_pago,
            operacion,
            vr_pago_operado,
            horas,
            vr_hora,
            porcentaje,
            dias,
            detalle,
            vr_deduccion,
            vr_devengado,
            vr_ingreso_base_cotizacion,
            vr_ingreso_base_prestacion,
            vr_ingreso_base_prestacion_vacacion,
            vr_ingreso_base_prestacion_indemnizacion,
            vr_ingreso_base_cotizacion_adicional,
            vr_base,
            codigo_novedad_fk,
            codigo_embargo_fk,
            codigo_adicional_fk,
            codigo_empresa_fk,
            vr_devengado_deducible,
            migracion
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        ON DUPLICATE KEY UPDATE codigo_pago_detalle_pk = codigo_pago_detalle_pk
    """

    insertados = 0
    errores = 0

    for inicio in range(0, total, batch_size):
        lote = df.iloc[inicio:inicio + batch_size]
        valores_lote = []

        for _, fila in lote.iterrows():
            valores = (
                limpiar_valor(fila['codigo_pago_detalle_pk'], 'entero'),
                limpiar_valor(fila['codigo_pago_fk'], 'entero'),
                limpiar_valor(fila['codigo_concepto_fk']),
                limpiar_valor(fila['codigo_licencia_fk'], 'entero'),
                limpiar_valor(fila['codigo_incapacidad_fk'], 'entero'),
                limpiar_valor(fila['codigo_credito_fk'], 'entero'),
                limpiar_valor(fila['codigo_vacacion_fk'], 'entero'),
                limpiar_valor(fila['vr_pago'], 'flotante'),
                limpiar_valor(fila['operacion'], 'entero') or 0,
                limpiar_valor(fila['vr_pago_operado'], 'flotante') or 0.0,
                limpiar_valor(fila['horas'], 'flotante') or 0.0,
                limpiar_valor(fila['vr_hora'], 'flotante') or 0.0,
                limpiar_valor(fila['porcentaje'], 'flotante') or 0.0,
                limpiar_valor(fila['dias'], 'entero') or 0,
                limpiar_valor(fila['detalle']),
                limpiar_valor(fila['vr_deduccion'], 'flotante'),
                limpiar_valor(fila['vr_devengado'], 'flotante'),
                limpiar_valor(fila['vr_ingreso_base_cotizacion'], 'flotante') or 0.0,
                limpiar_valor(fila['vr_ingreso_base_prestacion'], 'flotante') or 0.0,
                limpiar_valor(fila['vr_ingreso_base_prestacion_vacacion'], 'flotante') or 0.0,
                limpiar_valor(fila['vr_ingreso_base_prestacion_indemnizacion'], 'flotante') or 0.0,
                limpiar_valor(fila['vr_ingreso_base_cotizacion_adicional'], 'flotante'),
                limpiar_valor(fila['vr_base'], 'flotante'),
                limpiar_valor(fila['codigo_novedad_fk'], 'entero'),
                limpiar_valor(fila['codigo_embargo_fk'], 'entero'),
                limpiar_valor(fila['codigo_adicional_fk'], 'entero'),
                limpiar_valor(fila['codigo_empresa_fk'], 'entero') or 1,
                0.0,  # vr_devengado_deducible
                1,  # migracion = True
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
    insertar_rhu_pago_detalle()
