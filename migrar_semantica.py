from decouple import config
import mysql.connector
import psycopg2
import sys
import traceback

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

def procesar_contactos():
    pg_schema = config('PG_SCHEMA_NAME', default='')
    conexion, cursor, pg_conn, pg_cursor = crear_conexiones()
    
    try:
        pg_cursor.execute(f"SET search_path TO {pg_schema}")
        batch_size = 1000
        offset = 0        
        cursor.execute("SELECT COUNT(*) AS total FROM gen_tercero")
        result = cursor.fetchone()        
        total_records = result['total']
        print(f"Iniciando migración de {total_records} registros...")
        while offset < total_records:
            query = f"""
                SELECT * FROM gen_tercero LIMIT {batch_size} OFFSET {offset}
            """
            cursor.execute(query)
            registros = cursor.fetchall()  
            if not registros:
                break
            values_batch = []
            for registro in registros:                   
                cliente = bool(registro['cliente'])
                proveedor = bool(registro['proveedor'])
                empleado = bool(registro['empleado'])
                correo = registro['correo']
                if not correo or not isinstance(correo, str) or '@' not in correo:
                    correo = "correo@cliente.com"
                direccion = "Conocida"
                if registro['direccion']:
                    direccion = registro['direccion'][:100]
                telefono = registro['telefono']
                if telefono is None or telefono == "":
                    telefono = registro['celular']
                if telefono is None or telefono == "":
                    telefono = "1"
                nombre_corto = "Contacto"
                if registro['nombre_corto']:
                    nombre_corto = registro['nombre_corto']
                numero_identificacion = "1"
                if registro['numero_identificacion']:
                    numero_identificacion = registro['numero_identificacion']
                values = (
                    registro['codigo_tercero_pk'],
                    6,
                    numero_identificacion,
                    nombre_corto,
                    direccion,
                    telefono,
                    correo,
                    cliente,
                    proveedor,
                    empleado,
                    1,
                    1,
                    1
                )
                values_batch.append(values)
                                
            try:
                insert_query = f"""
                    INSERT INTO {pg_schema}.gen_contacto 
                    (id, identificacion_id, numero_identificacion, nombre_corto, direccion, telefono, correo, cliente, proveedor, empleado, ciudad_id,
                    regimen_id, tipo_persona_id)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (id) DO NOTHING;
                """              
                pg_cursor.executemany(insert_query, values_batch)
                pg_conn.commit()
                print(f"Lote {offset//batch_size + 1} completado ({offset + len(registros)}/{total_records} registros)")
            except Exception as e:
                print(f"Error en lote {offset//batch_size + 1}: {e}")
                pg_conn.rollback()    
                sys.exit(1)                        
            offset += batch_size 

    except Exception as e:
        print(f"Error durante el procesamiento de contactos: {e}")
        sys.exit(1)
        
    finally:
        cerrar_conexiones(conexion, pg_conn)             

def solicitar_anio():
    while True:
        try:
            anio = input("Ingrese el año a procesar (ej. 2022): ")
            if not anio.isdigit() or len(anio) != 4:
                print("Por favor ingrese un año válido de 4 dígitos.")
                continue
            return int(anio)
        except ValueError:
            print("Por favor ingrese un año válido (número de 4 dígitos).")

def procesar_movimientos(anio):
    pg_schema = config('PG_SCHEMA_NAME', default='')
    conexion, cursor, pg_conn, pg_cursor = crear_conexiones()    
    try:
        pg_cursor.execute(f"SET search_path TO {pg_schema}")        
        batch_size = 10000
        offset = 0        
        cursor.execute(f"SELECT COUNT(*) AS total FROM fin_movimiento WHERE anio={anio}")
        result = cursor.fetchone()        
        total_records = result['total']
        print(f"Iniciando migración de {total_records} registros...")
        while offset < total_records:
            query = f"""
                SELECT 
                    m.*, 
                    c.codigo_interface as codigo_comprobante,
                    cu.codigo_interface as codigo_cuenta,
                    cc.codigo_interface as codigo_centro_costo
                FROM 
                    fin_movimiento m 
                    LEFT JOIN fin_comprobante c ON m.codigo_comprobante_fk = c.codigo_comprobante_pk 
                    LEFT JOIN fin_cuenta cu ON m.codigo_cuenta_fk = cu.codigo_cuenta_pk 
                    LEFT JOIN fin_centro_costo cc ON m.codigo_centro_costo_fk = cc.codigo_centro_costo_pk
                WHERE m.anio={anio} 
                ORDER BY m.codigo_movimiento_pk 
                LIMIT {batch_size} OFFSET {offset}
            """
            cursor.execute(query)
            registros = cursor.fetchall()
            if not registros:
                break  
            values_batch = []
            for registro in registros:                                                   
                cierre = False
                periodo_crudo = str(registro['codigo_periodo_fk'])
                periodo = periodo_crudo[4:6]
                if periodo == '13':
                    cierre = True
                detalle = None
                if registro['descripcion']:
                    detalle = registro['descripcion'][:150]

                values = (
                    registro['codigo_movimiento_pk'],
                    registro['numero'],
                    registro['fecha'],
                    registro['vr_debito'],
                    registro['vr_credito'],
                    registro['vr_base'],
                    registro['naturaleza'],
                    detalle,
                    cierre,
                    registro['codigo_comprobante'],
                    registro['codigo_cuenta'],
                    registro['codigo_centro_costo'],
                    registro['codigo_periodo_fk'],
                    registro['codigo_tercero_fk']
                )
                values_batch.append(values)
            
            try:
                insert_query = f"""
                    INSERT INTO {pg_schema}.con_movimiento 
                    (id, numero, fecha, debito, credito, base, naturaleza, detalle, cierre, comprobante_id, cuenta_id,
                    grupo_id, periodo_id, contacto_id)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (id) DO NOTHING;
                """                
                pg_cursor.executemany(insert_query, values_batch)
                pg_conn.commit()
                print(f"Lote {offset//batch_size + 1} completado ({offset + len(registros)}/{total_records} registros)")
            except Exception as e:
                print(f"Error en lote {offset//batch_size + 1}: {e}")
                pg_conn.rollback()    
                sys.exit(1)                        
            offset += batch_size                    
    except Exception as e:
        print(f"\nERROR CRÍTICO durante el procesamiento: {str(e)}")
        print(f"Último offset procesado: {offset}")
        traceback.print_exc()
        print(f"Error durante el procesamiento de movimientos: {e}")
        sys.exit(1)
        
    finally:
        cerrar_conexiones(conexion, pg_conn)

def mostrar_menu():
    print("\nSeleccione una opción:")
    print("c - Procesar Contactos")
    print("m - Procesar Movimientos")
    print("s - Salir")    
    opcion = input("Opción: ").lower().strip()    
    if opcion == 'c':
        procesar_contactos()
    elif opcion == 'm':
        anio = solicitar_anio()
        procesar_movimientos(anio)                
    elif opcion == 's':
        print("Saliendo del programa...")
        sys.exit(0)
    else:
        print("Opción no válida. Intente nuevamente.")
        mostrar_menu()

if __name__ == "__main__":
    mostrar_menu()            