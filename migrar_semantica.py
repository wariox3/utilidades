from decouple import config
import mysql.connector
import psycopg2
import sys

def get_database_connections():    
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

def close_connections(conexion, pg_conn):
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
    conexion, cursor, pg_conn, pg_cursor = get_database_connections()
    
    try:
        pg_cursor.execute(f"SET search_path TO {pg_schema}")

        query = """
            SELECT * FROM gen_tercero where codigo_tercero_pk >= 17405 LIMIT 100000
        """
        cursor.execute(query)
        registros = cursor.fetchall()                      
        
        for registro in registros:                   
            try:
                insert_query = f"""
                    INSERT INTO {pg_schema}.gen_contacto 
                    (id, identificacion_id, numero_identificacion, nombre_corto, direccion, telefono, correo, cliente, proveedor, empleado, ciudad_id,
                    regimen_id, tipo_persona_id)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (id) DO NOTHING;
                """
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
                values = (
                    registro['codigo_tercero_pk'],
                    6,
                    registro['numero_identificacion'],
                    registro['nombre_corto'],
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
                pg_cursor.execute(insert_query, values)
                pg_conn.commit()
                print(f"Inserción exictosa codigo {registro['codigo_tercero_pk']}!")
            except Exception as e:
                print(f"Error insertando registro {registro.get('codigo_tercero_pk', 'N/A')}: {e}")
                pg_conn.rollback()
                sys.exit(1)
                
    except Exception as e:
        print(f"Error durante el procesamiento de contactos: {e}")
        sys.exit(1)
        
    finally:
        close_connections(conexion, pg_conn)             

def procesar_movimientos():
    pg_schema = config('PG_SCHEMA_NAME', default='')
    conexion, cursor, pg_conn, pg_cursor = get_database_connections()
    
    try:
        pg_cursor.execute(f"SET search_path TO {pg_schema}")

        query = """
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
            ORDER BY m.codigo_movimiento_pk 
            LIMIT 1
        """
        cursor.execute(query)
        registros = cursor.fetchall()                      
        
        for registro in registros:                   
            try:
                insert_query = f"""
                    INSERT INTO {pg_schema}.con_movimiento 
                    (id, numero, fecha, debito, credito, base, naturaleza, detalle, cierre, comprobante_id, cuenta_id,
                    grupo_id, periodo_id, contacto_id)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (id) DO NOTHING;
                """
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
                pg_cursor.execute(insert_query, values)
                pg_conn.commit()
                print(f"Inserción exictosa id {registro['codigo_movimiento_pk']}!")
            except Exception as e:
                print(f"Error insertando registro {registro.get('codigo_movimiento_pk', 'N/A')}: {e}")
                pg_conn.rollback()
                sys.exit(1)
                
    except Exception as e:
        print(f"Error durante el procesamiento de movimientos: {e}")
        sys.exit(1)
        
    finally:
        close_connections(conexion, pg_conn)

def mostrar_menu():
    print("\nSeleccione una opción:")
    print("c - Procesar Contactos")
    print("m - Procesar Movimientos")
    print("s - Salir")    
    opcion = input("Opción: ").lower().strip()    
    if opcion == 'c':
        procesar_contactos()
    elif opcion == 'm':
        procesar_movimientos()
    elif opcion == 's':
        print("Saliendo del programa...")
        sys.exit(0)
    else:
        print("Opción no válida. Intente nuevamente.")
        mostrar_menu()

if __name__ == "__main__":
    mostrar_menu()            