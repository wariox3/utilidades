from decouple import config
import mysql.connector
import psycopg2
import sys

def main():  
    
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
    pg_schema = config('PG_SCHEMA_NAME', default='veloenvios')
    try:        
        conexion = mysql.connector.connect(**parametros)
        cursor = conexion.cursor(dictionary=True)

        pg_conn = psycopg2.connect(**postgres_parametros)
        pg_cursor = pg_conn.cursor()
        pg_cursor.execute(f"SET search_path TO {pg_schema}")

        query = """
            SELECT * FROM gen_tercero LIMIT 100000
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
                values = (
                    registro['codigo_tercero_pk'],
                    6,
                    registro['numero_identificacion'],
                    registro['nombre_corto'],
                    registro['direccion'],
                    registro['telefono'],
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
                print(f"Inserción exitosa codigo {registro['codigo_tercero_pk']}!")
            except Exception as e:
                print(f"Error insertando registro {registro.get('codigo_tercero_pk', 'N/A')}: {e}")
                pg_conn.rollback()
                sys.exit(1)
                #continue            
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)
        
    finally:        
        if 'conexion' in locals() and conexion.is_connected():
            conexion.close()
        if 'pg_conn' in locals() and not pg_conn.closed:
            pg_conn.close()
        print("Conexiones cerradas.")            

if __name__ == "__main__":
    main()            