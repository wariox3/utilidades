from decouple import config
import psycopg2

def main():    
    print("Iniciando actualización de números de identificación...")
    postgres_parametros = {
        'user': config('PG_DATABASE_USER'),
        'password': config('PG_DATABASE_CLAVE'),
        'host': config('PG_DATABASE_HOST'),
        'port': config('PG_DATABASE_PORT'),
        'database': config('PG_DATABASE_NAME')
    }
    pg_conn = psycopg2.connect(**postgres_parametros)
    pg_cursor = pg_conn.cursor()
    try:
        query = """
        SELECT schema_name 
        FROM information_schema.schemata 
        WHERE schema_name NOT IN ('information_schema', 'pg_catalog', 'pg_toast', 'public')
        AND schema_name NOT LIKE 'pg_temp_%'
        AND schema_name NOT LIKE 'pg_toast_temp_%'
        ORDER BY schema_name
        """
        pg_cursor.execute(query)
        schemas = [row[0] for row in pg_cursor.fetchall()]
        print(f"Encontrados {len(schemas)} schemas")

        # Recorrer cada schema
        for schema in schemas:
            print(f"\n--- Procesando schema: {schema} ---")
            
            try:
                # Verificar si la tabla gen_empresa existe en este schema
                check_table_query = """
                SELECT EXISTS (
                    SELECT 1 
                    FROM information_schema.tables 
                    WHERE table_schema = %s 
                    AND table_name = 'gen_empresa'
                )
                """
                pg_cursor.execute(check_table_query, (schema,))
                tabla_existe = pg_cursor.fetchone()[0]
                
                if not tabla_existe:
                    print(f"  La tabla gen_empresa no existe en el schema {schema}")
                    continue
                
                # Consultar el número de identificación
                consulta_identificacion = f"""
                SELECT numero_identificacion 
                FROM "{schema}".gen_empresa
                LIMIT 1
                """
                
                pg_cursor.execute(consulta_identificacion)
                resultado = pg_cursor.fetchone()
                
                if resultado:
                    numero_identificacion = resultado[0]
                    print(f"  Número de identificación encontrado: {numero_identificacion}")
                    check_contenedor_query = """
                    SELECT COUNT(*) 
                    FROM public.cnt_contenedor 
                    WHERE schema_name = %s
                    """
                    pg_cursor.execute(check_contenedor_query, (schema,))
                    existe_registro = pg_cursor.fetchone()[0] > 0
                    
                    if existe_registro:
                        # Actualizar el registro existente
                        update_query = """
                        UPDATE public.cnt_contenedor 
                        SET numero_identificacion = %s
                        WHERE schema_name = %s
                        """
                        pg_cursor.execute(update_query, (numero_identificacion, schema))
                        pg_conn.commit()
                        print(f"  ✓ Registro actualizado en public.cnt_contenedor para schema {schema}")

                else:
                    print(f"  La tabla gen_empresa en {schema} está vacía o no tiene datos")                    
            except psycopg2.Error as e:
                if 'numero_identificacion' in str(e):
                    print(f"  Error: La columna 'numero_identificacion' no existe en gen_empresa del schema {schema}")
                else:
                    print(f"  Error procesando schema {schema}: {e}")
            except Exception as e:
                print(f"  Error inesperado en schema {schema}: {e}")
        


    except Exception as e:
        print(f"Error al obtener schemas: {e}")
    finally:
        pg_cursor.close()
        pg_conn.close()
        print("\nConexión cerrada.")


if __name__ == "__main__":
    main()            