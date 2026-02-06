import os
import subprocess
import datetime
import sys
import psycopg2
from psycopg2 import sql
from decouple import config
from pathlib import Path
from urllib.parse import quote

servidor = config('PG_DATABASE_HOST')
puerto = config('PG_DATABASE_PORT')
usuario = config('PG_DATABASE_USER')
clave = quote(config('PG_DATABASE_CLAVE'))
base_datos = config('PG_DATABASE_NAME')
timestamp = datetime.datetime.now().strftime("%y%m%d%H%M")
archivo_salida = f"/home/desarrollo/Escritorio/bditrio.sql"
base_datos_backup = "bditriobackup"

def backup():    
    connection_string = f"postgresql://{usuario}:{clave}@{servidor}:{puerto}/{base_datos}"    
    comando = [
        'pg_dump',
        '-d', connection_string,
        '-f', archivo_salida,
        '-v',
    ]

    try:
        subprocess.run(comando, check=True)
        print(f"✅ Dump generado correctamente: {archivo_salida}")
    except subprocess.CalledProcessError as e:
        print("❌ Error al generar el dump:", e)

def restaurar_backup():
    if not Path(archivo_salida).exists():
        print("❌ El archivo de backup no existe.")
        return False
    connection_string = f"postgresql://{usuario}:{clave}@{servidor}:{puerto}/bditriobackup"
    
    try:
        subprocess.run([
            'psql',
            '-d', connection_string,
            '-f', archivo_salida,
            '-v', 'ON_ERROR_STOP=1',  # Detiene la ejecución si hay un error SQL
            '--echo-errors',          # Muestra errores en la consola
        ], check=True)
        
        print(f"✅ Restauración exitosa en bditriobackup")
        return True
    except subprocess.CalledProcessError as e:
        print(f"❌ Error al restaurar: {e}")
        return False

def backup_estandar():    
    connection_string = f"postgresql://{usuario}:{clave}@{servidor}:{puerto}/{base_datos}"    
    comando = [
            'pg_dump',
            '-d', connection_string,
            '-f', archivo_salida,
            '-Fc',  # Formato personalizado (binary)
            '--no-owner',  # Excluye información de ownership
            '-b',  # Incluye blobs grandes
            '-v',  # Modo verbose
        ]    

    try:
        print("🔄 Iniciando backup...")        
        subprocess.run(comando, check=True)
        print(f"✅ Dump generado correctamente: {archivo_salida}")
    except subprocess.CalledProcessError as e:
        print("❌ Error al generar el dump:", e)

def restaurar_backup_estandar():

    if not Path(archivo_salida).exists():
        print(f"❌ El archivo de backup {archivo_salida} no existe.")
        return False
    try:                
        subprocess.run([
            'pg_restore',
            '--verbose',
            '--clean',
            '--no-owner',
            '--dbname', f"postgresql://postgres:70143086@localhost:5432/bditriobackup",
            archivo_salida
        ], check=True)
        
        print(f"✅ Restauración exitosa en bditriobackup")
        return True
    except subprocess.CalledProcessError as e:
        print(f"❌ Error al restaurar: {e}")
        return False    

def backup_schema(nombre_schema):
    connection_string = f"postgresql://{usuario}:{clave}@{servidor}:{puerto}/{base_datos}"
    
    try:
        # Obtener lista de schemas (excluyendo system schemas)
        conn = psycopg2.connect(connection_string)
        cur = conn.cursor()
        cur.execute("""
            SELECT schema_name 
            FROM information_schema.schemata 
            WHERE schema_name = %s;
        """, (nombre_schema,))      
        schemas = [row[0] for row in cur.fetchall()]
        cur.close()
        conn.close()
        
        print(f"📊 Encontrados {len(schemas)} schemas para backup")
        backup_dir = "/home/desarrollo/Escritorio/backup"
        for i, schema in enumerate(schemas, 1):
            archivo_schema = os.path.join(backup_dir, f"{schema}.backup")
            comando = [
                'pg_dump',
                '-d', connection_string,
                '-n', schema,
                '-f', archivo_schema,
                '-Fc',
                '--no-owner',
                '--no-acl',
                '--create',
                '--verbose'                
            ]
            
            try:
                print(f"🔄 [{i}/{len(schemas)}] Backup schema: {schema}")
                subprocess.run(comando, check=True, timeout=3600)  # Timeout de 1 hora
                print(f"✅ Schema completado: {schema}")
            except subprocess.TimeoutExpired:
                print(f"⏰ Timeout en schema: {schema}")
                continue
            except subprocess.CalledProcessError as e:
                print(f"❌ Error en schema {schema}: {e}")
                continue
        
        print(f"🎉 Backup completado. Directorio: {backup_dir}")
        
    except Exception as e:
        print(f"💥 Error general: {e}")

def restaurar_backup_schema():
    connection_string = f"postgresql://postgres:70143086@localhost:5432/bditriobackup"    
    try:
        backup_dir = "/home/desarrollo/Escritorio/backup"
        archivos_backup = [f for f in os.listdir(backup_dir) if f.endswith(".backup")]
        if not archivos_backup:
            print("⚠️ No se encontraron archivos .backup en el directorio.")
            return
        
        print(f"📦 Se encontraron {len(archivos_backup)} archivos de backup para restaurar.\n")


        # PRIMERO: Crear los schemas si no existen
        conn = psycopg2.connect(connection_string)
        cur = conn.cursor()
        for archivo in archivos_backup:
            nombre_schema = os.path.splitext(archivo)[0]
            print(f"🔧 Verificando/Creando schema: {nombre_schema}")
            cur.execute(f"CREATE SCHEMA IF NOT EXISTS {nombre_schema};")
        
        conn.commit()
        cur.close()
        conn.close()
        print("✅ Schemas verificados/creados\n")

        for i, archivo in enumerate(sorted(archivos_backup), 1):
            ruta_backup = os.path.join(backup_dir, archivo)
            nombre_schema = os.path.splitext(archivo)[0]
            
            print(f"🔄 [{i}/{len(archivos_backup)}] Restaurando schema: {nombre_schema}")
            
            # Restaurar el schema
            comando = [
                'pg_restore',
                '-d', connection_string,
                '-n', nombre_schema,
                '--no-owner',
                '--no-acl',
                '--verbose',
                ruta_backup
            ]
            
            try:
                subprocess.run(comando, check=True, timeout=3600)
                print(f"✅ Schema restaurado: {nombre_schema}\n")
            except subprocess.TimeoutExpired:
                print(f"⏰ Timeout en schema: {nombre_schema}")
            except subprocess.CalledProcessError as e:
                print(f"❌ Error al restaurar schema {nombre_schema}: {e}\n")

        print("🎉 Restauración completada.")

    except Exception as e:
        print(f"💥 Error general: {e}")

def modificar_dominio():
    try:
        connection_string = f"postgresql://postgres:70143086@localhost:5432/bditriobackup"
        conn = psycopg2.connect(connection_string)
        cursor = conn.cursor()
        
        select_query = sql.SQL("SELECT id, domain FROM public.cnt_dominio")
        cursor.execute(select_query)
        registros = cursor.fetchall()
        
        for registro in registros:
            id_registro, dominio_actual = registro
            if dominio_actual and '.' in dominio_actual:                                
                nuevo_dominio = dominio_actual.split('.')[0] + ".localhost"
                if id_registro == 1:                    
                    nuevo_dominio = "localhost"                
                update_query = sql.SQL("""
                    UPDATE public.cnt_dominio 
                    SET domain = %s 
                    WHERE id = %s
                """)
                cursor.execute(update_query, (nuevo_dominio, id_registro))    
        conn.commit()
        print("Dominios modificados exitosamente")
        
    except Exception as e:
        print(f"Error al modificar dominios: {e}")
        if 'conn' in locals():
            conn.rollback()
    finally:
        # Cerrar conexión
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()

def mostrar_menu():
    print("\nSeleccione una opción:")
    print("b - backup")
    print("r - restaurar")
    print("g - backup estandar")
    print("e - restaurar estandar")
    print("k - backup schema")
    print("t - restaurar schema")    
    print("m - modificar dominio")

    print("s - Salir")    
    opcion = input("Opción: ").lower().strip()    
    if opcion == 'b':
        backup()
    elif opcion == 'r':
        restaurar_backup()   
    elif opcion == 'g':
        backup_estandar()
    elif opcion == 'e':
        restaurar_backup_estandar()    
    elif opcion == 'k':
        nombre_schema = input("Ingrese el nombre del schema a respaldar: ").strip()
        backup_schema(nombre_schema)        
    elif opcion == 't':
        restaurar_backup_schema()            
    elif opcion == 'm':
        modificar_dominio()                                   
    elif opcion == 's':
        print("Saliendo del programa...")
        sys.exit(0)
    else:
        print("Opción no válida. Intente nuevamente.")
        mostrar_menu()

if __name__ == "__main__":
    mostrar_menu()