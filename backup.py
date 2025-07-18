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
archivo_salida = f"/home/desarrollo/Escritorio/{base_datos}.sql"
base_datos_backup = "bditriobackup"

def exportar_base_datos():    
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
            '--dbname', f"postgresql://{usuario}:{clave}@{servidor}:{puerto}/bditriobackup",
            archivo_salida
        ], check=True)
        
        print(f"✅ Restauración exitosa en bditriobackup")
        return True
    except subprocess.CalledProcessError as e:
        print(f"❌ Error al restaurar: {e}")
        return False    

def modificar_dominio():
    try:
        connection_string = f"postgresql://{usuario}:{clave}@{servidor}:{puerto}/bditriobackup"
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
    print("e - restaurar estandar")
    print("m - modificar dominio")

    print("s - Salir")    
    opcion = input("Opción: ").lower().strip()    
    if opcion == 'b':
        exportar_base_datos()
    elif opcion == 'r':
        restaurar_backup()   
    elif opcion == 'e':
        restaurar_backup_estandar()    
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