from decouple import config
import mysql.connector
import psycopg2
import sys
import traceback
import pandas as pd

def crear_conexiones():    
    parametros = {
        'user': config('DATABASE_USER'),
        'password': config('DATABASE_CLAVE'),
        'host': config('DATABASE_HOST'),
        'port': config('DATABASE_PORT'),
        'database': config('DATABASE_NAME')
    }
    
    try:
        conexion = mysql.connector.connect(**parametros)
        cursor = conexion.cursor(dictionary=True)        
        return conexion, cursor
        
    except Exception as e:
        print(f"Error al conectar a las bases de datos: {e}")
        sys.exit(1)

def cerrar_conexiones(conexion):
    try:
        if conexion and conexion.is_connected():
            conexion.close()
        print("Conexiones cerradas.")
    except Exception as e:
        print(f"Error al cerrar conexiones: {e}")   

def extraer_correos():
    
    conexion, cursor = crear_conexiones()
    try:
        query_bloqueados = "SELECT codigo_bloqueado_pk FROM bloqueado"
        cursor.execute(query_bloqueados)
        bloqueados = {row['codigo_bloqueado_pk'].lower().strip() for row in cursor.fetchall()}                
        query = "SELECT correo FROM correo"
        cursor.execute(query)
        registros = cursor.fetchall() 
        correos_unicos = set()
        for registro in registros:            
            correos = registro['correo'].split(';')
            for correo in correos:
                correo_limpio = correo.strip().lower()
                if correo_limpio and '@' in correo_limpio and correo_limpio not in bloqueados:
                    correos_unicos.add(correo_limpio)                    
        lista_correos = sorted(list(correos_unicos))
        df = pd.DataFrame(lista_correos, columns=['Correo Electrónico'])
        nombre_archivo = '/home/desarrollo/Escritorio/correos.xlsx'
        df.to_excel(nombre_archivo, index=False)        
        print(f"Se han extraído {len(lista_correos)} correos únicos y se han guardado en {nombre_archivo}")

    except Exception as e:
        print(f"Error durante el procesamiento: {e}")
        sys.exit(1)
        
    finally:
        cerrar_conexiones(conexion)             

def mostrar_menu():
    print("\nSeleccione una opción:")
    print("c - extraer correos")    
    print("s - Salir")    
    opcion = input("Opción: ").lower().strip()    
    if opcion == 'c':
        extraer_correos()               
    elif opcion == 's':
        print("Saliendo del programa...")
        sys.exit(0)
    else:
        print("Opción no válida. Intente nuevamente.")
        mostrar_menu()

if __name__ == "__main__":
    mostrar_menu()            