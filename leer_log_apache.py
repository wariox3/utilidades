from decouple import config
from scp import SCPClient
import mysql.connector
import psycopg2
import sys
import traceback
import re
import paramiko
import os

def crear_conexiones():    
    postgres_parametros = {
        'user': config('PG_DATABASE_USER'),
        'password': config('PG_DATABASE_CLAVE'),
        'host': config('PG_DATABASE_HOST'),
        'port': config('PG_DATABASE_PORT'),
        'database': config('PG_DATABASE_NAME')
    }
    
    try:
        pg_conn = psycopg2.connect(**postgres_parametros)
        pg_cursor = pg_conn.cursor()
        
        return pg_conn, pg_cursor
        
    except Exception as e:
        print(f"Error al conectar a las bases de datos: {e}")
        sys.exit(1)

def cerrar_conexiones(pg_conn):
    try:
        if pg_conn and not pg_conn.closed:
            pg_conn.close()
        print("Conexiones cerradas.")
    except Exception as e:
        print(f"Error al cerrar conexiones: {e}")   

def crear_tabla(pg_conn):
    with pg_conn.cursor() as cursor:
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS acceso (
            id SERIAL PRIMARY KEY,
            host TEXT,
            ip TEXT,
            remote_user TEXT,
            auth_user TEXT,
            timestamp TEXT,
            method TEXT,
            path TEXT,
            protocol TEXT,
            status INTEGER,
            bytes INTEGER,
            referer TEXT,
            user_agent TEXT,
            processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        ''')
        pg_conn.commit()

def descargar_archivo(archivo_log):
    try:
        host=config('SCP_HOST')
        username=config('SCP_USER')
        password=config('SCP_PASSWORD')
        remote_path='/var/log/apache2/access.log'
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(host, username=username, password=password)
                
        with SCPClient(ssh.get_transport()) as scp:
            scp.get(remote_path, archivo_log)
            
        print(f"Archivo descargado exitosamente a {archivo_log}")
        return True
    except Exception as e:
        print(f"Error al descargar el archivo: {e}")
        return False
    finally:
        if ssh:
            ssh.close()

def parse_log_line(line):
    try:
        # Patrón regex para parsear líneas del access.log
        #pattern = r'^(\S+) (\S+) (\S+) (\S+) \[([^\]]+)\] "(\S+) (\S+) (\S+)" (\d+) (\d+) "([^"]*)" "([^"]*)"'
        #match = re.match(pattern, line.strip())  # <-- Aquí se usa strip()
        #pattern = r'^(\S+) (\S+) (\S+) ([^[]+) \[([^\]]+)\] "(\S+) (\S+) (\S+)" (\d{3}) (\d+|-) "([^"]*)" "([^"]*)"'
        #match = re.match(pattern, line.strip())
        #pattern = r'^(\S+) (\S+) (\S+) (\S+) \[([^\]]+)\] "(?:(\\S+) (\\S+) (\\S+)|([^"]*))" (\d{3}) (\d+|-) "([^"]*)" "([^"]*)"'
        #match = re.match(pattern, line.strip())        
        pattern = r'^(\S+) (\S+) (\S+) (\S+) \[([^\]]+)\] "(\S+) (\S+) (\S+)" (\d{3}) (\d+|-) "([^"]*)" "([^"]*)"'
        match = re.match(pattern, line.strip())
        if not match:
            print(f"No se pudo parsear la línea: {line}")
            return None
        bytes_sent = 0 if match.group(10) == '-' else int(match.group(10))
        return {
            'host': match.group(1),
            'ip': match.group(2),
            'remote_user': match.group(3),
            'auth_user': match.group(4).strip(),
            'timestamp': match.group(5),
            'method': match.group(6),
            'path': match.group(7),
            'protocol': match.group(8),
            'status': int(match.group(9)),
            'bytes': bytes_sent,
            'referer': match.group(11),
            'user_agent': match.group(12)
        }
    except Exception as e:
        print(f"Error al parsear línea: {e}")
        return None

def campo_texto(texto=None, limite=None):
    if texto:
        texto = texto.strip()
        if limite:
            texto = texto[:limite]
        return texto
    return None

def insertar_log(pg_conn, logs_data):
    try:
        with pg_conn.cursor() as cursor:
            sql = '''
            INSERT INTO acceso (
                id, host, ip, remote_user, auth_user, timestamp, 
                method, path, protocol, status, bytes, 
                referer, user_agent
            ) VALUES (nextval('acceso_id_seq'), %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            '''
                        
            values = [(
                campo_texto(log['host'], 100),
                campo_texto(log['ip'], 50),
                campo_texto(log['remote_user'], 100),
                campo_texto(log['auth_user'], 250),
                campo_texto(log['timestamp'], 50),
                campo_texto(log['method'], 30),
                campo_texto(log['path'], 255),
                campo_texto(log['protocol'], 50),
                log['status'],
                log['bytes'],
                campo_texto(log['referer'], 255),
                campo_texto(log['user_agent'], 100)
            ) for log in logs_data]
                    
            cursor.executemany(sql, values)
            pg_conn.commit()
            print(f"Insertados {len(logs_data)} registros")

    except Exception as e:
        print(f"Error al insertar registro: {e}")
        raise

def procesar_archivo(pg_conn, archivo_log):
    try:
        crear_tabla(pg_conn)         
        batch_size = 10
        logs_data = []      
        with open(archivo_log, 'r') as f:
            for line in f:
                line = line.strip()
                if line:
                    log_data = parse_log_line(line)
                    if log_data:
                        logs_data.append(log_data)

                        if len(logs_data) >= batch_size:
                            insertar_log(pg_conn, logs_data)
                            logs_data = []
            if logs_data:
                insertar_log(pg_conn, logs_data)            
        print("Procesamiento del archivo completado con éxito.")
    except FileNotFoundError:
        print(f"Error: El archivo {archivo_log} no existe.")
    except Exception as e:
        print(f"Error durante el procesamiento del archivo: {e}")
        raise
    
if __name__ == "__main__":
    try:
        archivo_log= '/home/desarrollo/Escritorio/access.log'
        pg_conn, pg_cursor = crear_conexiones()        
        descargar_archivo(archivo_log)        
        procesar_archivo(pg_conn, archivo_log)
    except Exception as e:
        print(f"Error durante el procesamiento: {e}")
        sys.exit(1)        
    finally:        
        cerrar_conexiones(pg_conn)
        if os.path.exists(archivo_log):
            os.remove(archivo_log)

    
                