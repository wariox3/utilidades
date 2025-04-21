import subprocess
import datetime
from decouple import config


# Configuración de la conexión
host = config('DATABASE_HOST')
port = config('DATABASE_PORT')
usuario = config('DATABASE_USER')
clave = config('DATABASE_CLAVE')
basedatos = "bdinsepltda"
timestamp = datetime.datetime.now().strftime("%y%m%d%H%M")
archivo_salida = f"/home/desarrollo/Escritorio/{basedatos}_{timestamp}.sql"

def exportar_base_datos():    
    print(port)
    comando = [
        "mysqldump",
        f"--host={host}",
        f"--port={port}",
        f"--user={usuario}",
        f"--password={clave}",
        "--routines",
        "--events",
        "--single-transaction",
        "--quick",
        "--skip-lock-tables",
        basedatos,
        f"--ignore-table={basedatos}.gen_log"
    ]
    
    try:
        with open(archivo_salida, "w", encoding="utf-8") as f:
            subprocess.run(comando, check=True, stdout=f)
        print(f"✅ Dump generado correctamente: {archivo_salida}")
    except subprocess.CalledProcessError as e:
        print("❌ Error al generar el dump:", e)

if __name__ == "__main__":
    exportar_base_datos()