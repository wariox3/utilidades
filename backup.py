import subprocess
import os

# Configuración de la conexión
host = "tu-servidor-remoto.com"
port = 3306
usuario = "usuario_mysql"
contrasena = "contrasena_mysql"
basedatos = "nombre_base_datos"
tabla_excluir = "tabla_a_excluir"
archivo_salida = "dump_sin_tabla.sql"

def exportar_base_datos():
    # Comando mysqldump con exclusión de una tabla
    comando = [
        "mysqldump",
        f"--host={host}",
        f"--port={port}",
        f"--user={usuario}",
        f"--password={contrasena}",
        "--routines",
        "--events",
        "--single-transaction",
        "--quick",
        "--skip-lock-tables",
        basedatos,
        f"--ignore-table={basedatos}.{tabla_excluir}"
    ]

    # Ejecutar el comando y guardar la salida en un archivo
    with open(archivo_salida, "w", encoding="utf-8") as f:
        try:
            subprocess.run(comando, check=True, stdout=f)
            print(f"✅ Dump generado correctamente en '{archivo_salida}', excluyendo la tabla '{tabla_excluir}'.")
        except subprocess.CalledProcessError as e:
            print("❌ Error al generar el dump:", e)

if __name__ == "__main__":
    exportar_base_datos()