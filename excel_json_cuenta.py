import pandas as pd
import json

# Leer el archivo Excel
excel_file = '/home/desarrollo/Escritorio/puc_reddoc.xlsx'  # Reemplaza esto con la ruta de tu archivo Excel
df = pd.read_excel(excel_file)

# Transformar los datos al formato requerido
cuentas = []

for index, row in df.iterrows():
    cuenta = {
        "pk": row['id'],
        "campos": {
            "codigo": row['codigo'],
            "nombre": row['nombre'],
            "cuenta_clase": row['cuenta_clase_id'],
            "cuenta_grupo": row['cuenta_grupo_id'],
            "cuenta_cuenta": row['cuenta_cuenta_id'],
            "exige_base": row['exige_base'],
            "exige_contacto": row['exige_contacto'],
            "exige_grupo": row['exige_grupo'],
            "permite_movimiento": row['permite_movimiento'],
        }
    }
    cuentas.append(cuenta)

# Convertir la lista de cuentas a JSON
json_data = json.dumps(cuentas, indent=4, ensure_ascii=False)


# Guardar el JSON en un archivo
with open('/home/desarrollo/Escritorio/puc_reddoc.json', 'w', encoding='utf-8') as json_file:
    json_file.write(json_data)

print("El archivo JSON se ha creado exitosamente.")