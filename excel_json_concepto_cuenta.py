import pandas as pd
import json

# Leer el archivo Excel
excel_file = '/home/desarrollo/Escritorio/concepto_cuenta.xlsx'
df = pd.read_excel(excel_file)
conceptos_cuentas = []
for index, row in df.iterrows():
    concepto_cuenta = {
        "pk": row['id'],
        "campos": {
            "concepto": row['concepto_id'],
            "cuenta": row['cuenta_id'],
            "tipo_costo": row['tipo_costo_id']            
        }
    }
    conceptos_cuentas.append(concepto_cuenta)

# Convertir la lista de cuentas a JSON
json_data = json.dumps(conceptos_cuentas, indent=4, ensure_ascii=False)


# Guardar el JSON en un archivo
with open('/home/desarrollo/Escritorio/concepto_cuenta.json', 'w', encoding='utf-8') as json_file:
    json_file.write(json_data)

print("El archivo JSON se ha creado exitosamente.")