import pandas as pd
from decouple import config
from b2sdk.v2 import InMemoryAccountInfo, B2Api


def main():
    archivo_excel = pd.read_excel('/home/desarrollo/Escritorio/registros.xlsx')
    info = InMemoryAccountInfo()
    b2_api = B2Api(info)
    b2_api.authorize_account("production", config('B2_APPLICATION_KEY_ID'), config('B2_APPLICATION_KEY'))
    bucket = b2_api.get_bucket_by_name('semantica')
    for index, fila in archivo_excel.iterrows():        
        codigo_fichero = fila.iloc[0]
        file_name = f"energy/fichero/{codigo_fichero}.tif"
        try:                
            file_version = bucket.get_file_info_by_name(file_name)
            file_id = file_version.id_
            bucket.delete_file_version(file_id=file_id, file_name=file_name)
            print(f"Se eliminó el fichero {codigo_fichero}")
        except Exception as e:
            print(f"Error al procesar el fichero {codigo_fichero}: {str(e)}")      

if __name__ == "__main__":
    main()            