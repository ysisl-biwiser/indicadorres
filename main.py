import requests
import pandas as pd
import sqlalchemy
from datetime import datetime

def uf(engine):
    print("Realizando consulta: uf")
    tipo_indicador = "uf"


    list_data = []
  
 
    url = f"https://mindicador.cl/api/{tipo_indicador}"
    payload = {}
    headers = {}
    response = requests.request("GET", url, headers=headers, data=payload)
        # print(response.text)

    for item in response.json()['serie']:
        print(item)
        uf_dict = {}
        uf_dict['valor_uf'] = item['valor']
        # Convertir la cadena de fecha y hora a objeto datetime
        fecha_hora = datetime.strptime(item['fecha'], '%Y-%m-%dT%H:%M:%S.%fZ')
        
        # Obtener solo la fecha
        uf_dict['fecha'] = fecha_hora.date()
        list_data.append(uf_dict)

        # print(list_data)
    df_uf = pd.DataFrame(list_data)
    df_uf.to_sql('api_indicadores_uf', engine,  schema='public', if_exists='replace'
                 , index=False, chunksize=10000)
    print("Fin carga: uf")


if __name__ == '__main__':
    engine = sqlalchemy.create_engine('postgresql+psycopg2://postgres:Biwiser2024@localhost:5432/indicadores')
 
    uf(engine)
