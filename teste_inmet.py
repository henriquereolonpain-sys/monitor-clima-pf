#%%
import pandas as pd
import pandas_gbq 
import requests
from agrobr.sync import cepea
from datetime import datetime, timedelta


ID_PROJETO = 'monitor-passofundo'
NOME_DATASET = 'clima_dados'


LAT, LON = -28.2628, -52.4087 # PF

# Data 90 dias :30 estava pouco :D
hoje = datetime.now()
inicio = hoje - timedelta(days=90)
data_inicio = inicio.strftime('%Y-%m-%d')
data_fim = hoje.strftime('%Y-%m-%d')

print(f" Iniciando pipeline: {data_inicio} até {data_fim}")

try: #Open-Meteo ccoleta o clima
    
    print(" Coletando dados de clima...")
    url_clima = f"https://archive-api.open-meteo.com/v1/archive?latitude={LAT}&longitude={LON}&start_date={data_inicio}&end_date={data_fim}&daily=temperature_2m_max,precipitation_sum&timezone=America%2FSao_Paulo"
    res = requests.get(url_clima)
    df_clima = pd.DataFrame(res.json()['daily'])
    
  #trata dados clima
    df_clima = df_clima.rename(columns={'time': 'data', 'temperature_2m_max': 'temp_max', 'precipitation_sum': 'chuva_mm'})
    df_clima['data'] = pd.to_datetime(df_clima['data'])
    df_clima['data_carga'] = datetime.now()

    #dados milhu 
    print(" Buscando dados do CEPEA (Milho/Brasil)...")
    df_milho = cepea.indicador('milho')
    
    if not df_milho.empty:
        df_milho['data'] = pd.to_datetime(df_milho['data'])
        mask = (df_milho['data'] >= pd.to_datetime(data_inicio))
        df_milho_filtrado = df_milho.loc[mask].copy()
        
        df_milho_filtrado = df_milho_filtrado.rename(columns={'valor': 'preco_saca_reais'})
        df_milho_filtrado['data_carga'] = datetime.now()
        df_milho_final = df_milho_filtrado[['data', 'preco_saca_reais', 'data_carga']]
    else:
        print(" CEPEA não retornou dados.")
        df_milho_final = pd.DataFrame()

    # CARGA PARA O BIGQUERY - GOOGLE CLOUD 
    print(" Enviando Clima para o Google")
    pandas_gbq.to_gbq(df_clima, f"{NOME_DATASET}.historico_diario", project_id=ID_PROJETO, if_exists='replace')
    print(" Tabela CLIMA atualizada!")

    
    if not df_milho_final.empty:
        print(" Enviando Milho para o Google")
        pandas_gbq.to_gbq(df_milho_final, f"{NOME_DATASET}.precos_milho_cepea", project_id=ID_PROJETO, if_exists='replace')
        print(" Tabela CEPEA (Milho) atualizada!")
    else:
        print(" Tabela Milho NÃO foi atualizada (sem dados novos).")

    print(" Pipeline concluído com sucesso!")

except Exception as e:
    print(f" Erro crítico: {e}")