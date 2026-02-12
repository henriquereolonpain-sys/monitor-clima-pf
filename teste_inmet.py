
import pandas as pd
import pandas_gbq 
import requests
from agrobr.sync import cepea
from datetime import datetime, timedelta

# configs
ID_PROJETO = 'monitor-passofundo'
NOME_DATASET = 'clima_dados'
LAT, LON = -28.2628, -52.4087 # Passo Fundo - RS

# 90 dias pq 30 estava pouco
hoje = datetime.now()
inicio = hoje - timedelta(days=90)
data_inicio = inicio.strftime('%Y-%m-%d')
data_fim = hoje.strftime('%Y-%m-%d')

#API OpenM
try:
    url_clima = f"https://api.open-meteo.com/v1/forecast?latitude={LAT}&longitude={LON}&past_days=92&daily=temperature_2m_max,precipitation_sum&timezone=America%2FSao_Paulo"
    
    res = requests.get(url_clima, timeout=30)
    res.raise_for_status() 
    
    df_clima = pd.DataFrame(res.json()['daily'])
    
    
    df_clima = df_clima.rename(columns={'time': 'data', 'temperature_2m_max': 'temp_max', 'precipitation_sum': 'chuva_mm'})
    df_clima['data'] = pd.to_datetime(df_clima['data'])
    df_clima['data_carga'] = datetime.now()
    
    # CSV local BK
    df_clima.to_csv('clima_historico_90dias.csv', index=False)

#api do mihlo
    try:
        df_milho = cepea.indicador('milho')
        
        if not df_milho.empty:
            df_milho['data'] = pd.to_datetime(df_milho['data'])
            
            mask = (df_milho['data'] >= pd.to_datetime(data_inicio))
            df_milho_filtrado = df_milho.loc[mask].copy()
            
            df_milho_filtrado = df_milho_filtrado.rename(columns={'valor': 'preco_saca_reais'})
            df_milho_filtrado['data_carga'] = datetime.now()
            
            df_milho_final = df_milho_filtrado[['data', 'preco_saca_reais', 'data_carga']]
            
            df_milho_final.to_csv('milho_recentes.csv', index=False)
        else:
            df_milho_final = pd.DataFrame()
            
    except Exception as e:
        print(f"Erro{e}")
        df_milho_final = pd.DataFrame()

    # 3. CARREGA PARA O BIGQUERY
    pandas_gbq.to_gbq(df_clima, f"{NOME_DATASET}.historico_diario", project_id=ID_PROJETO, if_exists='replace')
    print("Tabela CLIMA atualizada")

    pandas_gbq.to_gbq(df_milho_final, f"{NOME_DATASET}.precos_milho_cepea", project_id=ID_PROJETO, if_exists='replace')
    print("Tabela MILH atualizada!")
    
    else:
    print("sem dados novos")
    
except Exception as e:
    print(f"Erro{e}")