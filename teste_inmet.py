import pandas as pd
import requests
from datetime import datetime, timedelta



ID_PROJETO = 'monitor-passofundo' 
NOME_DATASET = 'clima_dados'
NOME_TABELA = 'historico_diario'

# PF-RS
LAT = -28.2628
LON = -52.4087

# Data = 30 dias atrás até hoje
hoje = datetime.now()
inicio = hoje - timedelta(days=30)

data_inicio = inicio.strftime('%Y-%m-%d')
data_fim = hoje.strftime('%Y-%m-%d')

print(f" Buscando dados de {data_inicio} até {data_fim}...")

# EXTRAIR
url = f"https://archive-api.open-meteo.com/v1/archive?latitude={LAT}&longitude={LON}&start_date={data_inicio}&end_date={data_fim}&daily=temperature_2m_max,precipitation_sum&timezone=America%2FSao_Paulo"

try:
    response = requests.get(url)
    response.raise_for_status()
    dados = response.json()
    
    
    df = pd.DataFrame(dados['daily'])
    
    
    df = df.rename(columns={
        'time': 'data', 
        'temperature_2m_max': 'temp_max', 
        'precipitation_sum': 'chuva_mm'
    })

    
    df['data'] = pd.to_datetime(df['data'])
    

    df['data_carga'] = datetime.now()

    print(f"✅ Dados transformados! {len(df)} linhas prontas.")

    tabela_completa = f"{NOME_DATASET}.{NOME_TABELA}"
    
    print(f" Enviando para o BigQuery: {tabela_completa}...")
    
    
    df.to_gbq(destination_table=tabela_completa, 
              project_id=ID_PROJETO, 
              if_exists='replace')
              
    print(" Sucesso! Dados estão na nuvem.")

except Exception as e:
    print(f" Erro fatal: {e}")