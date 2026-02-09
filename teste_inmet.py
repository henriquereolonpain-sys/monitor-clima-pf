import pandas as pd
import requests
from datetime import datetime, timedelta

# --- 1. CONFIGURAÇÃO ---
# Coloque aqui o ID do seu projeto no Google Cloud
ID_PROJETO = 'monitor-passofundo' 
NOME_DATASET = 'clima_dados'
NOME_TABELA = 'historico_diario'

# Passo Fundo, RS
LAT = -28.2628
LON = -52.4087

# Datas (Janela móvel de 30 dias atrás até ontem)
hoje = datetime.now()
inicio = hoje - timedelta(days=30)

data_inicio = inicio.strftime('%Y-%m-%d')
data_fim = hoje.strftime('%Y-%m-%d')

print(f" Buscando dados de {data_inicio} até {data_fim}...")

# --- 2. EXTRAÇÃO (Extract) ---
url = f"https://archive-api.open-meteo.com/v1/archive?latitude={LAT}&longitude={LON}&start_date={data_inicio}&end_date={data_fim}&daily=temperature_2m_max,precipitation_sum&timezone=America%2FSao_Paulo"

try:
    response = requests.get(url)
    response.raise_for_status()
    dados = response.json()
    
    # --- 3. TRANSFORMAÇÃO (Transform) ---
    df = pd.DataFrame(dados['daily'])
    
    # Renomear colunas para português
    df = df.rename(columns={
        'time': 'data', 
        'temperature_2m_max': 'temp_max', 
        'precipitation_sum': 'chuva_mm'
    })

    # Converter coluna de data para o formato correto
    df['data'] = pd.to_datetime(df['data'])
    
    # Adicionar data de carga (para sabermos quando o robô rodou)
    df['data_carga'] = datetime.now()

    print(f"✅ Dados transformados! {len(df)} linhas prontas.")

    # --- 4. CARGA (Load) -> BigQuery ---
    tabela_completa = f"{NOME_DATASET}.{NOME_TABELA}"
    
    print(f" Enviando para o BigQuery: {tabela_completa}...")
    
    # if_exists='replace': Apaga e escreve tudo de novo (bom para corrigir dados passados)
    # if_exists='append': Só adiciona novos (bom para histórico eterno)
    # Vamos usar 'replace' por enquanto para garantir que os últimos 30 dias estejam sempre atualizados
    df.to_gbq(destination_table=tabela_completa, 
              project_id=ID_PROJETO, 
              if_exists='replace')
              
    print(" Sucesso! Dados estão na nuvem.")

except Exception as e:
    print(f" Erro fatal: {e}")