import requests
import pandas as pd
from datetime import datetime, timedelta

# --- CONFIGURAÇÃO DINÂMICA ---
# Pega a data de HOJE
hoje = datetime.now()
# Define o inicio para 30 dias atrás
inicio = hoje - timedelta(days=30)

# Formata para texto (YYYY-MM-DD) que a API exige
data_fim_str = hoje.strftime('%Y-%m-%d')
data_inicio_str = inicio.strftime('%Y-%m-%d')

# Coordenadas de Passo Fundo
LAT = -28.2628
LON = -52.4087

print(f"📡 Buscando dados de {data_inicio_str} até {data_fim_str}...")

url = f"https://archive-api.open-meteo.com/v1/archive?latitude={LAT}&longitude={LON}&start_date={data_inicio_str}&end_date={data_fim_str}&daily=temperature_2m_max,precipitation_sum&timezone=America%2FSao_Paulo"

try:
    response = requests.get(url, timeout=10)
    response.raise_for_status()
    dados = response.json()

    df = pd.DataFrame(dados['daily'])
    
    # Renomeando
    df = df.rename(columns={
        'time': 'Data', 
        'temperature_2m_max': 'Temp_Max', 
        'precipitation_sum': 'Chuva_mm'
    })

    # Adiciona uma coluna de "Data de Extração" para forçar mudança no arquivo
    df['Data_Extracao'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    print("✅ Dados recuperados!")
    
    # Salva substituindo o anterior
    df.to_csv('dados_passo_fundo.csv', index=False)
    print("📁 Arquivo atualizado.")

except Exception as e:
    print(f"❌ Erro: {e}")