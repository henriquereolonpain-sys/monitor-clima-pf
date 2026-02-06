#%%
import requests
import pandas as pd

# Coordenadas de Passo Fundo, RS
LAT = -28.2628
LON = -52.4087

# Vamos pegar os primeiros 10 dias de 2025
url = f"https://archive-api.open-meteo.com/v1/archive?latitude={LAT}&longitude={LON}&start_date=2025-01-01&end_date=2025-01-10&daily=temperature_2m_max,precipitation_sum&timezone=America%2FSao_Paulo"

print("📡 Conectando na Open-Meteo (Passo Fundo)...")

try:
    response = requests.get(url, timeout=10)
    response.raise_for_status()
    dados = response.json()

    # A estrutura do JSON deles é 'daily', então focamos ali
    df = pd.DataFrame(dados['daily'])
    
    # Renomeando para ficar bonito (PT-BR)
    df = df.rename(columns={
        'time': 'Data', 
        'temperature_2m_max': 'Temp_Max', 
        'precipitation_sum': 'Chuva_mm'
    })
# ... (seu código anterior fica igual)

    print("✅ Sucesso! Dados recuperados:")
    print(df.head())
    
    # SALVANDO EM CSV
    nome_arquivo = 'dados_passo_fundo.csv'
    df.to_csv(nome_arquivo, index=False)
    print(f"📁 Arquivo '{nome_arquivo}' salvo com sucesso na pasta!")

except Exception as e:
    print(f"❌ Erro: {e}")