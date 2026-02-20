
import pandas as pd
import pandas_gbq 
import requests
from io import StringIO
from datetime import datetime, timedelta

# configs
ID_PROJETO = 'monitor-passofundo'
NOME_DATASET = 'clima_dados'
LAT, LON = -28.2628, -52.4087 # Passo Fundo - RS

# 90 dias de histórico
hoje = datetime.now()
inicio = hoje - timedelta(days=90)
data_inicio = inicio.strftime('%Y-%m-%d')

df_clima = pd.DataFrame()
df_milho_final = pd.DataFrame()

#API OpenM
try:
    url_clima = f"https://api.open-meteo.com/v1/forecast?latitude={LAT}&longitude={LON}&past_days=92&daily=temperature_2m_max,precipitation_sum&timezone=America%2FSao_Paulo"
    
    res = requests.get(url_clima, timeout=30)
    res.raise_for_status() 
    
    df_clima = pd.DataFrame(res.json()['daily'])
    
    df_clima = df_clima.rename(columns={'time': 'data', 'temperature_2m_max': 'temp_max', 'precipitation_sum': 'chuva_mm'})
    df_clima['data'] = pd.to_datetime(df_clima['data'])
    df_clima['data_carga'] = datetime.now()

#web scraping do milho
try:
    url_na = "https://www.noticiasagricolas.com.br/cotacoes/milho/indicador-cepea-esalq-milho"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36"
    }
    
    res_milho = requests.get(url_na, headers=headers, timeout=30)
    res_milho.raise_for_status()
    
    tabelas = pd.read_html(StringIO(res_milho.text), decimal=',', thousands='.')

    df_milho = pd.DataFrame()
    for tb in tabelas:
        if 'Data' in tb.columns or 'Data ' in tb.columns:
            df_milho = tb
            break
            
    if not df_milho.empty:
        df_milho.columns = [col.strip() for col in df_milho.columns]
        df_milho = df_milho.rename(columns={'Data': 'data', 'Valor R$': 'valor', 'Preço R$': 'valor'})
        
        df_milho['data'] = pd.to_datetime(df_milho['data'], format='%d/%m/%Y', errors='coerce')
        df_milho = df_milho.dropna(subset=['data'])
        
        mask = (df_milho['data'] >= pd.to_datetime(data_inicio))
        df_milho_filtrado = df_milho.loc[mask].copy()
        
        if not df_milho_filtrado.empty:
            df_milho_filtrado = df_milho_filtrado.rename(columns={'valor': 'preco_saca_reais'})
            df_milho_filtrado['data_carga'] = datetime.now()
            
            df_milho_final = df_milho_filtrado[['data', 'preco_saca_reais', 'data_carga']]
            print("Dados de MILHO baixados do Noticias Agricolas com sucesso.")
        else:
            print("Dados de milho vazios apos filtro de data.")
    else:
        print("Nao foi possivel localizar a tabela de precos no HTML.")

except Exception as e:
    print(f"Erro ao baixar MILHO: {e}")

    # CARREGA PARA O BIGQUERY
try:
    if not df_clima.empty:
        pandas_gbq.to_gbq(df_clima, f"{NOME_DATASET}.historico_diario", project_id=ID_PROJETO, if_exists='replace')
        print("Sucesso: Tabela CLIMA atualizada no BigQuery.")
    
    if not df_milho_final.empty:
        pandas_gbq.to_gbq(df_milho_final, f"{NOME_DATASET}.precos_milho_cepea", project_id=ID_PROJETO, if_exists='replace')
        print("Sucesso: Tabela MILHO atualizada no BigQuery.")

except Exception as e:
    print(f"Erro no Upload para o BigQuery: {e}")