import pandas as pd
import pandas_gbq
import requests
from datetime import datetime, timedelta
from io import StringIO

# Configurações
ID_PROJETO = 'monitor-passofundo'
NOME_DATASET = 'clima_dados'
LAT, LON = -28.2628, -52.4087 

# Definição de datas
hoje = datetime.now()
inicio = hoje - timedelta(days=90)
data_inicio = inicio.strftime('%Y-%m-%d')

df_clima = pd.DataFrame()
df_milho_final = pd.DataFrame()

# ---------------------------------------------------------
# 1. API DO CLIMA (OPEN-METEO)
# ---------------------------------------------------------
try:
    url_clima = f"https://api.open-meteo.com/v1/forecast?latitude={LAT}&longitude={LON}&past_days=92&daily=temperature_2m_max,precipitation_sum&timezone=America%2FSao_Paulo"
    
    res = requests.get(url_clima, timeout=30)
    res.raise_for_status()
    dados = res.json()
    
    if 'daily' in dados:
        df_clima = pd.DataFrame(dados['daily'])
        df_clima = df_clima.rename(columns={'time': 'data', 'temperature_2m_max': 'temp_max', 'precipitation_sum': 'chuva_mm'})
        df_clima['data'] = pd.to_datetime(df_clima['data'])
        df_clima['data_carga'] = datetime.now()
        print("Dados de CLIMA baixados com sucesso.")
    else:
        print("Erro: Campo 'daily' não encontrado no JSON do clima.")

except Exception as e:
    print(f"Erro ao baixar CLIMA: {e}")

# ---------------------------------------------------------
# 2. WEB SCRAPING DO MILHO (NOTICIAS AGRICOLAS)
# ---------------------------------------------------------
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
        # Verifica se alguma das colunas contem a palavra 'Data'
        if any('Data' in str(col) for col in tb.columns):
            df_milho = tb
            break
            
    if not df_milho.empty:
        # SOLUÇÃO BLINDADA: Pega apenas as duas primeiras colunas (Data e Preço) independente do nome
        df_milho = df_milho.iloc[:, [0, 1]].copy()
        df_milho.columns = ['data', 'preco_saca_reais'] # Força o nome correto
        
        df_milho['data'] = pd.to_datetime(df_milho['data'], format='%d/%m/%Y', errors='coerce')
        df_milho = df_milho.dropna(subset=['data'])
        
        mask = (df_milho['data'] >= pd.to_datetime(data_inicio))
        df_milho_filtrado = df_milho.loc[mask].copy()
        
        if not df_milho_filtrado.empty:
            df_milho_filtrado['data_carga'] = datetime.now()
            
            df_milho_final = df_milho_filtrado[['data', 'preco_saca_reais', 'data_carga']]
            print("Dados de MILHO baixados do Noticias Agricolas com sucesso.")
        else:
            print("Dados de milho vazios apos filtro de data.")
    else:
        print("Nao foi possivel localizar a tabela de precos no HTML.")

except Exception as e:
    print(f"Erro ao baixar MILHO: {e}")

# ---------------------------------------------------------
# 3. CARGA PARA O BIGQUERY
# ---------------------------------------------------------
try:
    if not df_clima.empty:
        pandas_gbq.to_gbq(df_clima, f"{NOME_DATASET}.historico_diario", project_id=ID_PROJETO, if_exists='replace')
        print("Sucesso: Tabela CLIMA atualizada no BigQuery.")
    
    if not df_milho_final.empty:
        pandas_gbq.to_gbq(df_milho_final, f"{NOME_DATASET}.precos_milho_cepea", project_id=ID_PROJETO, if_exists='append')
        print("Sucesso: Tabela MILHO atualizada no BigQuery.")

except Exception as e:
    print(f"Erro no Upload para o BigQuery: {e}")