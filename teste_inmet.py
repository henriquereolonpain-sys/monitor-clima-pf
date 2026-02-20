import pandas as pd
import pandas_gbq
import requests
from datetime import datetime, timedelta
from io import StringIO

# Config
ID_PROJETO = 'monitor-passofundo'
NOME_DATASET = 'clima_dados'
LAT, LON = -28.2628, -52.4087 

# Definição de datas
hoje = datetime.now()
inicio = hoje - timedelta(days=90)
data_inicio = inicio.strftime('%Y-%m-%d')

df_clima = pd.DataFrame()
df_milho_final = pd.DataFrame()


# 1. API DO CLIMA (OPEN-METEO)

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


# 2. WEB SCRAPING DO MILHO (NOTICIAS AGRICOLAS - CMA PASSO FUNDO)

try:
    url_na = "https://www.noticiasagricolas.com.br/cotacoes/milho/milho-cma"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    }
    
    res_milho = requests.get(url_na, headers=headers, timeout=30)
    res_milho.raise_for_status()
    
    tabelas = pd.read_html(StringIO(res_milho.text))
    
    df_milho = pd.DataFrame()
    if tabelas:
        df_milho_bruto = tabelas[0]
        
        linha_pf = df_milho_bruto[df_milho_bruto.iloc[:, 0].astype(str).str.contains('Passo Fundo', case=False, na=False)]
        
        if not linha_pf.empty:
            preco_raw = str(linha_pf.iloc[0, 1]).strip()
            preco_pf = float(preco_raw.replace(',', '.'))
            
            data_cotacao = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
            
            df_milho_final = pd.DataFrame({
                'data': [data_cotacao],
                'preco_saca_reais': [preco_pf],
                'data_carga': [datetime.now()]
            })
            
            print(f"Dados de MILHO (Passo Fundo - CMA) baixados com sucesso: R$ {preco_pf}")
        else:
            print("Praca 'Passo Fundo/RS' nao encontrada na tabela do CMA.")
    else:
        print("Nenhuma tabela encontrada na pagina do CMA.")

except Exception as e:
    print(f"Erro ao baixar MILHO CMA: {e}")

# 3. CARGA PARA O BIGQUERY

try:
    if not df_clima.empty:
        pandas_gbq.to_gbq(df_clima, f"{NOME_DATASET}.historico_diario", project_id=ID_PROJETO, if_exists='replace')
        print("Sucesso: Tabela CLIMA atualizada no BigQuery.")
    
    if not df_milho_final.empty:
        pandas_gbq.to_gbq(df_milho_final, f"{NOME_DATASET}.precos_milho_cepea", project_id=ID_PROJETO, if_exists='append')
        print("Sucesso: Tabela MILHO atualizada no BigQuery.")

except Exception as e:
    print(f"Erro no Upload para o BigQuery: {e}")