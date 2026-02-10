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

try: #Open-Meteo coleta o clima
    print(" Coletando dados de clima...")
    url_clima = f"https://api.open-meteo.com/v1/forecast?latitude={LAT}&longitude={LON}&past_days=90&daily=temperature_2m_max,precipitation_sum&timezone=America%2FSao_Paulo"    res = requests.get(url_clima)
    df_clima = pd.DataFrame(res.json()['daily'])
    
    #trata dados clima
    df_clima = df_clima.rename(columns={'time': 'data', 'temperature_2m_max': 'temp_max', 'precipitation_sum': 'chuva_mm'})
    df_clima['data'] = pd.to_datetime(df_clima['data'])
    df_clima['data_carga'] = datetime.now()

# --- DADOS MILHU (FUSÃO: HISTÓRICO BCB + ATUAL AGROBR) ---
    print(" 🌽 Iniciando fusão: Histórico BCB + Atual AgroBR")
    
    # 1. Busca histórico de 90 dias no Banco Central (SGS 7778)
    # Aumentei o 'disfarce' pra o BCB não bloquear o GitHub
    url_bcb = f"https://api.bcb.gov.br/dados/serie/bcdata.sgs.7778/dados?formato=json&dataInicial={inicio.strftime('%d/%m/%Y')}&dataFinal={hoje.strftime('%d/%m/%Y')}"
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'application/json'
    }
    
    try:
        res_bcb = requests.get(url_bcb, headers=headers, timeout=60)
        res_bcb.raise_for_status() # Garante que se der erro ele pule pro except
        df_hist = pd.DataFrame(res_bcb.json())
        df_hist['data'] = pd.to_datetime(df_hist['data'], dayfirst=True)
        df_hist['valor'] = df_hist['valor'].str.replace(',', '.').astype(float)
        df_hist = df_hist.rename(columns={'valor': 'preco_saca_reais'})
        print(f" ✅ Boa! Peguei {len(df_hist)} dias no Banco Central.")
    except Exception as e:
        print(f" ⚠️ BCB falhou de novo ({e}). Usaremos só o que o AgroBR tiver.")
        df_hist = pd.DataFrame()

    # 2. Busca o dado mais "fresco" no AgroBR
    try:
        df_atual = cepea.indicador('milho')
        df_atual['data'] = pd.to_datetime(df_atual['data'])
        df_atual = df_atual.rename(columns={'valor': 'preco_saca_reais'})
        print(" ✅ Dados recentes do AgroBR capturados.")
    except:
        print(" ⚠️ Falha no AgroBR.")
        df_atual = pd.DataFrame()

    # 3. UNE TUDO (O drop_duplicates garante que não tenha data repetida)
    df_milho_final = pd.concat([df_hist, df_atual]).drop_duplicates(subset=['data'], keep='last')
    df_milho_final = df_milho_final[df_milho_final['data'] >= pd.to_datetime(data_inicio)]
    df_milho_final['data_carga'] = datetime.now()
    df_milho_final = df_milho_final[['data', 'preco_saca_reais', 'data_carga']].sort_values('data')
    #  CARREGA PARA O BIGQUERY
    print(" Enviando Clima para o Google")
    pandas_gbq.to_gbq(df_clima, f"{NOME_DATASET}.historico_diario", project_id=ID_PROJETO, if_exists='replace')
    print(" Tabela CLIMA atualizada!")

    if not df_milho_final.empty:
        print(f" Enviando {len(df_milho_final)} linhas de Milho para o Google")
        pandas_gbq.to_gbq(df_milho_final, f"{NOME_DATASET}.precos_milho_cepea", project_id=ID_PROJETO, if_exists='replace')
        print(" Tabela CEPEA (Milho) atualizada!")
    else:
        print(" Tabela Milho NÃO foi atualizada (sem dados novos).")

    print(" Pipeline concluído com sucesso!")

except Exception as e:
    print(f" Erro crítico: {e}")
# %%
