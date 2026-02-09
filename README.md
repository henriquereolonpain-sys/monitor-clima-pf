# Pipeline de Dados Meteorológicos - Passo Fundo/RS

Pipeline ETL que extrai dados diários da API Open-Meteo e carrega no Google BigQuery. O processo é totalmente automatizado via GitHub Actions.

## Visão Geral

O projeto monitora a temperatura máxima e precipitação diária. Executa um job diário para buscar os últimos 30 dias de dados (janela móvel), garantindo que o dataset na nuvem esteja sempre atualizado e consistente.

## Arquitetura

1. **Extração**: Script Python coleta dados brutos da API Open-Meteo.
2. **Transformação**: Limpeza e tipagem de dados utilizando Pandas.
3. **Carga**: Dados são enviados para o Google BigQuery (`clima_dados.historico_diario`).
4. **Orquestração**: GitHub Actions dispara o workflow diariamente às 07:00 (Horário de Brasília).

## Stack Tecnológico

- **Linguagem**: Python 3.9
- **Bibliotecas**: pandas, requests, pandas-gbq
- **Cloud**: Google BigQuery
- **CI/CD**: GitHub Actions


## Automação
O workflow é definido em .github/workflows/robo_coleta.yml. Utiliza uma secret do repositório (GOOGLE_APPLICATION_CREDENTIALS) para autenticação segura no Google Cloud Platform.