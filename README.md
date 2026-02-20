# Monitoramento de Preços de Milho e Variáveis Climáticas - Passo Fundo/RS

Este projeto automatiza a coleta, processamento e visualização de dados de preços de Milho (Praça Passo Fundo/RS - CMA) correlacionados com variáveis meteorológicas da região de Passo Fundo. A estrutura utiliza um pipeline de dados em nuvem para sustentar um dashboard de análise econômica, usando webscraping, API e google cloud.

---

## Arquitetura do Sistema

O projeto utiliza uma abordagem de armazenamento em camadas para garantir a resiliência dos dados:

1. **Coleta (Python):** Scripts executados via GitHub Actions extraem dados diariamente às 7 da manhã e mandam pra query.
2. **Armazenamento (BigQuery):** Data Warehouse centralizando dados históricos (sql) e dados em tempo real (API) vindos das _actions_.
3. **Processamento (SQL):** Views otimizadas realizam o tratamento de tipos de dados e a unificação das séries temporais, dentro do próprio BigQuery.
4. **Visualização (Looker Studio):** Dashboard interativo para análise de correlação e tendência, atualizado automaticamente pelo SQL do BigQuery no Cloud.

![PIPE](dashboard_milho.png/pipeline.png)

---

## Fontes de Dados

* **Precos do Milho:** Web Scraping customizado via BeautifulSoup e Pandas extraindo cotações do mercado físico (CMA) diretamente do Notícias Agrícolas.
* **Dados Climáticos:** Open-Meteo API (Forecast e Archive) para captura de precipitação e temperatura máxima a partir da última data estática.
* **Histórico:** Base de dados estática importada manualmente da notícias agrícolas no cloud (BigQuery) para garantir a continuidade da série desde 2025.

---

## Estrutura de Automação

A automação é gerenciada via GitHub Actions. O workflow garante que o banco de dados e os backups em CSV sejam atualizados sem intervenção manual.

```yaml
name: Atualizacao Diaria
on:
  schedule:
    - cron: '0 7 * * *'
permissions:
  contents: write
jobs:
  run-etl:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - name: Setup Python
        uses: actions/setup-python@v4
      - name: Execute
        run: |
          pip install pandas pandas-gbq
          python examples/teste_inmet.py
```
---

## Tratamento de Dados no BigQuery

Foi implementada uma View SQL para resolver conflitos de tipos de dados e garantir a integridade do JOIN entre as tabelas de clima e mercado.

```sql
CREATE OR REPLACE VIEW `monitor-passofundo.clima_dados.visao_completa_clima_milho` AS
SELECT 
    c.data,
    c.precipitacao as chuva_mm,
    c.temp_max,
    m.preco_saca_reais as preco_milho
FROM `monitor-passofundo.clima_dados.historico_diario` AS c
LEFT JOIN `monitor-passofundo.clima_dados.precos_milho_cepea` AS m
    ON CAST(c.data AS DATE) = CAST(m.data AS DATE)
ORDER BY c.data DESC
```
---
# Gráfico no LOOKER

Esse gráfico atualiza automaticamente todo dia depois da automação nas >actions< ser feita.

## Visualização do Projeto

![Dashboard de Monitoramento](dados_looker_webscrapi.pgn.png)

---

## Análise Econômica e Insights

A observação preliminar da série histórica indica uma correlação visual entre os regimes de precipitação em Passo Fundo/RS e a volatilidade dos preços do milho (Indicador - Milho Praça Passo Fundo/RS - CMA)

* **Comportamento de Curto Prazo:** É possível notar aumentos residuais nas cotações logo após períodos de chuva intensa, o que pode sugerir ajustes de oferta ou dificuldades logísticas momentâneas na região.
* **Proximos Passos Analiticos:** O projeto evoluirá para a aplicação de modelos econométricos de covariância e regressão linear. O objetivo é quantificar o impacto elástico das variáveis climáticas sobre a formação do preço local, isolando efeitos sazonais.

---

## Problemas enfrentados 
A principal barreira técnica deste projeto foi a escassez de APIs gratuitas que fornecessem séries históricas longas para o mercado físico de milho no Brasil (Ticker)

* Limitação da API: A solução encontrada foi fazer webscraping dos dados do site notícias agrícolas

* Estratégia de Mitigação: Para evitar uma análise superficial limitada a um curto período de tempo, foi adotada uma arquitetura híbrida. Realizou-se a extração manual de dados históricos diretamente do notícias agrícolas, que foram tratados e importados como uma base estática no BigQuery.

* Resultado: Através de uma operação de UNION via SQL, foi possível consolidar o histórico legado com a automação presente, garantindo uma série temporal robusta para a aplicação de modelos econométricos.
---

## Como Instalar e Executar

* Pré-requisitos

* Python 3.9+

* Google Cloud Project com API do BigQuery ativa.

* Service Account com permissao de "Editor de Dados do BigQuery".

* GitHub Repository para configuracao de Actions.


## Configuracao Local

1. **Clonar o repositório:**
```bash
   git clone [https://github.com/henriquereolonpain-sys/monitor-clima-pf.git](https://github.com/henriquereolonpain-sys/monitor-clima-pf.git)
   cd monitor-clima-pf
```

2. **Instalar Dependências:**
```Bash
    pip install -r requirements.txt
```

3. **Configurar credenciais:**
Salve o JSON da Service Account como google_credentials.json na raiz do projeto.

## Configuração do GitHub Actions
Cadastre o segredo no GitHub (Settings > Secrets > Actions):
Nome do Secret--> GOOGLE_CREDENTIALS    
Descrição -->   Conteúdo completo do arquivo JSON da Service Account.

## Estrutura do Repositório
1. .github/workflows/: Configuração da rotina de execução diária.

2. examples/teste_inmet.py: Script principal de ETL.

3. requirements.txt: Lista de bibliotecas necessárias.

4. *.csv: Arquivos de backup gerados automaticamente pelo pipeline.

---
Obrigado por ler até aqui, esse projeto totalizou 45-50 horas e me senti muito feliz quando vi que deu certo!! 🐻