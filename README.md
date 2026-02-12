# Monitoramento de Preços de Milho e Variáveis Climáticas - Passo Fundo/RS

Este projeto automatiza a coleta, processamento e visualização de dados de preços de milho (CEPEA) correlacionados com variáveis meteorológicas da região de Passo Fundo. A estrutura utiliza um pipeline de dados em nuvem para sustentar um dashboard de análise econômica.

---

## Arquitetura do Sistema

O projeto utiliza uma abordagem de armazenamento em camadas para garantir a resiliência dos dados:

1. **Coleta (Python):** Scripts executados via GitHub Actions extraem dados diariamente às 7 da manhã e mandam pra query.
2. **Armazenamento (BigQuery):** Data Warehouse centralizando dados históricos (CSV) e dados em tempo real (API) vindos das >actions<.
3. **Processamento (SQL):** Views otimizadas realizam o tratamento de tipos de dados e a unificação das séries temporais, dentro do próprio BigQuery.
4. **Visualização (Looker Studio):** Dashboard interativo para análise de correlação e tendência, atualizado automaticamente pelo SQL do BigQuery no Cloud.

![PIPE](dashboard_milho.png/pipeline.png)

---

## Fontes de Dados

* **Precos do Milho:** Indicador CEPEA/ESALQ via biblioteca AgroBR.
* **Dados Climáticos:** Open-Meteo API (Forecast e Archive) para captura de precipitação e temperatura máxima a partir da última data estática.
* **Histórico:** Base de dados estática importada manualmente da CEPEA no cloud (BigQuery) para garantir a continuidade da série desde 2025.

---

## Estrutura de Automação

A automação é gerenciada via GitHub Actions. O workflow garante que o banco de dados e os backups em CSV sejam atualizados sem intervenção manual.

```yaml
name: Atualizacao Diaria
on:
  schedule:
    - cron: '0 9 * * *'
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
          pip install pandas pandas-gbq requests agrobr pyarrow
          python examples/teste_inmet.py
```
---

## Tratamento de Dados no BigQuery

Foi implementada uma View SQL para resolver conflitos de tipos de dados e garantir a integridade do JOIN entre as tabelas de clima e mercado.

```yaml
        CREATE OR REPLACE VIEW `monitor-passofundo.clima_dados.visao_analitica_milho` AS
    WITH milho_unificado AS (
        SELECT CAST(data AS DATE) AS data, `a vista` AS preco FROM `milho_historico_estatico`
        UNION DISTINCT
        SELECT CAST(data AS DATE) AS data, preco_saca_reais AS preco FROM `precos_milho_cepea`
    )
    SELECT 
        CAST(c.data AS DATE) as data,
        c.chuva_mm,
        c.temp_max,
        m.preco AS preco_saca_reais
    FROM `historico_diario` c
    LEFT JOIN milho_unificado m ON CAST(c.data AS DATE) = m.data
    ORDER BY data ASC
```
---
# Gráfico no LOOKER

Esse gráfico atualiza automaticamente todo dia depois da automação nas >actions< ser feita.

## Visualização do Projeto

![Dashboard de Monitoramento](dashboard_milho.png/dados_looker_milho.png)

---

## Análise Econômica e Insights

A observação preliminar da série histórica indica uma correlação visual entre os regimes de precipitação em Passo Fundo/RS e a volatilidade dos preços do milho (Indicador CEPEA).

* **Comportamento de Curto Prazo:** É possível notar aumentos residuais nas cotações logo após períodos de chuva intensa, o que pode sugerir ajustes de oferta ou dificuldades logísticas momentâneas na região.
* **Proximos Passos Analiticos:** O projeto evoluirá para a aplicação de modelos econométricos de covariância e regressão linear. O objetivo é quantificar o impacto elástico das variáveis climáticas sobre a formação do preço local, isolando efeitos sazonais.

---

## Problemas enfrentados 
A principal barreira técnica deste projeto foi a escassez de APIs gratuitas que fornecessem séries históricas longas para o mercado físico de milho no Brasil (Ticker).

* Limitação da API: A solução encontrada via biblioteca AgroBR permitiu a captura automatizada de dados apenas a partir de 27 de janeiro de 2026.

* Estratégia de Mitigação: Para evitar uma análise superficial limitada a um curto período de tempo, foi adotada uma arquitetura híbrida. Realizou-se a extração manual de dados históricos diretamente do CEPEA, que foram tratados e importados como uma base estática no BigQuery.

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