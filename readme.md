# Análise de Pedágios e MDF-e

Este é um aplicativo web interno, construído com Streamlit, para automatizar a análise e contestação de cobranças de pedágio do "Sem Parar".

O sistema cruza as passagens de pedágio com os registros de MDF-e (Manifesto Eletrônico de Documentos Fiscais) de dois sistemas (Softran e Atua) para identificar cobranças indevidas.

## Lógica de Negócio

A principal regra de contestação é:

> Se um veículo passou por um pedágio **sem um MDF-e ativo** (ou seja, estava rodando vazio) e foi cobrado por uma categoria de eixos **maior** do que a sua quantidade de eixos quando vazio, essa cobrança é considerada indevida e deve ser estornada.

## Funcionalidades

* **Upload de Planilha:** Aceita arquivos Excel (`.xls` e `.xlsx`) do "Sem Parar" (requer aba `PASSAGENS PEDÁGIO`).
* **Integração com Banco de Dados:** Conecta-se a um MS SQL Server para buscar dados de MDF-e (Softran) e dados de veículos (Atua).
* **Processamento Rápido:** Utiliza um banco de dados **DuckDB** em memória para realizar as verificações de MDF-e, evitando milhares de consultas ao servidor principal.
* **Análise de Contestação:** Filtra automaticamente as passagens que se enquadram na regra de negócio.
* **Visualização de Dados:** Exibe uma tabela com os resultados, um resumo financeiro e um gráfico de ocorrências por placa.
* **Exportação:** Permite o download dos dados filtrados em formato `.csv`.

## Requisitos Técnicos

* Python 3.9+
* Acesso ao MS SQL Server (com o **ODBC Driver 17 for SQL Server** instalado na máquina ou contêiner que executa a aplicação).
* Bibliotecas Python (veja `requirements.txt`).

## Instalação e Configuração

1.  **Clone o repositório:**
    ```bash
    git clone https://github.com/pauloDalponte/AnalisePedagios
    cd AnalisePedagios
    ```

2.  **Crie um ambiente virtual e ative-o:**
    ```bash
    python -m venv .venv
    # Windows
    .venv\Scripts\activate
    # macOS/Linux
    source .venv/bin/activate
    ```

3.  **Instale as dependências:**
    ```bash
    pip install -r requirements.txt
    ```

4.  **Configure as Credenciais:**
    * Crie uma pasta `.streamlit` na raiz do projeto.
    * Dentro dela, crie um arquivo chamado `secrets.toml`.
    * Adicione suas credenciais do banco de dados neste arquivo. **Nunca versione este arquivo!**

## Como Usar

1.  **Inicie a aplicação Streamlit:**
    ```bash
    streamlit run app.py
    ```

2.  **Acesse o Aplicativo:**
    * Abra o navegador no endereço fornecido (geralmente `http://localhost:8501`).

3.  **Use a Interface:**
    * Clique em "Browse files" e selecione a planilha "Sem Parar" que deseja analisar.
    * Aguarde o processamento (que é iniciado automaticamente).
    * Analise os dados na tabela e no gráfico.
    * Clique no botão "📥 Baixar CSV processado" para salvar os resultados.

## Estrutura do `requirements.txt`

```txt
streamlit
pandas
sqlalchemy
duckdb
pyodbc
openpyxl
xlrd
altair