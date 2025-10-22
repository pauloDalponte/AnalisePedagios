# Changelog

Todos as mudanças notáveis neste projeto serão documentadas neste arquivo.

O formato é baseado em [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
e este projeto adere ao [Versionamento Semântico](https://semver.org/spec/v2.0.0.html).

## [1.0.0] - 2025-10-21

### Adicionado

* **Versão Inicial do Projeto**
* **Conectividade:**
    * Conexão com banco de dados MS SQL Server (`softran_bendo` e `atua_prod`) via SQLAlchemy e pyodbc.
* **Processamento de Dados:**
    * Carga inicial de dados de MDF-e (Softran e Atua) dos últimos 180 dias.
    * Uso de DuckDB em memória para aceleração de consultas de verificação.
    * Leitura de planilhas "Sem Parar" (formatos `.xls` e `.xlsx`), lendo a aba específica `PASSAGENS PEDÁGIO`.
    * Busca de quantidade de eixos vazios dos veículos no banco de dados.
* **Lógica de Negócio:**
    * Funções `verificar_mdfe_SP` e `verificar_mdfe_SP_Atua` para checar se havia um MDF-e ativo no momento da passagem do pedágio.
    * Cálculo de valores de estorno baseado na diferença entre eixos cobrados e eixos vazios (quando sem MDF-e).
* **Interface (Streamlit):**
    * Interface para upload de arquivo.
    * Exibição dos resultados (pedágios a serem contestados) em um DataFrame.
    * Exibição de resumo (Total de registros, Valor total de estornos).
    * Gráfico de barras (Altair) com contagem de ocorrências por placa.
    * Botão de download dos resultados em formato `.csv`.