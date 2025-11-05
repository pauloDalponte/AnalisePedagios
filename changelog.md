# Changelog

## [1.1.0] - 2025-11-04

### Adicionado
- **Interface de UI Fluida:** Adicionado `st.session_state` para armazenar o resultado da análise, evitando reprocessamentos. A interface agora usa `st.tabs` ("Dados", "Gráficos", "Ações") para organizar o conteúdo.
- **Interface de Código Fluente:** Implementada a classe `PedagioProcessor` que utiliza *method chaining* (ex: `.load_excel().enrich_with_eixos()...`), tornando o pipeline de dados mais legível e modular.
- **Segurança:** Adicionado suporte a `st.secrets` (`.streamlit/secrets.toml`) para carregar credenciais de banco de dados, removendo-as do código-fonte.
- **Testes (Robustez):** Adicionado o arquivo `test_app.py` com testes unitários e de integração usando `pytest`.  
  Cobertura inclui:  
  - Lógica de cálculo (`map_and_calculate_valores`)  
  - Regras de filtro (`filter_contestacoes`)  
  - Consultas no DuckDB (`check_mdfe_status_duckdb`)
- **Novas Métricas (KPIs):** Adicionados cartões `st.metric` para exibir o total de “Contestações Sugeridas” e o “Valor Total a Estornar”.
- **Novo Gráfico:** Adicionada a visualização `plot_valor_por_dia` (gráfico de linha) para rastrear o valor de estorno ao longo do tempo.
- **Funcionalidade de Persistência:** Adicionado botão “Salvar no Banco de Dados” para persistir resultados diretamente na tabela SQL Server `[dbo].[contestacoes]`.
- **Gerenciamento de Dependências:** Criado o arquivo `requirements.txt` para facilitar a instalação do ambiente.

### Alterado
- **Performance (Crítico):** A verificação de MDF-e (`verificar_mdfe_SP_Atua` e `verificar_mdfe_SP`) foi completamente refatorada.  
  A antiga abordagem com `df.apply()` (milhares de consultas SQL linha a linha) foi substituída por uma consulta vetorializada no DuckDB (`EXISTS`), reduzindo o tempo de execução de minutos para segundos.
- **Manutenibilidade:** A função monolítica `processar_planilha_sem_parar` foi refatorada em funções menores, puras e testáveis (`load_excel_data`, `enrich_with_eixos`, `filter_contestacoes`), orquestradas pela classe `PedagioProcessor`.
- **Legibilidade:** Substituídos “números mágicos” (ex: `61`, `180`, `'PASSAGENS PEDÁGIO'`) por constantes nomeadas (ex: `MAPA_CATEGORIAS_SP`, `DIAS_CONSULTA_MDFE`).
- **Cache:** Adicionado `ttl=3600` (1 hora) às funções de cache (`load_mdfe_atua`, `busca_eixos`), permitindo atualização periódica sem reiniciar o app.

### Corrigido
- **Robustez:** O processador principal agora captura `KeyError`, informando o usuário via `st.error` se colunas essenciais (ex: `'PLACA'`, `'DATA'`) estiverem ausentes no arquivo Excel, evitando falhas na aplicação.

### Removido
- **Risco de Segurança:** Removidas credenciais de banco de dados *hardcoded* (usuário, senha, servidor).
- **Gargalo de Performance:** Removida a iteração `df.apply()` que causava lentidão extrema ao consultar o DuckDB linha por linha.

---

## [1.0.0] - 2025-10-21

### Adicionado
- **Versão Inicial do Projeto**

#### Conectividade
- Conexão com banco de dados **MS SQL Server** (`softran_bendo` e `atua_prod`) via `SQLAlchemy` e `pyodbc`.

#### Processamento de Dados
- Carga inicial de dados de **MDF-e** (Softran e Atua) dos últimos 180 dias.
- Uso de **DuckDB em memória** para aceleração de consultas.
- Leitura de planilhas **Sem Parar** (`.xls`, `.xlsx`), aba específica *PASSAGENS PEDÁGIO*.
- Busca de **quantidade de eixos vazios** dos veículos no banco de dados.

#### Lógica de Negócio
- Funções `verificar_mdfe_SP` e `verificar_mdfe_SP_Atua` para checar MDF-e ativo durante a passagem.
- Cálculo de **valores de estorno** com base na diferença entre eixos cobrados e eixos vazios (quando sem MDF-e).

#### Interface (Streamlit)
- Interface para **upload de arquivo**.
- Exibição dos **resultados** (pedágios a serem contestados) em `DataFrame`.
- Exibição de **resumo** (Total de registros, Valor total de estornos).
- **Gráfico de barras** (Altair) com contagem de ocorrências por placa.
- **Botão de download** dos resultados em formato `.csv`.

---
