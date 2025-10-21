import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text
import duckdb
import io
import altair as alt


st.set_page_config(layout="wide")
st.title("🚧 Testando a Conexão...")

# --- Configurações de Conexão (Variáveis de Ambiente Recomendadas em Produção) ---
# Usando as mesmas configurações do código original
server = '192.168.0.210'
database = 'softran_bendo'
username = 'pedagio'
password = 'pedagioBendo'

connection_string = f'mssql+pyodbc://{username}:{password}@{server}/{database}?driver=ODBC+Driver+17+for+SQL+Server'

# --- Inicialização do Engine e Conexão ao DuckDB (Cache/Sessão) ---

@st.cache_resource
def get_sql_engine():
    """Cria e armazena em cache o engine de conexão SQL Server."""
    try:
        engine = create_engine(connection_string)
        with engine.connect() as connection:
            st.success("Conexão ao SQL Server bem-sucedida!")
        return engine
    except Exception as e:
        st.error(f"Ocorreu um erro ao tentar conectar ao SQL Server: {e}")
        return None

engine = get_sql_engine()

# Inicializa o DuckDB in-memory para ser usado durante a sessão
@st.cache_resource
def get_duckdb_conn():
    """Cria e armazena em cache a conexão DuckDB in-memory."""
    return duckdb.connect(':memory:')

duckdb_conn = get_duckdb_conn()

# --- Funções de Consulta e Carga de Dados (Cache) ---

@st.cache_data(show_spinner="Consultando MDF-e emitidos (Atua)...")
def load_mdfe_atua(_engine):  # <-- Corrigido aqui: _engine
    """Carrega dados de MDF-e emitidos do SQL Server."""
    if _engine is None:  # <-- Corrigido aqui: _engine
        return pd.DataFrame()
    queryMdfeAtua = """
        SELECT man.datahora, man.updated_at, vei.placa
        FROM atua_prod.dbo.manifesto man
        INNER JOIN atua_prod.dbo.veiculos vei ON vei.Idatua = man.idVeiculo
        where man.datahora >= DATEADD(DAY, -180, GETDATE())
        ORDER BY man.datahora
    """
    try:
        # Use o parâmetro corrigido: _engine
        mdfeEmitidosAtua = pd.read_sql(queryMdfeAtua, _engine) 
        return mdfeEmitidosAtua
    except Exception as e:
        st.error(f"Erro ao carregar MDF-e Atua: {e}")
        return pd.DataFrame()

# Carrega os dados na inicialização/primeira execução
mdfeEmitidosAtua = load_mdfe_atua(engine)

# Carrega os dados de MDF-e para o DuckDB
if not mdfeEmitidosAtua.empty:
    try:
        duckdb_conn.execute("DROP TABLE IF EXISTS manifestos") # Garante que está limpo
        # Define os tipos corretos para as colunas do DuckDB
        duckdb_conn.execute("""
            CREATE TABLE IF NOT EXISTS manifestos 
            (dataHora TIMESTAMP, updated_at TIMESTAMP, placa varchar(10))
        """)
        duckdb_conn.execute("INSERT INTO manifestos SELECT * FROM mdfeEmitidosAtua")
    except Exception as e:
        st.error(f"Erro ao carregar dados no DuckDB: {e}")

# --- Funções de Lógica de Negócio ---

def verificar_mdfe_SP_Atua(row, duckdb_conn):
    """Verifica MDF-e aberto na base Atua para uma passagem de pedágio."""
    placa = row['PLACA']
    data_passagem = row['Data Passagem'].strftime('%Y-%m-%d %H:%M:%S') # Formato compatível com SQL/DuckDB

    query_mdfe = f"""
        SELECT mdfe.placa
        FROM manifestos mdfe
        WHERE mdfe.placa = '{placa}'
        AND mdfe.datahora < TIMESTAMP '{data_passagem}'
        AND (mdfe.updated_at IS NULL OR
        mdfe.updated_at > TIMESTAMP '{data_passagem}')
    """
    mdfe_aberto = duckdb_conn.execute(query_mdfe).df()
    return len(mdfe_aberto) > 0

@st.cache_data(show_spinner="Buscando eixos das placas...")
def busca_eixos(_lista_placas, _engine):  # <-- CORRIGIDO AQUI
    """Busca o número de eixos vazios para as placas no SQL Server."""
    if _engine is None:  # <-- Use a variável corrigida
        return {}
    placas_str = ','.join([f"'{placa}'" for placa in _lista_placas])

    query_eixos = f"""
        select placa,nreixos,nrEixosVazio,tipoApelido
        from atua_prod.dbo.veiculos V
        WHERE V.placa in ({placas_str})
    """
    try:
        resultados = pd.read_sql(query_eixos, _engine)  # <-- Use a variável corrigida
        dict_placas_eixos = dict(zip(resultados['placa'], resultados['nrEixosVazio']))
        return dict_placas_eixos
    except Exception as e:
        st.error(f"Erro no pd.read_sql (busca_eixos): {e}")
        return {}

def obter_quantidade_eixos_SP(row, dict_placas_eixos):
    """Obtém a quantidade de eixos vazios do dicionário."""
    placa = row['PLACA']
    return dict_placas_eixos.get(placa, 0)

def plot_contestacoes_por_placa(df):
    """Gera um gráfico de barras da contagem de contestações por PLACA."""
    if "PLACA" in df.columns and not df.empty:
        # Garante que as placas são strings e não nulas para a contagem
        df_plot = df[df["PLACA"].notna()].copy()

        chart = alt.Chart(df_plot).mark_bar().encode(
            # X: Placa (Nominal/string)
            x=alt.X('PLACA:N', title='Placa'),
            # Y: Contagem de ocorrências
            y=alt.Y('count()', title='Quantidade de Contestações'),
            # Cor: pela placa para diferenciar as barras (opcional)
            color='PLACA:N', 
            # Tooltip para exibir os dados ao passar o mouse
            tooltip=['PLACA', alt.Tooltip('count()', title='Quantidade')]
        ).properties(
            title='Contestações Sugeridas por Placa'
        ).interactive() # Permite zoom e pan

        st.header("4. Visualização: Contestações por Placa")
        st.altair_chart(chart, use_container_width=True)
        
import pandas as pd
import streamlit as st
import altair as alt

def plot_valor_por_dia(df, chart_type='bar'):
    """
    Função de depuração para gerar o gráfico de valor diário.
    """
    st.subheader("--- INÍCIO DA ANÁLISE DO GRÁFICO ---")

    if df.empty:
        st.error("ERRO INICIAL: A função de plotagem recebeu um DataFrame vazio. O problema está na filtragem principal, antes do gráfico.")
        return

    st.write(f"1. A função recebeu um DataFrame com {len(df)} linhas.")
    
    if "DATA" not in df.columns or "Valor Estorno" not in df.columns:
        st.error("ERRO INICIAL: As colunas 'DATA' ou 'Valor Estorno' não foram encontradas no DataFrame.")
        return

    st.write("2. Amostra dos dados recebidos (colunas DATA e Valor Estorno):")
    st.dataframe(df[['DATA', 'Valor Estorno']].head())
    st.write("Tipos de dados originais:", df[['DATA', 'Valor Estorno']].dtypes.to_dict())

    # --- Início do Processamento ---
    df_plot = df.copy()

    # ETAPA DE CONVERSÃO DE VALOR
    st.write("3. Convertendo a coluna 'Valor Estorno' para número...")
    # Primeiro, verifica se é um objeto/string. Se for, limpa e converte.
    if df_plot['Valor Estorno'].dtype == 'object':
        df_plot['Valor Estorno'] = df_plot['Valor Estorno'].str.replace(',', '.', regex=False)
    
    df_plot['Valor Estorno'] = pd.to_numeric(df_plot['Valor Estorno'], errors='coerce')
    nulos_valor = df_plot['Valor Estorno'].isnull().sum()
    st.write(f"-> Após converter 'Valor Estorno', existem {nulos_valor} valores nulos/inválidos.")

    # ETAPA DE CONVERSÃO DE DATA
    st.write("4. Convertendo a coluna 'DATA' para data...")
    df_plot['DATA'] = pd.to_datetime(df_plot['DATA'], format='%d/%m/%Y', errors='coerce')
    nulos_data = df_plot['DATA'].isnull().sum()
    st.write(f"-> Após converter 'DATA', existem {nulos_data} datas nulas/inválidas.")

    # ETAPA DE LIMPEZA
    st.write("5. Removendo todas as linhas que tiveram erro na conversão...")
    df_plot.dropna(subset=['DATA', 'Valor Estorno'], inplace=True)
    st.write(f"-> **RESULTADO: Restaram {len(df_plot)} linhas no DataFrame após a limpeza.**")

    if df_plot.empty:
        st.error("PROBLEMA ENCONTRADO: Nenhuma linha sobrou após a limpeza. Verifique o formato exato das colunas 'DATA' (precisa ser 'dd/mm/aaaa') e 'Valor Estorno' (precisa ser um número com ponto ou vírgula decimal) no seu arquivo original.")
        return

    # ETAPA DE AGRUPAMENTO
    st.write("6. Agrupando os dados por dia e somando os valores...")
    df_agrupado = df_plot.groupby(df_plot['DATA'].dt.date).agg(
        Valor_Total_Estorno=('Valor Estorno', 'sum')
    ).reset_index()
    
    st.write("7. DataFrame final que será desenhado:")
    st.dataframe(df_agrupado)
    st.subheader("--- FIM DA ANÁLISE ---")

    # ETAPA FINAL: DESENHAR O GRÁFICO
    base = alt.Chart(df_agrupado).encode(
        x=alt.X('DATA:T', title='Data da Passagem'),
        y=alt.Y('Valor_Total_Estorno:Q', title='Valor Total de Estorno (R$)'),
        tooltip=['DATA:T', 'Valor_Total_Estorno:Q']
    )
    
    if chart_type == 'line':
        chart = base.mark_line(point=True)
    else:
        chart = base.mark_bar()
        
    st.header("5. Visualização: Valor de Estorno Diário")
    st.altair_chart(chart, use_container_width=True)

def salvar_no_banco(df, engine):
    """Salva o DataFrame de contestações sugeridas na tabela 'contestacoes' do SQL Server."""

    df_save = df.drop(columns=['mdfe_aberto', 'mdfe_aberto_Atua'], errors='ignore').copy()

    try:
        # Tabela e Esquema de Destino
        table_name = 'contestacoes'
        schema_name = 'dbo' # Assumindo o esquema padrão 'dbo'

        # Inserção no Banco de Dados
        rows_inserted = df_save.to_sql(
            name=table_name,
            con=engine,
            schema=schema_name,
            if_exists='append',
            index=False

        )
        return True, f"Sucesso: {rows_inserted} linhas de contestações salvas na tabela {schema_name}.{table_name}."

    except Exception as e:
        return False, f"Erro ao salvar no SQL Server: {e}"

# --- Função Principal de Processamento ---

def processar_planilha_sem_parar(uploaded_file, engine, duckdb_conn):
    """Lê, processa e filtra o arquivo de pedágios Sem Parar."""
    try:
        # Lê o arquivo XLS ou XLSX
        pedagios_df = pd.read_excel(uploaded_file, sheet_name='PASSAGENS PEDÁGIO', engine='xlrd')

        # Passos de processamento
        pedagios_df = pedagios_df.drop(columns=['PREFIXO'])

        pedagios_df['Data Passagem'] = pedagios_df['DATA'] + ' ' + pedagios_df['HORA']
        # Usando 'dayfirst=True' para lidar com o formato 'DD/MM/YYYY HH:MM:SS'
        pedagios_df['Data Passagem'] = pd.to_datetime(pedagios_df['Data Passagem'], format='%d/%m/%Y %H:%M:%S', errors='coerce')
        pedagios_df.dropna(subset=['Data Passagem'], inplace=True)

        lista_placas = list(pedagios_df['PLACA'].unique())
        dict_placas_eixos = busca_eixos(lista_placas, engine)

        # Quantidade de eixos
        pedagios_df['Quantidade Eixos Vazio'] = pedagios_df.apply(
            lambda row: obter_quantidade_eixos_SP(row, dict_placas_eixos), axis=1
        )
        pedagios_df['Quantidade Eixos Vazio'] = pd.to_numeric(pedagios_df['Quantidade Eixos Vazio'], errors='coerce')
        pedagios_df = pedagios_df.dropna(subset=['Quantidade Eixos Vazio'])
        pedagios_df['Quantidade Eixos Vazio'] = pedagios_df['Quantidade Eixos Vazio'].astype(int)

        st.info("Verificando MDF-e... Isso pode levar alguns minutos.")

        # O uso de apply com consulta SQL ou DuckDB em Streamlit sem cache pode ser lento.
        # Poderíamos otimizar usando JOINs no DuckDB/SQL, mas mantendo a lógica original:

        # 1. MDF-e Atua (DuckDB)
        pedagios_df['mdfe_aberto_Atua'] = pedagios_df.apply(
            lambda row: verificar_mdfe_SP_Atua(row, duckdb_conn), axis=1
        )


        # Conversões e cálculos
        pedagios_df['VALOR'] = pedagios_df['VALOR'].astype(str).str.replace(',', '.', regex=False)
        pedagios_df['VALOR'] = pd.to_numeric(pedagios_df['VALOR'], errors='coerce')
        pedagios_df['CATEG'] = pd.to_numeric(pedagios_df['CATEG'], errors='coerce')
        pedagios_df.dropna(subset=['VALOR', 'CATEG'], inplace=True)

        # Conversão de categorias
        pedagios_df.loc[pedagios_df['CATEG'] == 61, 'CATEG'] = 7
        pedagios_df.loc[pedagios_df['CATEG'] == 62, 'CATEG'] = 8
        pedagios_df.loc[pedagios_df['CATEG'] == 63, 'CATEG'] = 9
        pedagios_df.loc[pedagios_df['CATEG'] == 64, 'CATEG'] = 10

        # Cálculos financeiros
        pedagios_df['Valor por eixo'] = pedagios_df['VALOR'] / pedagios_df['CATEG']
        pedagios_df['Valor Correto'] = (pedagios_df['Quantidade Eixos Vazio'] * pedagios_df['Valor por eixo']).round(2)
        pedagios_df['Valor Estorno'] = (pedagios_df['VALOR'] - pedagios_df['Valor Correto']).round(2)
        pedagios_df['Fatura'] = ''

        # Filtragem para contestações
        contestacao_df = pedagios_df[
            (pedagios_df['mdfe_aberto_Atua'] == False) &
            (pedagios_df['Quantidade Eixos Vazio'] > 0) &
            (pedagios_df['CATEG'] > pedagios_df['Quantidade Eixos Vazio'])
        ].copy() # Usar .copy() para evitar SettingWithCopyWarning

        # Seleção e remoção de colunas
        colunas_final = ['PLACA', 'TAG','Fatura','DATA','HORA','RODOVIA','PRAÇA','VALOR','Valor Correto','Valor Estorno','CATEG','Quantidade Eixos Vazio','mdfe_aberto','mdfe_aberto_Atua']

        # Garante que apenas as colunas necessárias e existentes estejam presentes
        contestacao_df = contestacao_df[[col for col in colunas_final if col in contestacao_df.columns]]

        return contestacao_df

    except Exception as e:
        st.error(f"Ocorreu um erro no processamento: {e}")
        return pd.DataFrame()

# --- Estrutura do Aplicativo Streamlit ---

st.title("🚛 Análise de Pedágios e MDF-e")

if engine is None:
    st.stop() # Para a execução se a conexão SQL falhou

# 1. Upload do Arquivo
st.header("1. Seleção da Planilha Sem Parar (.xls)")
uploaded_file = st.file_uploader(
    "Escolha o arquivo .xls ou .xlsx (Sem Parar - PASSAGENS PEDÁGIO)", 
    type=['xls', 'xlsx']
)

# 2. Processamento
if uploaded_file is not None:
    if st.button("Analisar Planilha"):
        with st.spinner("Processando dados e verificando MDF-e..."):
            contestacao_df = processar_planilha_sem_parar(uploaded_file, engine, duckdb_conn)

        if not contestacao_df.empty:
            st.header("2. Resultados da Análise: Contestações Sugeridas")
            st.success(f"Foram encontradas {len(contestacao_df)} passagens sugeridas para contestação.")
            st.dataframe(contestacao_df)

            plot_contestacoes_por_placa(contestacao_df)
            
            plot_valor_por_dia(contestacao_df)
            
            if st.button("Salvar Contestações na Tabela 'contestacoes'"):
                
                # Chamada da função de salvar
                success, message = salvar_no_banco(contestacao_df, engine)
                
                if success:
                    st.success(message)
                else:
                    st.error(message)

            # 3. Download do Resultado
            st.header("3. Download do Arquivo de Saída")

            # Converte o DataFrame para CSV no formato do código original (separador ';')
            csv = contestacao_df.to_csv(index=False, sep=';', encoding='utf-8')

            # Adiciona o botão de download
            st.download_button(
                label="Baixar CSV de Contestações",
                data=csv,
                file_name='contestacoes_sem_parar_analise.csv',
                mime='text/csv',
            )
        else:
            st.info("Nenhuma passagem encontrada que atenda aos critérios de contestação (MDFe fechado e eixos cobrados a mais).")

# 4. Exibição de Dados de Apoio (Opcional, para debug/info)
st.sidebar.header("Dados Carregados")
st.sidebar.metric("MDF-e Atua Carregados (DuckDB)", len(mdfeEmitidosAtua))