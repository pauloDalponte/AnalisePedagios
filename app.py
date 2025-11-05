import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text
import duckdb
import io
import altair as alt

MAPA_CATEGORIAS_SP = { 61: 7, 62: 8, 63: 9, 64: 10 }
DIAS_CONSULTA_MDFE = 180
NOME_PLANILHA_EXCEL = 'PASSAGENS PEDÁGIO'
TABELA_DESTINO_CONTESTACOES = 'contestacoes'
ESQUEMA_DESTINO_CONTESTACOES = 'dbo'

st.set_page_config(layout="wide")
st.title("🚛 Análise de Pedágios (v3 - Interface Fluente)")

if 'contestacao_df' not in st.session_state:
    st.session_state.contestacao_df = pd.DataFrame()
if 'analysis_done' not in st.session_state:
    st.session_state.analysis_done = False

try:
    db_creds = st.secrets["db_credentials"]
    connection_string = (
        f'mssql+pyodbc://{db_creds["username"]}:{db_creds["password"]}'
        f'@{db_creds["server"]}/{db_creds["database"]}'
        '?driver=ODBC+Driver+17+for+SQL+Server'
    )
except (KeyError, FileNotFoundError):
    st.error(
        "Erro de Segurança: As credenciais do banco de dados não foram "
        "encontradas no 'secrets.toml'. "
        "Crie o arquivo .streamlit/secrets.toml."
    )
    st.stop()

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

@st.cache_resource
def get_duckdb_conn():
    """Cria e armazena em cache a conexão DuckDB in-memory."""
    return duckdb.connect(':memory:')

@st.cache_data(show_spinner="Consultando MDF-e emitidos (Atua)...", ttl=3600)
def load_mdfe_atua(_engine):
    """Carrega dados de MDF-e emitidos do SQL Server."""
    if _engine is None: return pd.DataFrame()
    queryMdfeAtua = f"""
        SELECT man.datahora, man.updated_at, vei.placa
        FROM atua_prod.dbo.manifesto man
        INNER JOIN atua_prod.dbo.veiculos vei ON vei.Idatua = man.idVeiculo
        where man.datahora >= DATEADD(DAY, -{DIAS_CONSULTA_MDFE}, GETDATE())
    """
    try:
        return pd.read_sql(queryMdfeAtua, _engine)
    except Exception as e:
        st.error(f"Erro ao carregar MDF-e Atua: {e}")
        return pd.DataFrame()

@st.cache_data(show_spinner="Buscando eixos das placas...", ttl=3600)
def busca_eixos(_lista_placas, _engine):
    """Busca o número de eixos vazios para as placas no SQL Server."""
    if _engine is None or not _lista_placas: return {}
    placas_str = ','.join([f"'{placa}'" for placa in _lista_placas])
    query_eixos = f"""
        select placa, nrEixosVazio
        from atua_prod.dbo.veiculos V
        WHERE V.placa in ({placas_str})
    """
    try:
        resultados = pd.read_sql(query_eixos, _engine)
        return dict(zip(resultados['placa'], resultados['nrEixosVazio']))
    except Exception as e:
        st.error(f"Erro no pd.read_sql (busca_eixos): {e}")
        return {}

def load_excel_data(uploaded_file):
    """Lê o arquivo Excel e faz a limpeza inicial de datas."""
    pedagios_df = pd.read_excel(uploaded_file, sheet_name=NOME_PLANILHA_EXCEL, engine='xlrd')
    pedagios_df = pedagios_df.drop(columns=['PREFIXO'])
    pedagios_df['Data Passagem'] = pedagios_df['DATA'] + ' ' + pedagios_df['HORA']
    pedagios_df['Data Passagem'] = pd.to_datetime(pedagios_df['Data Passagem'], format='%d/%m/%Y %H:%M:%S', errors='coerce')
    pedagios_df.dropna(subset=['Data Passagem'], inplace=True)
    return pedagios_df

def enrich_with_eixos(pedagios_df, engine):
    """Busca eixos no DB e mapeia para o DataFrame."""
    lista_placas = list(pedagios_df['PLACA'].unique())
    dict_placas_eixos = busca_eixos(lista_placas, engine)
    df = pedagios_df.copy()
    df['Quantidade Eixos Vazio'] = df['PLACA'].map(dict_placas_eixos)
    df['Quantidade Eixos Vazio'] = pd.to_numeric(df['Quantidade Eixos Vazio'], errors='coerce')
    df.dropna(subset=['Quantidade Eixos Vazio'], inplace=True)
    df['Quantidade Eixos Vazio'] = df['Quantidade Eixos Vazio'].astype(int)
    return df

def map_and_calculate_valores(pedagios_df):
    """Converte tipos, mapeia categorias e calcula valores de estorno."""
    df = pedagios_df.copy()
    df['VALOR'] = df['VALOR'].astype(str).str.replace(',', '.', regex=False)
    df['VALOR'] = pd.to_numeric(df['VALOR'], errors='coerce')
    df['CATEG'] = pd.to_numeric(df['CATEG'], errors='coerce')
    df.dropna(subset=['VALOR', 'CATEG'], inplace=True)
    
    df['CATEG'] = df['CATEG'].replace(MAPA_CATEGORIAS_SP)
    
    df = df[df['CATEG'] > 0] 

    df['Valor por eixo'] = df['VALOR'] / df['CATEG']
    df['Valor Correto'] = (df['Quantidade Eixos Vazio'] * df['Valor por eixo']).round(2)
    df['Valor Estorno'] = (df['VALOR'] - df['Valor Correto']).round(2)
    df['Fatura'] = ''
    return df

def check_mdfe_status_duckdb(pedagios_df, duckdb_conn):
    """Verifica o status do MDF-e para todas as passagens (lógica pura)."""
    try:
        duckdb_conn.register('p', pedagios_df)

        query = """
        SELECT
            p.*,
            EXISTS (
                SELECT 1
                FROM manifestos m
                WHERE
                    m.placa = p.PLACA
                    AND p."Data Passagem" >= m.dataHora 
                    AND (p."Data Passagem" <= m.updated_at OR m.updated_at IS NULL)
            ) as mdfe_aberto_Atua
        FROM p
        """
        result_df = duckdb_conn.execute(query).df()
        return result_df

    finally:
        duckdb_conn.unregister('p')

def filter_contestacoes(pedagios_df):
    """Filtra o DataFrame final para as contestações."""
    contestacao_df = pedagios_df[
        (pedagios_df['mdfe_aberto_Atua'] == False) &
        (pedagios_df['Quantidade Eixos Vazio'] > 0) &
        (pedagios_df['CATEG'] > pedagios_df['Quantidade Eixos Vazio'])
    ].copy()
    
    colunas_final = [
        'PLACA', 'TAG', 'Fatura', 'DATA', 'HORA', 'RODOVIA', 'PRAÇA', 
        'VALOR', 'Valor Correto', 'Valor Estorno', 'CATEG', 
        'Quantidade Eixos Vazio', 'mdfe_aberto_Atua'
    ]
    colunas_existentes = [col for col in colunas_final if col in contestacao_df.columns]
    return contestacao_df[colunas_existentes]


class PedagioProcessor:
    """
    Orquestra o processo de análise de pedágios usando uma interface fluente.
    """
    def __init__(self, engine, duckdb_conn):
        self.engine = engine
        self.duckdb_conn = duckdb_conn
        self.df = pd.DataFrame()
        self._error = None

    def _check_state(self):
        """Verifica se um erro já ocorreu ou se o DF está vazio."""
        return self._error is None and not (self.df is None or self.df.empty)

    def load_excel(self, uploaded_file):
        """Etapa 1: Carrega dados do Excel."""
        if self._error: return self
        try:
            self.df = load_excel_data(uploaded_file)
        except KeyError as e:
            self._error = (
                f"Erro: A coluna '{e.name}' não foi encontrada no arquivo Excel. "
                f"Verifique se a planilha está no formato correto (aba '{NOME_PLANILHA_EXCEL}')."
            )
        except Exception as e:
            self._error = f"Erro ao ler o arquivo Excel: {e}"
        return self

    def enrich_with_eixos(self):
        """Etapa 2: Busca eixos no banco de dados SQL."""
        if not self._check_state(): return self
        try:
            self.df = enrich_with_eixos(self.df, self.engine)
        except Exception as e:
            self._error = f"Erro ao buscar eixos no SQL Server: {e}"
        return self

    def calculate_valores(self):
        """Etapa 3: Mapeia categorias e calcula valores de estorno."""
        if not self._check_state(): return self
        self.df = map_and_calculate_valores(self.df)
        if self.df.empty:
            pass 
        return self

    def check_mdfe_status(self):
        """Etapa 4: Verifica o status do MDF-e no DuckDB."""
        if not self._check_state(): return self
        st.info("Verificando MDF-e (Otimizado com DuckDB)...") 
        try:
            self.df = check_mdfe_status_duckdb(self.df, self.duckdb_conn)
        except Exception as e:
            self._error = f"Erro ao verificar status do MDF-e no DuckDB: {e}"
        return self

    def filter_contestacoes(self):
        """Etapa 5: Filtra o resultado final."""
        if not self._check_state(): return self
        self.df = filter_contestacoes(self.df)
        return self

    def get_results(self):
        """Retorna o DataFrame final ou exibe um erro se ocorreu."""
        if self._error:
            st.error(self._error)
            return pd.DataFrame()
        
        if self.df.empty:
            st.info("Análise concluída. Nenhuma passagem encontrada que atenda aos critérios de contestação.")
            return pd.DataFrame()
            
        return self.df

def plot_contestacoes_por_placa(df):
    if df.empty or "PLACA" not in df.columns: return
    df_plot = df[df["PLACA"].notna()].copy()
    chart = alt.Chart(df_plot).mark_bar().encode(
        x=alt.X('PLACA:N', title='Placa'),
        y=alt.Y('count()', title='Quantidade de Contestações'),
        color='PLACA:N',
        tooltip=['PLACA', alt.Tooltip('count()', title='Quantidade')]
    ).properties(title='Contestações Sugeridas por Placa').interactive()
    st.altair_chart(chart, use_container_width=True)

def plot_valor_por_dia(df):
    if df.empty or "DATA" not in df.columns or "Valor Estorno" not in df.columns:
        return
    df_plot = df.copy()
    if df_plot['Valor Estorno'].dtype == 'object':
        df_plot['Valor Estorno'] = df_plot['Valor Estorno'].str.replace(',', '.', regex=False)
    df_plot['Valor Estorno'] = pd.to_numeric(df_plot['Valor Estorno'], errors='coerce')
    df_plot['DATA'] = pd.to_datetime(df_plot['DATA'], format='%d/%m/%Y', errors='coerce')
    df_plot.dropna(subset=['DATA', 'Valor Estorno'], inplace=True)
    if df_plot.empty: return
    df_agrupado = df_plot.groupby(df_plot['DATA'].dt.date).agg(
        Valor_Total_Estorno=('Valor Estorno', 'sum')
    ).reset_index()
    base = alt.Chart(df_agrupado).encode(
        x=alt.X('DATA:T', title='Data da Passagem'),
        y=alt.Y('Valor_Total_Estorno:Q', title='Valor Total de Estorno (R$)'),
        tooltip=['DATA:T', 'Valor_Total_Estorno:Q']
    )
    chart = base.mark_line(point=True).interactive()
    st.altair_chart(chart, use_container_width=True)

def salvar_no_banco(df, engine):
    """Salva o DataFrame de contestações sugeridas na tabela 'contestacoes'."""
    table_name = TABELA_DESTINO_CONTESTACOES
    schema_name = ESQUEMA_DESTINO_CONTESTACOES
    df_save = df.drop(columns=['mdfe_aberto_Atua'], errors='ignore').copy()
    try:
        rows_inserted = df_save.to_sql(
            name=table_name, con=engine, schema=schema_name,
            if_exists='append', index=False
        )
        return True, f"Sucesso: {rows_inserted} linhas salvas na tabela {schema_name}.{table_name}."
    except Exception as e:
        return False, f"Erro ao salvar no SQL Server: {e}"
engine = get_sql_engine()
duckdb_conn = get_duckdb_conn()

if engine is None:
    st.error("A conexão com o SQL Server falhou. A aplicação não pode continuar.")
    st.stop()

mdfeEmitidosAtua = load_mdfe_atua(engine)
if not mdfeEmitidosAtua.empty:
    try:
        duckdb_conn.execute("CREATE OR REPLACE TABLE manifestos AS SELECT * FROM mdfeEmitidosAtua")
        st.sidebar.metric("MDF-e (Atua) em memória", len(mdfeEmitidosAtua))
    except Exception as e:
        st.error(f"Erro ao carregar dados no DuckDB: {e}")
        st.stop()
else:
    st.warning("Não foi possível carregar os dados de MDF-e do SQL Server.")

st.header("1. Upload da Planilha")
uploaded_file = st.file_uploader(
    f"Escolha o arquivo .xls ou .xlsx (Aba: {NOME_PLANILHA_EXCEL})",
    type=['xls', 'xlsx']
)

if uploaded_file is not None:
    if st.button("Analisar Planilha"):
        with st.spinner("Processando dados e verificando MDF-e..."):
            
            processor = PedagioProcessor(engine, duckdb_conn)
            
            results = (
                processor.load_excel(uploaded_file)
                         .enrich_with_eixos()
                         .calculate_valores()
                         .check_mdfe_status()
                         .filter_contestacoes()
                         .get_results()
            )
            
            st.session_state.contestacao_df = results
            st.session_state.analysis_done = True

if st.session_state.analysis_done:
    if not st.session_state.contestacao_df.empty:
        df_resultado = st.session_state.contestacao_df
        
        st.header("2. Resultados da Análise")
        
        total_estorno = df_resultado['Valor Estorno'].sum()
        total_passagens = len(df_resultado)
        col1, col2 = st.columns(2)
        col1.metric("Contestações Sugeridas", f"{total_passagens} passagens")
        col2.metric("Valor Total a Estornar", f"R$ {total_estorno:,.2f}")
        
        tab_grid, tab_charts, tab_actions = st.tabs(
            ["📊 Dados para Contestação", "📈 Gráficos", "💾 Ações (Salvar/Download)"]
        )

        with tab_grid:
            st.info("Abaixo estão todas as passagens que se qualificam para contestação.")
            st.dataframe(df_resultado)

        with tab_charts:
            st.subheader("Valor de Estorno por Dia")
            plot_valor_por_dia(df_resultado)
            st.subheader("Contestações por Placa")
            plot_contestacoes_por_placa(df_resultado)
            
        with tab_actions:
            st.header("Salvar ou Baixar os Resultados")
            
            st.subheader("Download")
            csv = df_resultado.to_csv(index=False, sep=';', encoding='utf-8')
            st.download_button(
                label="Baixar CSV de Contestações",
                data=csv,
                file_name='contestacoes_sem_parar_analise.csv',
                mime='text/csv',
            )
            
            st.subheader("Salvar no Banco de Dados")
            if st.button("Salvar Contestações na Tabela 'contestacoes'"):
                success, message = salvar_no_banco(df_resultado, engine)
                if success:
                    st.success(message)
                else:
                    st.error(message)

    else:
        pass