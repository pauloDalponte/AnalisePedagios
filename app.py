import streamlit as st
import pandas as pd

from src.config import NOME_PLANILHA_EXCEL
from src.database.connection import get_sql_engine, get_duckdb_conn
from src.database.queries import load_mdfe_atua, salvar_no_banco
from src.processing.processor import PedagioProcessor
from src.visualization.charts import plot_contestacoes_por_placa, plot_valor_por_dia

st.set_page_config(layout="wide")
st.title("🚛 Análise de Pedágios")

if 'contestacao_df' not in st.session_state:
    st.session_state.contestacao_df = pd.DataFrame()
if 'analysis_done' not in st.session_state:
    st.session_state.analysis_done = False

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
