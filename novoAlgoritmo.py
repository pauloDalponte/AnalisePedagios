import streamlit as st
import pandas as pd
from sqlalchemy import create_engine
import duckdb
import io
import os
import altair as alt

server = '192.168.0.210'
database = 'softran_bendo'
username = 'pedagio'
password = 'pedagioBendo'

connection_string = f'mssql+pyodbc://{username}:{password}@{server}/{database}?driver=ODBC+Driver+17+for+SQL+Server'

placas = []

engine = create_engine(connection_string)

try:
    with engine.connect() as connection:
        print("Conexao bem-sucedida!")
except Exception as e:
    print(f"Ocorreu um erro ao tentar conectar ao SQL Server: {e}")

queryMdfe= """
    SELECT mdfe.CdEmpresa,mdfe.CdSeqMDFe,mdfe.InSitSefaz,mdfe.DtIntegracao,int.nrplaca FROM GTCMFESF mdfe
    left join GTCMFE int on mdfe.CdSeqMDFe = int.CdSeqMDFe and mdfe.CdEmpresa = int.CdEmpresa
    where (mdfe.InSitSefaz = 100 or mdfe.InSitSefaz = 135 ) and mdfe.DtIntegracao >= DATEADD(DAY, -180, GETDATE())
    ORDER BY mdfe.CdEmpresa,mdfe.CdSeqMDFe
"""
mdfeEmitidos = pd.read_sql(queryMdfe, engine)

queryMdfeAtua= """
    SELECT man.datahora,man.updated_at,vei.placa
    FROM atua_prod.dbo.manifesto man
    INNER JOIN atua_prod.dbo.veiculos vei ON vei.Idatua = man.idVeiculo
    where man.datahora >= DATEADD(DAY, -180, GETDATE())
    ORDER BY man.datahora
"""
mdfeEmitidosAtua = pd.read_sql(queryMdfeAtua, engine)

duckdb_conn = duckdb.connect(':memory:')
duckdb_conn.execute("CREATE TABLE IF NOT EXISTS GTCMFESF (CdEmpresa INTEGER, CdSeqMDFe INTEGER, InSitSefaz INTEGER, DtIntegracao TIMESTAMP, nrPlaca varchar(10))")
duckdb_conn.execute("INSERT INTO GTCMFESF SELECT * FROM mdfeEmitidos")

duckdb_conn.execute("CREATE TABLE IF NOT EXISTS manifestos (dataHora TIMESTAMP,updated_at TIMESTAMP, placa varchar(10))")
duckdb_conn.execute("INSERT INTO manifestos SELECT * FROM mdfeEmitidosAtua")

def read_excel_auto(file):
    ext = os.path.splitext(file.name)[1].lower()
    if ext == '.xlsx':
        return pd.read_excel(io.BytesIO(file.read()), sheet_name='PASSAGENS PEDÁGIO', engine='openpyxl')
    elif ext == '.xls':
        try:
            import xlrd
        except ImportError:
            st.error("Para arquivos .xls instale xlrd >= 2.0.1: pip install xlrd --upgrade")
            return pd.DataFrame()
        return pd.read_excel(io.BytesIO(file.read()), sheet_name='PASSAGENS PEDÁGIO', engine='xlrd')
    else:
        st.error("Somente arquivos Excel (.xls ou .xlsx) são suportados")
        return pd.DataFrame()

def verificar_mdfe_SP_Atua(row, duckdb_conn):
    placa = row['PLACA']
    data_passagem = row['Data Passagem']

    query_mdfe = f"""
        SELECT mdfe.placa
        FROM manifestos mdfe
        WHERE mdfe.placa = '{placa}'
        AND mdfe.datahora < '{data_passagem}'
        AND (
        mdfe.updated_at IS NULL OR
        mdfe.updated_at  > '{data_passagem}')
    """
    mdfe_aberto = duckdb_conn.execute(query_mdfe).df()
    return len(mdfe_aberto) > 0

def verificar_mdfe_SP(row, duckdb_conn):
    placa = row['PLACA']
    data_passagem = row['Data Passagem']

    query_mdfe = f"""
        SELECT mdfe.CdEmpresa, mdfe.CdSeqMDFe, mdfe.InSitSefaz, mdfe.DtIntegracao,mdfe.nrplaca
        FROM GTCMFESF mdfe
        WHERE mdfe.nrplaca = '{placa}'
        AND mdfe.InSitSefaz = 100 AND mdfe.DtIntegracao < '{data_passagem}'
        AND NOT EXISTS (
            SELECT *
            FROM GTCMFESF mdfe2
            WHERE mdfe2.cdempresa = mdfe.CdEmpresa
            AND mdfe2.cdseqmdfe = mdfe.CdSeqMDFe AND mdfe2.insitsefaz = 135 AND mdfe2.dtintegracao < '{data_passagem}'
        )ORDER BY mdfe.DtIntegracao
    """
    mdfe_aberto = duckdb_conn.execute(query_mdfe).df()
    return len(mdfe_aberto) > 0


def busca_eixos(lista_placas,engine):
    placas_str = ','.join([f"'{placa}'" for placa in lista_placas])

    query_eixos = f"""
        select placa,nreixos,nrEixosVazio,tipoApelido
        from atua_prod.dbo.veiculos V
        WHERE V.placa in ({placas_str})
    """
    try:
        resultados = pd.read_sql(query_eixos, engine)
    except Exception as e:
        print("Erro no pd.read_sql:", e)

    dict_placas_eixos = dict(zip(resultados['placa'], resultados['nrEixosVazio']))

    return dict_placas_eixos

def obter_quantidade_eixos_SP(row, dict_placas_eixos):
    placa = row['PLACA']
    return dict_placas_eixos.get(placa, 0)

def processar_planilha_sem_parar(input_file):
    pedagios_df = read_excel_auto(uploaded_file)
    if pedagios_df.empty:
        return pedagios_df

    pedagios_df = pedagios_df.drop(columns=['PREFIXO'], errors='ignore')

    #juntar data e hora e formatar para comparar no select futuro
    pedagios_df['Data Passagem'] = pedagios_df['DATA'] + ' ' + pedagios_df['HORA']
    pedagios_df['Data Passagem'] = pd.to_datetime(pedagios_df['Data Passagem'], format='%d/%m/%Y %H:%M:%S')
    #pegar as placas que tem na planilha para depois pegar os eixos
    lista_placas = list(pedagios_df['PLACA'].unique())
    dict_placas_eixos = busca_eixos(lista_placas, engine)

    #quantidade de eixos que a placa tem
    pedagios_df['Quantidade Eixos Vazio'] = pedagios_df.apply(lambda row: obter_quantidade_eixos_SP(row, dict_placas_eixos), axis=1)

    pedagios_df['Quantidade Eixos Vazio'] = pd.to_numeric(pedagios_df['Quantidade Eixos Vazio'], errors='coerce')
    pedagios_df = pedagios_df.dropna(subset=['Quantidade Eixos Vazio'])
    pedagios_df['Quantidade Eixos Vazio'] = pedagios_df['Quantidade Eixos Vazio'].astype(int)

    pedagios_df['mdfe_aberto'] = pedagios_df.apply(lambda row: verificar_mdfe_SP(row, duckdb_conn), axis=1)
    pedagios_df['mdfe_aberto_Atua'] = pedagios_df.apply(lambda row: verificar_mdfe_SP_Atua(row, duckdb_conn), axis=1)
    pedagios_df['VALOR'] = pedagios_df['VALOR'].replace(',', '.', regex=False) 
    pedagios_df['VALOR'] = pd.to_numeric(pedagios_df['VALOR'], errors='coerce')
    pedagios_df['CATEG'] = pd.to_numeric(pedagios_df['CATEG'], errors='coerce')

    #converte as categorias do sem parar em quantidades de eixos cobrados
    pedagios_df.loc[pedagios_df['CATEG'] == 61, 'CATEG'] = 7
    pedagios_df.loc[pedagios_df['CATEG'] == 62, 'CATEG'] = 8
    pedagios_df.loc[pedagios_df['CATEG'] == 63, 'CATEG'] = 9
    pedagios_df.loc[pedagios_df['CATEG'] == 64, 'CATEG'] = 10
    print('aqui')
    #calcula o valor por eixo cobrado
    pedagios_df['Valor por eixo'] = pedagios_df['VALOR'] / pedagios_df['CATEG'].round(2)

    #calcula o valor que deveria ter sido cobrado pela praça de pedagio
    pedagios_df['Valor Correto'] = (pedagios_df['Quantidade Eixos Vazio'] * pedagios_df['Valor por eixo']).round(2)

    #diferenca de valor cobrado com valor correto
    pedagios_df['Valor Estorno'] = (pedagios_df['VALOR'] - pedagios_df['Valor Correto']).round(2)
    pedagios_df['Fatura'] = ''
    colunas = ['PLACA', 'TAG','Fatura','DATA','HORA','RODOVIA','PRAÇA','VALOR','Valor Correto','Valor Estorno','CATEG','Quantidade Eixos Vazio','mdfe_aberto','mdfe_aberto_Atua','Valor por eixo','Data Passagem','MARCA']
    pedagios_df = pedagios_df[colunas]
    contestacao_df = pedagios_df[
                                (pedagios_df['mdfe_aberto'] == False) &
                                (pedagios_df['mdfe_aberto_Atua'] == False) & #se nao tinha mdfe aberto na atua
                                (pedagios_df['CATEG'] > pedagios_df['Quantidade Eixos Vazio']) #se a quantidade de eixos cobrado é maior que a quantidade vazia da carreta
                            ]

    #remove colunas desnecessárias
    contestacao_df = contestacao_df.drop(columns=['Valor por eixo','Data Passagem','MARCA'])

    return contestacao_df

# =========================
# STREAMLIT
# =========================
st.title("🚛 Sistema de Análise de Pedágios e MDF-e")

uploaded_file = st.file_uploader("Carregue a planilha Sem Parar (.xls ou .xlsx)", type=["xls","xlsx"])

if uploaded_file:
    df = processar_planilha_sem_parar(uploaded_file)

    if not df.empty:
        st.success("✅ Processamento concluído!")
        st.dataframe(df)

        st.subheader("📊 Resumo")
        st.write("Total de registros:", len(df))
        if "Valor Estorno" in df.columns:
            st.write("💰 Valor total de estornos:", df["Valor Estorno"].sum())

        # Gráfico Altair
        if "PLACA" in df.columns:
            df_plot = df[df["PLACA"].notna()]
            chart = alt.Chart(df_plot).mark_bar().encode(
                x=alt.X('PLACA:N', title='Placa'),
                y=alt.Y('count()', title='Quantidade'),
                tooltip=['PLACA', 'count()']
            ).properties(width=700, height=400)
            st.altair_chart(chart, use_container_width=True)

        # Download CSV
        csv = df.to_csv(index=False, sep=";", encoding="utf-8")
        st.download_button("📥 Baixar CSV processado", csv, "resultado.csv", "text/csv")