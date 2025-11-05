import streamlit as st
import pandas as pd
from src.config import DIAS_CONSULTA_MDFE, TABELA_DESTINO_CONTESTACOES, ESQUEMA_DESTINO_CONTESTACOES


@st.cache_data(show_spinner="Consultando MDF-e emitidos (Atua)...", ttl=3600)
def load_mdfe_atua(_engine):
    if _engine is None:
        return pd.DataFrame()
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
    if _engine is None or not _lista_placas:
        return {}
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


def salvar_no_banco(df, engine):
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

