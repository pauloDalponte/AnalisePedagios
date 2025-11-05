import streamlit as st
from sqlalchemy import create_engine
import duckdb


def get_connection_string():
    try:
        db_creds = st.secrets["db_credentials"]
        connection_string = (
            f'mssql+pyodbc://{db_creds["username"]}:{db_creds["password"]}'
            f'@{db_creds["server"]}/{db_creds["database"]}'
            '?driver=ODBC+Driver+17+for+SQL+Server'
        )
        return connection_string
    except (KeyError, FileNotFoundError):
        st.error(
            "Erro de Segurança: As credenciais do banco de dados não foram "
            "encontradas no 'secrets.toml'. "
            "Crie o arquivo .streamlit/secrets.toml."
        )
        st.stop()
        return None


@st.cache_resource
def get_sql_engine():
    connection_string = get_connection_string()
    if connection_string is None:
        return None
    
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
    return duckdb.connect(':memory:')

