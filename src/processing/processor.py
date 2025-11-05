import streamlit as st
import pandas as pd
from src.processing.excel_loader import load_excel_data
from src.processing.data_enrichment import enrich_with_eixos
from src.processing.calculations import map_and_calculate_valores
from src.processing.mdfe_checker import check_mdfe_status_duckdb
from src.processing.filters import filter_contestacoes
from src.config import NOME_PLANILHA_EXCEL


class PedagioProcessor:
    def __init__(self, engine, duckdb_conn):
        self.engine = engine
        self.duckdb_conn = duckdb_conn
        self.df = pd.DataFrame()
        self._error = None

    def _check_state(self):
        return self._error is None and not (self.df is None or self.df.empty)

    def load_excel(self, uploaded_file):
        if self._error:
            return self
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
        if not self._check_state():
            return self
        try:
            self.df = enrich_with_eixos(self.df, self.engine)
        except Exception as e:
            self._error = f"Erro ao buscar eixos no SQL Server: {e}"
        return self

    def calculate_valores(self):
        if not self._check_state():
            return self
        self.df = map_and_calculate_valores(self.df)
        if self.df.empty:
            pass
        return self

    def check_mdfe_status(self):
        if not self._check_state():
            return self
        st.info("Verificando MDF-e (Otimizado com DuckDB)...")
        try:
            self.df = check_mdfe_status_duckdb(self.df, self.duckdb_conn)
        except Exception as e:
            self._error = f"Erro ao verificar status do MDF-e no DuckDB: {e}"
        return self

    def filter_contestacoes(self):
        if not self._check_state():
            return self
        self.df = filter_contestacoes(self.df)
        return self

    def get_results(self):
        if self._error:
            st.error(self._error)
            return pd.DataFrame()

        if self.df.empty:
            st.info("Análise concluída. Nenhuma passagem encontrada que atenda aos critérios de contestação.")
            return pd.DataFrame()

        return self.df

