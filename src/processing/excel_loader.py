import pandas as pd
from src.config import NOME_PLANILHA_EXCEL


def load_excel_data(uploaded_file):
    pedagios_df = pd.read_excel(uploaded_file, sheet_name=NOME_PLANILHA_EXCEL, engine='xlrd')
    pedagios_df = pedagios_df.drop(columns=['PREFIXO'])
    pedagios_df['Data Passagem'] = pedagios_df['DATA'] + ' ' + pedagios_df['HORA']
    pedagios_df['Data Passagem'] = pd.to_datetime(
        pedagios_df['Data Passagem'], format='%d/%m/%Y %H:%M:%S', errors='coerce'
    )
    pedagios_df.dropna(subset=['Data Passagem'], inplace=True)
    return pedagios_df

