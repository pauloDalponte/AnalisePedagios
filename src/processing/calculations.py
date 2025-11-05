import pandas as pd
from src.config import MAPA_CATEGORIAS_SP


def map_and_calculate_valores(pedagios_df):
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

