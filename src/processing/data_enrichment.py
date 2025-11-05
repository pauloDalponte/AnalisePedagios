import pandas as pd
from src.database.queries import busca_eixos


def enrich_with_eixos(pedagios_df, engine):
    lista_placas = list(pedagios_df['PLACA'].unique())
    dict_placas_eixos = busca_eixos(lista_placas, engine)
    df = pedagios_df.copy()
    df['Quantidade Eixos Vazio'] = df['PLACA'].map(dict_placas_eixos)
    df['Quantidade Eixos Vazio'] = pd.to_numeric(df['Quantidade Eixos Vazio'], errors='coerce')
    df.dropna(subset=['Quantidade Eixos Vazio'], inplace=True)
    df['Quantidade Eixos Vazio'] = df['Quantidade Eixos Vazio'].astype(int)
    return df

