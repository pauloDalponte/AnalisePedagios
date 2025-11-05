import pandas as pd


def filter_contestacoes(pedagios_df):
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

