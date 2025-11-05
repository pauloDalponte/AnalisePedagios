import streamlit as st
import pandas as pd
import altair as alt


def plot_contestacoes_por_placa(df):
    if df.empty or "PLACA" not in df.columns:
        return
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
    if df_plot.empty:
        return
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

