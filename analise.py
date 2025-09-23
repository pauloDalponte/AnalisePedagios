import streamlit as st
import pandas as pd
import altair as alt

st.set_page_config(page_title="Dashboard Pedágios", layout="wide")
st.title("🚛 Dashboard de Pedágios e Estornos")

# Upload da planilha final
uploaded_file = st.file_uploader("Carregue a planilha final (.csv ou .xlsx)", type=["csv", "xls", "xlsx"])

if uploaded_file:
    # Ler planilha dependendo do formato
    if uploaded_file.name.endswith(".csv"):
        df = pd.read_csv(uploaded_file, sep=';', encoding='utf-8')
    else:
        df = pd.read_excel(uploaded_file)

    st.success("✅ Planilha carregada com sucesso!")

    # Mostrar dados
    st.subheader("📑 Dados")
    st.dataframe(df)

    # Estatísticas gerais
    st.subheader("📊 Resumo")
    st.write("Total de registros:", len(df))
    if "Valor Estorno" in df.columns:
        st.write("💰 Valor total de estornos:", df["Valor Estorno"].sum())

    # ======================
    # Gráficos
    # ======================
    st.subheader("📈 Gráficos Interativos")

    # 1. Ocorrências por Placa
    if "PLACA" in df.columns:
        st.markdown("**Ocorrências por Placa**")
        chart_placa = alt.Chart(df).mark_bar().encode(
            x=alt.X('PLACA:N', sort='-y', title='Placa'),
            y=alt.Y('count()', title='Quantidade'),
            tooltip=['PLACA', 'count()']
        ).properties(width=700, height=400)
        st.altair_chart(chart_placa, use_container_width=True)

    # 2. Total de Estornos por Praça
    if "PRAÇA" in df.columns and "Valor Estorno" in df.columns:
        st.markdown("**Valor de Estornos por Praça**")
        chart_praca = alt.Chart(df).mark_bar().encode(
            x=alt.X('PRAÇA:N', title='Praça', sort='-y'),
            y=alt.Y('sum(Valor Estorno):Q', title='Total Estornos (R$)'),
            tooltip=['PRAÇA', 'sum(Valor Estorno)']
        ).properties(width=700, height=400)
        st.altair_chart(chart_praca, use_container_width=True)

    # 3. Total de Estornos por Dia
    if "DATA" in df.columns and "Valor Estorno" in df.columns:
        st.markdown("**Valor de Estornos ao Longo do Tempo**")
        df['DATA'] = pd.to_datetime(df['DATA'], dayfirst=True)
        chart_data = alt.Chart(df).mark_line(point=True).encode(
            x='DATA:T',
            y='sum(Valor Estorno):Q',
            tooltip=['DATA:T', 'sum(Valor Estorno)']
        ).properties(width=700, height=400)
        st.altair_chart(chart_data, use_container_width=True)

    # 4. Estornos por Categoria de Eixos (CATEG)
    if "CATEG" in df.columns and "Valor Estorno" in df.columns:
        st.markdown("**Valor de Estornos por Categoria de Eixos**")
        chart_categ = alt.Chart(df).mark_bar().encode(
            x=alt.X('CATEG:N', title='Categoria de Eixos'),
            y=alt.Y('sum(Valor Estorno):Q', title='Total Estornos (R$)'),
            tooltip=['CATEG', 'sum(Valor Estorno)']
        ).properties(width=700, height=400)
        st.altair_chart(chart_categ, use_container_width=True)

    # 5. Distribuição de Valores Estorno
    if "Valor Estorno" in df.columns:
        st.markdown("**Distribuição de Valor Estorno**")
        chart_hist = alt.Chart(df).mark_bar().encode(
            alt.X('Valor Estorno:Q', bin=True),
            y='count()',
            tooltip=['count()']
        ).properties(width=700, height=400)
        st.altair_chart(chart_hist, use_container_width=True)

    # ======================
    # Download CSV processado
    # ======================
    csv = df.to_csv(index=False, sep=';', encoding='utf-8')
    st.download_button("📥 Baixar CSV", csv, "resultado.csv", "text/csv")
