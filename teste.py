import pandas as pd 
from sqlalchemy import create_engine
import tkinter as tk
from tkinter import filedialog
from tkinter import messagebox
import pyodbc

server = 'srv-seniorbd' 
database = 'softran_bendo'  
username = 'pedagio'    
password = 'pedagioBendo' 

connection_string = f'mssql+pyodbc://{username}:{password}@{server}/{database}?driver=ODBC+Driver+17+for+SQL+Server'

placas = [] 

engine = create_engine(connection_string)

print(pyodbc.drivers())

try:
    with engine.connect() as connection:
        print("Conexão bem-sucedida!")
except Exception as e:
    print(f"Ocorreu um erro ao tentar conectar ao SQL Server: {e}")

def verificar_mdfe(row, engine):
    placa = row['Placa']
    data_passagem = row['Data Passagem']
    query_mdfe = f"""
        SELECT mdfe.CdEmpresa, mdfe.CdSeqMDFe, mdfe.InSitSefaz, mdfe.DtIntegracao
        FROM GTCMFESF mdfe
        LEFT JOIN GTCMFE int ON mdfe.CdSeqMDFe = int.CdSeqMDFe 
        AND mdfe.CdEmpresa = int.CdEmpresa
        WHERE int.nrplaca = '{placa}'
        AND mdfe.InSitSefaz = 100
        AND mdfe.DtIntegracao < CONVERT(datetime, '{data_passagem}', 120)
        AND NOT EXISTS (
            SELECT * 
            FROM GTCMFESF mdfe2 
            WHERE mdfe2.cdempresa = mdfe.CdEmpresa
            AND mdfe2.cdseqmdfe = mdfe.CdSeqMDFe 
            AND mdfe2.insitsefaz = 135 
            AND mdfe2.dtintegracao < CONVERT(datetime, '{data_passagem}', 120)
        )
        ORDER BY mdfe.DtIntegracao
    """
    mdfe_aberto = pd.read_sql(query_mdfe, engine)
    return len(mdfe_aberto) > 0

def obter_quantidade_eixos(row, engine):
    placa = row['Placa']
    print(f'quantidade eixo placa {placa}')
    query_veiculo = f"""
    SELECT Qtd_Eixos_vazio FROM softran_bendo.dbo.bendo_dados_veiculos ('{placa}')
    """
    resultado = pd.read_sql(query_veiculo, engine)
    if not resultado.empty:
        return resultado['Qtd_Eixos_vazio'].iloc[0]
    else:
        return None

def processar_planilha(input_file, output_file):
    try:
        pedagios_df = pd.read_csv(input_file, encoding='utf-8', delimiter=';')  
        pedagios_df = pedagios_df.drop(columns = pedagios_df.columns[0])
        pedagios_df = pedagios_df.iloc[7:].reset_index(drop=True)
        pedagios_df.columns = pedagios_df.iloc[0]
        pedagios_df = pedagios_df[1:].reset_index(drop=True)
        pedagios_df = pedagios_df.drop(columns=['Tipo De Tag', 'Apelido', 'Hierarquia','Data do Processamento'])
        pedagios_df['Número de Eixos Cobrados'] = pedagios_df['Categoria cobrada'].str.extract(r'(\d)')
        pedagios_df = pedagios_df.dropna(subset=['Número de Eixos Cobrados'])
        pedagios_df['Número de Eixos Cobrados'] = pedagios_df['Número de Eixos Cobrados'].astype(int)
        pedagios_df['Data Passagem'] = pedagios_df['Data da Transação'] + ' ' + pedagios_df['Hora da Transação']
        pedagios_df['Data Passagem'] = pd.to_datetime(pedagios_df['Data Passagem'], format='%d/%m/%Y %H:%M:%S')
        
        pedagios_df = pedagios_df.drop(columns=['Data da Transação', 'Hora da Transação'])
        colunas = ['Data Passagem'] + [col for col in pedagios_df.columns if col != 'Data Passagem']
        pedagios_df = pedagios_df[colunas]
        
        pedagios_df = pedagios_df.iloc[:500] #cortar a partir da linha
        
        
        
        pedagios_df['Quantidade Eixos Real'] =  pedagios_df['Placa'].apply(lambda x: obter_quantidade_eixos({'Placa': x}, engine))
        
        pedagios_df['mdfe_aberto'] = pedagios_df.apply(lambda row: verificar_mdfe(row, engine), axis=1)
        
        contestacao_df = pedagios_df[(pedagios_df['mdfe_aberto'] == False) & 
                                    (pedagios_df['Número de Eixos Cobrados'] > pedagios_df['Quantidade Eixos Real'])]
        
        contestacao_df.to_csv(output_file, index=False, sep=';', encoding='utf-8')
        messagebox.showinfo("Concluído", f"Análise concluída. Verifique o arquivo '{output_file}'.")
        
    except Exception as e:
        messagebox.showerror("Erro", f"Ocorreu um erro: {e}")

def escolher_arquivo():
    arquivo = filedialog.askopenfilename(title="Selecione a planilha de pedágios", filetypes=[("CSV files", "*.csv")])
    if arquivo:
        output_arquivo = filedialog.asksaveasfilename(defaultextension=".csv", title="Salvar arquivo de saída", filetypes=[("CSV files", "*.csv")])
        if output_arquivo:
            processar_planilha(arquivo, output_arquivo)

app = tk.Tk()
app.title("Sistema de Análise de Pedágios e MDF-e")
app.geometry("800x600")

btn_processar = tk.Button(app, text="Selecionar Planilha e Processar", command=escolher_arquivo)
btn_processar.pack(expand=True)

app.mainloop()
