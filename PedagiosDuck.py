import pandas as pd 
from sqlalchemy import create_engine,text
import tkinter as tk
from tkinter import filedialog
from tkinter import messagebox
import duckdb 

server = '192.168.0.210' 
database = 'softran_bendo'  
username = 'pedagio'    
password = 'pedagioBendo' 

connection_string = f'mssql+pyodbc://{username}:{password}@{server}/{database}?driver=ODBC+Driver+17+for+SQL+Server'

placas = [] 

engine = create_engine(connection_string)

try:
    with engine.connect() as connection:
        print("Conexao bem-sucedida!")
except Exception as e:
    print(f"Ocorreu um erro ao tentar conectar ao SQL Server: {e}")

queryMdfe= """
    SELECT mdfe.CdEmpresa,mdfe.CdSeqMDFe,mdfe.InSitSefaz,mdfe.DtIntegracao,int.nrplaca FROM GTCMFESF mdfe
    left join GTCMFE int on mdfe.CdSeqMDFe = int.CdSeqMDFe and mdfe.CdEmpresa = int.CdEmpresa 
    where (mdfe.InSitSefaz = 100 or mdfe.InSitSefaz = 135 ) and mdfe.DtIntegracao >= DATEADD(DAY, -180, GETDATE())
    ORDER BY mdfe.CdEmpresa,mdfe.CdSeqMDFe
"""
mdfeEmitidos = pd.read_sql(queryMdfe, engine)

queryPedagios = """select cod_transacao,placa,estabelecimento,endereco,valor_estorno,status_estorno,dataContestacao,dataEstorno
                    from b011ped 
                    where status_estorno = 'N'"""
                    
contestacoesPendentes = pd.read_sql(queryPedagios,engine)

duckdb_conn = duckdb.connect(':memory:')
duckdb_conn.execute("CREATE TABLE IF NOT EXISTS GTCMFESF (CdEmpresa INTEGER, CdSeqMDFe INTEGER, InSitSefaz INTEGER, DtIntegracao TIMESTAMP, nrPlaca varchar(10))")
duckdb_conn.execute("INSERT INTO GTCMFESF SELECT * FROM mdfeEmitidos")

duckdb_conn.execute("""create table if not exists b011ped (cod_transacao varchar(100),
                    placa varchar(10),
                    estabelecimento varchar(150),
                    endereco varchar(200),
                    valor_estorno float,
                    status_estorno char(1),
                    dataContestacao timestamp,
                    dataEstorno timestamp)""")

duckdb_conn.execute("INSERT INTO b011ped SELECT * FROM contestacoesPendentes")

def verificar_contestacoes(row, duckdb_conn, engine):
    transacao = row['Código da transação']

    query_contestacao = f"""
        SELECT cod_transacao 
        FROM b011ped
        WHERE cod_transacao = '{transacao}'
    """
    contestacaoPendente = duckdb_conn.execute(query_contestacao).df()
    
    if contestacaoPendente.empty:
        print(f"Nenhuma contestacao pendente para a transacao {transacao}.")
        
    if not contestacaoPendente.empty:
        try:
            with engine.begin() as connection:  # `begin` inicia uma transação e faz commit automaticamente
                query_update = text("""
                    UPDATE b011ped 
                    SET status_estorno = 'S', dataEstorno = GETDATE() 
                    WHERE cod_transacao = :transacao
                """)
                
                connection.execute(query_update, {"transacao": transacao})
                print(f"Transacao {transacao} atualizada com sucesso.")
        
        except Exception as e:
            print(f"Ocorreu um erro ao atualizar a transação {transacao}: {e}")

def verificar_mdfe(row, duckdb_conn):
    placa = row['Placa']
    data_passagem = row['Data Passagem']
    query_mdfe = f"""
        SELECT mdfe.CdEmpresa, mdfe.CdSeqMDFe, mdfe.InSitSefaz, mdfe.DtIntegracao,mdfe.nrplaca
        FROM GTCMFESF mdfe
        WHERE mdfe.nrplaca = '{placa}' AND mdfe.InSitSefaz = 100 AND mdfe.DtIntegracao < '{data_passagem}'
        AND NOT EXISTS (
            SELECT * FROM GTCMFESF mdfe2 
            WHERE mdfe2.cdempresa = mdfe.CdEmpresa
            AND mdfe2.cdseqmdfe = mdfe.CdSeqMDFe AND mdfe2.insitsefaz = 135 AND mdfe2.dtintegracao < '{data_passagem}'
        )ORDER BY mdfe.DtIntegracao
    """
    mdfe_aberto = duckdb_conn.execute(query_mdfe).df()
    return len(mdfe_aberto) > 0

def verificar_mdfe_SP(row, duckdb_conn):
    placa = row['PLACA']
    data_passagem = row['Data Passagem']
    query_mdfe = f"""
        SELECT mdfe.CdEmpresa, mdfe.CdSeqMDFe, mdfe.InSitSefaz, mdfe.DtIntegracao,mdfe.nrplaca
        FROM GTCMFESF mdfe
        WHERE mdfe.nrplaca = '{placa}'
        AND mdfe.InSitSefaz = 100 AND mdfe.DtIntegracao < '{data_passagem}'
        AND NOT EXISTS (
            SELECT * 
            FROM GTCMFESF mdfe2 
            WHERE mdfe2.cdempresa = mdfe.CdEmpresa
            AND mdfe2.cdseqmdfe = mdfe.CdSeqMDFe AND mdfe2.insitsefaz = 135 AND mdfe2.dtintegracao < '{data_passagem}'
        )ORDER BY mdfe.DtIntegracao
    """
    mdfe_aberto = duckdb_conn.execute(query_mdfe).df()
    return len(mdfe_aberto) > 0

def busca_eixos(lista_placas,engine):
    placas_str = ','.join([f"'{placa}'" for placa in lista_placas])
    query_eixos = f"""
        SELECT V.NrPlaca as placa
            ,E.NrPlacaReboque1 as Reb1
            ,E.NrPlacaReboque2 as Reb2
            ,E.NrPlacaReboque3 as Reb3
            ,(  SELECT MM.DsModelo
                FROM SISVEICU VV
                LEFT JOIN SISMdVei MM ON VV.CdModCarroceria = MM.CdModelo
                WHERE VV.NrPlaca = (SELECT CASE WHEN (E.NrPlacaReboque3 IS NOT NULL AND len(E.NrPlacaReboque3) > 1) THEN E.NrPlacaReboque3
                                                WHEN (E.NrPlacaReboque2 IS NOT NULL AND len(E.NrPlacaReboque2) > 1) THEN E.NrPlacaReboque2
                                                WHEN (E.NrPlacaReboque1 IS NOT NULL AND len(E.NrPlacaReboque1) > 1) THEN E.NrPlacaReboque1
                                                ELSE V.NrPlaca END)
            ) AS TIPO
            ,(  SELECT (CASE MM.CdModelo   
                        WHEN 8041 THEN 3
                        WHEN 8042 THEN 4
                        WHEN 8043 THEN 6
                        WHEN 8044 THEN 5
                        WHEN 8045 THEN 7
                        WHEN 8046 THEN 6
                        WHEN 8047 THEN 9
                        WHEN 8048 THEN 9
                        WHEN 8049 THEN 4
                        WHEN 8050 THEN 6
                        WHEN 8051 THEN 3
                        WHEN 8052 THEN 9
                        WHEN 8107 THEN 7
                        WHEN 8142 THEN 6
                        WHEN 8147 THEN 7
                        WHEN 8149 THEN 9
                        WHEN 8166 THEN 9
                        WHEN 8167 THEN 3
                        WHEN 8172 THEN 9
                        WHEN 125575 THEN 4
                        WHEN 125574 THEN 5
                        WHEN 125573 THEN 5
                        WHEN 125572 THEN 4
                        WHEN 125568 THEN 7
                        WHEN 125601 THEN 4
            END)
                FROM SISVEICU VV
                LEFT JOIN SISMdVei MM ON VV.CdModCarroceria = MM.CdModelo
                WHERE VV.NrPlaca = (SELECT CASE WHEN (E.NrPlacaReboque3 IS NOT NULL AND len(E.NrPlacaReboque3) > 1) THEN E.NrPlacaReboque3
                                                WHEN (E.NrPlacaReboque2 IS NOT NULL AND len(E.NrPlacaReboque2) > 1) THEN E.NrPlacaReboque2
                                                WHEN (E.NrPlacaReboque1 IS NOT NULL AND len(E.NrPlacaReboque1) > 1) THEN E.NrPlacaReboque1
                                                ELSE V.NrPlaca END)
            ) AS Qtd_Eixos
            ,(  SELECT (CASE MM.CdModelo   
                        WHEN 8041 THEN 2
                        WHEN 8042 THEN 2
                        WHEN 8043 THEN 4
                        WHEN 8044 THEN 3
                        WHEN 8045 THEN 4
                        WHEN 8046 THEN 4
                        WHEN 8047 THEN 7
                        WHEN 8048 THEN 7
                        WHEN 8049 THEN 2
                        WHEN 8050 THEN 4
                        WHEN 8051 THEN 2
                        WHEN 8052 THEN 7
                        WHEN 8107 THEN 4
                        WHEN 8142 THEN 4
                        WHEN 8147 THEN 4
                        WHEN 8149 THEN 7
                        WHEN 8166 THEN 7
                        WHEN 8167 THEN 2
                        WHEN 8172 THEN 7
                        WHEN 125575 THEN 2
                        WHEN 125574 THEN 3
                        WHEN 125573 THEN 3
                        WHEN 125572 THEN 2
                        WHEN 125568 THEN 4
                        WHEN 125601 THEN 2
            END)
                FROM SISVEICU VV
                LEFT JOIN SISMdVei MM ON VV.CdModCarroceria = MM.CdModelo
                WHERE VV.NrPlaca = (SELECT CASE WHEN (E.NrPlacaReboque3 IS NOT NULL AND len(E.NrPlacaReboque3) > 1) THEN E.NrPlacaReboque3
                                                WHEN (E.NrPlacaReboque2 IS NOT NULL AND len(E.NrPlacaReboque2) > 1) THEN E.NrPlacaReboque2
                                                WHEN (E.NrPlacaReboque1 IS NOT NULL AND len(E.NrPlacaReboque1) > 1) THEN E.NrPlacaReboque1
                                                ELSE V.NrPlaca END)
            ) AS Qtd_Eixos_vazio
        FROM SISVeicu V
        LEFT JOIN GFVENGAT E ON E.NrPlaca = V.NrPlaca 
            AND E.DtEngate = (SELECT TOP 1 DtEngate FROM GFVENGAT WHERE NrPlaca = V.NrPlaca ORDER BY DtEngate DESC,HrEngate DESC)
            AND E.HrEngate = (SELECT TOP 1 HrEngate FROM GFVENGAT WHERE NrPlaca = V.NrPlaca ORDER BY DtEngate DESC,HrEngate DESC)  
        WHERE V.NrPlaca in ({placas_str}) 
    """
    resultados = pd.read_sql(query_eixos, engine)
    
    dict_placas_eixos = dict(zip(resultados['placa'], resultados['Qtd_Eixos_vazio']))
    
    return dict_placas_eixos

def obter_quantidade_eixos(row, dict_placas_eixos):   
    placa = row['Placa']
    return dict_placas_eixos.get(placa, 0)

def obter_quantidade_eixos_SP(row, dict_placas_eixos):   
    placa = row['PLACA']
    return dict_placas_eixos.get(placa, 0)

def processar_planilha(input_file, output_file):
    try:
        pedagios_df = pd.read_csv(input_file, encoding='utf-8', delimiter=';')
        pedagios_df = pedagios_df.iloc[7:].reset_index(drop=True)
        pedagios_df.columns = pedagios_df.iloc[0]
        pedagios_df = pedagios_df[1:].reset_index(drop=True)
        pedagios_df = pedagios_df.drop(columns=['Tipo De Tag', 'Apelido', 'Hierarquia'])
        pedagios_df = pedagios_df[pedagios_df.duplicated(subset=['Código da transação'], keep=False) == False]
        # pedagios_df = pedagios_df.iloc[:150]
     
        pedagios_df['Número de Eixos Cobrados'] = pedagios_df['Categoria cobrada'].str.extract(r'(\d)')
        pedagios_df = pedagios_df.dropna(subset=['Número de Eixos Cobrados'])
        pedagios_df['Número de Eixos Cobrados'] = pedagios_df['Número de Eixos Cobrados'].astype(int)
        
        pedagios_df['Data Passagem'] = pedagios_df['Data da Transação'] + ' ' + pedagios_df['Hora da Transação']
        pedagios_df['Data Passagem'] = pd.to_datetime(pedagios_df['Data Passagem'], format='%d/%m/%Y %H:%M:%S')
        pedagios_df = pedagios_df.drop(columns=['Data da Transação', 'Hora da Transação'])
        
        pedagios_df['Data do Processamento'] = pd.to_datetime(pedagios_df['Data Passagem'], format='%d/%m/%Y %H:%M:%S')

        pedagios_df = pedagios_df[pedagios_df['Placa']!= 'QIZ4E82']                
        pedagios_df = pedagios_df[pedagios_df['Tipo de veículo']!= 'Passeio']      
        pedagios_df = pedagios_df[pedagios_df['Tipo de veículo']!= 'Ônibus']       
        
        lista_placas = list(pedagios_df['Placa'].unique())
        dict_placas_eixos = busca_eixos(lista_placas, engine)
        
        pedagios_df['Quantidade Eixos Vazio'] = pedagios_df.apply(lambda row: obter_quantidade_eixos(row, dict_placas_eixos), axis=1)
        
        pedagios_df['Quantidade Eixos Vazio'] = pd.to_numeric(pedagios_df['Quantidade Eixos Vazio'], errors='coerce')
        pedagios_df = pedagios_df.dropna(subset=['Quantidade Eixos Vazio'])
        pedagios_df['Quantidade Eixos Vazio'] = pedagios_df['Quantidade Eixos Vazio'].astype(int)
        
        pedagios_df['mdfe_aberto'] = pedagios_df.apply(lambda row: verificar_mdfe(row, duckdb_conn), axis=1)
        
        pedagios_df = pedagios_df[pedagios_df['mdfe_aberto']!= 'True']
        pedagios_df['Valor da Transação(R$)'] = pedagios_df['Valor da Transação(R$)'].str.replace(',', '.', regex=False) 
        pedagios_df['Valor da Transação(R$)'] = pd.to_numeric(pedagios_df['Valor da Transação(R$)'], errors='coerce') 
        pedagios_df['Valor por eixo'] = pedagios_df['Valor da Transação(R$)'] / pedagios_df['Número de Eixos Cobrados'].round(2)
        pedagios_df['Valor Correto'] = (pedagios_df['Quantidade Eixos Vazio'] * pedagios_df['Valor por eixo']).round(2)
        pedagios_df['Valor Estorno'] = (pedagios_df['Valor da Transação(R$)'] - pedagios_df['Valor Correto']).round(2)
        
        estornos_df= pedagios_df[(pedagios_df['mdfe_aberto'] == False) & 
                                    (pedagios_df['Número de Eixos Cobrados'] > pedagios_df['Quantidade Eixos Vazio'])]
        
        estornos_df = estornos_df[estornos_df['Quantidade Eixos Vazio']!= 0]
        
        estornos_df = estornos_df.drop(columns=['Data do Processamento','Tipo de Transação','Tipo de veículo','Marca do veículo','Modelo do veículo',
                                                'Categoria Cadastrada','mdfe_aberto','Valor por eixo','Valor Correto','Quantidade Eixos Vazio',
                                                'Categoria cobrada','Valor utilizado de vale-pedágio (R$)','Valor cobrado (R$)',
                                                'Valor da Transação(R$)','Data Passagem','Número de Eixos Cobrados'])
        estornos_df['status_estorno'] = 'N'
        estornos_df['dataContestacao'] = pd.to_datetime('now').normalize()
        estornos_df['dataEstorno'] = ''
        estornos_df.rename(columns={
            'Código da transação': 'cod_transacao',
            'Placa': 'placa',
            'Estabelecimento':'estabelecimento',
            'Endereço':'endereco',
            'estornadoPedagio': 'status_estorno',
            'Valor Estorno': 'valor_estorno'
        }, inplace=True)
        estornos_df = estornos_df.fillna('')

        try:
            estornos_df.to_sql('b011ped', con=engine, index=False, if_exists='append')
            print("Dados inseridos com sucesso no SQL Server.")
        except Exception as e:
            print(f"Erro ao inserir dados no SQL Server: {e}")
            
        print('Aqui')
            
        revisa_estorno = pedagios_df[pedagios_df['Tipo de Transação'] == 'Estorno Pedágio']

        revisa_estorno.apply(lambda row: verificar_contestacoes(row, duckdb_conn,engine), axis=1)
        print('Aqui')
        contestacao_df = pedagios_df[(pedagios_df['mdfe_aberto'] == False) & 
                                    (pedagios_df['Número de Eixos Cobrados'] > pedagios_df['Quantidade Eixos Vazio'])]
        
        contestacao_df = contestacao_df.drop(columns=['Valor por eixo'])
        print('Aqui')
        colunas = ['Código da transação','Data do Processamento','Data Passagem','Tipo de Transação','Placa','Tipo de veículo','Marca do veículo','Modelo do veículo','Categoria Cadastrada','Categoria cobrada','Estabelecimento','Endereço','Valor da Transação(R$)','Valor utilizado de vale-pedágio (R$)','Valor cobrado (R$)','Número de Eixos Cobrados','Quantidade Eixos Vazio','mdfe_aberto','Valor Correto','Valor Estorno']
        contestacao_df = contestacao_df[colunas]
        contestacao_df = contestacao_df[contestacao_df['Quantidade Eixos Vazio']!= 0]

        contestacao_df['Valor Estorno'] = contestacao_df['Valor Estorno'].apply(lambda x: str(x).replace('.', ','))
        
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
 

def processar_planilha_sem_parar(input_file, output_file):
    try:
        pedagios_df = pd.read_excel(input_file,sheet_name='PASSAGENS PEDÁGIO', engine='xlrd')  
        pedagios_df = pedagios_df.drop(columns=['PREFIXO'])

        pedagios_df['Data Passagem'] = pedagios_df['DATA'] + ' ' + pedagios_df['HORA']
        pedagios_df['Data Passagem'] = pd.to_datetime(pedagios_df['Data Passagem'], format='%d/%m/%Y %H:%M:%S')
        
        
        lista_placas = list(pedagios_df['PLACA'].unique())
        dict_placas_eixos = busca_eixos(lista_placas, engine)
        pedagios_df['Quantidade Eixos Vazio'] = pedagios_df.apply(lambda row: obter_quantidade_eixos_SP(row, dict_placas_eixos), axis=1)
        
        pedagios_df['Quantidade Eixos Vazio'] = pd.to_numeric(pedagios_df['Quantidade Eixos Vazio'], errors='coerce')
        pedagios_df = pedagios_df.dropna(subset=['Quantidade Eixos Vazio'])
        pedagios_df['Quantidade Eixos Vazio'] = pedagios_df['Quantidade Eixos Vazio'].astype(int)
        
        pedagios_df['mdfe_aberto'] = pedagios_df.apply(lambda row: verificar_mdfe_SP(row, duckdb_conn), axis=1)
        
        pedagios_df['VALOR'] = pedagios_df['VALOR'].replace(',', '.', regex=False) 
        pedagios_df['VALOR'] = pd.to_numeric(pedagios_df['VALOR'], errors='coerce')
        pedagios_df['CATEG'] = pd.to_numeric(pedagios_df['CATEG'], errors='coerce')
         
        pedagios_df.loc[pedagios_df['CATEG'] == 61, 'CATEG'] = 7
        pedagios_df.loc[pedagios_df['CATEG'] == 62, 'CATEG'] = 8
        pedagios_df.loc[pedagios_df['CATEG'] == 63, 'CATEG'] = 9
        pedagios_df.loc[pedagios_df['CATEG'] == 64, 'CATEG'] = 10

        pedagios_df['Valor por eixo'] = pedagios_df['VALOR'] / pedagios_df['CATEG'].round(2)

        pedagios_df['Valor Correto'] = (pedagios_df['Quantidade Eixos Vazio'] * pedagios_df['Valor por eixo']).round(2)

        pedagios_df['Valor Estorno'] = (pedagios_df['VALOR'] - pedagios_df['Valor Correto']).round(2)
        pedagios_df['Fatura'] = ''
        
        colunas = ['PLACA', 'TAG','Fatura','DATA','HORA','RODOVIA','PRAÇA','VALOR','Valor Correto','Valor Estorno','CATEG','Quantidade Eixos Vazio','mdfe_aberto','Valor por eixo','Data Passagem','MARCA']
        pedagios_df = pedagios_df[colunas]
        
        contestacao_df = pedagios_df[(pedagios_df['mdfe_aberto'] == False) & 
                                    (pedagios_df['CATEG'] > pedagios_df['Quantidade Eixos Vazio'])
                                    ]
        contestacao_df = contestacao_df.drop(columns=['Valor por eixo','Data Passagem','MARCA'])
        contestacao_df.to_csv(output_file, index=False, sep=';', encoding='utf-8')
        messagebox.showinfo("Concluído", f"Análise concluída. Verifique o arquivo '{output_file}'.")
        
    except Exception as e:
        messagebox.showerror("Erro", f"Ocorreu um erro: {e}") 
            
def escolher_arquivo_sem_parar():
    arquivo = filedialog.askopenfilename(title="Selecione a planilha de pedágios", filetypes=[("XLS files", "*.xls")])
    if arquivo:
        output_arquivo = filedialog.asksaveasfilename(defaultextension=".csv", title="Salvar arquivo de saída", filetypes=[("CSV files", "*.csv")])
        if output_arquivo:
            processar_planilha_sem_parar(arquivo, output_arquivo)

app = tk.Tk()
app.title("Sistema de Análise de Pedágios e MDF-e")
app.geometry("800x600")

btn_processar = tk.Button(app, text="Selecionar Planilha Repom", command=escolher_arquivo)
btn_processar.pack(expand=True)

btn_processar = tk.Button(app, text="Selecionar Planilha Sem Parar", command=escolher_arquivo_sem_parar)
btn_processar.pack(expand=True)

app.mainloop()
