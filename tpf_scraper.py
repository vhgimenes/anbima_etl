"""
Author: Victor Gimenes
Date: 15/12/2022

Módulo responsável pelo download e armazenamento das marcações de TPF da ANBIMA.
Inspiração: https://github.com/wilsonfreitas/MorthIO_TitulosPublicos_ANBIMA/blob/master/scraper.py
"""

#%%
# Importando bibliotecas
import pandas as pd
import requests
import workdays as wd
from datetime import datetime as dt
from datetime import timedelta as timedelta
import sys
import time

# Removendo os warnings
import warnings
warnings.filterwarnings("ignore")

# Criando o parser para português
from textparser import PortugueseRulesParser
pp = PortugueseRulesParser() 

# Adicionando o path de libraries locais  
from bz_holidays import scrape_anbima_holidays as bz
from mail_man import post_messages as pm 

def get_last_refresh_date():
    """Função criada para retornar a última data com dados da tabela tblAnbimaTPF"""
    sql = """SELECT TOP(1) [REF_DATE]  FROM [tblAnbimaTPF] ORDER BY REF_DATE DESC"""
    date = pd.to_datetime(db.read_tbl_custom(sql).values[0][0],format="%Y-%m-%d").date()
    return date 
        
def extract_anbima_tpf(init_date:pd.datetime,final_date:pd.datetime,holidays:pd.Series) -> pd.DataFrame:
    # sourcery skip: extract-method, remove-unnecessary-else, swap-if-else-branches
    """
    Função que extrai os aquivos de "timeldeta" dias de marcações da
    ANBIMA dos títulos públicos do Tesouro Direto. 
    """
    print('Iniciando rotina de extração dos arquivos TPF Anbima!\n')
    
    # Criando contador de tempo de processamento
    processing_time = time.process_time() 
    
    # Criando Range de datas úteis
    weekmask = "Mon Tue Wed Thu Fri"
    mydates = pd.bdate_range(start=init_date, end=final_date,
                             holidays=holidays, freq='C',
                             weekmask = weekmask).tolist()
    tables = [] # Criando a lista que irá guardar as tabelas de marcação de cada um dos dias 
    # Extraindo a marcação data a data
    try:
        for date in mydates:
            # date = mydates[0]
            print(f'Extraindo o arquivo de TPF referente ao dia {date.strftime("%d/%m/%Y")}.')
            # Extraindo os dados do txt como text
            url = 'http://www.anbima.com.br/informacoes/merc-sec/arqs/ms{date}.txt'.format(date=dt.strftime(date, '%y%m%d'))
            res = requests.get(url, stream=True, verify=False)
            text = res.text # Extraindo o arquivo como texto
            if res.status_code != 404:
                print('Processando o arquivo.')
                print(f'Resposta da API: {res.status_code}')
                # Tratando as linhas do texto e pulando as três primeiras linhas do arquivo
                text = text.split('\r\n')[3:]
                # Removendo as linhas vazias do relatório
                while("" in text): text.remove("")
                # Construindo o dataframe a partir do text
                table_rows = []
                for line in text:
                    row = line.split('@') # quebrando as colunas do arquivo
                    rows = [row[0],
                            (row[0]+row[4][6:]+row[4][4:6]+row[4][:4]),
                            f'{row[1][:4]}-{row[1][4:6]}-{row[1][6:]}',
                            row[2],
                            f'{row[3][:4]}-{row[3][4:6]}-{row[3][6:]}',
                            f'{row[4][:4]}-{row[4][4:6]}-{row[4][6:]}',
                            pp.parse(row[5]),
                            pp.parse(row[6]),
                            pp.parse(row[7]),
                            pp.parse(row[8]),
                            pp.parse(row[9])]
                    table_rows.append(rows)
                table = pd.DataFrame(table_rows)
                # Ajustando o nome das colunas  
                table.columns = ["TIPO",
                                "ID",
                                "REF_DATE",
                                "COD_SELIC",
                                "DATA_EMISSAO",
                                "DATA_VENC",
                                "TX_MAX",
                                "TX_MIN",
                                "TX_IND",
                                "PU",
                                "DESV_PAD"]
                # Armazenando a tabela na lista tables
                tables.append(table)
                print('Arquivo extraído com sucesso!\n')
            else:
                if date == mydates[-1]:
                    raise ValueError(f'arquivo referente ao dia {date.strftime("%d/%m/%Y")} ainda não foi liberado pela Anbima, rodar novamente depois\n')
                else:
                    print('Arquivo não foi encontrado.\n')
                    print(f'Resposta da API: {res.status_code}')
                    
        # Concatenado todas as tabelas de marcações diárias em uma só
        print('Gerando arquivo final com todas as marcações.')
        table_master = pd.concat(tables,axis=0)
        
        # Formatando para tbl
        print('Convertendo para o formato de tbl.\n')
        table_master = table_master.melt(['REF_DATE','ID','TIPO','COD_SELIC','DATA_EMISSAO','DATA_VENC'], var_name='VAR_TYPE', value_name='VALUE').replace('--',0)
        
        # Armazenando dentro da pasta 
        print('Armazenando o arquivo.\n')
        db.upsert_tbl("[dbo].[tblAnbimaTPF]",['ID','REF_DATE','VAR_TYPE'],table_master)        
        print('Rotina finalizada com sucesso!')
        print(f'Tempo de execução: {processing_time}\n')
    except Exception as e:
        print('Erro na rotina, stopando o processo, checar manualmente.\n')
        raise e

def main():  # sourcery skip: extract-method
    # Criando conexão com o canal de avisos do Teams 
    teams_conn = pm.get_connector_mesa_teams()
    # Inciando a rotina
    try:
        # Data e Hora de referência
        now = dt.now()
        today = now.date()
        # Criando as datas para busca das marcações na API da Anbima
        holidays = bz.holidays() # Extraindo os feriados da Anbima
        yesterday = wd.workday(today,-1,holidays)
        final_date = yesterday if now.hour < 18 else today # data final de análise dependera do horário
        # Ultima data com dados da tabela
        last_refresh = get_last_refresh_date() # data inicial da análise dependerá da data da última observação no Azure   
        # Check para ver se já foi atualizada
        if final_date > last_refresh:
            # A data inicial será D+1 da última data de atualização do Azure
            init_date = wd.workday(last_refresh,1,holidays) # Iremos atualizar o D=1 da data da última observaçõ no Azure
            #! Descomentar para recalcular o histórico
            # init_date = dt(2019,1,1).date() 
            # final_date = dt(2023,1,5).date()
            # Extraido as marcações para a data desejada   
            extract_anbima_tpf(init_date,final_date,holidays)
            pm.send_teams_message(teams_conn,
                                  title = "✅ Extração e Upload do arquivo TPF Anbima feitos com sucesso!",
                                  content = f"""Data: {today.strftime("%d/%m/%Y")}. \n\n Horário: {now.strftime("%H:%M:%S")}. \n\n Diretório: T:\GESTAO\MACRO\DEV\\1.WEB\\5.TPF_ANBIMA\\tpf_scraper.py.""")
        else:
            pm.send_teams_message(teams_conn,
                                  title = "✋ Upload do arquivo TPF Anbima já foi feito hoje!",
                                  content = f"""Data: {today.strftime("%d/%m/%Y")}. \n\n Horário: {now.strftime("%H:%M:%S")}. \n\n Diretório: T:\GESTAO\MACRO\DEV\\1.WEB\\5.TPF_ANBIMA\\tpf_scraper.py.""")
    except Exception as e:
        pm.send_teams_message(teams_conn,
                              title = "❌ Erro na extração e/ou upload do Arquivo TPF Anbima.",
                              content = f"""Erro: {e}. \n\n\n Data: {today.strftime("%d/%m/%Y")}. \n\n Horário: {now.strftime("%H:%M:%S")}. \n\n Diretório: T:\GESTAO\MACRO\DEV\\1.WEB\\5.TPF_ANBIMA\\tpf_scraper.py.""")

if __name__ == '__main__':
    main()
