# -*- coding: utf-8 -*-
"""cvm_analise_informacoes_cadastro_fundos.ipynb
"""
import os
import pandas as pd

from dotenv import load_dotenv
from dotenv import find_dotenv
load_dotenv(find_dotenv())
import platform

import sys
sys.path.insert(0, os.environ.get('PROJECTS_PATH_FOLDER'))
from ximp_utils.database import get_conn_sirat
from ximp_utils.log import registra_log
from ximp_utils.requests import get_html_response
from ximp_utils.requests import post_response
from ximp_utils.requests import download
from ximp_utils.email import send_email


def create_download_folder():
    # Create directory
    dirName = os.path.join('downloads')
 
    try:
        # Create target Directory
        os.mkdir(dirName)
        print("Directory", dirName, "Created ")
    except Exception:
        print("Directory", dirName, "already exists")


def get_list_files_cvm_site():
    url = 'http://dados.cvm.gov.br/dados/FI/CAD/DADOS/'
    response = get_html_response(url, stream=False, headers=None)

    links = []
    for link in response.html.absolute_links:
        if not link.endswith('.csv'):
            continue
        links.append(link)

    return links


def main():
    # main
    create_download_folder()

    link = sorted(get_list_files_cvm_site())[-1]

    print('O último arquivo disponível é esse {}'.format(link))

    file_name = link.split('/')[-1]
    file_path = os.path.join('downloads', file_name)

    if not os.path.exists(file_path):
        print('Fazendo download do arquivo {}'.format(link))
        download(link, file_path)

    print('Lendo csv no pandas dataframe')
    df = pd.read_csv(file_path, sep=";", encoding="latin1")
    print(df.head())

    print(df.shape)

    print('Colunas do CSV:')
    print(df.columns.values)

    # lista apenas os fundos em funcionamento
    df = df[df['SIT'] == 'EM FUNCIONAMENTO NORMAL']
    print(df.shape)

    # lista apenas os fundos administrados pela CAIXA ECONOMICA FEDERAL
    df = df[df['ADMIN'] == 'CAIXA ECONOMICA FEDERAL']
    print(df.shape)

    # formata o campo CO_PRD com o cnpj 
    df['CO_PRD'] = df['CNPJ_FUNDO'].str.replace(".", "")
    df['CO_PRD'] = df['CO_PRD'].str.replace("-", "")
    df['CO_PRD'] = df['CO_PRD'].str.replace("/", "")

    # engine de conexão com o banco de dados
    print('Lendo base de dados SIRAT - tabela produto')
    sql_conn = get_conn_sirat()
    sql_query = 'SELECT * FROM [SIRAT].[dbo].[Produto]'
    df_produto = pd.read_sql_query(sql_query, con=sql_conn)

    df_inner = pd.merge(df, df_produto, how='inner', on = 'CO_PRD')
    df_left = pd.merge(df, df_produto, how='left', on = 'CO_PRD')

    df_saida = pd.concat([df_left,df_inner]).drop_duplicates(keep=False)
    print(df_saida.head())
    msg = 'Existem {} fundos na CVM que não foram cadastrados na Rotina Diária'
    print(msg.format(len(df_saida)))

    if len(df_saida) > 0:
        emailFrom = 'gerat04@mail.caixa'
        emailTo = 'gerat04@mail.caixa'
        subject = 'SIRAT - {} fundo(s) não cadastrado(s) na rotina diária'.format(len(df_saida))
        mensagem = df_saida[['CO_PRD', 'DENOM_SOCIAL']].to_string()
        send_email(emailFrom, emailTo, subject, mensagem)

    # registra o log
    conn = get_conn_sirat()
    user = os.environ.get("USERNAME", platform.node())
    log_registrado = registra_log(conn, 'verifica_fundos_cvm_nao_cadastrados_no_sirat', user)

    if log_registrado:
        print('LOG registrado com sucesso')


if __name__ == '__main__':
    main()