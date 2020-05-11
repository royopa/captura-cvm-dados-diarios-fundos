# -*- coding: utf-8 -*-
import os
import sys

sys.path.insert(0, os.environ.get('PROJECTS_PATH_FOLDER'))

import platform
from datetime import datetime
from datetime import date
from datetime import timedelta
import dateutil.relativedelta
from sqlalchemy import create_engine

import shutil
import time

import update_database
import cvm_analise_informacoes_cadastro_fundos
from bs4 import BeautifulSoup
import pandas as pd

from dotenv import load_dotenv
from dotenv import find_dotenv
load_dotenv(find_dotenv())

from ximp_utils.requests import get_response
from ximp_utils.requests import download
from ximp_utils.database import get_conn_sirat
from ximp_utils.log import registra_log


def get_links_arquivos(base_url, engine):
    # acessa a tabela da cvm com a lista dos arquivos
    df = pd.read_html(base_url, attrs = {'id': 'indexlist'})[0]
    # cria as colunas com os nomes corretos
    df['url'] = df['Name']
    df['dt_modificacao'] = df['Last modified']
    # seleciona apenas as colunas que serão usadas
    df = df[['url', 'dt_modificacao']]
    # formata o caminho completo da url
    df['url'] = base_url + df['url']
    # formata o campo dt_modificação
    df['dt_modificacao'] = pd.to_datetime(
        df['dt_modificacao'],
        format='%Y-%m-%d %H:%M',
        errors='coerce'
    )
    df['dt_captura'] = datetime.today()
    df = df.dropna(subset=['dt_modificacao'])
    return df


def importa_df_links(df, nome_tabela, engine):
    df.to_sql(
        nome_tabela,
        engine,
        if_exists='append',
        index=False
    )
    print(f'{len(df)} registros importados com sucesso')
    return True


def prepara_pasta_inicial(folder_name):
    if os.path.exists(folder_name) is True:
        shutil.rmtree(folder_name)

    if os.path.exists(folder_name) is False:
        os.mkdir(folder_name)


def main():
    prepara_pasta_inicial('downloads')

    # engine de conexão com o banco de dados
    print('Criando conexão com o banco de dados')
    engine = create_engine(
        os.environ.get('SQLALCHEMY_DATABASE_URI'),
        connect_args={'connect_timeout': 9999999}
    )

    # as informações diárias da CVM são atualizadas diariamente com 2 dias 
    # de atraso, então pega a data de d-2 como referência
    data_referencia = date.today() - timedelta(days=2)
    
    # utiliza também o arquivo do mês anterior para calcular o percentual de resgate
    data_mes_anterior = data_referencia + dateutil.relativedelta.relativedelta(months=-1)    

    # abre a página da CVM para verificar os arquivos disponíveis
    base_url= 'http://dados.cvm.gov.br/dados/FI/DOC/INF_DIARIO/DADOS/'

    # pega o link dos arquivos e salva no banco de dados
    print('Capturando urls para serem baixadas')
    df_links = get_links_arquivos(base_url, engine)

    # percorre os df_links
    for index, row in df_links.iterrows():
        url = row['url']
        file_name = url.split('/')[-1]
        file_path = os.path.join('downloads', file_name)

        # Pula arquivos que não são csv
        if file_name.endswith('.csv') is False:
            continue

        # baixa arquivo do mes anterior
        if data_mes_anterior.strftime('%Y%m') in file_name:
            print(f'{index} Baixando arquivo {url}')
            download(url, file_path)
            continue

        # baixa arquivo do mes atual
        if data_referencia.strftime('%Y%m') in file_name:
            print(f'{index} Baixando arquivo {url}')
            download(url, file_path)
            continue

    # importa para a base de dados
    importa_df_links(df_links, 'fundos_cvm_info_diaria_links', engine)

    # conexão com banco de dados
    conn = get_conn_sirat()
    # registra o log
    user = os.environ.get("USERNAME", platform.node())

    log_registrado = registra_log(conn, 'captura_dados_diarios_fundos_cvm', user)

    if log_registrado:
        print('LOG registrado com sucesso')
        update_database.main()
        cvm_analise_informacoes_cadastro_fundos.main()


if __name__ == '__main__':
    main()
