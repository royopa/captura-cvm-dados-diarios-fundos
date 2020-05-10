# -*- coding: utf-8 -*-
import os
import sys

sys.path.insert(0, os.environ.get('PROJECTS_PATH_FOLDER'))

import platform
from datetime import datetime
from datetime import date
from datetime import timedelta
import dateutil.relativedelta

import shutil
import time

import update_database
import cvm_analise_informacoes_cadastro_fundos
from bs4 import BeautifulSoup

from dotenv import load_dotenv
from dotenv import find_dotenv
load_dotenv(find_dotenv())

from ximp_utils.requests import get_response
from ximp_utils.requests import download
from ximp_utils.database import get_conn_sirat
from ximp_utils.log import registra_log


def get_ultima_data_modificacao_site_cvm(soup):
    # verifica as datas de atualização
    datas = soup.findAll("td", {"class": "indexcollastmod"})
    data_ultima_atualizacao = datas[-1].text.strip()
    return datetime.strptime(data_ultima_atualizacao, '%Y-%m-%d %H:%M')


def prepara_pasta_inicial(folder_name):
    if os.path.exists(folder_name) is True:
        shutil.rmtree(folder_name)

    if os.path.exists(folder_name) is False:
        os.mkdir(folder_name)


def main():
    prepara_pasta_inicial('downloads')

    # as informações diárias da CVM são atualizadas diariamente com 2 dias 
    # de atraso, então pega a data de d-2 como referência
    data_referencia = date.today() - timedelta(days=2)
    # utiliza também o arquivo do mês anterior para fazer o percentual de resgate
    data_mes_anterior = data_referencia + dateutil.relativedelta.relativedelta(months=-1)    

    # abre a página da CVM para verificar os arquivos disponíveis
    base_url= 'http://dados.cvm.gov.br/dados/FI/DOC/INF_DIARIO/DADOS/'
    response = get_response(base_url, False)
    html = response.text

    # transforma o html retornado num objeto beautifulsoup
    soup = BeautifulSoup(html, 'lxml')
    
    # dados ainda não atualizados no site da CVM
    data_ultima_atualizacao = get_ultima_data_modificacao_site_cvm(soup)
    if datetime.today() < data_ultima_atualizacao:
        print('Sai do programa, dados na CVM não atualizados')
        exit()

    # recupera os links da página
    links = soup.find_all('a')

    for link in links:
        href = link.attrs["href"]
        if href.endswith('.csv'):
            url = base_url+href
            file_path = os.path.join('downloads', href)

            # baixa arquivo do mes anterior
            if data_mes_anterior.strftime('%Y%m') in href:
                print('Baixando arquivo', url)
                download(url, file_path)
                continue

            # baixa arquivo do mes atual
            if data_referencia.strftime('%Y%m') in href:
                print('Baixando arquivo', url)
                download(url, file_path)
                continue

    # conexão com banco de dados
    #conn = get_conn_sirat()
    # registra o log
    user = os.environ.get("USERNAME", platform.node())

    #log_registrado = registra_log(conn, 'captura_dados_diarios_fundos_cvm', user)

    #if log_registrado:
        #print('LOG registrado com sucesso')
        #update_database.main()
        #cvm_analise_informacoes_cadastro_fundos.main()


if __name__ == '__main__':
    main()
