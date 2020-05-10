# -*- coding: utf-8 -*-
import os
import pandas as pd
import platform
import dask.dataframe as dd
from sqlalchemy import create_engine
from sqlalchemy import text
import time

from dotenv import load_dotenv
from dotenv import find_dotenv
load_dotenv(find_dotenv())

import sys
sys.path.insert(0, os.environ.get('PROJECTS_PATH_FOLDER'))

from ximp_utils.database import get_conn_sirat
from ximp_utils.log import registra_log


def delete_registros_antigos(engine, dt_min):
    sql = text('DELETE FROM fundos_cvm_info_diaria WHERE "DT_REF" >= :dt_ref')
    result = engine.execute(sql, dt_ref=dt_min)
    return True


def main():
    # engine de conexão com o banco de dados
    print('Criando conexão com o banco de dados')
    engine = create_engine(
        'postgresql+psycopg2://test:test@localhost:5406/test',
        #os.environ.get('SQLALCHEMY_DATABASE_URI'),
        connect_args={'connect_timeout': 9999999}
    )

    base_path = os.path.join('downloads')

    # lê todos os arquivos CSVs da pasta usando o dask
    df = dd.read_csv(
        'downloads/inf_diario_fi_*.csv',
        sep=';',
        header=0,
        encoding='latin1'
    )

    df = df.compute()
    print(f'O dataframe tem {len(df)} registros')

    # cria e formata o campo DT_REF, com a data de referência - errors='raise'
    print('Formatando campo DT_REF')
    df['DT_COMPTC'] = pd.to_datetime(
        df['DT_COMPTC'],
        format='%Y-%m-%d',
        errors='coerce'
    )
    df = df.dropna(subset=['DT_COMPTC'])
    df['DT_REF'] = df['DT_COMPTC']

    # formata o campo CO_PRD com o cnpj
    df['CO_PRD'] = df['CNPJ_FUNDO'].str.replace(".", "")
    df['CO_PRD'] = df['CO_PRD'].str.replace("-", "")
    df['CO_PRD'] = df['CO_PRD'].str.replace("/", "")
    df['CO_PRD'] = df['CO_PRD'].str.zfill(14)

    # seta os índices do dataframe
    df.set_index(['CO_PRD', 'DT_REF'])

    # faz o sort do df
    df.sort_values(by=['CO_PRD', 'DT_REF'], inplace=True)

    print('Antes de remover os duplicados', len(df))
    # remove registros duplicados, baseados no CO_PRD e DT_REF
    df = df.drop_duplicates(subset=['CO_PRD', 'DT_REF'], keep='last')
    print('Depois de remover os duplicados', len(df))

    # cria uma nova coluna com o percentual de resgate para o dia
    df['PC_RESG'] = (df['RESG_DIA'] / df.groupby(['CO_PRD'])['VL_PATRIM_LIQ'].shift(1))

    # remove registros em que não se conseguiu calcular o PC_RESG
    df = df.dropna(subset=['PC_RESG'])

    print('Data mínima do arquivo baixado', df['DT_REF'].min().date())
    print('Data máxima do arquivo baixado', df['DT_REF'].max().date())

    print('Limpa registros antigos da tabela que serão substituídos')
    dt_min = df['DT_REF'].min().date()
    delete_registros_antigos(engine, dt_min)

    nome_tabela = 'fundos_cvm_info_diaria'

    print('Importando as datas para a tabela')
    df_datas = df['DT_REF'].unique()
    df_datas = pd.DataFrame(df_datas)
    df_datas.to_sql(
        nome_tabela+'_datas',
        engine,
        if_exists='replace',
        index=False,
        chunksize=1000000
    )

    print('Importando registros para a tabela', nome_tabela)

    df.to_sql(
        nome_tabela,
        engine,
        if_exists='append',
        index_label='pk_fundos_cvm_info_diaria',
        index=False,
        chunksize=1000000
    )

    print('{} registros importados com sucesso'.format(len(df)))

    print('Importando as datas para a tabela')
    df_datas = df['DT_REF'].unique().tolist()
    df_datas = pd.DataFrame(df_datas, columns=['DT_REF'], index=['DT_REF'])
    df_datas.to_sql(
        nome_tabela+'_datas',
        engine,
        if_exists='replace',
        index=False,
        chunksize=1000000
    )

    # conexão com banco de dados
    # conn = get_conn_sirat()
    # registra o log
    user = os.environ.get("USERNAME", platform.node())
    #log_registrado = registra_log(conn, 'atualiza_database_dados_diarios_fundos_cvm', user)

    #if log_registrado:
        #print('LOG registrado com sucesso')


if __name__ == '__main__':
    start_time = time.time()
    main()
    print("--- %s seconds ---" % (time.time() - start_time))
