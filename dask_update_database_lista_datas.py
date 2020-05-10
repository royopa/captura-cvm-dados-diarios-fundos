# -*- coding: utf-8 -*-
import os
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy import text
import dask.dataframe as dd
from dotenv import load_dotenv
from dotenv import find_dotenv
load_dotenv(find_dotenv())


def main():
    # engine de conexão com o banco de dados
    engine = create_engine(os.environ.get('SQLALCHEMY_DATABASE_URI'))

    # lê todos os arquivos CSVs da pasta usando o dask
    df = dd.read_csv('downloads/inf_diario_fi_*.csv', sep=';', header=0)
    print(f'O dataframe tem {len(df)} registros')

    # formata o campo CO_PRD com o cnpj
    print('Formatando campo CO_PRD')
    df = df.dropna(subset=['CNPJ_FUNDO'])
    df['CO_PRD'] = df['CNPJ_FUNDO'].str.replace(".", "")
    df['CO_PRD'] = df['CO_PRD'].str.replace("-", "")
    df['CO_PRD'] = df['CO_PRD'].str.replace("/", "")
    df['CO_PRD'] = df['CO_PRD'].str.zfill(14)

    # converte o dataframe dask para pandas
    print('Convertendo o df dask para pandas')
    df = df.compute()

    # cria e formata o campo DT_REF, com a data de referência - errors='raise'
    print('Formatando campo DT_REF')
    df.assign(DT_COMPTC=pd.to_datetime(df['DT_COMPTC'], format='%Y-%m-%d', errors='coerce'))
    df = df.dropna(subset=['DT_COMPTC'])
    df['DT_REF'] = df['DT_COMPTC']

    print(f' Antes de remover duplicados, o dataframe tem {len(df)} registros')
    # remove registros duplicados, baseados no CO_PRD e DT_REF
    print('Removendo registros duplicados')
    df = df.drop_duplicates(subset=['CO_PRD', 'DT_REF'], keep='last')
    print(f'Depois de remover duplicados, o dataframe tem {len(df)} registros')

    # cria uma nova coluna com o percentual de resgate para o dia
    print('Calculando campo PC_RESG')
    df['PC_RESG'] = (df['RESG_DIA'] / df.groupby(['CO_PRD'])['VL_PATRIM_LIQ'].shift(1))

    print(len(df))

    print(df.head())

    print(df.tail())

    print('Salvando arquivo de saída')
    df.to_csv('saida.csv', index=False)


if __name__ == '__main__':
    main()
