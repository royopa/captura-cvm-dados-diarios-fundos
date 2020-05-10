# -*- coding: utf-8 -*-
import os
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy import text
from dotenv import load_dotenv
from dotenv import find_dotenv
load_dotenv(find_dotenv())


def remove_registros(engine, mes, ano):
    sql_delete = 'DELETE FROM fundos_cvm_info_diaria'
    sql = "{} WHERE date_part('year', \"DT_REF\") = {} AND date_part('month', \"DT_REF\") = {}"
    sql = sql.format(sql_delete, int(ano), int(mes))
    sql = text(sql)
    print(sql)
    engine.execute(sql)


def main():
    # engine de conexão com o banco de dados
    engine = create_engine(os.environ.get('SQLALCHEMY_DATABASE_URI'))

    base_path = os.path.join('downloads')

    df = None    
    
    for file_name in os.listdir(base_path):
        if file_name.endswith('.csv') is False:
            continue
    
        file_path = os.path.join(base_path, file_name)
        print('Lendo arquivo', file_path)

        print(file_name.split('inf_diario_fi_')[1].split('.csv')[0])
        data_arquivo = file_name.split('inf_diario_fi_')[1].split('.csv')[0]
        ano = data_arquivo[0:4].strip()
        mes = data_arquivo[4:]

        # remove os registros antes de importar na base
        print('Removendo registros da mesma data')
        remove_registros(engine, mes, ano)

        df_import = pd.read_csv(file_path, sep=';', header=0)        
        # remove o arquivo já importado
        os.remove(file_path)
        
        if df is None:
            df = df_import
            continue

        # faz o merge
        df = pd.concat([df, df_import])
        del(df_import)

    if df is None:
        return        

    # cria e formata o campo DT_REF, com a data de referência
    df['DT_REF'] = pd.to_datetime(df['DT_COMPTC'])
   
    # formata o campo CO_PRD com o cnpj 
    df['CO_PRD'] = df['CNPJ_FUNDO'].str.replace(".", "")
    df['CO_PRD'] = df['CO_PRD'].str.replace("-", "")
    df['CO_PRD'] = df['CO_PRD'].str.replace("/", "")
    
    # seta os índices do dataframe
    df.set_index(['CO_PRD', 'DT_REF'])

    print('Antes de remover os duplicados', len(df))
    # remove registros duplicados, baseados no CO_PRD e DT_REF
    df = df.drop_duplicates(subset=['CO_PRD', 'DT_REF'], keep=False)
    print('Depois de remover os duplicados', len(df))

    # cria uma nova coluna com o percentual de resgate para o dia
    df['PC_RESG'] = (df['RESG_DIA'] / df.groupby(['CO_PRD'])['VL_PATRIM_LIQ'].shift(1))
        
    nome_tabela = 'fundos_cvm_info_diaria'
    print('Importando registros para a tabela', nome_tabela)
        
    # https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.to_sql.html
    df.to_sql(
        nome_tabela,
        engine,
        if_exists='append',
        index_label='pk_fundos_cvm_info_diaria',
        index=False,
        chunksize=1000
    )
        
    print('{} registros importados com sucesso'.format(len(df)))


if __name__ == '__main__':
    main()
