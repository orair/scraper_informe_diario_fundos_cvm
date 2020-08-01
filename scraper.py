
# -*- coding: utf-8 -*-
import os
from pathlib import Path
# morph.io requires this db filename, but scraperwiki doesn't nicely
# expose a way to alter this. So we'll fiddle our environment ourselves
# before our pipeline modules load.
os.environ['SCRAPERWIKI_DATABASE_NAME'] = 'sqlite:///data.sqlite'
import datetime
from datetime import datetime, timedelta
import click
import scraperwiki
import pandas as pd
from six.moves import urllib
import shutil
import zipfile
import requests
import bizdays
import sqlite3
import sqlalchemy
import tqdm

@click.command("Gerenciador do Scraper dos Informes Diários de Fundos de Investimentos da CVM")
@click.option('--skip_informacoes_cadastrais', 
                default=False, is_flag=True, 
                show_default=True)
@click.option('--skip_informe_diario_historico', 
                default=False, is_flag=True, 
                show_default=True)
@click.option('--ano_inicial', 
                default=lambda: 
                    os.environ.get('MORPH_SCRAPER_INFORME_CVM_ANO_INICIAL', 2019), 
                show_default="Variável de ambiente MORPH_SCRAPER_INFORME_DIARIO_CVM_ANO_INICIAL ou o valor padrão 2018")
@click.option('--compara_antes_insercao', 
                default=lambda: 
                    os.environ.get('MORPH_SCRAPER_COMPARA_ANTES_INSERCAO', 'N'), 
                show_default=True)
def executa_scraper(skip_informacoes_cadastrais=False, skip_informe_diario_historico=False, ano_inicial=2018,
    compara_antes_insercao=True):
    init()

    if (compara_antes_insercao == 'N'):
        compara_antes_insercao=False
    else: compara_antes_insercao=True

    executa_scraper_informe_diario_por_periodo(obtem_ultimo_periodo(), compara_antes_insercao)

    if (not skip_informacoes_cadastrais):
        executa_scraper_dados_cadastrais()

    print(f'Ano inicial para buscar os informes diários {ano_inicial}')
    if (not skip_informe_diario_historico):
        executa_scraper_informe_diario_historico(int(ano_inicial))

def executa_scraper_informe_diario_historico(ano_inicial):
    periodos = obtem_periodos(ano_inicial)
    for periodo in periodos: 
        executa_scraper_informe_diario_por_periodo(periodo)

def executa_scraper_informe_diario_por_periodo(periodo, compara_antes_insercao=True):
    df2 = None
    result, informe_diario_df = captura_arquivo_informe(periodo)
    
    # Caso tenha sido obtida e lido um novo arquivo com sucesso...
    if not informe_diario_df.empty and result in (1,2):
        informe_diario_df.sort_values(by=['COD_CNPJ', 'DT_REF'])
        df2 = None
        if compara_antes_insercao:
            df2 = recupera_informe_diario(periodo)
        else: df2 = pd.DataFrame()
        if not df2.empty:
            df2['DT_REF']=pd.to_datetime(df2['DT_REF'], errors='coerce', format='%Y-%m-%d')
        existe_dados_origem=(informe_diario_df is not None \
        and not informe_diario_df.empty)
        existe_dados_diferentes=(df2.empty or (not informe_diario_df.equals(df2)))
    else:
        existe_dados_origem = False    
    if  existe_dados_origem and existe_dados_diferentes:
        if df2.empty:
            salva_informe_periodo(informe_diario_df, periodo)
        else:
            print('Iniciando a comparação dos dados recebidos e dos dados já inseridos no banco de dados...')
            novos_dados=[]
            #print(type(df2), type(informe_diario_df))
            #list2=df2.values.tolist()
            #print(list2)
            #set2=set(df2.values.tolist()) 
            set2 = set(map(tuple, df2.values.tolist()))
            set1 = set(map(tuple, informe_diario_df.values.tolist())) 
            res = list(set2 ^ set1) 
            novos_dados_df = pd.DataFrame(data=res, columns=informe_diario_df.columns)
            novos_dados_df.sort_values(by=['COD_CNPJ', 'DT_REF'])
            #print(novos_dados_df.head())
            #merge_df=pd.merge(informe_diario_df,df2, how='left', indicator=True)
            #merge_df=pd.merge_ordered(informe_diario_df, df2, how='left', suffixes=['', '_'])
            #print('merge finalizado')
            #print(merge_df.columns)
            #print(len(merge_df.index), len(informe_diario_df.index))
            #print(merge_df.head())
            #del(df2)
            #del(informe_diario_df)
            #novos_dados_df=merge_df[merge_df['_merge']=='left_only']
            #novos_dados_df=novos_dados_df.drop(['_merge'], axis=1)

            #del(merge_df)
            
            print (f'Foram encontrados {len(informe_diario_df.index)} registros no arquivo, sendo {len(novos_dados_df.index)} novos registros...')
            # Como o scraperwiki fornece apenas o save que faz um autocommit 
            # por registro, só vamos salvar no banco os registros que 
            # já identificarmos que são realmente novos dados 
            salva_informe_periodo(novos_dados_df, periodo)
    
    else:
        print (f'Não foram encontrados novos registros no arquivo para o periodo {periodo}...')


def recupera_informe_diario(periodo):
    query=f"COD_CNPJ, DT_REF, CNPJ_FUNDO, DT_COMPTC, VL_TOTAL, VL_QUOTA, VL_PATRIM_LIQ, CAPTC_DIA, RESG_DIA, NR_COTST from informe_diario where strftime('%Y%m', DT_REF) = '{periodo}' order by COD_CNPJ, DT_REF"
    result=scraperwiki.sql.select(query)
    df=pd.DataFrame(result)
    return df

def captura_arquivo_informe(periodo):
    base_url = f'http://dados.cvm.gov.br/dados/FI/DOC/INF_DIARIO/DADOS/'
    filename=f'inf_diario_fi_{periodo}.csv'

    print(f'Verifica necessidade de download dos informes diários de {periodo}')
    result=_download_file(base_url, filename)

    # No início de meses, ainda não existira o arquivo com as cotas dos meses correntes,
    # Por isto, vamos retornar simplesmente um dataframe vazio caso não encontre 
    # o arquivo na url consultada
    if result == 404:
        df_empty = pd.DataFrame({'A' : []})
        print(f'Não foi encontrado o arquivo {filename} no servidor da CVM, se for o início de um mês e o arquivo se referir ao mês atual, isto é esperado...')
        return 0, df_empty
        
    try:
        # Realiza a leitura dos dados como csv
        # O delimitador dos csvs da CVM é o ';'
        # Como os dados estão salvos em ISO-8859-1, 
        # é necessário alterar o encoding durante a leitura dos dados
        df = pd.read_csv(
            filename,
            sep=';',
            encoding='latin1'
        )
    except (IOError, urllib.error.HTTPError) as err:        
        print('Falha na leitura do arquivo ', filename ,'...', err)
        print(type(err))    # the exception instance
        print(err.args)     # arguments stored in .args
        df_empty = pd.DataFrame({'A' : []})
        return 0, df_empty
    except Exception as err:
        print('Erro ao baixar arquivo', filename, '...', err)
        print(type(err))    # the exception instance
        print(err.args)     # arguments stored in .args
        df_empty = pd.DataFrame({'A' : []})
        return 0, df_empty

    # Cria um campo só com os números do CNPJ na primeira coluna
    df.insert(0, 'COD_CNPJ', df['CNPJ_FUNDO'].str.replace(r'\D+', '').str.zfill(14))
    # Cria um campo com a data formatada na segunda coluna
    df.insert(1, 'DT_REF', pd.to_datetime(df['DT_COMPTC'], errors='coerce', format='%Y-%m-%d'))

    return result, df

def obtem_ultimo_periodo():
    yesterday = datetime.today() - timedelta(days=1)
    ano = int(yesterday.strftime('%Y'))
    mes = int(yesterday.strftime('%m'))
    periodo=f'{ano:04}{mes:02}'
    return periodo

def obtem_periodos(ano_inicial=2018):
    periodos=[]

    today = datetime.today()
    yesterday = today - timedelta(days=1)
    ano_final = int(yesterday.strftime('%Y'))
    mes_final = int(yesterday.strftime('%m'))

    for ano in range(ano_inicial, ano_final+1):
        for mes in range(1,13):
            # evita pegar anos futuros, visto que o arquivo ainda não existe
            if ano == ano_final and mes > mes_final:
                break

            periodo=f'{ano:04}{mes:02}'
            periodos.append(periodo)
    
    periodos.sort(reverse=True)

    # TODO: Retirar períodos maiores que 12 meses que já tiverem sido carregados
    # "Os arquivos referentes aos meses M-2, M-3, ..., até M-11 
    # serão atualizados semanalmente com as eventuais reapresentações.

    return periodos

def salva_informe_periodo(informe_diario_df, periodo):
    if informe_diario_df is None or informe_diario_df.empty:
        print('Recebeu dados vazios!')
        return False
    
    try:
        print("Excluindo índices para inserção mais eficiente...")
        drop_indexes_informe_diario()
        print(f'Iniciando inserção no banco de dados de {len(informe_diario_df.index)} registros.')
        
        records_list=informe_diario_df.to_dict('records')
        batch_size = 5000
        chunks = (len(records_list) - 1) // batch_size + 1
        for i in tqdm.tqdm(range(chunks)):
            batch = records_list[i*batch_size:(i+1) * batch_size]
            scraperwiki.sqlite.save(unique_keys=['COD_CNPJ', 'DT_REF'], data=batch, table_name='informe_diario')
        #for row in records_list:
        #for row in tqdm.tqdm(records_list):
            #print('linha a ser inserida no banco', row)
            #scraperwiki.sqlite.save(unique_keys=['COD_CNPJ', 'DT_REF'], data=row, table_name='informe_diario')
        print('Recriando os índices da tabela informe diário...')
        create_indexes_informe_diario()
        print('Carga no banco de dados finalizada...')
    except Exception as err:
        print(f'Falha ao salvar registros no banco de dados para o período {periodo}', err)
        print(type(err))    # the exception instance
        print(err.args)     # arguments stored in .args
        return None

def executa_scraper_dados_cadastrais():
    from bizdays import Calendar
    cal = Calendar.load('feriados_nacionais_ANBIMA.csv')
    
    # tentaremos obter dados cadastrais de três dias atrás
    periodo = (datetime.today()- timedelta(days=3))
    while not cal.isbizday(periodo):
        periodo = periodo - datetime.timedelta(days=1)    

    periodo = periodo.strftime('%Y%m%d')
    print (f'Serão obtidos os dados cadastrais publicados pela CVM em {periodo}')
    df = captura_arquivo_dados_cadastrais(periodo)
    if df is None or df.empty:
        print('Recebeu dados vazios!')
        return False
    return salva_dados_cadastrais(df)

def captura_arquivo_dados_cadastrais(periodo):
    base_url = f'http://dados.cvm.gov.br/dados/FI/CAD/DADOS/'
    filename=f'inf_cadastral_fi_{periodo}.csv'

    print(f'Verifica necessidade de download dos dados cadastrais dos fundos de investimento')
    _download_file(base_url, filename)

    df = None
    try:
        # Realiza a leitura dos dados como csv
        # O delimitador dos csvs da CVM é o ';'
        # Como os dados estão salvos em ISO-8859-1, 
        # é necessário alterar o encoding durante a leitura dos dados
        df = pd.read_csv(
            filename,
            sep=';',
            encoding='latin1'
        )
        #print(df.head())
    except (IOError, urllib.error.HTTPError) as err:        
        print('Falha na leitura do arquivo ', filename, '...', err)
        print(type(err))    # the exception instance
        print(err.args)     # arguments stored in .args
        return None
    except Exception as err:
        print('Erro ao ler arquivo localmente', filename, '...', err)
        print(type(err))    # the exception instance
        print(err.args)     # arguments stored in .args
        return None

    print(f'Foram lidos {len(df.index)} registros do arquivos.')

    # Filtra por situações dos fundos
    #    CANCELADA
    #    EM FUNCIONAMENTO NORMAL
    #    FASE PRÉ-OPERACIONAL
    situacoesDescartadas=['CANCELADA', 'FASE PRÉ-OPERACIONAL']
    df=df[~df.SIT.isin(situacoesDescartadas)]

    print(f'Após o filtro dos fundos cancelados ou em fase pré-operacional, obteve-se {len(df.index)} fundos.')

    # Cria um campo só com os números do CNPJ
    df['COD_CNPJ'] = df['CNPJ_FUNDO'].str.replace(r'\D+', '').str.zfill(14)

    # TODO: Descartar colunas desinteressantes
    #
    # Filtra as colunas que interessam
    # idxColunas=[0,1,5,10,12,13,14,15,16,17,18,19,20,21,24,25,26,27,28,29,34,35]
    return df

def salva_dados_cadastrais(df):
    if df is None or df.empty:
        print('Recebeu dados cadastrais vazios!')
        return False

    try:
        records_list=df.to_dict('records')
        batch_size = 1000
        chunks = (len(records_list) - 1) // batch_size + 1
        for i in tqdm.tqdm(range(chunks)):
            batch = records_list[i*batch_size:(i+1) * batch_size]
            scraperwiki.sqlite.save(unique_keys=['COD_CNPJ'], data=batch, table_name='dados_cadastrais')
        #for row in df.to_dict('records'):
        #    scraperwiki.sqlite.save(unique_keys=['COD_CNPJ'], data=row, table_name='dados_cadastrais')

    except Exception as err:
        print(f'Falha ao salvar registros no banco de dados dos dados cadastrais dos fundos', err)
        print(type(err))    # the exception instance
        print(err.args)     # arguments stored in .args
        return None

def init():
    init_database()

def init_database():
    create_tables()
    #drop_indexes()
    create_indexes()
    create_views()
    
def create_tables():
    """ Será necessário criar a tabela inicialmente pois o SQlite infere os tipos errados
       o que faz falhar a carga
    """
    sql_create_table_dados_cadastrais='''CREATE TABLE IF NOT EXISTS dados_cadastrais (
        "COD_CNPJ" TEXT NOT NULL PRIMARY KEY,
        "CNPJ_FUNDO" TEXT NOT NULL, 	
        "DENOM_SOCIAL" TEXT, 	
        "DT_REG" TEXT, 	
        "DT_CONST" TEXT, 	
        "DT_CANCEL" TEXT, 	
        "SIT" TEXT, 	
        "DT_INI_SIT" TEXT, 	
        "DT_INI_ATIV" TEXT, 	
        "DT_INI_EXERC" TEXT, 	
        "DT_FIM_EXERC" TEXT, 	
        "CLASSE" TEXT, 	
        "DT_INI_CLASSE" TEXT, 	
        "RENTAB_FUNDO" TEXT, 	
        "CONDOM" TEXT, 	
        "FUNDO_COTAS" TEXT, 	
        "FUNDO_EXCLUSIVO" TEXT, 	
        "TRIB_LPRAZO" TEXT, 	
        "INVEST_QUALIF" TEXT, 	
        "TAXA_PERFM" TEXT, 	
        "INF_TAXA_PERFM" TEXT, 	
        "TAXA_ADM" TEXT, 	
        "INF_TAXA_ADM" TEXT, 	
        "VL_PATRIM_LIQ" TEXT, 	
        "DT_PATRIM_LIQ" TEXT, 	
        "DIRETOR" TEXT, 	
        "CNPJ_ADMIN" TEXT, 	
        "ADMIN" TEXT, 	
        "PF_PJ_GESTOR" TEXT, 	
        "CPF_CNPJ_GESTOR" TEXT, 	
        "GESTOR" TEXT, 	
        "CNPJ_AUDITOR" TEXT, 	
        "AUDITOR" TEXT, 	
        "CNPJ_CUSTODIANTE" TEXT, 	
        "CUSTODIANTE" TEXT, 	
        "CNPJ_CONTROLADOR" TEXT, 	
        "CONTROLADOR" TEXT
        );    
    '''
    #scraperwiki.sqlite.execute('DROP TABLE dados_cadastrais;')

    scraperwiki.sqlite.execute(sql_create_table_dados_cadastrais)

    sql_create_table='''CREATE TABLE IF NOT EXISTS informe_diario (
        "COD_CNPJ" TEXT NOT NULL, 	
        "CNPJ_FUNDO" TEXT NOT NULL, 	
        "DT_REF" DATE NOT NULL, 	
        "DT_COMPTC" TEXT NOT NULL, 	
        "VL_TOTAL" NUMERIC, 	
        "VL_QUOTA" NUMERIC, 	
        "VL_PATRIM_LIQ" NUMERIC, 	
        "CAPTC_DIA" NUMERIC, 	
        "RESG_DIA" NUMERIC, 	
        "NR_COTST" INTEGER, 	
        PRIMARY KEY(COD_CNPJ, DT_REF)
    );    
    '''
    scraperwiki.sqlite.execute(sql_create_table)

def drop_indexes_dados_cadastrais():
    scraperwiki.sqlite.execute('DROP INDEX IF EXISTS idx_dados_cadastrais_01;')
    scraperwiki.sqlite.execute('DROP INDEX IF EXISTS idx_dados_cadastrais_02;')
    scraperwiki.sqlite.execute('DROP INDEX IF EXISTS idx_dados_cadastrais_03;')
    scraperwiki.sqlite.execute('DROP INDEX IF EXISTS idx_dados_cadastrais_04;')
    scraperwiki.sqlite.execute('DROP INDEX IF EXISTS idx_dados_cadastrais_05;')
    scraperwiki.sqlite.execute('DROP INDEX IF EXISTS idx_dados_cadastrais_06;')
    scraperwiki.sqlite.execute('DROP INDEX IF EXISTS idx_dados_cadastrais_07;')
    scraperwiki.sqlite.execute('DROP INDEX IF EXISTS idx_dados_cadastrais_08;')
 
def drop_indexes_informe_diario():
    scraperwiki.sqlite.execute('DROP INDEX IF EXISTS idx_informe_diario_01;')
    scraperwiki.sqlite.execute('DROP INDEX IF EXISTS idx_informe_diario_02;')
    scraperwiki.sqlite.execute('DROP INDEX IF EXISTS idx_informe_diario_03;')
    scraperwiki.sqlite.execute('DROP INDEX IF EXISTS idx_informe_diario_04;')
    scraperwiki.sqlite.execute('DROP INDEX IF EXISTS idx_informe_diario_05;')
    scraperwiki.sqlite.execute('DROP INDEX IF EXISTS idx_informe_diario_06;')
    scraperwiki.sqlite.execute('DROP INDEX IF EXISTS idx_informe_diario_07;')
    scraperwiki.sqlite.execute('DROP INDEX IF EXISTS idx_informe_diario_08;')
    
def create_indexes():
    create_indexes_dados_cadastrais()
    create_indexes_informe_diario()
    
def create_indexes_dados_cadastrais():
    print('Criando índices na tabela dados cadastrais...')
    sql_create_idx='''CREATE UNIQUE INDEX IF NOT EXISTS idx_dados_cadastrais_02
        ON dados_cadastrais (COD_CNPJ);
    '''
    scraperwiki.sqlite.execute(sql_create_idx)

    sql_create_idx='''CREATE UNIQUE INDEX IF NOT EXISTS idx_dados_cadastrais_01
        ON dados_cadastrais (CNPJ_FUNDO);
    '''
    scraperwiki.sqlite.execute(sql_create_idx)

    sql_create_idx='''CREATE INDEX IF NOT EXISTS idx_dados_cadastrais_03
        ON dados_cadastrais (DENOM_SOCIAL);
    '''
    scraperwiki.sqlite.execute(sql_create_idx)
    
    sql_create_idx='''CREATE INDEX IF NOT EXISTS idx_dados_cadastrais_04
        ON dados_cadastrais (SIT);
    '''
    scraperwiki.sqlite.execute(sql_create_idx)
 
    sql_create_idx='''CREATE INDEX IF NOT EXISTS idx_dados_cadastrais_05
        ON dados_cadastrais (CLASSE);
    '''
    scraperwiki.sqlite.execute(sql_create_idx)
 
    sql_create_idx='''CREATE INDEX IF NOT EXISTS idx_dados_cadastrais_06
        ON dados_cadastrais (CNPJ_ADMIN);
    '''
    scraperwiki.sqlite.execute(sql_create_idx)
 
    sql_create_idx='''CREATE INDEX IF NOT EXISTS idx_dados_cadastrais_07
        ON dados_cadastrais (CPF_CNPJ_GESTOR);
    '''
    scraperwiki.sqlite.execute(sql_create_idx)

    sql_create_idx='''CREATE INDEX IF NOT EXISTS idx_dados_cadastrais_08
        ON dados_cadastrais (CNPJ_CONTROLADOR);
    '''
    scraperwiki.sqlite.execute(sql_create_idx)

def create_indexes_informe_diario():
    print('Criando índices na tabela informe diário...')
    sql_create_idx='''CREATE INDEX IF NOT EXISTS idx_informe_diario_01
        ON informe_diario (COD_CNPJ);
    '''
    scraperwiki.sqlite.execute(sql_create_idx)

    sql_create_idx='''CREATE INDEX IF NOT EXISTS idx_informe_diario_02
        ON informe_diario (CNPJ_FUNDO);
    '''
    scraperwiki.sqlite.execute(sql_create_idx)
   
    sql_create_idx='''CREATE INDEX IF NOT EXISTS idx_informe_diario_03
        ON informe_diario (DT_REF);
    '''
    scraperwiki.sqlite.execute(sql_create_idx)
    sql_create_idx='''CREATE INDEX IF NOT EXISTS idx_informe_diario_04
        ON informe_diario (DT_COMPTC);
    '''
    scraperwiki.sqlite.execute(sql_create_idx)
    sql_create_idx='''CREATE UNIQUE INDEX IF NOT EXISTS idx_informe_diario_05
        ON informe_diario (COD_CNPJ, DT_REF);
    '''
    scraperwiki.sqlite.execute(sql_create_idx)

    sql_create_idx='''CREATE UNIQUE INDEX IF NOT EXISTS idx_informe_diario_06
        ON informe_diario (CNPJ_FUNDO, DT_REF);
    '''
    scraperwiki.sqlite.execute(sql_create_idx)

    #lista_indices=scraperwiki.sqlite.execute('PRAGMA index_list(''informe_diario'');')
    #print(lista_indices)
    #for idx in lista_indices:
    #    idx_name=idx
    #    print(idx_name)
    #    columns=scraperwiki.sqlite.execute('PRAGMA index_info('+idx_name+');')
    #    print('columns: ', columns)


def create_views():
    # Evitamos usar a sintaxe que especifica as colunas da view porque
    # este só foi adicionada à versão do SQLite 3.9.0 (2015-10-14)

    sql_drop_view='''DROP VIEW IF EXISTS ultima_data;'''
    sql_create_view='''CREATE VIEW IF NOT EXISTS ultima_data as select max(d.DT_REF) as DT_REF from informe_diario d;'''
    #sql_create_view='''CREATE VIEW IF NOT EXISTS ultima_data(DT_REF) as select max(d.DT_REF) as DT_REF from informe_diario d;'''
    try:
        print('Criação da view de ultima_data')
        scraperwiki.sqlite.execute(sql_drop_view)        
        scraperwiki.sqlite.execute(sql_create_view)        
    except (sqlite3.OperationalError, sqlalchemy.exc.OperationalError) as err:        
        print('Falha na criação da view...', err)
        print(type(err))    # the exception instance
        print(err.args)     # arguments stored in .args
    
    sql_drop_view='''DROP VIEW IF EXISTS ultima_quota;'''
    sql_create_view='''CREATE VIEW IF NOT EXISTS ultima_quota as 
        select c.COD_CNPJ, c.CNPJ_FUNDO, c.DENOM_SOCIAL, i.DT_REF, i.VL_QUOTA
        FROM dados_cadastrais c
        inner join informe_diario i on (c.COD_CNPJ=i.COD_CNPJ)
        where not exists (
            select 1 from informe_diario i2
            where i2.COD_CNPJ=i.COD_CNPJ
            and i2.DT_REF > i.DT_REF
        );
    '''
#    sql_create_view='''
#        CREATE VIEW IF NOT EXISTS ultima_quota(
#            COD_CNPJ, CNPJ_FUNDO, DENOM_SOCIAL, 
#            DT_REF, VL_QUOTA) AS select COD_CNPJ, CNPJ_FUNDO, DENOM_SOCIAL, i.DT_REF, i.VL_QUOTA
#        FROM dados_cadastrais c
#        inner join informe_diario d on (d.COD_CNPJ=c.COD_CNPJ)
#        where d.DT_REF IN (select DT_REF from ultima_data u);
#    '''
    try:
        print('Criação da view de ultima_quota')
        scraperwiki.sqlite.execute(sql_drop_view)        
        scraperwiki.sqlite.execute(sql_create_view)        
    except (sqlite3.OperationalError, sqlalchemy.exc.OperationalError) as err:        
        print('Falha na criação da view...', err)
        print(type(err))    # the exception instance
        print(err.args)     # arguments stored in .args

def captura_arquivo_composicao_carteira(periodo):
    periodo=202005
    base_url = f'http://dados.cvm.gov.br/dados/FI/DOC/CDA/DADOS/'
    local_filename=f'cda_fi_{periodo}.zip'
    
    #blc1_filename=f'cda_fi_BLC_1_{periodo}.csv'
    blc_filenames=[]
    for x in range(1, 9):
        blc_filenames.append(f'cda_fi_BLC_{x}_{periodo}.csv')

    print(f'Iniciando download das composições das carteiras de fundos de investimento')
    _download_file(base_url, local_filename)
    
    cda_data_df=[];
    with zipfile.ZipFile(local_filename) as z:
        for filename in blc_filenames:
            with z.open(filename) as f:
                # Realiza a leitura dos dados como csv
                # O delimitador dos csvs da CVM é o ';'
                # Como os dados estão salvos em ISO-8859-1, 
                # é necessário alterar o encoding durante a leitura dos dados
                df = pd.read_csv(
                    f, 
                    sep=';',
                    encoding='latin1'
                )
                #print(df.head())    # print the first 5 rows
                print(f'Foram lidos {len(df.index)} registros do arquivo {filename}.')
                cda_data_df.append(df)
 
    print(f'Total de dataframes obtidos: {len(cda_data_df)}')
    return cda_data_df

def _download_file(base_url, filename):
    url = f'{base_url}/{filename}'
    local_filename=f'{filename}'
    
    try: 
        head_request = requests.head(url)
        if (head_request.status_code!=200):
            #print('Falha ao verificar url...', head_request)
            return 404

        remote_size = int(head_request.headers.get('Content-Length', 0))
        local_path=Path(local_filename)
        local_size = local_path.stat().st_size if local_path.exists() else -2 
        
        if remote_size != local_size:
            print(f'Downloading file {url} com o tamanho {remote_size}...')
            #(filename, headers)=urllib.request.urlretrieve(url, local_filename)

            # Streaming, so we can iterate over the response.
            r = requests.get(url, stream=True)
            progress_bar = tqdm.tqdm(
                total=local_size, unit='B', unit_scale=True, desc=url.split('/')[-1])
            # Lê o arquivo incrementalmente exibindo uma barra de progresso (tqdm)
            with open(local_filename, 'wb') as f:
                for chunk in r.iter_content(chunk_size = 2048):
                    f.write(chunk)
                    progress_bar.update(2048)
            progress_bar.close()
            print('Download finalizado com sucesso.')
            
            return 1
        else:
            print(f'Já foi encontrado o arquivo {local_filename} localmente. Não será realizado download.')
            return 2
    except (IOError, urllib.error.HTTPError) as err:        
        print('Falha na leitura do arquivo ', url ,'...', err)
        print(type(err))    # the exception instance
        print(err.args)     # arguments stored in .args
        return None
    except Exception as err:
        print('Erro ao baixar arquivo', url, '...', err)
        print(type(err))    # the exception instance
        print(err.args)     # arguments stored in .args
        return None

if __name__ == '__main__':
    #    captura_arquivo_composicao_carteira('')
    #    exit()

    print (f'variável de ambiente {os.environ.get("SCRAPERWIKI_DATABASE_NAME")}')
    executa_scraper()

    # O scraperwiki do Morph.IO não é compatível com o Python3
    # Também é difícil ficar configurando a variável de ambiente neste contexto
    # Por esta razão, deixamos escrever no local padrão do scraperwiki
    # E posteriormente copiamos o database para o diretório esperado pelo Morph.io
    if os.path.exists('scraperwiki.sqlite'):
        print('Renomeando arquivo sqlite')
        shutil.copy('scraperwiki.sqlite', 'data.sqlite')
