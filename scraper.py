
# -*- coding: utf-8 -*-
import os
from pathlib import Path
# morph.io requires this db filename, but scraperwiki doesn't nicely
# expose a way to alter this. So we'll fiddle our environment ourselves
# before our pipeline modules load.
os.environ['SCRAPERWIKI_DATABASE_NAME'] = 'sqlite:///data.sqlite'
import datetime
from datetime import datetime, timedelta
from dateutil.relativedelta import *
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
from pangres import upsert
import sqlalchemy

@click.command("Gerenciador do Scraper dos Informes Diários de Fundos de Investimentos da CVM")
@click.option('--skip_informe_diario_atual', 
                default=lambda:
                    os.environ.get('MORPH_SCRAPER_SKIP_INFORME_ATUAL', 'N'),
                show_default="Variável de ambiente MORPH_SCRAPER_SKIP_INFORME_ATUAL ou N")
@click.option('--skip_informacoes_cadastrais', 
                default=lambda:
                    os.environ.get('MORPH_SCRAPER_SKIP_INF_CAD', 'N'),
                show_default="Variável de ambiente MORPH_SCRAPER_SKIP_INF_CAD ou N")
                
@click.option('--skip_informe_diario_historico', 
                default=lambda:
                    os.environ.get('MORPH_SCRAPER_SKIP_INFORME_HIST', 'N'),
                show_default="Variável de ambiente MORPH_SCRAPER_SKIP_INFORME_HIST ou N")
@click.option('--skip_salva_dados_cadastrais_remoto', 
                default=lambda:
                    os.environ.get('MORPH_SCRAPER_SKIP_DADOS_CAD_REMOTO', 'N'),
                show_default="Variável de ambiente MORPH_SCRAPER_SKIP_DADOS_CAD_REMOTO ou N")
@click.option('--periodo_inicial', 
                default=lambda: 
                    os.environ.get('MORPH_SCRAPER_INFORME_CVM_PERIODO_INICIAL', (datetime.today() - timedelta(days=32)).strftime('%Y%m')),
                show_default="Parâmetro deve ser informado no formato \"AAAAMM\". Variável de ambiente MORPH_SCRAPER_INFORME_DIARIO_CVM_PERIODO_INICIAL ou o valor padrão (últimos dois meses)")
@click.option('--compara_antes_insercao', 
                default=lambda: 
                    os.environ.get('MORPH_SCRAPER_COMPARA_ANTES_INSERCAO', 'N'), 
                show_default="Variável de ambiente MORPH_SCRAPER_COMPARA_ANTES ou valor padrão N")
@click.option('--limpa_acervo_antigo', 
                default=lambda: 
                    os.environ.get('MORPH_SCRAPER_LIMPA_ACERVO_ANTIGO', 'S'), 
                show_default="Variável de ambiente MORPH_SCRAPER_LIMPA_ACERVO_ANTIGO ou valor padrão N")
@click.option('--enable_remotedb', 
                default=lambda: 
                    os.environ.get('MORPH_SCRAPER_ENABLE_REMOTEDB', 'S'), 
                show_default="Variável de ambiente MORPH_SCRAPER_ENABLE_REMOTEDB ou valor padrão N")
@click.option('--sqlalchemy_dburi',
                default=lambda: 
                    os.environ.get('MORPH_SQLALCHEMY_DATABASE_URI'), 
                help="DBURI para acesso ao banco de dados. Ex: mysql+pymysql://aaa:xxx@remotemysql.com:3306/aaa. Default Variável de ambiente MORPH_SQLALCHEMY_DATABASE_URI")
def executa_scraper(skip_informe_diario_atual='N', skip_informacoes_cadastrais='N', skip_informe_diario_historico='N', skip_salva_dados_cadastrais_remoto='N', periodo_inicial='202012',
    compara_antes_insercao='N', limpa_acervo_antigo='S', enable_remotedb='S', sqlalchemy_dburi=None):
    print(f'Período inicial para buscar os informes diários {periodo_inicial}')
    init()
    #print ('enable_remote_db', enable_remotedb)

    if (skip_informe_diario_atual == 'N'):
        skip_informe_diario_atual = False
    else: skip_informe_diario_atual = True
    
    if (skip_informacoes_cadastrais == 'N'):
        skip_informacoes_cadastrais=False
    else: skip_informacoes_cadastrais=True
    
    if (skip_informe_diario_historico == 'N'):  
        skip_informe_diario_historico=False
    else: skip_informe_diario_historico=True

    if (skip_salva_dados_cadastrais_remoto == 'N'):  
        skip_salva_dados_cadastrais_remoto=False
    else: skip_salva_dados_cadastrais_remoto=True
 
    if (compara_antes_insercao == 'N'):
        compara_antes_insercao=False
    else: compara_antes_insercao=True

    if (limpa_acervo_antigo == 'N'):
        limpa_acervo_antigo = False
    else: limpa_acervo_antigo = True

    if (enable_remotedb == 'S'):
        enable_remotedb = True
    else: enable_remotedb = False
    #print ('enable_remote_db', enable_remotedb)

    engine=None
    if (enable_remotedb):
        #Init engine
        if (sqlalchemy_dburi):
            #print ('Iniciando engine para a base de dados', sqlalchemy_dburi)
            print ('Iniciando engine para a base de dados')
            #engine = sqlalchemy.create_engine(sqlalchemy_dburi, echo=True)
            engine = sqlalchemy.create_engine(sqlalchemy_dburi)
        else:
            print('Desabilitando enable_remotedb pois não foi encontrado a URI para acesso ao banco.')
            enable_remotedb=False
    #else: print('Carga em banco de dados está desabilitada')
    
    if (limpa_acervo_antigo):
        executa_limpeza_acervo_antigo(enable_remotedb, engine)
   
    if (not skip_informe_diario_atual):
        executa_scraper_informe_diario_por_periodo(obtem_ultimo_periodo(), compara_antes_insercao, enable_remotedb, engine, False)

    if (not skip_informacoes_cadastrais):
        executa_scraper_dados_cadastrais(enable_remotedb, skip_salva_dados_cadastrais_remoto, engine)

    print(f'Período inicial para buscar os informes diários {periodo_inicial}')
    if (not skip_informe_diario_historico):
        executa_scraper_informe_diario_historico(periodo_inicial, compara_antes_insercao, enable_remotedb, engine)

    if (enable_remotedb):
        importa_dados_remotos(engine)

def executa_scraper_informe_diario_historico(periodo_inicial, compara_antes_insercao, enable_remotedb, engine):
    periodos = obtem_periodos(periodo_inicial)
    for periodo in periodos: 
        executa_scraper_informe_diario_por_periodo(periodo, compara_antes_insercao, enable_remotedb, engine, True)

def executa_scraper_informe_diario_por_periodo(periodo, compara_antes_insercao, enable_remotedb, engine, limpa_dados_diarios=True):
    df2 = None
    result, informe_diario_df = captura_arquivo_informe(periodo)
   
    print(f'Leitura dos dados do arquivo concluída para o período {periodo}...')
    existe_dados_origem=(informe_diario_df is not None \
        and not informe_diario_df.empty)   

    # Caso tenha sido obtida e lido um novo arquivo com sucesso...
    if existe_dados_origem and result in (1,2):
        if (limpa_dados_diarios):
            print('Ignorando dados diários desnecessários presentes no informe...')
            grp = informe_diario_df.groupby(by=['COD_CNPJ'])
            ultimo_informe_df=grp.apply(lambda g: g[g['DT_REF'] == g['DT_REF'].max()])
            ultimo_informe_df.reset_index(inplace=True, drop=True)
            informe_diario_df=ultimo_informe_df
            print(f'Número de registros após ignorar os dados diários desnecessário {len(informe_diario_df.index)}...')
            #print(informe_diario_df)
        if (enable_remotedb):
            carrega_informe_remoto(informe_diario_df, engine)
        else: carrega_informe_local(informe_diario_df, periodo, compara_antes_insercao)
    else:
        print (f'Não foram encontrados novos registros no arquivo para o periodo {periodo}...')

def carrega_informe_remoto(informe_diario_df, engine):
    print('Inserindo informe diário no banco de dados remoto...')
    informe_diario_df.set_index(['COD_CNPJ', 'DT_REF'], inplace=True)

    # it does not matter if if_row_exists is set
    # to "update" or "ignore" for table creation
    upsert(engine=engine,
        df=informe_diario_df,
        table_name='informe_diario',
        if_row_exists='update'
        #,dtype=dtype
    )
    
    print('Finalizada inserção de informe diário no banco de dados remoto...')

def carrega_informe_local(informe_diario_df, periodo, compara_antes_insercao):
    print('Inserindo informe diário no banco de dados local...')

    informe_diario_df.sort_values(by=['COD_CNPJ', 'DT_REF'], axis='index')
                   
    df2 = None
    if compara_antes_insercao:
        df2 = recupera_informe_diario(periodo)
    else: df2 = pd.DataFrame()

    if not df2.empty:
        df2['DT_REF']=pd.to_datetime(df2['DT_REF'], errors='coerce', format='%Y-%m-%d')

    existe_dados_diferentes=(df2.empty or (not informe_diario_df.equals(df2)))
    
    if existe_dados_diferentes:
        if df2.empty:
            salva_informe_periodo(informe_diario_df)
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
            salva_informe_periodo(novos_dados_df)
    else:
        print ('Não encontrou novos dados a serem atualizados localmente')

def recupera_informe_diario(periodo):
    query=f"COD_CNPJ, DT_REF, CNPJ_FUNDO, DT_COMPTC, VL_TOTAL, VL_QUOTA, VL_PATRIM_LIQ, CAPTC_DIA, RESG_DIA, NR_COTST from informe_diario where strftime('%Y%m', DT_REF) = '{periodo}' order by COD_CNPJ, DT_REF"
    result=scraperwiki.sql.select(query)
    df=pd.DataFrame(result)
    return df

def importa_dados_remotos(engine):
    print ('Iniciando importação dos dados remotos na base local')
    last_month = datetime.today() - timedelta(days=30)
    last_month = last_month.strftime('%Y-%m-%d')
    sql=f'''select COD_CNPJ,
            CNPJ_FUNDO,
            DT_REF,
            DT_COMPTC,
            VL_QUOTA
        from informe_diario
        where informe_diario.DT_REF >= '{last_month}' or 
        not exists (
            select 1 from informe_diario d2
            where informe_diario.COD_CNPJ = d2.COD_CNPJ
            and d2.DT_REF > informe_diario.DT_REF
            and informe_diario.ANO_REF = d2.ANO_REF
            and informe_diario.MES_REF = d2.MES_REF
        )'''
    
    try:
        print('Carregando informes diários a partir da base remota...')
        informe_diario_df = pd.read_sql(
            sql,
            con=engine,
            parse_dates=['DT_REF']
        )
        
        salva_informe_periodo(informe_diario_df)
 
    except (sqlite3.OperationalError, sqlalchemy.exc.OperationalError) as err:        
        print('Falha ao carregar dados da base remota...', err)
        print(type(err))    # the exception instance
        print(err.args)     # arguments stored in .args


   
def captura_arquivo_informe(periodo):
    base_url = f'https://dados.cvm.gov.br/dados/FI/DOC/INF_DIARIO/DADOS'
    filename=f'inf_diario_fi_{periodo}.zip'
    url = f'{base_url}/{filename}'

    print(f'Verifica necessidade de download dos informes diários de {periodo}')
    result=_download_file(base_url, filename)

    # No início de meses, ainda não existira o arquivo com as cotas dos meses correntes,
    # Por isto, vamos retornar simplesmente um dataframe vazio caso não encontre 
    # o arquivo na url consultada
    if result not in (1,2):
        df_empty = pd.DataFrame({'A' : []})
        print(f'Não foi encontrado o arquivo {filename} no servidor da CVM, se for o início de um mês e o arquivo se referir ao mês atual, isto é esperado... URL: {url}. HTTP STATUS CODE: {result}')
        return 0, df_empty
        
    try:
        # Realiza a leitura dos dados como csv
        # O delimitador dos csvs da CVM é o ';'
        # Como os dados estão salvos em ISO-8859-1, 
        # é necessário alterar o encoding durante a leitura dos dados
        print(f'Iniciando leitura dos informes diários de {periodo}')
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
    df.insert(0, 'COD_CNPJ', df['CNPJ_FUNDO'].str.replace(r'\D+', '', regex=True).str.zfill(14))
    # Cria um campo com a data formatada na segunda coluna
    df.insert(1, 'DT_REF', pd.to_datetime(df['DT_COMPTC'], errors='coerce', format='%Y-%m-%d'))

    return result, df

def obtem_ultimo_periodo():
    yesterday = datetime.today() - timedelta(days=1)
    ano = int(yesterday.strftime('%Y'))
    mes = int(yesterday.strftime('%m'))
    periodo=f'{ano:04}{mes:02}'
    return periodo

def obtem_periodos(periodo_inicial='201912'):
    periodos=[]

    today = datetime.today()
    last_month = today - timedelta(days=31)
    ano_final = int(last_month.strftime('%Y'))
    mes_final = int(last_month.strftime('%m'))

    # Create date object
    data_inicial = datetime.strptime(periodo_inicial, "%Y%m")
    ano_inicial = int(data_inicial.strftime('%Y'))
    mes_inicial = int(data_inicial.strftime('%m'))

    for ano in range(ano_inicial, ano_final+1):
        primeiro_mes=1

        if (ano == ano_inicial): 
            primeiro_mes=mes_inicial;
        
        for mes in range(primeiro_mes,13):
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

def salva_informe_periodo_scraper_wiki(informe_diario_df):
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
        print(f'Falha ao salvar registros no banco de dados.', err)
        print(type(err))    # the exception instance
        print(err.args)     # arguments stored in .args
        return None

def salva_informe_periodo(informe_diario_df):
    if informe_diario_df is None or informe_diario_df.empty:
        return False
    # Salvamos um CSV que o Dolt vai ler
    # O modo 'a' (append) com header apenas se o arquivo não existir
    file_exists = os.path.isfile('dados_informe.csv')
    informe_diario_df.to_csv('dados_informe.csv', mode='a', index=False, header=not file_exists)
    print(f"Dados exportados para dados_informe.csv")

def executa_scraper_dados_cadastrais(enable_remotedb, skip_salva_dados_cadastrais_remoto, engine):
    #from bizdays import Calendar
    #cal = Calendar.load('feriados_nacionais_ANBIMA.csv')
    
    # tentaremos obter dados cadastrais de três dias atrás
    #periodo = (datetime.today()- timedelta(days=3))
    #while not cal.isbizday(periodo):
    #    periodo = periodo - timedelta(days=1)    

    #periodo = periodo.strftime('%Y%m%d')
    print (f'Serão obtidos os dados cadastrais publicados pela CVM...')
    df = captura_arquivo_dados_cadastrais()
    if df is None or df.empty:
        print('Recebeu dados vazios!')
        return False
    return salva_dados_cadastrais(df, enable_remotedb, skip_salva_dados_cadastrais_remoto, engine)

def captura_arquivo_dados_cadastrais():
    base_url = f'https://dados.cvm.gov.br/dados/FI/CAD/DADOS/'
    filename=f'cad_fi.csv'
    url = f'{base_url}/{filename}'

    print(f'Verifica necessidade de download dos dados cadastrais dos fundos de investimento')
    _download_file(base_url, filename)

    df = None
    try:
        # Realiza a leitura dos dados como csv
        # O delimitador dos csvs da CVM é o ';'
        # Como os dados estão salvos em ISO-8859-1, 
        # é necessário alterar o encoding durante a leitura dos dados
        # "TP_FUNDO", "COD_CNPJ", "CNPJ_FUNDO", "DENOM_SOCIAL", "DT_REG", "DT_CONST", "CD_CVM", "DT_CANCEL", "SIT", "DT_INI_SIT", 
        # "DT_INI_ATIV", "DT_INI_EXERC", "DT_FIM_EXERC", "CLASSE", "DT_INI_CLASSE", "RENTAB_FUNDO", "CONDOM", "FUNDO_COTAS", "FUNDO_EXCLUSIVO", "TRIB_LPRAZO", 
        # "ENTID_INVEST", "TAXA_PERFM", "INF_TAXA_PERFM", "TAXA_ADM", "INF_TAXA_ADM", "VL_PATRIM_LIQ", "DT_PATRIM_LIQ", #"DIRETOR", "CNPJ_ADMIN", "ADMIN", 
        # "PF_PJ_GESTOR", "CPF_CNPJ_GESTOR", "GESTOR", "CNPJ_AUDITOR", "AUDITOR", "CNPJ_CUSTODIANTE", "CUSTODIANTE", "CNPJ_CONTROLADOR", "CONTROLADOR", "PUBLICO_ALVO"
        #columns (14,17,18,20,22,24,27,37,38) 
        tipos={'CLASSE': 'string',
        'CONDOM': 'string',
        'FUNDO_COTAS': 'string',
        'TRIB_LPRAZO': 'string',
        'TAXA_PERFM': 'string',
        'INF_TAXA_ADM': 'string',
        'DIRETOR': 'string',
        'CNPJ_CONTROLADOR': 'string',
        }
        df = pd.read_csv(
            filename,
            sep=';',
            encoding='latin1',
            dtypes=tipos
        )
        #print(df.head())
    except (IOError, urllib.error.HTTPError) as err:        
        print('Falha na leitura do arquivo {} a partir da url {} com o erro {}'.format(filename, url, err))
        print(type(err))    # the exception instance
        print(err.args)     # arguments stored in .args
        return None
    except Exception as err:
        print('Erro ao ler arquivo localmente {} a partir da url {} com o erro {}'.format(filename, url, err))
        print(type(err))    # the exception instance
        print(err.args)     # arguments stored in .args
        return None

    print(f'Foram lidos {len(df.index)} registros do arquivos.')

    df=df[df['TP_FUNDO']=='FI']
    
    print(f'Após o filtro de apenas fundos do tipo de fundos de investimentos, obteve-se {len(df.index)} fundos.')
 
    # Filtra por situações dos fundos
    #    CANCELADA
    #    EM FUNCIONAMENTO NORMAL
    #    FASE PRÉ-OPERACIONAL
    situacoesDescartadas=['CANCELADA', 'FASE PRÉ-OPERACIONAL']
    df=df[~df.SIT.isin(situacoesDescartadas)]

    print(f'Após o filtro dos fundos cancelados ou em fase pré-operacional, obteve-se {len(df.index)} fundos.')

    df.drop_duplicates(subset=['TP_FUNDO', 'CNPJ_FUNDO'], keep ='first', inplace=True)
    print(f'Após o filtro removendo os fundos duplicados, obteve-se {len(df.index)} fundos.')



   # Cria um campo só com os números do CNPJ
    
    df['COD_CNPJ'] = df['CNPJ_FUNDO'].str.replace(r'\D+', '', regex=True).str.zfill(14)

    # TODO: Descartar colunas desinteressantes
    #
    # Filtra as colunas que interessam
    # idxColunas=[0,1,5,10,12,13,14,15,16,17,18,19,20,21,24,25,26,27,28,29,34,35]
    return df

def salva_dados_cadastrais(df, enable_remotedb, skip_salva_dados_cadastrais_remoto, engine):
    if df is None or df.empty:
        print('Recebeu dados cadastrais vazios!')
        return False
    if (enable_remotedb and not skip_salva_dados_cadastrais_remoto):
        salva_dados_cadastrais_remoto(df, engine)
    else:
        print('Serão salvos os dados localmente...')
        salva_dados_cadastrais_local(df)
    
def salva_dados_cadastrais_remoto(df, engine):
    try:
        print('Salvando dados cadastrais no banco de dados remoto...')
        
        #df=df[df['COD_CNPJ']=='97711801000105']
        df.set_index(['TP_FUNDO', 'COD_CNPJ'], inplace=True)

        # it does not matter if if_row_exists is set
        # to "update" or "ignore" for table creation
        upsert(engine=engine,
            df=df,
            table_name='dados_cadastrais',
            if_row_exists='update'
            #,dtype=dtype
        )
    except IndexError as err:
        print(f'Falha de índice ao salvar registros dos dados cadastrais no banco de dados remoto...', err)
        print ('Índice', df.index.names)
        print(df.index[df.index.duplicated(keep=False)])
        print(type(err))    # the exception instance
        print(err.args)     # arguments stored in .args
    except Exception as err:
        print(f'Falha ao salvar registros dos dados cadastrais no banco de dados remoto...', err)
        print(type(err))    # the exception instance
        print(err.args)     # arguments stored in .args
        return None   

def salva_dados_cadastrais_local_scraperwiki(df):
    try:
        print('Salvando dados cadastrais no banco de dados local...')
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

def salva_dados_cadastrais_local(df):
    # O DoltHub prefere sobrescrever o cadastro completo (que é menor)
    df.to_csv('dados_cadastrais.csv', index=False)
    print("Cadastro exportado para dados_cadastrais.csv")

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
        TP_FUNDO TEXT,
        "COD_CNPJ" TEXT NOT NULL PRIMARY KEY,
        "CNPJ_FUNDO" TEXT NOT NULL, 	
        "DENOM_SOCIAL" TEXT, 	
        "DT_REG" TEXT, 	
        "DT_CONST" TEXT, 
        "CD_CVM" TEXT,	
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
        "ENTID_INVEST" TEXT, 	
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
    base_url = f'https://dados.cvm.gov.br/dados/FI/DOC/CDA/DADOS/'
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
        status_code=head_request.status_code
        if (head_request.status_code!=200):
            if (status_code==301):
                print(f'Retornou código de que o artefato foi movido permanentemente. Acesse a documentação e verifique se há atualizações {url}')
            print(f'Falha ao verificar url status code {status_code} e...{head_request} url {url}')
            return status_code

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

def executa_limpeza_acervo_antigo(enable_remotedb, engine):
    if (enable_remotedb):
        executa_limpeza_acervo_antigo_remoto(engine)
    executa_limpeza_acervo_antigo_local()

def executa_limpeza_acervo_antigo_remoto(engine):
    last_month = datetime.today() - timedelta(days=30)
    last_month = last_month.strftime('%Y-%m-%d')

    sql_delete=f'''with ultima_quota AS
        (
		select i3.COD_CNPJ, max(i3.DT_REF) as DT_REF, i3.ANO_REF, i3.MES_REF
		from informe_diario i3
		group by COD_CNPJ, ANO_REF, MES_REF
        )
        delete from informe_diario i
	left join ultima_quota i2
	on (
		i.COD_CNPJ=i2.COD_CNPJ
		and i.ANO_REF=i2.ANO_REF
		and i.MES_REF=i2.MES_REF
	)
	where i.DT_REF < '{last_month}' and
	i.DT_REF < i2.DT_REF
        ''' 

    sql_delete=f''' 
        delete from informe_diario 
	where DT_REF < '{last_month}' 
        and (COD_CNPJ, DT_REF) IN 
        ( 
                select i2.COD_CNPJ, i2.DT_REF from informe_diario i2
                where
                exists (select 1 from informe_diario i3
                    where
                    i2.COD_CNPJ=i3.COD_CNPJ
                    and i2.ANO_REF=i3.ANO_REF
	            and i2.MES_REF=i3.MES_REF
	            and i3.DT_REF < i2.DT_REF
                )
	)
        ''' 

    #sql_pg_delete=f'''with ultima_quota AS
    #    (
    #		select i3."COD_CNPJ", max(i3."DT_REF") as "DT_REF", i3."ANO_REF", i3."MES_REF"
    #		from informe_diario i3
    #		group by "COD_CNPJ", "ANO_REF", "MES_REF"
    #    )
    #    delete from informe_diario i
    #	where exists (select 1 from ultima_quota i2
    #	    where
    #	    i."COD_CNPJ"=i2."COD_CNPJ"
    #	    and i."ANO_REF"=i2."ANO_REF"
    #	    and i."MES_REF"=i2."MES_REF"
    #	    and i."DT_REF" < i2."DT_REF"
    #	)
    #	and i."DT_REF" < '{last_month}'
    #        ''' 

    try:
        print('Apagando acervo antigo da base remota...')
        #print(sql_delete)
        with engine.connect() as connection:
            result = connection.execute(sql_delete)
        print('Limpeza executada com sucesso...')
    except (sqlite3.OperationalError, sqlalchemy.exc.OperationalError) as err:        
        print('Falha ao apagar acervo antigo...', err)
        print(type(err))    # the exception instance
        print(err.args)     # arguments stored in .args



def executa_limpeza_acervo_antigo_local():
    last_month = datetime.today() - timedelta(days=30)
    last_month = last_month.strftime('%Y-%m-%d')

    sql_delete=f'''delete from informe_diario
        where informe_diario.DT_REF < '{last_month}' and 
        exists (
            select 1 from informe_diario d2
            where informe_diario.COD_CNPJ = d2.COD_CNPJ
            and informe_diario.DT_REF < d2.DT_REF
            and strftime('%Y%m', informe_diario.DT_REF) = strftime('%Y%m', d2.DT_REF)
        )'''
       #d. 
    try:
        print('Apagando acervo antigo da base local...')
        #print(sql_delete)
        scraperwiki.sqlite.execute(sql_delete)
        print('Limpeza executada com sucesso...')
    except (sqlite3.OperationalError, sqlalchemy.exc.OperationalError) as err:        
        print('Falha ao apagar acervo antigo...', err)
        print(type(err))    # the exception instance
        print(err.args)     # arguments stored in .args

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
