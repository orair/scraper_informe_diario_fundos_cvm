
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

@click.command("Gerenciador do Scraper dos Informes Diários de Fundos de Investimentos da CVM")
@click.option('--skip_informacoes_cadastrais', 
                default=False, is_flag=True, 
                show_default=True)
@click.option('--skip_informe_diario', 
                default=False, is_flag=True, 
                show_default=True)
@click.option('--ano_inicial', 
                default=lambda: 
                    os.environ.get('SCRAPER_INFORME_CVM_ANO_INICIAL', 2019), 
                show_default="Variável de ambiente SCRAPER_INFORME_DIARIO_CVM_ANO_INICIAL ou o valor padrão 2018")
def executa_scraper(skip_informacoes_cadastrais=False, skip_informe_diario=False, ano_inicial=2019):
    init()

    if (not skip_informacoes_cadastrais):
        executa_scraper_dados_cadastrais()
   
    if (not skip_informe_diario):
        executa_scraper_informe_diario(ano_inicial)

def executa_scraper_informe_diario(ano_inicial):
    periodos=obtem_periodos(ano_inicial)
    for periodo in periodos: 
        informe_diario_df=captura_arquivo(periodo)
        # Verifica se recebeu os dados ok
        if informe_diario_df is not None and not informe_diario_df.empty:
            print(f'Salvando dados obtidos com {len(informe_diario_df.index)} registros.')
            salva_periodo(informe_diario_df, periodo)

def captura_arquivo(periodo):
    base_url = f'http://dados.cvm.gov.br/dados/FI/DOC/INF_DIARIO/DADOS/'
    filename=f'inf_diario_fi_{periodo}.csv'

    print(f'Verifica necessidade de download dos informes diários de {periodo}')
    _download_file(base_url, filename)

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
        return None
    except Exception as err:
        print('Erro ao baixar arquivo', filename, '...', err)
        print(type(err))    # the exception instance
        print(err.args)     # arguments stored in .args
        return None

    # Cria um campo só com os números do CNPJ na primeira coluna
    df.insert(0, 'COD_CNPJ', df['CNPJ_FUNDO'].str.replace(r'\D+', '').str.zfill(14))
    # Cria um campo com a data formatada na segunda coluna
    df.insert(1, 'DT_REF', pd.to_datetime(df['DT_COMPTC'], errors='coerce', format='%Y-%m-%d'))

    return df

def obtem_periodos(ano_inicial=2018):
    periodos=[]

    today = datetime.today()
    ano_final = int(today.strftime('%Y'))
    mes_final = int(today.strftime('%m'))

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

def salva_periodo(informe_diario_df, periodo):
    if informe_diario_df is None or informe_diario_df.empty:
        print('Recebeu dados vazios!')
        return False

    try:
        for row in informe_diario_df.to_dict('records'):
            scraperwiki.sqlite.save(unique_keys=['COD_CNPJ', 'DT_REF'], data=row, table_name='informe_diario')
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
        for row in df.to_dict('records'):
            scraperwiki.sqlite.save(unique_keys=['COD_CNPJ'], data=row, table_name='dados_cadastrais')
    except Exception as err:
        print(f'Falha ao salvar registros no banco de dados dos dados cadastrais dos fundos', err)
        print(type(err))    # the exception instance
        print(err.args)     # arguments stored in .args
        return None

def init():
    init_database()

def init_database():
    """ Será necessário criar a tabela inicialmente pois o SQlite infere os tipos errados
       o que faz falhar a carga
    """
    sql_create_table_dados_cadastrais='''CREATE TABLE IF NOT EXISTS dados_cadastrais (
        "COD_CNPJ" TEXT,
        "CNPJ_FUNDO" TEXT, 	
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

    sql_create_table='''CREATE TABLE IF NOT EXISTS informe_diario (
        "COD_CNPJ" TEXT, 	
        "CNPJ_FUNDO" TEXT, 	
        "DT_REF" DATE, 	
        "DT_COMPTC" TEXT, 	
        "VL_TOTAL" NUMERIC, 	
        "VL_QUOTA" NUMERIC, 	
        "VL_PATRIM_LIQ" NUMERIC, 	
        "CAPTC_DIA" NUMERIC, 	
        "RESG_DIA" NUMERIC, 	
        "NR_COTST" INTEGER 	
    );    
    '''
    scraperwiki.sqlite.execute(sql_create_table)
    
    #scraperwiki.sqlite.execute('DROP INDEX IF EXISTS idx_informe_diario_01;')
    #scraperwiki.sqlite.execute('DROP INDEX IF EXISTS idx_informe_diario_02;')
    #scraperwiki.sqlite.execute('DROP INDEX IF EXISTS idx_informe_diario_03;')

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

    lista_indices=scraperwiki.sqlite.execute('PRAGMA index_list(''informe_diario'');')
    print(lista_indices)
    #for idx in lista_indices:
    #    idx_name=idx
    #    print(idx_name)
    #    columns=scraperwiki.sqlite.execute('PRAGMA index_info('+idx_name+');')
    #    print('columns: ', columns)

    sql_create_view='''
        CREATE VIEW IF NOT EXISTS ultima_cota(
            COD_CNPJ, 
            CNPJ_FUNDO, 
            DENOM_SOCIAL, 
            DT_REF, 
            VL_TOTAL, 
            VL_QUOTA,
            VL_PATRIM_LIQ, 
            CAPTC_DIA, 
            RESG_DIA, 
            NR_COTST
        ) 
        AS select COD_CNPJ, CNPJ_FUNDO, DENOM_SOCIAL, i.DT_REF, i.VL_TOTAL, i.VL_QUOTA, i.VL_PATRIM_LIQ, i.CAPTC_DIA, i.RESG_DIA, i.NR_COTST
        FROM dados_cadastrais c
        inner join informe_diario d on (d.COD_CNPJ=c.COD_CNPJ)
        where d.DT_REF IN (select max(d2.DT_REF) from informe_diario d2);
    '''
    #scraperwiki.sqlite.execute(sql_create_view)


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
            print('Falha ao verificar url...', head_request)
            return None

        remote_size = int(head_request.headers['Content-Length'])
        local_path=Path(local_filename)
        local_size = local_path.stat().st_size if local_path.exists() else -2 
        
        if remote_size != local_size:
            print(f'Downloading file {url} ...')
            (filename, headers)=urllib.request.urlretrieve(url, local_filename)        
        else:
            print(f'Já foi encontrado o arquivo {local_filename} localmente. Não será realizado download.')
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
