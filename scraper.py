
# -*- coding: utf-8 -*-
import os
import datetime
from datetime import datetime, timedelta
import click
import scraperwiki
import pandas as pd
import urllib
import shutil

@click.command("Gerenciador do Scraper dos Informes Diários de Fundos de Investimentos da CVM")
@click.option('--skip_informacoes_cadastrais', 
                default=False, is_flag=True, 
                show_default=True)
@click.option('--skip_informe_diario', 
                default=False, is_flag=True, 
                show_default=True)
@click.option('--ano_inicial', 
                default=lambda: 
                    os.environ.get('SCRAPER_INFORME_CVM_ANO_INICIAL', 2018), 
                show_default="Variável de ambiente SCRAPER_INFORME_DIARIO_CVM_ANO_INICIAL ou o valor padrão 2018")
def executa_scraper(skip_informacoes_cadastrais=False, skip_informe_diario=False, ano_inicial=2018):
    init_database()

    if (not skip_informacoes_cadastrais):
        executa_scraper_dados_cadastrais()

    if (not skip_informe_diario):
        executa_scraper_informe_diario(ano_inicial)


def executa_scraper_informe_diario(ano_inicial):
    periodos=obtem_periodos(ano_inicial)
    for periodo in periodos: 
        print(f'Iniciando captura do arquivo de informe diário para o período {periodo}')
        informe_diario_df=captura_arquivo(periodo)
        # Verifica se recebeu os dados ok
        if informe_diario_df is not None and not informe_diario_df.empty:
            print(f'Salvando dados obtidos com {informe_diario_df.size} registros.')
            salva_periodo(informe_diario_df, periodo)

def captura_arquivo(periodo):
    url = f'http://dados.cvm.gov.br/dados/FI/DOC/INF_DIARIO/DADOS/inf_diario_fi_{periodo}.csv'

    try:
        # Realiza a leitura dos dados como csv
        # O delimitador dos csvs da CVM é o ';'
        # Como os dados estão salvos em ISO-8859-1, 
        # é necessário alterar o encoding durante a leitura dos dados
        informe_diario_df = pd.read_csv(
            url,
            sep=';',
            encoding='latin1'
        )
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

    # Cria um campo só com os números do CNPJ
    informe_diario_df['COD_CNPJ'] = informe_diario_df['CNPJ_FUNDO'].str.replace(r'\D+', '').str.zfill(14)
    informe_diario_df['DT_REF'] = pd.to_datetime(informe_diario_df['DT_COMPTC'], errors='coerce').dt.strftime('%Y-%m-%d')
    
    return informe_diario_df

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
    # scraperwiki.sqlite.save(unique_keys=['name'], data={"name": "susan", "occupation": "software developer"})
    # 
    # # An arbitrary query against the database
    # scraperwiki.sql.select("* from data where 'name'='peter'")

    # You don't have to do things with the ScraperWiki and lxml libraries.
    # You can use whatever libraries you want: https://morph.io/documentation/python
    # All that matters is that your final data is written to an SQLite database
    # called "data.sqlite" in the current working directory which has at least a table
    # called "data".

    if informe_diario_df is None or informe_diario_df.empty:
        print('Recebeu dados vazios!')
        return False

    try:
        for row in informe_diario_df.to_dict('records'):
            scraperwiki.sqlite.save(unique_keys=informe_diario_df.columns.values.tolist(), data=row, table_name='informe_diario')
    except Exception as err:
        print(f'Falha ao salvar registros no banco de dados para o período {periodo}', err)
        print(type(err))    # the exception instance
        print(err.args)     # arguments stored in .args
        return None

def executa_scraper_dados_cadastrais():
    periodo = (datetime.today()- timedelta(days=7)).strftime('%Y%m%d')
    df = captura_arquivo_dados_cadastrais(periodo)
    if df is None or df.empty:
        print('Recebeu dados vazios!')
        return False
    return salva_dados_cadastrais(df)

def captura_arquivo_dados_cadastrais(periodo):
    url = f'http://dados.cvm.gov.br/dados/FI/CAD/DADOS/inf_cadastral_fi_{periodo}.csv'

    print(f'Iniciando download dos dados cadastrais dos fundos de investimento a partir do arquivo {url}')

    try:
        # Realiza a leitura dos dados como csv
        # O delimitador dos csvs da CVM é o ';'
        # Como os dados estão salvos em ISO-8859-1, 
        # é necessário alterar o encoding durante a leitura dos dados
        df = pd.read_csv(
            url,
            sep=';',
            encoding='latin1'
        )
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

    print(f'Foram lidos {df.size} registros do arquivos.')

    # Filtra por situações dos fundos
    #    CANCELADA
    #    EM FUNCIONAMENTO NORMAL
    #    FASE PRÉ-OPERACIONAL
    situacoesDescartadas=['CANCELADA', 'FASE PRÉ-OPERACIONAL']
    df=df[~df.SIT.isin(situacoesDescartadas)]

    print(f'Após o filtros dos fundos cancelados ou em fase pré-operacional, obteve-se {df.size} fundos.')

    # Cria um campo só com os números do CNPJ
    df['COD_CNPJ'] = df['CNPJ_FUNDO'].str.replace(r'\D+', '').str.zfill(14)

    # TODO: Descartar colunas desinteressantes
    #
    # Filtra as colunas que interessam
    # idxColunas=[0,1,5,10,12,13,14,15,16,17,18,19,20,21,24,25,26,27,28,29,34,35]
    return df

def salva_dados_cadastrais(df):
    # scraperwiki.sqlite.save(unique_keys=['name'], data={"name": "susan", "occupation": "software developer"})
    # 
    # # An arbitrary query against the database
    # scraperwiki.sql.select("* from data where 'name'='peter'")

    # You don't have to do things with the ScraperWiki and lxml libraries.
    # You can use whatever libraries you want: https://morph.io/documentation/python
    # All that matters is that your final data is written to an SQLite database
    # called "data.sqlite" in the current working directory which has at least a table
    # called "data".

    if df is None or df.empty:
        print('Recebeu dados cadastrais vazios!')
        return False

    #print(scraperwiki.sql.show_tables())
   
    try:
        for row in df.to_dict('records'):
            scraperwiki.sqlite.save(unique_keys=df.columns.values.tolist(), data=row, table_name='dados_cadastrais')
    except Exception as err:
        print(f'Falha ao salvar registros no banco de dados dos dados cadastrais dos fundos', err)
        print(type(err))    # the exception instance
        print(err.args)     # arguments stored in .args
        return None

def init_database():
    """ Será necessário criar a tabela inicialmente pois o SQlite infere os tipos errados
       o que faz falhar a carga
    """
    sql_create_table_dados_cadastrais='''CREATE TABLE IF NOT EXISTS dados_cadastrais (
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
        "CONTROLADOR" TEXT, 	
        "COD_CNPJ" TEXT);    
    '''
    #scraperwiki.sqlite.execute('DROP TABLE dados_cadastrais;')

    scraperwiki.sqlite.execute(sql_create_table_dados_cadastrais)

    # TODO: Criar os índices do banco de dados
    #create_command='CREATE [UNIQUE] INDEX index_name '
    #              + 'ON table_name(column_list); '
 
if __name__ == '__main__':
    print (f'variável de ambiente {os.environ.get("SCRAPERWIKI_DATABASE_NAME")}')
    executa_scraper()

    # O scraperwiki do Morph.IO não é compatível com o Python3
    # Também é difícil ficar configurando a variável de ambiente neste contexto
    # Por esta razão, deixamos escrever no local padrão do scraperwiki
    # E posteriormente copiamos o database para o diretório esperado pelo Morph.io
    if os.path.exists('scraperwiki.sqlite'):
        print('Renomeando arquivo sqlite')
        shutil.copy('scraperwiki.sqlite', 'data.sqlite')
