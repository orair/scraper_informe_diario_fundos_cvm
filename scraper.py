
# -*- coding: utf-8 -*-
import os
import datetime
import click
import scraperwiki
import pandas as pd
import urllib

@click.command("Gerenciador do Scraper dos Informes Diários de Fundos de Investimentos da CVM")
@click.option('--ano_inicial', 
                default=lambda: 
                    os.environ.get('SCRAPER_INFORME_CVM_ANO_INICIAL', 2018), 
                show_default="Variável de ambiente SCRAPER_INFORME_DIARIO_CVM_ANO_INICIAL ou o valor padrão 2018")
@click.option('--skip_informacoes_cadastrais', 
                default=False, is_flag=True, 
                show_default=True)
def executa_scraper(ano_inicial, skip_informacoes_cadastrais):
    # TODO: Permitir mudar o database name
    # Caso vá testar localmente utilizando a biblioteca padrão do scraper wiki,
    # será necessário especificar a variável de ambiente para o valor padrão do morph.io
    # os.environ['SCRAPERWIKI_DATABASE_NAME'] = 'sqlite:///data.sqlite'

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
    
    return informe_diario_df;

def obtem_periodos(ano_inicial=2018):
    periodos=[]

    today = datetime.date.today()
    ano_final = int(today.strftime('%Y'))
    mes_final = int(today.strftime('%m'))

    for ano in range(ano_inicial, ano_final+1):
        for mes in range(1,13):
            # evita pegar anos futuros, visto que o arquivo ainda não existe
            if ano == ano_final and mes > mes_final:
                break

            periodo=f'{ano:04}{mes:02}'
            periodos.append(periodo)

    return periodos;

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
 
if __name__ == '__main__':
    executa_scraper();

    # Criar os índices do banco de dados
    #create_command='CREATE [UNIQUE] INDEX index_name '
    #              + 'ON table_name(column_list); '
    # TODO: scraperwiki.sqlite.execute(sql[, vars], verbose=1)
