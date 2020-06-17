This is a scraper that runs on [Morph](https://morph.io). To get started [see the documentation](https://morph.io/documentation)

## Projeto de Scraper dos Informes Diários dos Fundos de Investimentos do Brasil

Este projeto objetiva principalmente resolver a necessidade de obtenção das informações das cotas dos fundos de investimento. Essas cotas são utilizadas para o acompanhamento do desempenho dos fundos.

As cotas são obtidas diretamente do Portal de Dados Abertos da CVM:
* http://dados.cvm.gov.br/ 

Neste portal existem várias bases de dados sobre os fundos de investimentos. O foco deste projeto encontra-se nos informes diários:
* Fundos de Investimento: Documentos: Informe Diário

Caso não seja um desenvolvedor e não consiga utilizar com qualidade este scraper, sugere-se buscar uma solução com interface gráfica mais bem desenvolvida como o https://infofundos.com.br/.

Esse projeto foi desenvolvido com o objetivo de encapsular a parte de obtenção dos dados e a utilização da API do morph.io para a obtenção das cotas.

A documentação da API do Morph.io pode ser encontrada em:
* https://morph.io/documentation/api

Em particular, pretende-se utilizar a API do Morph.io para consumir os dados de dentro do Google SpreadSheets por meio do Google Apps script:
* https://developers.google.com/apps-script

## FORMATO DOS DADOS
A documentação da CVM para este dataset pode ser obtida em: 
* http://dados.cvm.gov.br/dataset/fi-doc-inf_diario

### Documentação da CVM sobre o dataset
O *INFORME DIÁRIO* é um demonstrativo que contém as seguintes informações do fundo, relativas à data de competência:
* Valor total da carteira do fundo;
* Patrimônio líquido;
* Valor da cota;
* Captações realizadas no dia;
* Resgates pagos no dia;
* Número de cotistas

O conjunto de dados disponibiliza os informes diários referentes aos Fundos de Investimento nos últimos doze meses.

Os arquivos referentes aos meses corrente (M) e anterior (M-1) serão atualizados diariamente com as eventuais reapresentações. A atualização ocorre de terça a sábado, às 08:00h, com os dados recebidos pelo CVMWeb até as 23:59h do dia anterior.

Os arquivos referentes aos meses M-2, M-3, ..., até M-11 serão atualizados semanalmente com as eventuais reapresentações.

Como o dicionário de dados pode ser atualizado pela CVM, sugere-se a consulta ao dicionário de dados que pode ser encontrado na página do dataset:
* http://dados.cvm.gov.br/dados/FI/DOC/INF_DIARIO/META/meta_inf_diario_fi.txt

## Decisões de projeto
No projeto optou-se por obter as cotas a partir de 2018. 
Para obter os dados de 2017 será necessário alterar a variável de ambiente SCRAPER_INFORME_CVM_ANO_INICIAL.

Nota: Como os dados anteriores a 2017 estão compactados, este projeto não contemplou a obtenção destas informações anteriores a 2017.

## Agendamento
TODO: Deve-se definir uma política de agendamento e documentá-la

## Variáveis de ambiente
SCRAPER_INFORME_CVM_ANO_INICIAL
default: 2018

### Variáveis de ambiente do Morph.io
Conforme a documentação do ScraperWiki utilizado pelo Morph.io:
https://github.com/sensiblecodeio/scraperwiki-python

SCRAPERWIKI_DATABASE_NAME
default: scraperwiki.sqlite - name of database

SCRAPERWIKI_DATABASE_TIMEOUT
default: 300 - number of seconds database will wait for a lock
