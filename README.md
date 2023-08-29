
# O Projeto

Este projeto consiste em um Datapipeline, desenvolvido para extrair, transformar e disponibilizar dados valiosos de forma eficiente. Desde a fase inicial de extração até a entrega de informações tratadas, nosso Datapipeline oferece uma abordagem abrangente para impulsionar insights e tomada de decisões fundamentadas.

Utilizaremos as mais recentes tecnologias, tornando o projeto capaz de extrair dados de várias fontes, independentemente do tamanho ou complexidade, garantindo uma coleta confiável.

## Metodologia

O projeto será desenvolvido seguindo uma configuração bem definida, utilizando as seguintes tecnologias para garantir uma implementação robusta e escalável:

- Python: A linguagem de programação Python foi escolhida para as etapas de extração e tratamento inicial dos dados. Sua versatilidade e rica biblioteca de pacotes facilitam a manipulação e preparação dos dados, garantindo uma base sólida para as próximas etapas do pipeline.

- GCP (Google Cloud Platform): Optamos pela GCP como nosso provedor de nuvem devido à sua confiabilidade e diversas ferramentas de gerenciamento de dados. Faremos uso de um Bucket para armazenar os dados brutos e, posteriormente, disponibilizá-los em uma tabela no BigQuery. Isso nos proporcionará armazenamento seguro e escalável, além de uma plataforma poderosa para análise de dados.

- DBT (Data Build Tool): Utilizaremos o DBT para realizar o refinamento dos dados, transformando-os em informações valiosas. Com suas funcionalidades de modelagem e organização, poderemos criar pipelines de dados eficientes e fáceis de manter, garantindo a confiabilidade dos resultados.

- Airflow: A automação é fundamental em nosso projeto, e o Airflow será o orquestrador escolhido para agendar e gerenciar todo o processo de forma automática. Com ele, poderemos definir fluxos de trabalho complexos e monitorar o progresso do pipeline de maneira eficiente.

- Docker (Contêiner): Para garantir a portabilidade e isolamento de nossos componentes, empacotaremos nossa aplicação em contêineres usando o Docker. Isso nos permitirá executar os mesmos processos de forma consistente em diferentes ambientes, desde o desenvolvimento até a produção.

## Dados de Viagens de Táxis, Veículos de Aluguel e Limousines de Nova York

Este projeto utilizará dados de viagens fornecidos pela NYC Taxi & Limousine Commission, agência responsável por licenciar e regular os táxis, veículos de aluguel e limousines da cidade de Nova York (NY). Os dados consistem em informações detalhadas sobre as viagens realizadas por esses veículos em determinado período de tempo.

Estes dados são públicos e podem ser encontrados no link abaixo: https://www.nyc.gov/site/tlc/index.page


## Arquitetura do Projeto

![Alt text](./assets/data_pipeline_architecture.jpg)

## Como rodar este projeto em sua máquina?

1 - Clone o repositório do GitHub:

Abra um terminal e navegue até o diretório onde deseja clonar o projeto. Execute o seguinte comando:

```shell
git clone https://github.com/vfdesouza/NYC-Taxi-Data-Pipeline-ELT.git
```
2 - Instale o Pyenv para gerenciar ambientes virtuais:

Siga as instruções no [repositório oficial do Pyenv](https://github.com/pyenv/pyenv#installation) para instalar o Pyenv em sua máquina. Isso permitirá que você crie um ambiente virtual isolado para o projeto.

3 - Crie e ative um ambiente virtual:

Navegue até o diretório do projeto e crie um ambiente virtual usando o Python desejado (3.11, no caso) com o Pyenv:

```shell
pyenv install 3.11.0
pyenv virtualenv 3.11.0 nome-do-ambiente
pyenv activate nome-do-ambiente
```
4 - Instale as dependências do projeto:

Dentro do diretório do projeto, instale as dependências Python especificadas no arquivo requirements.txt:

```shell
pip install -r requirements.txt
```

5 - Instale o Docker e o Airflow:

Siga as instruções de instalação do [Airflow](https://airflow.apache.org/docs/apache-airflow/stable/installation/installing-from-pypi.html) e [Docker](https://pypi.org/project/docker/) no seu sistema operacional. Você pode optar por usar o [WSL](https://learn.microsoft.com/pt-br/windows/wsl/install) no Windows ou uma VM com Linux para isso.

6 - Crie o arquivo de credenciais do Google:

Crie uma conta gratuita no Google Cloud Platform (GCP) e gere um arquivo JSON com as credenciais de serviço para acessar o Google Cloud Storage e o BigQuery. Guarde esse arquivo em um local seguro.

7 - Configure as variáveis de ambiente:

No diretório do projeto, crie um arquivo .env com as seguintes variáveis de ambiente:

```shell
GOOGLE_APPLICATION_CREDENTIALS=/caminho/para/credentials/file.json
URL_BASE=https://d37ci6vzurychx.cloudfront.net/trip-data/
BUCKET_NAME=bucket-name
DATASET_RAW_ZONE=raw_zone
GOOGLE_GCP_PROJECT=project-google-name
```

8 - Recomendação: Instale o DBT localmente para testes antes de enviar a imagem docker para o docker hub:

Instale o [dbt-core com o adaptador do Big Query](https://docs.getdbt.com/docs/core/pip-install):

No seu ambiente virtual instale o dbt-bigquery:

```shell
pip install dbt-bigquery
```
9 - Ajuste as credenciais no nyc_dbt\profiles.yml e teste as configurações e conexões do projeto:

Indique corretamente o caminho do arquivo de credenciais .json do google na chave keyfile do arquivo nyc_dbt\profiles.yml.

Obs.: no Windows:

```shell
dbt debug --profiles-dir ..\\caminho\\do\\projeto\\nyc_dbt\\ --project-dir ..\\caminho\\do\\projeto\\nyc_dbt\\
```

10 - Criei o arquivo dockerfile para criação da imagem que será usada na dag do airflow:

O dockerfile para este projeto possui a seguinte configuração:

```shell
FROM python:3.11-slim as builder

RUN apt-get update && apt-get upgrade -y && apt-get clean

WORKDIR /app

COPY requirements.txt .
RUN pip install --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt

FROM python:3.11-slim

WORKDIR /app

COPY --from=builder /usr/local/lib/python3.11/site-packages /usr/local/lib/python3.11/site-packages
COPY . /app

CMD ["tail", "-f", "/dev/null"]
```

Para criar a imagem:

```shell
docker build -t nome-da-sua-imagem:tag .
```

11 - Crie uma conta no [docker hub](https://hub.docker.com/) e envie  a sua imagem para o repositório:

Obs.: para enviar a sua imagem para o repositório, você precisa estar logado nela no terminal linux. Para isso, confira este [link](https://docs.docker.com/engine/reference/commandline/login/).

Após ter criado a conta no docker hub, com o docker instalado, envie a imagem criada para o repositório:

```shell
docker tag nome-da-sua-imagem:tag seu-usuario-docker-hub/nome-da-sua-imagem:tag
docker push nome-da-sua-imagem:tag
```
12 - Com o airflow instalado, crie a sua dag:

O código da dag esta no diretório \config\airflow\airflow_dags no arquivo dag-airflow.py.

Obs.: no código da dag, na variável "image_python", altere o valor e indique a mesma imagem enviada para o docker hub.

- Copie o arquivo da dag e cole no local que você instalou o airflow, em uma pasta chamada "dags".
- Inicie o Airflow:

```shell
airflow standalone
```

Obs.: é importante analisar o log gerado após este comando, pois nele terá a porta de acesso a UI do Airflow e as credenciais para login.

Depois de todos esses passos, basta executar a dag e conferir os resultados no log do Airflow. Se tudo der certo, os arquivos serão baixados e salvos no Storage do GCP e as tabelas serão criadas no BigQuery.
## Conclusão

Em resumo, o nosso projeto tem como foco principal a aplicação de ferramentas de engenharia de dados para extrair e processar informações provenientes de diversas fontes. Através dessa abordagem, buscamos garantir a qualidade e integridade dos dados, independentemente de sua origem.

Nossa jornada envolveu a exploração de diferentes tecnologias e a adaptação a desafios variados. O resultado final é um sistema que viabiliza a gestão eficaz de dados, permitindo que nossos esforços se concentrem na análise e insights, em vez de tarefas repetitivas.
