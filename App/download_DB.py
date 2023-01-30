
import pandas as pd
from datetime import date,datetime
import os
from utils import *

"""
Cria as conexões com o banco de dados skate e baixa as tabelas vMonitoramentoLeilao, vMonitoramentoUG e
vMonitoramentoUsina. Recomenada-se executar essa células apenas quando deseja-se atualizar o banco de dados SKATE, já esse processo pode demorar alguns minutos para ser executado. Após baixadas, as tabelas são salvas em formato parquet que podem ser lidos com a função pandas.read_parquet(). 
"""

change_2_script_dir()

server = 'SAG003\SBD172'
database = 'FiscalizacaoGeracao'
skate_engine = create_odbc_engine(server,database)


def download_db(download_path = None, queries_path = None, lista_download = ['vmonitoramentoleilao', 'vmonitoramentoug', 'vmonitoramentousina', 'vrapeelacesso', 'vrapeelcontratorecurso', 'vrapeelcronograma', 'vrapeelempreendimento', 'vrapeellicenciamento', 'vrapeeloperacaoug'],data=False,force_download=True):

    if(force_download and data):
        raise ValueError(f"Argumentos force_download e data não podem ser ambos não nulos.")

    print("\n" + " Baixando arquivos ".center(60, "*") + "\n")
    if not download_path:
        skate_downloads_folder_name = "SKATE_Downloads"
        root_path = os.path.join(get_standard_folder_path("Documents"), "Previsor")
        download_path = os.path.join(root_path,skate_downloads_folder_name)


    if not queries_path:
        queries_path = "./Queries/"

    # Caso não seja informada a data referente um arquivo que já foi baixado previamente, serão baixadas
    # as informações do SKATE no dia de hoje.
    if not data:
        today = date.today().strftime('%Y_%m_%d')
        download_directory = f"{download_path}/{today}/"
    else:
        download_directory = f"{download_path}/{data}/"
    
    log_path = f'{download_directory}/log.pickle'

    if not os.path.exists(download_directory):
        if data:
            raise ValueError(f"O diretório '{download_directory}' não existe")
        os.makedirs(download_directory)
        print(f"Novo diretório criado: {download_directory}")

    if not os.path.exists(log_path):
        log = {}
        print("Criando arquivo de log...")
    else:
        log = load_pickle(log_path)
        print("Carregando arquivo de log...")



    # Baixa informações, caso já não tenham sido baixadas
    for db_name in lista_download:
        file_path = f"{download_directory}/{db_name}.gzip"
        if (not os.path.exists(file_path)) or force_download:
            # Caso em que foi pedido uma data e um arquivo não foi baixado.
            if data:
                raise ValueError(f"Arquivo não encontrado: {file_path}.")
            query =  read_file(os.path.join(queries_path,f"{db_name}.txt"))
            db = pd.read_sql_query(query,skate_engine)
            log[db_name] = datetime.now()       
            for col in db.columns:
                if (col[:3] == "Dat"):
                    db[col] = pd.to_datetime(db[col],errors="coerce")
                if (col[:3] == "Mda"):
                    db[col] = db[col].astype(float)
                if (col[:3] == "Ide"):
                    db[col] = db[col].astype(int)
            db.to_parquet(file_path)
            print(f"'{db_name}.gzip' salvo em '{download_directory}'.")
        else:
            if not data:
                print(f"{db_name} já foi baixado no dia: {log[db_name].strftime('Dia: %d/%m/%y - Horário: %H:%M:%S')}. Portanto não foi baixado novamente.")
            else:
                print(f"O arquivo {db_name}, que foi baixado em {log[db_name].strftime('Dia: %d/%m/%y - Horário: %H:%M:%S')}, foi carregado.")
    save_pickle(log,log_path)

    
    print("\n" + "*".center(60, "*") + "\n")
    return download_directory


if __name__ == "__main__":

    # Muda working directory
    change_2_script_dir()

    hoje =  pd.to_datetime(date.today())
    hoje_str = hoje.strftime(r'%Y_%m_%d')

    # Nome da pasta onde serão baixadas as informações do SKATE 
    skate_downloads_folder_name = "SKATE_Downloads"

    # Pasta onde serão salvas todas as informações do SKATE (Pasta Documentos)
    root_path = os.path.join(get_standard_folder_path("Documents"), "Previsor")

    # Caminho da pasta dos downloads
    download_path = os.path.join(root_path,skate_downloads_folder_name)

    # Caminho da pasta dos resultados das previsões
    previsoes_path = os.path.join(root_path,"Previsoes")

    download_directory = download_db(download_path,force_download=True, lista_download=["vmonitoramentoleilao","vmonitoramentoug" ,"vmonitoramentousina"])