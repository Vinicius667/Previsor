
import pandas as pd
from datetime import date,datetime
import os
from utils import *
import shutil


"""
Cria as conexões com o banco de dados skate e baixa as tabelas vMonitoramentoLeilao, vMonitoramentoUG e
vMonitoramentoUsina. Recomenada-se executar essa células apenas quando deseja-se atualizar o banco de dados SKATE, já esse processo pode demorar alguns minutos para ser executado. Após baixadas, as tabelas são salvas em formato parquet que podem ser lidos com a função pandas.read_parquet(). 
"""

change_2_script_dir()

server = 'SAG003\SBD172'
database = 'FiscalizacaoGeracao'
skate_engine = create_odbc_engine(server,database)
biu_download_cols  = ['vmonitoramentoleilao', 'vmonitoramentoug', 'vmonitoramentousina', 'vrapeelacesso', 'vrapeelcontratorecurso', 'vrapeelcronograma', 'vrapeelempreendimento', 'vrapeellicenciamento', 'vrapeeloperacaoug']
TESTE = True
TESTE_DIR = './Teste_files'

def atualizar_db(download_path,cols=False,perguntar=False):
    if not cols:
        cols = biu_download_cols
    atualizar = True
    perguntar_atualizar = True
    last_download_path = last_download(download_path, cols=biu_download_cols)

    if not last_download_path:
        atualizar = True
        perguntar_atualizar = False


    perguntar_atualizar = perguntar_atualizar & perguntar

    if perguntar_atualizar:
        clear_console()
        print("Os últimos downloads encontrados são: \n")
        show_download_pickle(last_download_path)

        dict_atualizar = {
            0 : "Não",
            1 : "Sim"
        }
        print("Deseja baixar os dados novamente?")
        show_options(dict_atualizar)
        opcao_atualizar = get_num(dict_atualizar)

        if opcao_atualizar == 0:
            atualizar = False

    if atualizar:
        download_db(download_path,lista_download=cols)
        return True
    return False

def download_db(download_path = None, queries_path = None, lista_download = biu_download_cols,test=TESTE):
    
    print("\n" + " Baixando arquivos ".center(60, "*") + "\n")
    if not download_path:
        root_path = os.path.join(get_standard_folder_path("Documents"), "Previsor")
        download_path = os.path.join(root_path,'Download')


    if not queries_path:
        queries_path = "./Queries/"

    # Caso não seja informada a data referente um arquivo que já foi baixado previamente, serão baixadas
    # as informações do SKATE no dia de hoje.


    download_directory = download_path
    log_path = f'{download_directory}/log.pickle'

    if not os.path.exists(log_path):
        log = {}
        print("Criando arquivo de log...")
    else:
        log = load_pickle(log_path)
        print("Carregando arquivo de log...")



    # Baixa informações, caso já não tenham sido baixadas
    if not test:
        for db_name in lista_download:
            print(os.path.join(queries_path,f"{db_name}.txt"))
            file_path = f"{download_directory}/{db_name}.gzip"
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
        save_pickle(log,log_path)
    else:
        print(f"Movendo arquivos de teste:")
        for db_name in lista_download:
            file_path = f"{download_directory}/{db_name}.gzip"
            file_path_test = os.path.join(TESTE_DIR,f'{db_name}.gzip')
            print(f"{file_path_test} >>> {file_path}")

            shutil.copyfile(file_path_test,file_path)

        log_path_test = os.path.join(TESTE_DIR,'log.pickle')
        shutil.copyfile(log_path_test,log_path)

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

    download_directory = download_db(download_path, lista_download=["vmonitoramentoleilao","vmonitoramentoug" ,"vmonitoramentousina"])