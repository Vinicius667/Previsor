import os

# Caso o escript esteja sendo executado a partir de outro diretório
# muda o working directory para o caminho do script
os.chdir(os.path.dirname(__file__))

from Calcular_Previsao import calcular_previsao
from download_DB import download_db
from checar_rapeel import checar_Rapeel
from BIU import biu
import pandas as pd
import sys
from utils import *
import psutil
from datetime import date

# Caminho onde serão salvos os arquivos
#root_path = r"S:\BD\SKATE\BIU\Python"
#if not os.path.exists(root_path):
root_path = os.path.join(get_standard_folder_path("Documents"), "Previsor")
print(f"root_path: {root_path}")

# Onde serão salvos os arquivos baixados dos bancos de dados
download_path = os.path.join(root_path, "SKATE_Downloads")

# Onde serão salvas as previsões
previsoes_path = os.path.join(root_path, "Previsoes")

# Onde serão salvos os arquivos do BIU
biu_path = os.path.join(root_path, "BIU")

# Arquivos das previsões
checar_vrapeelcronograna_path = os.path.join(root_path, "Checar_Rapeel")
################################## Funções ##################################
def create_previsor_folders():
    # Cria pastas padrão necessárias
    create_folder(root_path)
    create_folder(download_path)
    create_folder(previsoes_path)
    create_folder(checar_vrapeelcronograna_path)
    create_folder(biu_path)




def menu_principal(download_directory):
    clear_console()
    # Apresenta menu principal
    global  atualizar

    dict_menu_principal = {
        0: "Sair",
        1: "Calcular previsão da base de dados",
        2: "Atualizar base de dados",
        3: "Checar Rapeel",
        4: "Gerar BIU"

    }
    show_options(dict_menu_principal)
    opcao_menu = get_num(dict_menu_principal)

    clear_console()
    print(dict_menu_principal[opcao_menu])
    print("\n")

    if atualizar and opcao_menu in [1,4]:
        download_directory = download_db(force_download=True)
        atualizar = False


    if opcao_menu == 0:
        print("Sessão terminada.")
        return 0

    if opcao_menu == 1:
        clear_console()
        calcular_previsao(download_directory,previsoes_path,perguntar=True)
        input("Aperte enter para retornar ao menu.")

    if opcao_menu == 2:
        download_directory = download_db(download_path, force_download=True)
        input("Aperte enter para retornar ao menu.")
        atualizar = False

    if opcao_menu == 3:
        lista_download=["vmonitoramentoug", "vmonitoramentousina"]
        if not os.path.exists(os.path.join(download_directory,'vrapeelcronograna.gzip')):
            lista_download.append('vrapeelcronograna')
        download_directory = download_db(download_path, force_download=True,lista_download=lista_download)
        checar_Rapeel(download_directory, checar_vrapeelcronograna_path)
        input("Aperte enter para retornar ao menu.")
    
    if opcao_menu == 4: 
        previsao_file = calcular_previsao(download_directory,previsoes_path,perguntar=False)
        biu(download_directory,biu_path)
        input("Aperte enter para retornar ao menu.")
        
    return opcao_menu

#############################################################################

last_download_path = last_download(download_path)


atualizar = True
perguntar_atualizar = True
download_directory = ""

if not last_download_path:
    atualizar = True
    perguntar_atualizar = False

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

    if not opcao_atualizar:
        atualizar = False
        download_directory = last_download_path


clear_console()
create_previsor_folders()
while (True):
    opcao_menu = menu_principal(download_directory)
    if opcao_menu == 0:
        break
