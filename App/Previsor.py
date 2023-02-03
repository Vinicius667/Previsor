import os

# Caso o escript esteja sendo executado a partir de outro diretório
# muda o working directory para o caminho do script
os.chdir(os.path.dirname(__file__))

from Calcular_Previsao import calcular_previsao
from download_DB import download_db,atualizar_db
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

# Onde serão salvos os arquivos baixados dos bancos de dados
download_path = os.path.join(root_path, "Download")

# Pasta Previsor/Download_Atual
    # Pode ser modificada por: 
        # checar_Rapeel (via opcao 3), 
        # calcular_previsao (via opcao 1),
        # download_db (via opcao 2)


# Onde serão salvas as previsões
previsoes_path = os.path.join(root_path, "Previsoes")



# Onde serão salvos os arquivos do BIU
biu_path = os.path.join(root_path, "BIU")
estagio_I = os.path.join(biu_path, "Estagio_I")
estagio_II = os.path.join(biu_path, "Estagio_II")
biu_download_path = os.path.join(biu_path, "Download")

# Onde serão salvos os arquivos da checagem - Não possui pasta downloads - Sempre usa a atual
checar_rapeel = os.path.join(root_path, "Checar_Rapeel")

checar_vrapeelcronograna_path = os.path.join(root_path, "Checar_Rapeel")



################################## Funções ##################################
def create_previsor_folders():
    # Cria pastas padrão necessárias
    create_folder(root_path)
    create_folder(download_path)
    create_folder(previsoes_path)
    create_folder(checar_vrapeelcronograna_path)
    create_folder(biu_path)
    create_folder(biu_download_path)
    create_folder(estagio_I)
    create_folder(estagio_II)


def menu_principal():
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

    if opcao_menu == 0:
        print("Sessão terminada.")
        return 0

    if opcao_menu == 1:
        clear_console()
        calcular_previsao(download_path,previsoes_path,perguntar=True)
        input("Aperte enter para retornar ao menu.")

    if opcao_menu == 2:
        download_db(download_path, force_download=True)
        input("Aperte enter para retornar ao menu.")

    if opcao_menu == 3:
        download_db(download_path, force_download=True)
        atualizar_db(perguntar=True)
        checar_Rapeel(download_path, checar_vrapeelcronograna_path)
        input("Aperte enter para retornar ao menu.")
     
    if opcao_menu == 4: 
        biu(biu_path,download_path)
        input("Aperte enter para retornar ao menu.")
        
    return opcao_menu

#############################################################################
clear_console()
create_previsor_folders()
while (True):
    opcao_menu = menu_principal()
    if opcao_menu == 0:
        break

