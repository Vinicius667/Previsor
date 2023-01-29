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
from tkinter import Tk
from tkinter.filedialog import askopenfilename
import psutil
from datetime import date


hoje = pd.to_datetime(date.today())
hoje_str = hoje.strftime(r'%Y_%m_%d')
skate_downloads_folder_name = "SKATE_Downloads"
root_path = os.path.join(get_standard_folder_path("Documents"), "Previsor")
download_path = os.path.join(root_path, skate_downloads_folder_name)
previsoes_path = os.path.join(root_path, "Previsoes")
checar_vrapeelcronograna_path = os.path.join(root_path, "Checar_Rapeel")


def create_previsor_folders():
    # Cria pastas padrão necessárias
    create_folder(root_path)
    create_folder(download_path)
    create_folder(previsoes_path)
    create_folder(checar_vrapeelcronograna_path)



def get_biu():
    Tk().withdraw()
    filename = askopenfilename()
    return filename


def menu_principal(directory):
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

    if atualizar and opcao_menu in [1,3,4]:
        directory = download_db(force_download=True)
        atualizar = False


    if opcao_menu == 0:
        print("Sessão terminada.")
        return 0

    if opcao_menu == 1:
        clear_console()
        calcular_previsao(directory,previsoes_path,perguntar=True)
        _ = input("Aperte enter para retornar ao menu.")

    if opcao_menu == 2:
        directory = download_db(download_path, force_download=True)
        _ = input("Aperte enter para retornar ao menu.")
        atualizar = False

    if opcao_menu == 3:
        global biu
        print("Data do início do BIU:")
        inicio_biu = get_date()
        print("Selecione o arquivo do BIU\n")
        biu_file_path = get_biu()
        directory = download_db(download_path, force_download=True, 
        lista_download=["vrapeelcronograna", "vmonitoramentoug", "vmonitoramentousina"])
        
        checar_Rapeel(biu_file_path, directory, inicio_biu, checar_vrapeelcronograna_path)
        input("Aperte enter para retornar ao menu.")
    
    if opcao_menu == 4: 
        previsao_file = calcular_previsao(directory,previsoes_path,perguntar=False)
        biu()
        input("Aperte enter para retornar ao menu.")
        
    return opcao_menu


last_download_path = last_download(download_path)


atualizar = True
perguntar_atualizar = True
directory = ""

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
        directory = last_download_path
        print("Definido"*10)


clear_console()
create_previsor_folders()
while (True):
    opcao_menu = menu_principal(directory)
    if opcao_menu == 0:
        break
