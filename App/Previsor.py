from Calcular_Previsao import calcular_previsao
from download_DB import download_db
import pandas as pd
from datetime import date
import os,sys
from win32com.shell import shell, shellcon
from utils import *
import re

root = get_standard_folder_path("Documents")
hoje =  pd.to_datetime(date.today())
hoje_str = hoje.strftime(r'%Y_%m_%d')
skate_downloads_folder_name = "SKATE_Downloads"
root_path = os.path.join(get_standard_folder_path("Documents"), "Previsor")
download_path = os.path.join(root_path,skate_downloads_folder_name)
previsoes_path = os.path.join(root_path,"Previsoes")


def create_previsor_folders():
    create_folder(root_path)
    create_folder(download_path)
    create_folder(previsoes_path)


def get_num(valid_nums):
    num = input("")
    msg_error = f"{num} não é uma opção válida."
    try:
        num = int(num)
    except ValueError:
        print(msg_error)
        return -1
    if num not in valid_nums:
        print(msg_error)
        return -1
    return num


def get_date():
    data_input = input("Data no formato DD/MM/YYYY: ")
    msg_error = f"{data_input} não é uma data válida."
    try:
        data_input =  re.sub(r"\s", "", data_input, 0)
        data = re.findall(r"(\d{2})/(\d{2})/(\d{4})", data_input)[0]
        dia, mes, ano = [int(el) for el in data]
        data = pd.Timestamp(day=dia,month=mes,year=ano)
    except:
        raise ValueError(msg_error)
    return data


def show_options(dict):
    print("Escolha uma opção:")
    for num,text in dict.items():
        print(f"{num}) - {text}")


def calc_previsao():
    global result, df
    print("\n" + " Baixando arquivos ".center(60,"*") + "\n")
    directory = download_db(download_path,previsoes_path,lista_download=["skate_leilao","skate_ug" ,"skate_usinas"])
    print("\n" +"*".center(60,"*") + "\n")
    result,df = calcular_previsao(directory)
    return result,df


def menu_principal():
    global result,df
    dict_menu_principal = {
        0 : "Sair",
        1 : "Calcular previsão da base de dados",
        2 : "Atualizar base de dados",
        #3 : "Calcular previsão de teste",

    }
    show_options(dict_menu_principal)
    num = get_num(dict_menu_principal)
    if num == 0:
        print("Sessão terminada.")
        return 0

    if num == 1:
        print("Calculando previsão...")
        result,df = calc_previsao()
        if result == "Ok":
            print("Exportando arquivo com previsões...")
            previsao_file= os.path.join(previsoes_path,f"Previsao_OC_{hoje_str}.xlsx")
            df.to_excel(previsao_file,index=False)
            os.system('cls' if os.name == 'nt' else 'clear')
            print(f"Arquivo exportado: {previsao_file}\n\n")

    if num == 2:
        directory = download_db(download_path,previsoes_path,force_download=True, lista_download=["skate_leilao","skate_ug" ,"skate_usinas"])


create_previsor_folders()

while(True):
    option = menu_principal()
    if option == 0:
        break