from sqlalchemy.engine import URL
from sqlalchemy import create_engine
import pickle
from win32com.shell import shell, shellcon
import os
import re
from datetime import date
import pandas as pd

# Recebe um inteiro dado uma lista de valores válidos
def get_num(valid_nums,return_on_error = 0):
    num = input("")
    msg_error = f"{num} não é uma opção válida."
    try:
        num = int(num)
    except ValueError:
        print(msg_error)
        return return_on_error
    if num not in valid_nums:
        print(msg_error)
        return return_on_error
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


# Mostra opções para o usuário escolher
def show_options(dict):
    print("Escolha uma opção:")
    for num,text in dict.items():
        print(f"{num}) - {text}")

# Lê arquivo
def read_file(path:str):
    with open(path, 'r') as f:
        lines = "".join(f.readlines())
    return lines

# Retorna caminho de pastas padrões do Windows
def get_standard_folder_path(folder):
    try:
        csidl = {
           "Documents":  shellcon.CSIDL_PERSONAL,
           "Desktop": shellcon.CSIDL_DESKTOP,
        }[folder]
    except KeyError:
        raise ValueError("Pasta não encontrada.")

    # https://learn.microsoft.com/en-us/windows/win32/shell/knownfolderid
    return shell.SHGetFolderPath(0, csidl, None, 0)

# Cria pasta caso já não tenha sido criada
def create_folder(path):
    if not os.path.exists(path):
        os.mkdir(path)
        print(f"Diretório criado: {path}")

# cria engine para conexão ao banco de dados
def create_odbc_engine(server,database):
    connection_string = 'DRIVER={SQL Server Native Client 11.0};SERVER='+server+';DATABASE='+database+';Trusted_Connection='+'YES'
    connection_url = URL.create("mssql+pyodbc", query={"odbc_connect": connection_string})
    engine = create_engine(connection_url)
    return engine

# Salva variável com o pickle
def save_pickle(var,path:str):
    with open(path, 'wb') as handle:
        pickle.dump(var, handle, protocol=pickle.HIGHEST_PROTOCOL)

# Carrega variável com o pickle
def load_pickle(path:str):
    with open(path, 'rb') as handle:
        var = pickle.load(handle)
        return var

# Apaga console
def clear_console():
    os.system('cls' if os.name=='nt' else 'clear')

if __name__ == "__main__":
    clear_console()
    print("Ok")