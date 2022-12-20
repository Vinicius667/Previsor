from sqlalchemy.engine import URL
from sqlalchemy import create_engine
import sys
import os
import re
import pickle
import pandas as pd


if sys.platform == "win32":
    from win32com.shell import shell, shellcon # type: ignore


def get_num(valid_nums, return_on_error=0):
    # Recebe um inteiro dado uma lista de valores válidos
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
    # Recebe data
    data_input = input("Data no formato DD/MM/YYYY: ")
    msg_error = f"{data_input} não é uma data válida."
    try:
        data_input = re.sub(r"\s", "", data_input, 0)
        data = re.findall(r"(\d{2})/(\d{2})/(\d{4})", data_input)[0]
        dia, mes, ano = [int(el) for el in data]
        data = pd.Timestamp(day=dia, month=mes, year=ano)
    except:
        raise ValueError(msg_error)
    return data


def show_options(dict):
    # Mostra opções para o usuário escolher
    print("Escolha uma opção:")
    for num, text in dict.items():
        print(f"{num}) - {text}")


def read_file(path: str):
    # Lê arquivo
    with open(path, 'r') as f:
        lines = "".join(f.readlines())
    return lines


def get_standard_folder_path(folder):
    # Retorna caminho de pastas padrões do Windows
    # Tenta acessar no linux. Cria caso não houver
    try:
        if sys.platform == "win32":
            csidl = {
                "Documents":  shellcon.CSIDL_PERSONAL,
                "Desktop": shellcon.CSIDL_DESKTOP,
            }[folder]
            # https://learn.microsoft.com/en-us/windows/win32/shell/knownfolderid
            folder_path = shell.SHGetFolderPath(0, csidl, None, 0)
        else:
            user_path = os.path.expanduser("~")
            folder_path = os.path.join(user_path, folder)
            create_folder(folder_path)
    except KeyError:
        raise ValueError("Pasta não encontrada.")
    return folder_path


def create_folder(path):
    # Cria pasta caso já não tenha sido criada
    if not os.path.exists(path):
        os.mkdir(path)
        print(f"Diretório criado: {path}")



def create_odbc_engine(server, database):
    # Cria engine para conexão ao banco de dados
    connection_string = 'DRIVER={SQL Server Native Client 11.0};SERVER=' + \
        server+';DATABASE='+database+';Trusted_Connection='+'YES'
    connection_url = URL.create(
        "mssql+pyodbc", query={"odbc_connect": connection_string})
    engine = create_engine(connection_url)
    return engine


def save_pickle(var, path: str):
    # Salva variável com o pickle
    with open(path, 'wb') as handle:
        pickle.dump(var, handle, protocol=pickle.HIGHEST_PROTOCOL)


def load_pickle(path: str):
    # Carrega variável com o pickle
    with open(path, 'rb') as handle:
        var = pickle.load(handle)
        return var


def clear_console():
    # Apaga console
    os.system('cls' if os.name == 'nt' else 'clear')


if __name__ == "__main__":
    clear_console()
    print("Ok")
