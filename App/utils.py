from sqlalchemy.engine import URL
from sqlalchemy import create_engine
import sys
import os
import re
import pickle
import pandas as pd
import subprocess
import inspect

if sys.platform == "win32":
    from win32com.shell import shell, shellcon # type: ignore



def read_file(path: str,encoding = "utf-8"):
    # Lê arquivo
    #Encoding: utf-8, utf-16, utf-32, utf-16-be, utf-16-le, utf-32-be, utf-32-le
    with open(path, 'r', encoding = encoding) as f:
        lines = "".join(f.readlines())
    return lines


def write_file(text:str,path:str,encoding = "utf-8"):
    text = text.encode(encoding)
    # Escreve arquivo
    with open(path, 'wb') as f:
        f.write(text)


def change_2_script_dir():
    # Change working directory to caller script
    cwd = os.getcwd()
    script_path = inspect.stack()[1][0].f_code.co_filename
    script_path = os.path.dirname(os.path.realpath(script_path))
    if cwd != script_path:
        os.chdir(script_path)
        print(f"Current working directory was changed to: {script_path}")


def get_num(valid_nums, return_on_error=0, input_msg = "Opção: ",):
    # Recebe um inteiro dado uma lista de valores válidos
    num = input(input_msg)
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


def read_file(path: str,encoding = "utf-8"):
    # Lê arquivo
    with open(path, 'r', encoding = encoding) as f:
        lines = "".join(f.readlines())
    return lines


def write_file(text:str,path:str):
    # Escreve arquivo
    with open(path, 'w') as f:
        f.write_file(text)

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


def open_folder(path):
    if sys.platform == "win32":
        os.startfile(path)
    elif sys.platform == "linux":
        subprocess.Popen(["xdg-open", path])
    else:
        raise ValueError("OS não suportada")

def perguntar_abrir_pasta(path, msg = "Deseja abrir pasta com os arquivos exportados?"):
    print("\n")
    print(msg)

    options = {
        0: "Não",
        1: "Sim"
    }
    show_options(options)
    opcao_pasta= get_num(options)

    if opcao_pasta:
        open_folder(path)


# Cria parquet de arquivos .xlsx .csv e json para leitura mais rápida
# Checa intersecção de valores em alguma coluna
# Junta arquivos em um dataframe e retorna-o
# Apesar de serem retornados todos arquivos concatenados em um único dataframe,
# cada arquivo lido gera um arquivo partquet separadamente 
def sheet_to_parquet(file_paths:list,colunas=None,parquet_folder:str=False,colunas_sem_repetir:list=False,ignore_parquets=False):
    """
    file_paths => lista de arquivos a serem lidos
    colunas => colunas que serão lidas em todos os arquivos. Caso não seja passada uma lista, serão utilizadas todas as colunas do primeiro arquivo.
    parquet_folder => pasta onde serão salvos os parquets. Caso omisso, será criada uma pasta ./Parquets na mesma pasta em que cada arquivo foi lido
    colunas_sem_repetir => Checa para essas colunas se há valores que existe em mais de um dos arquivos
    ignore_parquets => Caso True, independentemente se os parquets tenham sido criados, o arquivo original será lido e um novo parquet será criado
    """        

    if colunas:
        df = pd.DataFrame(columns=colunas)
    for k,file_path in enumerate(file_paths):
        parquet_criado = False
        file_name,extension = os.path.split(file_path)[1].split(".")
        if not parquet_folder:
            parquet_folder = f"{os.path.split(file_path)[0]}/Parquets/"
        parquet_file_path= f"{parquet_folder}{file_name}.gzip"
        if os.path.exists(parquet_file_path) and not ignore_parquets:
            print(f"Lendo arquivo: {parquet_file_path}")
            df_dummy = pd.read_parquet(parquet_file_path)
            parquet_criado = True
        else: 
            print(f"Lendo arquivo: {file_path}")
            if extension == "xlsx":
                if (k == 0) and (not colunas):
                    df_dummy = pd.read_excel(file_path)
                    colunas = df_dummy.columns.to_list()
                else:
                    df_dummy = pd.read_excel(file_path)[colunas]
            elif extension == "csv":
                df_dummy = pd.read_csv(file_path)[colunas]
            elif extension == "json":
                df_dummy = pd.read_json(file_path)[colunas]
            else:
                raise ValueError(f"Extensão não suportada: {extension}")
        if  (k==0):
            df = pd.DataFrame(columns=colunas)
            
            if not os.path.exists(parquet_folder):
                print(f"Criando diretório: {parquet_folder}")
                os.mkdir(parquet_folder)
        if colunas_sem_repetir:
            for coluna_sem_repetir in colunas_sem_repetir:  
                valores_repetidos = np.intersect1d(df[coluna_sem_repetir].unique(), df_dummy[coluna_sem_repetir].unique())
                if len(valores_repetidos) > 0:
                    raise ValueError(f"No arquivo {file_path} na coluna {coluna_sem_repetir} há valores que também estão em outro arquvio: {', '.join(valores_repetidos)}.")
        if not parquet_criado:
            print(f"Criando arquivo: {parquet_file_path}")
            create_folder(parquet_folder)
            df_dummy.to_parquet(parquet_file_path)
        df = pd.concat([df,df_dummy],ignore_index=True)
    return df


if __name__ == "__main__":
    clear_console()
    path = os.path.dirname(__file__)
    open_folder(path)

