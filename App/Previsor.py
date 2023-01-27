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


def calc_previsao():
    # Calcula previsão
    global result, skate_merged
    directory = download_db(download_path, lista_download=[
                            "vmonitoramentoleilao", "vmonitoramentoug", "vmonitoramentousina"])
    result, skate_merged = calcular_previsao(directory)
    skate_export = skate_merged[['NomUsina', 'IdeUsinaOutorga', 'NumUgUsina', 'SigTipoGeracao',
                                 'Previsao_OC', 'Dat_OC_obrigacao', 'DatMonitoramento', 'FaseAtual', 'Indicador','flagOPTeste30dias','DatPrevisaoIniciobra','DatInicioObraOutorgado']].copy()
    skate_export["Previsao_OC"] = skate_export.Previsao_OC.dt.normalize()
    skate_export["DatMonitoramento"] = skate_export.DatMonitoramento.dt.normalize()
    return result, skate_export


def get_biu():
    Tk().withdraw()
    filename = askopenfilename()
    return filename


def menu_principal():
    clear_console()
    # Apresenta menu principal
    global skate_merged, skate_export, atualizar
    global result, df
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
        print("Calculando previsão...")
        result, skate_export = calc_previsao()
        if result == "Ok":
            segundos_estimado = ((2901 / psutil.cpu_freq().max)
                                * (skate_export.shape[0] / 50_000) * 24)
            print(
                f"Exportando arquivo com previsões... Previsão: {segundos_estimado:.0f} segundos.")
            previsao_file_excel = os.path.join(
                previsoes_path, f"Previsao_OC_{hoje_str}.xlsx")
            previsao_file_parquet = os.path.join(previsoes_path, f"Previsao_OC_{hoje_str}.gzip")
            previsor_detalhe = os.path.join(
                previsoes_path, f"Previsao_OC_detalhada_{hoje_str}.xlsx")
            skate_export.to_excel(previsao_file_excel, index=False)
            skate_export.to_parquet(previsao_file_parquet, index=False)
            print(f"Arquivo exportado: {previsao_file_excel}\n")
            segundos_estimado = ((2901 / psutil.cpu_freq().max)
                                * (skate_merged.shape[0] / 50_000) * 120)
            print(
                f"Deseja exportar arquivo detalahado? Previsão: {segundos_estimado:.0f} segundos.\n")

            options = {
                0: "Não",
                1: "Sim"
            }
            show_options(options)
            opcao_previsao = get_num(options)

            if opcao_previsao:
                print("Exportando arquivo de previsão detalhado...")
                skate_merged.to_excel(previsor_detalhe, index=False)
                print(f"Arquivo exportado: {previsor_detalhe}\n\n")
            perguntar_abrir_pasta(previsoes_path)
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
        directory = download_db(download_path, force_download=False, lista_download=[
                                "vrapeelcronograna", "vmonitoramentoug", "vmonitoramentousina"])
        
        checar_Rapeel(biu_file_path, directory, inicio_biu, checar_vrapeelcronograna_path)
        input("Aperte enter para retornar ao menu.")
    
    if opcao_menu == 4: 
        pass
        
    return opcao_menu


last_download = os.path.join(download_path,last_download(download_path))

atualizar = True
perguntar_atualizar = True


if not last_download:
    atualizar = False
else:
    last_download_path = os.path.join(download_path,last_download)

if perguntar_atualizar:
    clear_console()
    print("Os últimos downloads encontrados são: \n")
    log_path = os.path.join(last_download_path,"log.pickle")
    log = load_pickle(log_path)

    for db_name in log:
        print(f"{db_name} - {log[db_name].strftime('Dia: %d/%m/%y - Horário: %H:%M:%S')}\n")


dict_atualizar = {
    0 : "Não",
    1 : "Sim"
}
print("Deseja baixar bancos de dados novamente?")
show_options(dict_atualizar)
opcao_atualizar = get_num(dict_atualizar)

if not opcao_atualizar:
    atualizar = False


clear_console()
create_previsor_folders()
while (True):
    opcao_menu = menu_principal()
    if opcao_menu == 0:
        break
