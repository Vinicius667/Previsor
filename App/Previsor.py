import os
os.chdir(os.path.dirname(__file__))

from Calcular_Previsao import calcular_previsao
from download_DB import download_db
from checar_rapeel import checar_Rapeel
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
checar_rapeel_path = os.path.join(root_path, "Checar_Rapeel")


def create_previsor_folders():
    # Cria pastas padrão necessárias
    create_folder(root_path)
    create_folder(download_path)
    create_folder(previsoes_path)
    create_folder(checar_rapeel_path)


def calc_previsao():
    # Calcula previsão
    global result, skate_merged
    print("\n" + " Baixando arquivos ".center(60, "*") + "\n")
    directory = download_db(download_path, lista_download=[
                            "skate_leilao", "skate_ug", "skate_usinas"])
    print("\n" + "*".center(60, "*") + "\n")
    result, skate_merged = calcular_previsao(directory)
    skate_export = skate_merged[['NomUsina', 'IdeUsinaOutorga', 'NumUgUsina', 'SigTipoGeracao',
                                 'Previsao_OC', 'Dat_OC_obrigacao', 'DatMonitoramento', 'FaseAtual', 'Indicador']].copy()
    skate_export["Previsao_OC"] = skate_export.Previsao_OC.dt.normalize()
    skate_export["DatMonitoramento"] = skate_export.DatMonitoramento.dt.normalize()
    return result, skate_export


def get_biu():
    Tk().withdraw()
    filename = askopenfilename()
    return filename


def menu_principal():
    # Apresenta menu principal
    global skate_merged, skate_export
    global result, df
    dict_menu_principal = {
        0: "Sair",
        1: "Calcular previsão da base de dados",
        2: "Atualizar base de dados",
        3: "Checar Rapeel",

    }
    show_options(dict_menu_principal)
    opcao_menu = get_num(dict_menu_principal)
    if opcao_menu == 0:
        print("Sessão terminada.")
        return 0

    if opcao_menu == 1:
        clear_console()
        print("Calculando previsão...")
        result, skate_export = calc_previsao()
        if result == "Ok":
            minutos_estimado = ((2901 / psutil.cpu_freq().max)
                                * (skate_export.shape[0] / 50_000) * 24)/60
            print(
                f"Exportando arquivo com previsões... Previsão: {minutos_estimado:.2f} minutos.")
            previsao_file = os.path.join(
                previsoes_path, f"Previsao_OC_{hoje_str}.xlsx")
            previsor_detalhe = os.path.join(
                previsoes_path, f"Previsao_OC_detalhada_{hoje_str}.xlsx")
            skate_export.to_excel(previsao_file, index=False)
            print(f"Arquivo exportado: {previsao_file}\n")
            minutos_estimado = ((2901 / psutil.cpu_freq().max)
                                * (skate_merged.shape[0] / 50_000) * 120)/60
            print(
                f"Deseja exportar arquivo detalahado? Previsão: {minutos_estimado:.2f} minutos.\n")

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

    if opcao_menu == 2:
        directory = download_db(download_path, force_download=True, lista_download=[
                                "skate_ug", "skate_usinas", "leilao"])

    if opcao_menu == 3:
        global biu
        print("Data do início do BIU:")
        inicio_biu = get_date()
        print("Selecione o arquivo do BIU")
        directory = download_db(download_path, force_download=False, lista_download=[
                                "rapeel", "skate_ug", "skate_usinas"])
        biu_file_path = get_biu()
        checar_Rapeel(biu_file_path, directory, inicio_biu, checar_rapeel_path)
    return opcao_menu

clear_console()
create_previsor_folders()
while (True):
    opcao_menu = menu_principal()
    if opcao_menu == 0:
        break
