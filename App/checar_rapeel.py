import os 
import pandas as pd
from datetime import date
from utils import *

def checar_Rapeel(biu_file_path, directory, inicio_biu,checar_rapeel_path):
    biu =  pd.read_excel(biu_file_path)
    # Carrega dataframe com dados do skate
    skate_ug = pd.read_parquet(f"{directory}skate_ug.gzip")
    skate_usinas = pd.read_parquet(f"{directory}skate_usinas.gzip")
    rapeel = pd.read_parquet(f"{directory}rapeel.gzip")
    rapeel = rapeel.loc[rapeel.groupby("IdeUsinaOutorga").DthEnvio.idxmax()]
    skate_usinas = pd.merge(skate_usinas,rapeel,"left",validate="one_to_one")
    hoje =  pd.to_datetime(date.today())
    futuro_proximo = inicio_biu + pd.Timedelta(60,"D")
    lista_fiscais = biu.Fiscal.unique()
    skate_usinas.DatPrevisaoIniciobra = pd.to_datetime(skate_usinas.DatPrevisaoIniciobra)
    mask_biu = skate_usinas.IdeUsinaOutorga.isin(biu.IdeUsinaOutorga)
    skate_usinas = pd.merge(skate_usinas[mask_biu],biu[["IdeUsinaOutorga","Fiscal"]],on="IdeUsinaOutorga",how="left")
    mask_feita = skate_usinas.DatMonitoramento > inicio_biu
    mask_previsao_passado = skate_usinas.DatPrevisaoIniciobra < hoje
    mask_previsao_IO_proxima = skate_usinas.DatPrevisaoIniciobra < futuro_proximo
    mask_checar_IO =   ((
        (skate_usinas.DatPrevisaoIniciobra < skate_usinas.DatInicioObraOutorgado)  & 
        (skate_usinas.DatPrevisaoIniciobra < skate_usinas.DatPrevistaAprovacaoIII)) | mask_previsao_passado | 
        (mask_previsao_IO_proxima & (skate_usinas.DatPrevistaAprovacaoIII > futuro_proximo)))

    biu_justificativa = biu[~biu.Justificativadaprevisao_new.str.startswith("Analisar")]
    checar_justificativa = pd.merge(biu_justificativa[["IdeUsinaOutorga","NomUsina","Justificativadaprevisao_new"]],
            skate_usinas[mask_feita][["IdeUsinaOutorga","DscJustificativaPrevisao","Fiscal"]],
            how="inner")
    checar_justificativa= checar_justificativa[(checar_justificativa.Justificativadaprevisao_new != checar_justificativa.DscJustificativaPrevisao) & (checar_justificativa.Fiscal != "Endrizzo (AUTOM)")]
    biu_manual = biu[biu.manual.notna() & biu.PrevisaoOC_Regra_TS.notna()][["IdeUsinaOutorga","NomUsina","PrevisaoOC_Regra_TS","Fiscal"]]
    skate_ug_sem_OC = skate_ug[skate_ug.DatLiberOpComerRealizado.isna()][["IdeUsinaOutorga","NumUgUsina","DatPrevisaoSFGComercial"]]
    checar_prev_OC = pd.merge(skate_ug_sem_OC,biu_manual,how="inner")
    checar_prev_OC = checar_prev_OC[((checar_prev_OC.DatPrevisaoSFGComercial - checar_prev_OC.PrevisaoOC_Regra_TS).abs()) > pd.Timedelta(120,"D")].drop_duplicates(subset="IdeUsinaOutorga")
    checar_prev_OC = pd.merge(skate_usinas[["IdeUsinaOutorga","DatMonitoramento"]],checar_prev_OC,on="IdeUsinaOutorga",how="right")
    checar_prev_OC = checar_prev_OC[checar_prev_OC.DatMonitoramento > inicio_biu]
    clear_console()
    for fiscal in lista_fiscais:
        usinas_fiscal = biu[biu.Fiscal == fiscal].IdeUsinaOutorga
        mask_fiscal = skate_usinas.IdeUsinaOutorga.isin(usinas_fiscal)
        quant_total = skate_usinas[mask_fiscal].shape[0]
        quant_feitas_fiscal = skate_usinas[mask_fiscal & mask_feita].shape[0]
        quant_previsoes_IO_checar = skate_usinas[mask_checar_IO & mask_fiscal & mask_feita].shape[0]
        quant_just_checar = checar_justificativa[checar_justificativa.Fiscal == fiscal].shape[0]
        quant_checar_OC = checar_prev_OC[checar_prev_OC.Fiscal == fiscal].shape[0]
        progresso = 100*quant_feitas_fiscal/quant_total
        print(f"{fiscal}: {progresso:.1f} %  - {quant_feitas_fiscal} feitas  - {quant_total - quant_feitas_fiscal} a fazer  - {quant_previsoes_IO_checar} previsões IO a serem checadas - {quant_just_checar} justificativas a serem checadas - {quant_checar_OC} datas de previsão OC a serem checadas.\n")
        if fiscal == "Márcio":
            #raise
            pass  
    print("\n"*3)
    checar_IO = skate_usinas[mask_checar_IO & mask_feita][["IdeUsinaOutorga","NomUsina","DatPrevisaoIniciobra","DatInicioObraOutorgado","DatPrevistaAprovacaoIII","Fiscal","DatMonitoramento"]]
    a_fazer = skate_usinas[(~mask_feita)][["IdeUsinaOutorga","NomUsina","Fiscal","DatMonitoramento"]]
    a_fazer = a_fazer[(a_fazer.Fiscal != "Endrizzo (AUTOM)")]

    save_files_path = os.path.join(
        checar_rapeel_path,
        hoje.strftime("%Y_%m_%d")
    )
    create_folder(save_files_path)

    checar_IO.to_excel(os.path.join(
        save_files_path,
        'checar_IO.xlsx'),index=False)

    a_fazer.to_excel(os.path.join(
        save_files_path,
        'a_fazer.xlsx'),index=False)

    checar_justificativa.to_excel(os.path.join(
        save_files_path,
        'checar_justificativa.xlsx'),index=False)

    checar_prev_OC.to_excel(os.path.join(
        save_files_path,
       'checar_prev_OC.xlsx'),index=False)

    print(f"Arquivos para checagem exportados: {save_files_path}\n")

    perguntar_abrir_pasta(save_files_path)