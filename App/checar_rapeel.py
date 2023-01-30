import os 
import pandas as pd
from datetime import date
from utils import *

def checar_Rapeel(directory,checar_vrapeelcronograna_path):
    all_cols_used = ['vmonitoramentoug','vmonitoramentousina','vrapeelcronograna']
    log = get_log_file(directory)
    
    print("Data do início do BIU:")
    inicio_biu = get_date()
    print("Selecione o arquivo do BIU\n")
    biu_file_path = get_file()
    biu =  pd.read_excel(biu_file_path)
    #Carrega dataframe com dados do skate
    vmonitoramentoug = pd.read_parquet(f"{directory}vmonitoramentoug.gzip")
    vmonitoramentousina = pd.read_parquet(f"{directory}vmonitoramentousina.gzip")
    vrapeelcronograna = pd.read_parquet(f"{directory}vrapeelcronograna.gzip")
    vrapeelcronograna = vrapeelcronograna.loc[vrapeelcronograna.groupby("IdeUsinaOutorga").DthEnvio.idxmax()]
    vmonitoramentousina = pd.merge(vmonitoramentousina,vrapeelcronograna,"left",validate="one_to_one")
    hoje =  pd.to_datetime(date.today())
    futuro_proximo = inicio_biu + pd.Timedelta(60,"D")
    lista_fiscais = biu.Fiscal.unique()
    vmonitoramentousina.DatPrevisaoIniciobra = pd.to_datetime(vmonitoramentousina.DatPrevisaoIniciobra)
    mask_biu = vmonitoramentousina.IdeUsinaOutorga.isin(biu.IdeUsinaOutorga)
    vmonitoramentousina = pd.merge(vmonitoramentousina[mask_biu],biu[["IdeUsinaOutorga","Fiscal"]],on="IdeUsinaOutorga",how="left")
    mask_feita = vmonitoramentousina.DatMonitoramento > inicio_biu
    mask_previsao_passado = vmonitoramentousina.DatPrevisaoIniciobra < hoje
    mask_previsao_IO_proxima = vmonitoramentousina.DatPrevisaoIniciobra < futuro_proximo
    mask_checar_IO =   ((
        (vmonitoramentousina.DatPrevisaoIniciobra < vmonitoramentousina.DatInicioObraOutorgado)  & 
        (vmonitoramentousina.DatPrevisaoIniciobra < vmonitoramentousina.DatPrevistaAprovacaoIII)) | mask_previsao_passado | 
        (mask_previsao_IO_proxima & (vmonitoramentousina.DatPrevistaAprovacaoIII > futuro_proximo)))

    biu_justificativa = biu[~biu.Justificativadaprevisao_new.str.startswith("Analisar")]
    checar_justificativa = pd.merge(biu_justificativa[["IdeUsinaOutorga","NomUsina","Justificativadaprevisao_new"]],
            vmonitoramentousina[mask_feita][["IdeUsinaOutorga","DscJustificativaPrevisao","Fiscal"]],
            how="inner")
    checar_justificativa= checar_justificativa[(checar_justificativa.Justificativadaprevisao_new != checar_justificativa.DscJustificativaPrevisao)]
    biu_manual = biu[biu.manual.notna() & biu.PrevisaoOC_Regra_TS.notna()][["IdeUsinaOutorga","NomUsina","PrevisaoOC_Regra_TS","Fiscal"]]
    vmonitoramentoug_sem_OC = vmonitoramentoug[vmonitoramentoug.DatLiberOpComerRealizado.isna()][["IdeUsinaOutorga","NumUgUsina","DatPrevisaoSFGComercial"]]
    checar_prev_OC = pd.merge(vmonitoramentoug_sem_OC,biu_manual,how="inner")
    checar_prev_OC = checar_prev_OC[((checar_prev_OC.DatPrevisaoSFGComercial - checar_prev_OC.PrevisaoOC_Regra_TS).abs()) > pd.Timedelta(120,"D")].drop_duplicates(subset="IdeUsinaOutorga")
    checar_prev_OC = pd.merge(vmonitoramentousina[["IdeUsinaOutorga","DatMonitoramento"]],checar_prev_OC,on="IdeUsinaOutorga",how="right")
    checar_prev_OC = checar_prev_OC[checar_prev_OC.DatMonitoramento > inicio_biu]
    clear_console()
    for fiscal in lista_fiscais:
        usinas_fiscal = biu[biu.Fiscal == fiscal].IdeUsinaOutorga
        mask_fiscal = vmonitoramentousina.IdeUsinaOutorga.isin(usinas_fiscal)
        quant_total = vmonitoramentousina[mask_fiscal].shape[0]
        quant_feitas_fiscal = vmonitoramentousina[mask_fiscal & mask_feita].shape[0]
        quant_previsoes_IO_checar = vmonitoramentousina[mask_checar_IO & mask_fiscal & mask_feita].shape[0]
        quant_just_checar = checar_justificativa[checar_justificativa.Fiscal == fiscal].shape[0]
        quant_checar_OC = checar_prev_OC[checar_prev_OC.Fiscal == fiscal].shape[0]
        progresso = 100*quant_feitas_fiscal/quant_total
        print(f"{fiscal}: {progresso:.1f} %  - {quant_feitas_fiscal} feitas  - {quant_total - quant_feitas_fiscal} a fazer  - {quant_previsoes_IO_checar} previsões IO a serem checadas - {quant_just_checar} justificativas a serem checadas - {quant_checar_OC} datas de previsão OC a serem checadas.\n")
        if fiscal == "Márcio":
            #raise
            pass  
    print("\n"*3)
    checar_IO = vmonitoramentousina[mask_checar_IO & mask_feita][["IdeUsinaOutorga","NomUsina","DatPrevisaoIniciobra","DatInicioObraOutorgado","DatPrevistaAprovacaoIII","Fiscal","DatMonitoramento"]]
    a_fazer = vmonitoramentousina[(~mask_feita)][["IdeUsinaOutorga","NomUsina","Fiscal","DatMonitoramento"]]

    save_files_path = os.path.join(
        checar_vrapeelcronograna_path,
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