import os, sys
from utils import *
from download_DB import download_db
import glob
import pandas as pd
import numpy as np

from datetime import date,datetime,timedelta
hoje =  pd.to_datetime(date.today() + timedelta(days=25)) 
final_do_mes = pd.to_datetime(datetime(hoje.year,hoje.month+1,1) - timedelta(seconds=1))
hoje_str = date.today().strftime('%Y_%m_%d')

# Lista de colunas que serão usadas várias vezes para a realização de merges entre os dataframes
list_id_data = ['DthEnvio','IdeUsinaOutorga']
list_id_ug = ['IdeUsinaOutorga','NumOperacaoUg']

def biu(download_directory,biu_path):
    log = get_log_file(download_directory)
    
    partial_file_name =  get_standard_file_name(['vmonitoramentoug', 'vmonitoramentousina','vrapeellicenciamento'],log)
    biu_directory = os.path.join(biu_path,hoje_str)
    list_casos = ['Caso_I','Caso_II_a','Caso_II_b','Caso_III']
    options = {
    0 : "Voltar",
    1 : "Gerar BIU (Estágio I)",
    2 : "Recalcular BIU (Estágio II)"
    }
    show_options(options)
    option = get_num(options)

    if option == 1:
        print("Gerando arquivos do BIU...")
        print(biu_directory)
        create_folder(biu_directory)
        df_usina,df_ug = generate_BIU(download_directory)

    
        dic = {
        'NumOperacaoUg':'NumUgUsina',
        'regranovapmo': 'PrevisaoOC_regra',
        'dscjustificativaregranova' : 'Justificativadaprevisao_new',
        'criterionovopmo' : 'CriterioPrevisao',
        'dsccriterionovo':'DscCriterioPrevisao',
        'Previsao_OC':'CalculoPrevisorOC',
        'Dat_OC_obrigacao':'OC_Obrigacao',
        'DatPrevistaComercial':'PrevisaoOC_rapeel_max', 
        'DscJustificativaPrevisao':'DscJustificativaPrevisaoAtual',
        'DatPrevisaoIniciobra':'prev_IO_SFG',
        'prev_IO':'prev_IO_rapeel'
    } 
    df_ug = df_ug.rename(columns=dic)
    df_usina = df_usina.rename(columns=dic)


    # Caso I
    file_name = os.path.join(biu_directory,f"Caso_I_{partial_file_name}.xlsx")
    df_usina[df_usina.Caso_I][['IdeUsinaOutorga','DatInicioObraOutorgado','prev_IO_rapeel','prev_IO_SFG','DatMonitoramento','DthEnvio']].to_excel(file_name)

    # Caso II_a
    file_name = os.path.join(biu_directory,f"Caso_II_a_{partial_file_name}.xlsx")
    df_usina[df_usina.Caso_II_a][['IdeUsinaOutorga','DatInicioObraOutorgado','prev_IO_rapeel','prev_IO_SFG','DatMonitoramento','DthEnvio']].to_excel(file_name)

    # Caso II-b e III
    file_name = os.path.join(biu_directory,f"BIU_{partial_file_name}.xlsx")
    df_usina[df_usina.Caso_II_b | df_usina.Caso_III].to_excel(file_name)
    
    print("Arquivos exportados...")




def generate_BIU(download_directory):
    # Lista de colunas que serão usadas várias vezes para a realização de merges entre os dataframes
    list_id_data = ['DthEnvio','IdeUsinaOutorga']
    list_id_ug = ['IdeUsinaOutorga','NumOperacaoUg']
    # Informações de Usinas
    ## Carrega banco de dados
    cols = ["IdeUsinaOutorga",'NomUsina','CodCegFormatado','DatMonitoramento',"DatCanteiroObraRealizado","DatDesvioRioRealizado","DatEnchimentoRealizado","DatConclusaoSisTransRealizado","DatPrevisaoIniciobra","IdcObraParalisada","IdcUsinaMonitorada","DatMontagemOutorgado","DatConcretagemRealizado","DatConclusaoTorresRealizado","DatInicioObraOutorgado","DatConcretagemOutorgado","DatSisTransmissaoRealizado","IdcSituacaoObra","IdcSemPrevisao","DatComissionamentoUGRealizado","SigTipoGeracao","DscComercializacaoEnergia","DatInicioObraRealizado","DatMontagemRealizado",'DscJustificativaPrevisao']
    df_usina = pd.read_parquet(os.path.join(download_directory,"vmonitoramentousina.gzip"))[cols]
    df_usina = df_usina[df_usina.IdcUsinaMonitorada == "Sim"].reset_index(drop=True)
    monitoramentoleilao  = pd.read_parquet(os.path.join(download_directory,"vmonitoramentoleilao.gzip"))[['IdeUsinaOutorga','CodLeilao', 'DatInicioSuprimento']]
    monitoramentoleilao.dropna(subset='DatInicioSuprimento',inplace=True)
    monitoramentoleilao = monitoramentoleilao.loc[monitoramentoleilao.groupby('IdeUsinaOutorga').DatInicioSuprimento.idxmin()]
    df_usina = pd.merge(df_usina,monitoramentoleilao,how="left",on="IdeUsinaOutorga")
    # Carrega BD vrapeelcronograma => rapeel_cronograma
    cols = ['IdeUsinaOutorga','CodCeg','DthEnvio','DatRealizacaoII','DatPrevistaAprovacaoIII','DatRealizacaoIII','DatRealizacaoIX','DatRealizacaoXI','DatRealizacaoXIII','DatRealizacaoXIV','DatRealizacaoXII','DatRealizacaoVII','DatRealizacaoVIII','DatRealizacaoVI','DatRealizacaoX','DatRealizacaoV','DatRealizacaoIV']
    rename_cols = {
            'DatRealizacaoII' : 'canteiroReal',
            'DatPrevistaAprovacaoIII' : 'prev_IO',
            'DatRealizacaoIII' : 'IO_real',
            'DatRealizacaoIX' : 'DesvRio_real',
            'DatRealizacaoXI' : 'Ench_Real',
            'DatRealizacaoXIII' : 'IOTrans_Real',
            'DatRealizacaoXIV' : 'Conc_Trans_REAL',
            'DatRealizacaoXII' : 'Comiss_Real',
            'DatRealizacaoVII' : 'ME_Real_conc_eol',
            'DatRealizacaoVIII' : 'Conc_Combust_Real'}
            
    rapeel_cronograma = pd.read_parquet(os.path.join(download_directory,"vrapeelcronograma.gzip"))[cols].rename(columns=rename_cols)
    rapeel_cronograma = rapeel_cronograma.loc[rapeel_cronograma.groupby('IdeUsinaOutorga').DthEnvio.idxmax()]
    df_usina = pd.merge(df_usina,rapeel_cronograma,how="left",on="IdeUsinaOutorga")
    cols = ['DthEnvio','IdeUsinaOutorga','DatConclusaoIII',]
    rename_cols = {'DatConclusaoIII':'DatConclusaoACL'}
    mercado = pd.read_parquet(os.path.join(download_directory,"vrapeelcontratorecurso.gzip"))[cols].rename(columns=rename_cols)
    mercado = mercado.loc[mercado.groupby("IdeUsinaOutorga").DthEnvio.idxmax()]
    del mercado['DthEnvio']
    df_usina = pd.merge(df_usina,mercado,on="IdeUsinaOutorga",how="left")
    cols = ["DatValidadeIV","DatValidadeI","DthEnvio","IdeUsinaOutorga","DatValidadeV"]
    rename_cols = {'DatValidadeIV': 'DatValidadeLI','DatValidadeI': 'DatValidadeLP','DatValidadeV': 'DatValidadeLO'}
    ambiental_datas = pd.read_parquet(os.path.join(download_directory,"vrapeellicenciamento.gzip"))[cols].rename(columns=rename_cols)
    ambiental_datas = ambiental_datas.loc[ambiental_datas.groupby('IdeUsinaOutorga').DthEnvio.idxmax()]
    del ambiental_datas['DthEnvio']
    df_usina = pd.merge(df_usina,ambiental_datas,on="IdeUsinaOutorga",how="left")
    cols = ["DthEnvio","NomSitContratoI","NomSitContratoIV","NomSituacaoContratoIII","NomSitContratoII","IdeUsinaOutorga"]
    rename_cols = {'NomSitContratoI': 'NomSitContratoCCD','NomSitContratoIV': 'NomSitContratoCUST','NomSituacaoContratoIII': 'NomSituacaoContratoCUSD','NomSitContratoII': 'NomSitContratoCCT'}
    acesso_contratos = pd.read_parquet(os.path.join(download_directory,"vrapeelacesso.gzip"))[cols].rename(columns=rename_cols)
    acesso_contratos = acesso_contratos.loc[acesso_contratos.groupby('IdeUsinaOutorga').DthEnvio.idxmax()]
    del acesso_contratos['DthEnvio']
    df_usina = pd.merge(df_usina,acesso_contratos,on="IdeUsinaOutorga",how="left")
    ##  Cria colunas com definições e regras
    df_usina["classe"] = df_usina.CodCegFormatado.str.slice(4,6)
    df_usina["CC_real"] = pd.to_datetime(pd.NA)
    df_usina.loc[df_usina.classe == "PH","CC_real"] = df_usina.DatRealizacaoIV
    df_usina.loc[df_usina.classe == "CV","CC_real"] = df_usina.DatRealizacaoV
    df_usina["ME_real"] = df_usina.DatRealizacaoX
    df_usina.loc[df_usina.classe == "CV","ME_real"] = df_usina.DatRealizacaoVI
    dict_validades = {
        'DatValidadeLO' : 'LO',
        'DatValidadeLI' : 'LI',
        'DatValidadeLP' : "LP"
    }

    df_usina['ValidadeAmbiental'] = pd.NA
    for validade in dict_validades:
        df_usina.loc[df_usina.ValidadeAmbiental.isna(),'ValidadeAmbiental'] = df_usina[validade]
    df_usina[['IdeUsinaOutorga'] +list(dict_validades.keys()) + ['ValidadeAmbiental']].sample(5)
    #WORK.dadosAcesso t1.'DthEnvio' LABEL="datadainformacao" AS datadainformacao

    df_usina['CondicaoAmbiental'] = pd.NA

    for validade in reversed(dict_validades):
        df_usina.loc[df_usina[validade].notna(),'CondicaoAmbiental'] = dict_validades[validade]

    df_usina.loc[(df_usina.CondicaoAmbiental.isna()) & (df_usina.DthEnvio.notna()),'CondicaoAmbiental'] = 'Sem LP'
    df_usina.loc[(df_usina.CondicaoAmbiental.isna()),'CondicaoAmbiental'] = 'Não Informado'


    df_usina[list(dict_validades) + ['ValidadeAmbiental','CondicaoAmbiental']].sample(10)

    #display(df_usina[list(dict_validades.keys()) + ['CondicaoAmbiental','ValidadeAmbiental']])

    df_usina['CondicaoConexao'] = "Não informado"

    nsa = "Não se Aplica"
    nass = "Não Assinado"
    valido = "Válido"
    vencido = "Vencido"

    df_usina.loc[
        ((df_usina.NomSitContratoCCD ==  nsa)&
        (df_usina.NomSitContratoCCT == nsa) &
        df_usina.DthEnvio.notna()
        )
        ,'CondicaoConexao'] = "Verificar"

    df_usina.loc[
    (    ((df_usina.NomSitContratoCCD ==  valido) |
        (df_usina.NomSitContratoCCT == valido)) &
        df_usina.DthEnvio.notna())
        
        ,'CondicaoConexao'] = "OK"

    df_usina.loc[
        (((df_usina.NomSitContratoCCD ==  vencido) |
        (df_usina.NomSitContratoCCT == vencido) |
        (df_usina.NomSitContratoCCD == nass) |
        (df_usina.NomSitContratoCCT == nass )) &
        df_usina.DthEnvio.notna())
        ,'CondicaoConexao'] = "Sem Conexão"
    #display(df_usina[['IdeUsinaOutorga'] + ['NomSitContratoCCD','NomSitContratoCCT','DthEnvio','CondicaoConexao']].sample(10))


    df_usina['CondicaoUso'] = "Não informado"

    df_usina.loc[
        ((df_usina.NomSituacaoContratoCUSD ==  nsa)&
        (df_usina.NomSitContratoCUST == nsa) 
        )
        ,'CondicaoUso'] = "Verificar"


    df_usina.loc[
        ((df_usina.NomSituacaoContratoCUSD ==  valido) |
        (df_usina.NomSitContratoCUST == valido) 
        )
        ,'CondicaoUso'] = "OK"

    df_usina.loc[
        (((df_usina.NomSituacaoContratoCUSD ==  vencido) |
        (df_usina.NomSitContratoCUST == vencido) |
        (df_usina.NomSituacaoContratoCUSD == nass) |
        (df_usina.NomSitContratoCUST == nass )))
        ,'CondicaoUso'] = "Sem Uso"

    #display(df_usina[['IdeUsinaOutorga'] + ['NomSituacaoContratoCUSD','NomSitContratoCUST','CondicaoConexao']].sample(10))


    df_usina["PPA"] = "Ambos"

    df_usina.loc[
        (((df_usina.DscComercializacaoEnergia == "ACR") & 
            df_usina.DatConclusaoACL.isna())),
        "PPA"] = "ACR"

    df_usina.loc[
        (((df_usina.DscComercializacaoEnergia == "Fora do ACR") & 
            df_usina.DatConclusaoACL.isna())),
        "PPA"] = "Nenhum"

    df_usina.loc[
        (((df_usina.DscComercializacaoEnergia == "Fora do ACR") & 
            df_usina.DatConclusaoACL.notna())),
        "PPA"] = "ACL"

    #display(df_usina[['IdeUsinaOutorga'] + ['DscComercializacaoEnergia','DatConclusaoACL','PPA']].sample(10))
    ea = "Em andamento"
    na = "Não Iniciada"
    nenhum = "Nenhum"
    paralisada = "Paralisada"
    ok = "OK"
    slp = "Sem LP"
    sim = "Sim"
    df_usina['criterio_novo'] = np.select(
        [
            (df_usina.IdcSemPrevisao == sim),
            (df_usina.IdcSituacaoObra == ea),
            (( df_usina.IdcSituacaoObra == na)  &  (df_usina.PPA != nenhum) ),
            ((df_usina.IdcSituacaoObra == na) & (df_usina.PPA == nenhum) & (df_usina.CondicaoAmbiental.isin(["LI","LO"])) & (df_usina.CondicaoUso == ok)),
            ((df_usina.IdcSituacaoObra == na) & (df_usina.PPA == nenhum) & (df_usina.CondicaoAmbiental == "LP") & (df_usina.CondicaoUso == ok)),
            ((df_usina.IdcSituacaoObra == na) & (df_usina.PPA == nenhum) & (df_usina.CondicaoAmbiental.isin(["LI","LO"])) & (df_usina.CondicaoUso != ok)),
            ((df_usina.IdcSituacaoObra == na) & (df_usina.PPA == nenhum) & (df_usina.CondicaoAmbiental == "LP") & (df_usina.CondicaoUso != ok)),
            (df_usina.IdcSituacaoObra == paralisada),
            (df_usina.CondicaoAmbiental == "Sem LP"),
            ((df_usina.DthEnvio.isna()) & (df_usina.PPA == nenhum)),
            
        ],
        [9,0,1,2,4,3,5,8,6,7],
        default=-8
    )


    #display(df_usina[['IdeUsinaOutorga','IdcSituacaoObra','PPA','CondicaoAmbiental','CondicaoUso','criterio_novo']].sample(5))
    dict_marcos_homologar = {
        'DatInicioObraRealizado' : 'IO_real',
        'DatConcretagemRealizado' : 'CC_real',
        'DatMontagemRealizado' : 'ME_real',
        'DatEnchimentoRealizado' : 'Ench_Real',
        'DatSisTransmissaoRealizado' : 'IOTrans_Real',
        'DatCanteiroObraRealizado' : 'canteiroReal',
        'DatComissionamentoUGRealizado' : 'Comiss_Real',
        'DatConclusaoSisTransRealizado' : 'Conc_Trans_REAL',
        'DatDesvioRioRealizado' : 'DesvRio_real',
        'DatConclusaoTorresRealizado' : 'ME_Real_conc_eol'}
        
    df_usina['Homologar_Marcos'] = False
    df_usina['Dsc_Marcos_a_Homologar'] = ''


    for marco_monitoramento, marco_rapeel in dict_marcos_homologar.items():
        mask = (df_usina[marco_monitoramento].isna()) & (df_usina[marco_rapeel].notna())
        df_usina['Homologar_Marcos'] |= mask
        df_usina.loc[mask,'Dsc_Marcos_a_Homologar'] += f'{marco_monitoramento}, '

    df_usina.Dsc_Marcos_a_Homologar = df_usina.Dsc_Marcos_a_Homologar.str.slice(0,-2)
    """df_usina['Homologar_Marcos'] =  np.select(
        [((df_usina['DatInicioObraRealizado'].isna()) & (df_usina['IO_real'].notna())) |
        ((df_usina['DatConcretagemRealizado'].isna()) & (df_usina['CC_real'].notna())) | 
        ((df_usina['DatMontagemRealizado'].isna()) & (df_usina['ME_real'].notna())) | 
        ((df_usina['DatEnchimentoRealizado'].isna()) & (df_usina['Ench_Real'].notna())) | 
        ((df_usina['DatSisTransmissaoRealizado'].isna()) & (df_usina['IOTrans_Real'].notna())) |
        ((df_usina['DatCanteiroObraRealizado'].isna()) & (df_usina['canteiroReal'].notna())) |
        ((df_usina['DatComissionamentoUGRealizado'].isna()) & (df_usina['Comiss_Real'].notna())) | 
        ((df_usina['DatConclusaoSisTransRealizado'].isna()) & (df_usina['Conc_Trans_REAL'].notna())) | 
        ((df_usina['DatDesvioRioRealizado'].isna()) & (df_usina['DesvRio_real'].notna())) |
        ((df_usina['DatConclusaoTorresRealizado'].isna()) & (df_usina['ME_Real_conc_eol'].notna()))],

        [True],
        default= False
    )
    """


    df_usina['Revisar_IO'] =  np.select(
    [(df_usina.DatPrevisaoIniciobra < (hoje + pd.Timedelta(15,"D"))) |
    (df_usina.prev_IO > df_usina.DatPrevisaoIniciobra)],
    [True],
    default = False)
    usinas_selecionadas = read_file("./usinas_selecionadas.txt").split("\n")
    usinas_selecionadas = [int(num) for num in usinas_selecionadas]
    df_usina["Usina_Selecionada"] = False
    df_usina.loc[df_usina.IdeUsinaOutorga.isin(usinas_selecionadas),'Usina_Selecionada'] = True

    df_usina['Paralisada'] = False
    df_usina.loc[df_usina.IdcObraParalisada=="Sim",'Paralisada'] = True

    df_usina['Sem_Previsao'] = False
    df_usina.loc[df_usina.IdcSemPrevisao=="Sim",'Sem_Previsao'] = True
    # Informações de UGs
    ## Carrega banco de dados
    cols = ["DatUGInicioOpComerOutorgado","IdcMonitorada","IdeUsinaOutorga",'NumUgUsina',"DatLiberacaoSFGTeste","DatInicioOpTesteOutorgada",'DatLiberOpComerRealizado']
    monitoramentoug = pd.read_parquet(os.path.join(download_directory,"vmonitoramentoug.gzip"))[cols].rename(columns={'NumUgUsina':'NumOperacaoUg'})
    # Carrega BD vrapeeloperacaoug => ug_rapeel
    cols = ["IdeUsinaOutorga",'NumOperacaoUg',"DthEnvio",'DatPrevistaComercial']
    ug_rapeel = pd.read_parquet(os.path.join(download_directory,"vrapeeloperacaoug.gzip"))[cols]
    ug_rapeel = ug_rapeel.loc[ug_rapeel.groupby(['IdeUsinaOutorga','NumOperacaoUg']).DthEnvio.idxmax()]
    list_cols_used =   ['IdeUsinaOutorga','IdcObraParalisada','IdcSemPrevisao','criterio_novo','DscJustificativaPrevisao']
    df_ug = pd.merge(df_usina[list_cols_used],monitoramentoug,on="IdeUsinaOutorga",how='left')
    df_ug = pd.merge(df_ug,ug_rapeel,on=list_id_ug,how='left')
    df_ug = df_ug.loc[df_ug.DatLiberOpComerRealizado.isna()].reset_index(drop=True)
    previsao_path = os.path.join(get_standard_folder_path("Documents"),r"Previsor\Previsoes")
    previsao_file = sorted(glob.glob(f"{previsao_path}/*.gzip"))[-1]
    calculo_previsao = pd.read_parquet(previsao_file)
    calculo_previsao.FaseAtual =calculo_previsao.FaseAtual.str.slice(0,-3)
    calculo_previsao.Indicador =calculo_previsao.Indicador/100
    cols = ["Dat_OC_obrigacao","DatPrevisaoIniciobra","FASE","Ind_crono_norm","IdeUsinaOutorga","flagOPTeste30dias",'Previsao_OC']
    rename_cols = {'FaseAtual':'FASE','Indicador':'Ind_crono_norm','NumUgUsina':'NumOperacaoUg'}
    calculo_previsao = calculo_previsao.rename(columns=rename_cols)

    for col in ['SigTipoGeracao','DatMonitoramento','DatInicioObraOutorgado','DatPrevisaoIniciobra']:
        if col in calculo_previsao.columns:
            calculo_previsao.drop([col],inplace=True,axis=1)
    df_ug = pd.merge(df_ug,calculo_previsao,on=list_id_ug,how="left")
    ##  Cria colunas com definições e regras
    ################################ criterionovopmo ################################
    list_condicoes = [
        ((df_ug['criterio_novo'] == 0) & (df_ug['FASE'] != "OT")),
        ((df_ug['criterio_novo'] == 0) & (df_ug['FASE'] == "OT") & (df_ug['flagOPTeste30dias'] == 1)),
        ((df_ug['criterio_novo'] == 0) & (df_ug['FASE'] == "OT")),
        ((df_ug['criterio_novo'] == 1)),
        ((df_ug['criterio_novo'] == 2)),
        ((df_ug['criterio_novo'] == 3)),
        ((df_ug['criterio_novo'] == 5)),
        ((df_ug['criterio_novo'] == 4)),
        ((df_ug['criterio_novo'] == 6)),
        ((df_ug['criterio_novo'] == 7)),
        ((df_ug['criterio_novo'] == 8)),
        ((df_ug['criterio_novo'] == 9)),
    ]

    list_values = [0.1, 0.2, 0.3, 1, 2, 3, 5, 4, 6, 7, 8, 9]

    df_ug['criterionovopmo'] = np.select(list_condicoes,list_values)


    ################################ regranovapmo ################################

    df_ug.loc[(df_ug['criterio_novo'] == 0) & (df_ug['FASE'] != "OT"),'regranovapmo'] = df_ug[['DatPrevistaComercial','Previsao_OC']].max(axis=1)


    df_ug.loc[(df_ug['criterio_novo'] == 0) & (df_ug['FASE'] == "OT") & (df_ug['flagOPTeste30dias'] == 1),'regranovapmo'] = ( hoje + pd.Timedelta(60,"D"))

    df_ug.loc[(df_ug['criterio_novo'] == 0) & (df_ug['FASE'] == "OT") & (df_ug['Previsao_OC'].isna()),'regranovapmo'] = df_ug['DatPrevistaComercial']


    df_ug.loc[(df_ug['criterio_novo'] == 0) & (df_ug['FASE'] == "OT") & (df_ug['Previsao_OC'].notna()),'regranovapmo'] = df_ug['Previsao_OC']

    df_ug.loc[(df_ug['criterio_novo']== 1) | (df_ug['criterio_novo']== 2) ,'regranovapmo'] = df_ug[['DatPrevistaComercial','Previsao_OC','Dat_OC_obrigacao']].max(axis=1)
    df_ug["Dummy"] = hoje + pd.Timedelta(5*365,'D')

    df_ug.loc[((df_ug['criterio_novo'] == 3) | (df_ug['criterio_novo'] == 4) | (df_ug['criterio_novo'] == 5) ),'regranovapmo'] = df_ug[['DatPrevistaComercial','Previsao_OC','Dat_OC_obrigacao','Dummy']].max(axis=1)


    df_ug.loc[df_ug['criterio_novo']== 6,'regranovapmo'] = pd.NA

    df_ug["Dummy"] = hoje + pd.Timedelta(6*365,'D')
    df_ug.loc[df_ug['criterio_novo']== 7,'regranovapmo'] =  df_ug[['DatPrevistaComercial','Previsao_OC','Dat_OC_obrigacao','Dummy']].max(axis=1)


    df_ug.loc[df_ug['criterio_novo']== 8,'regranovapmo'] = df_ug[['DatPrevistaComercial','Dat_OC_obrigacao']].max(axis=1)

    df_ug.loc[(df_ug['criterio_novo'] == 9),'regranovapmo'] = pd.NA

    serie_usinas_previsao_OC_passado = df_ug[(df_ug.Previsao_OC < final_do_mes) | (df_ug.Previsao_OC.isna())].IdeUsinaOutorga.drop_duplicates()
    serie_usinas_previsao_em_teste = df_ug[df_ug.criterionovopmo.isin([0.2,0.3])].IdeUsinaOutorga.drop_duplicates()

    serie_flagOPTeste30dias = df_ug[df_ug.flagOPTeste30dias == 1].IdeUsinaOutorga.drop_duplicates()
    df_usina["flagOPTeste30dias"] = False

    df_usina.loc[df_usina.IdeUsinaOutorga.isin(serie_flagOPTeste30dias),'flagOPTeste30dias'] = True
    df_usina["Prev_OC_passado"] = False
    df_usina.loc[df_usina.IdeUsinaOutorga.isin(serie_usinas_previsao_OC_passado),'Prev_OC_passado'] = True

    df_usina["Em_teste"] = False
    df_usina.loc[df_usina.IdeUsinaOutorga.isin(serie_usinas_previsao_em_teste),'Em_teste'] = True

    df_usina["Sem_Monitoramento"] = False
    df_usina.loc[df_usina.DatMonitoramento < (hoje - pd.Timedelta(4*30, unit="D")),'Sem_Monitoramento'] = True
    df_usina["Manual"] = False
    df_usina.loc[(df_usina.Paralisada | df_usina.Sem_Previsao | 
        df_usina.Usina_Selecionada| df_usina.Prev_OC_passado | df_usina.Em_teste | 
        (df_usina.DscJustificativaPrevisao == 'Situação das obras de conexão e linha de transmissão associada.')),
        'Manual'] = True
    df_usina['Caso_I'] = df_usina['Caso_II_a'] = df_usina['Caso_II_b'] = df_usina['Caso_III'] = False
    list_casos = ['Caso_I','Caso_II_a','Caso_II_b','Caso_III']
    df_usina['Caso_I'] = (~df_usina.Manual) & (~df_usina.Revisar_IO) & (~ df_usina.Homologar_Marcos) #& df_usina.Sem_Monitoramento

    df_usina['Caso_II_a'] = (~df_usina.Manual) & (df_usina.Revisar_IO) & (~ df_usina.Homologar_Marcos)

    df_usina['Caso_II_b'] = (~df_usina.Manual)  & ((df_usina.Homologar_Marcos) | (df_usina.Homologar_Marcos & df_usina.Revisar_IO))


    df_usina['Caso_III'] = (
        (df_usina.Manual & df_usina.Sem_Monitoramento ) | 
        (df_usina.Manual & df_usina.Homologar_Marcos ) |
        (df_usina.Manual & df_usina.Revisar_IO ) |
        df_usina.Em_teste )
    ## Análise de casos BIU
    list_criterios_BIU = ['Manual', 'Revisar_IO','Homologar_Marcos', 'Em_teste','Sem_Monitoramento']
    casos = df_usina[list_criterios_BIU +  list_casos].value_counts().reset_index().sort_values(by='Caso_III')
    casos
    mask_casos_selecionados = casos[list_casos].any(axis=1)
    casos[mask_casos_selecionados]
    list_criterios = ['criterio_novo',"criterionovopmo"]

    df_usina_criterio = df_ug.loc[df_ug.groupby("IdeUsinaOutorga").criterionovopmo.idxmax()][["IdeUsinaOutorga","FASE",'flagOPTeste30dias'] + list_criterios]
    ################################ dscjustificativaregranova ################################
    list_condicoes = [
        (df_usina_criterio['criterionovopmo'] == 9),
        (df_usina_criterio['criterionovopmo'] == 8),
        (df_usina_criterio['criterionovopmo'] == 0.2),
        (df_usina_criterio['criterionovopmo'] == 0.1) | (df_usina_criterio['criterionovopmo'] == 0.3),
        (df_usina_criterio['criterionovopmo'] == 1),
        df_usina_criterio['criterionovopmo']==2,
        df_usina_criterio['criterionovopmo']==3,
        (df_usina_criterio.criterionovopmo== 4),
        (df_usina_criterio.criterionovopmo== 5),
        (df_usina_criterio.criterionovopmo== 6),
        (df_usina_criterio.criterionovopmo== 7),
    ]

    list_values = [
        "Analisar justificativa: Revogação da outorga em avaliação ou Demandas judiciais ou Inviabilidade da implantação da usina",
        "Paralisação de obras",
        "Analisar justificativa: Sem Licença de Operação (LO) ou Alterações de características técnicas ou Estágio das obras da conexão associada ou Demandas judiciais ou Paralisação de obras",
        "Estágio das obras da usina",
        "Compromisso de venda de energia - PPA",
        "Acesso a rede contratado - CUST/CUSD",
        "Acesso a rede não contratado - CUST/CUSD",
        "Sem Licença de Instalação - LI",
        "Sem Licença de Instalação - LI",
        "Nenhuma licença ambiental válida",
        "Sem RAPEEL"
    ]

    df_usina_criterio['dscjustificativaregranova'] = np.select(list_condicoes,list_values)



    ################################ dsccriterionovo ################################
    list_condicoes = [
        ((df_usina_criterio['criterio_novo'] == 9)),
        ((df_usina_criterio['criterio_novo'] == 0) & (df_usina_criterio['FASE'] != "OT")),
        ((df_usina_criterio['criterio_novo'] == 0) & (df_usina_criterio['FASE'] == "OT") & (df_usina_criterio['flagOPTeste30dias'] == 1)),
        ((df_usina_criterio['criterio_novo'] == 0) & (df_usina_criterio['FASE'] == "OT")),
        ((df_usina_criterio['criterio_novo'] == 1)),
        ((df_usina_criterio['criterio_novo'] == 2)),
        ((df_usina_criterio['criterio_novo'] == 3)),
        ((df_usina_criterio['criterio_novo'] == 4)),
        ((df_usina_criterio['criterio_novo'] == 5)),
        ((df_usina_criterio['criterio_novo'] == 7)),
        ((df_usina_criterio['criterio_novo'] == 6)),
        ((df_usina_criterio['criterio_novo'] == 8))
    ]

    list_values = [
        "Usina Viabilidade Baixa análise da fiscalização",
        "Usina em obras = Previsão OC maior entre data calculada e data RAPEEL",
        "Usina em Teste há mais de 30 dias = Previsão OC próximos 60 dias",
        "Usina em Teste = Previsão OC conforme data calculada",
        "Usina sem obras com PPA = Previsão OC maior entre data calculada, data RAPEEL e data compromisso",
        "Usina sem obras com CUST = Previsão OC maior entre data calculada, data RAPEEL e data compromisso",
        "Usina sem obras, sem cust, sem PPA e com LI  = Previsão OC handicap de 5 anos",
        "Usina sem obras, com cust, sem PPA e sem LI  = Previsão OC handicap de 5 anos",
        "Usina sem obras, sem cust, sem PPA e sem LI  = Previsão OC handicap de 5 anos",
        "Usina sem obras, sem PPA e sem RAPEEL  = Previsão OC handicap de 5 anos",
        "Usina sem LP = Previsão OC sem previsão","Usina obras paralisadas = Previsão OC maior entre data RAPEEL e data compromisso"
    ]
    df_usina_criterio['dsccriterionovo'] = np.select(list_condicoes,list_values)
    list_justificativas = ['dsccriterionovo','dscjustificativaregranova']
    df_usina = pd.merge(df_usina,
    df_usina_criterio[['IdeUsinaOutorga','criterionovopmo'] + list_justificativas],
    on = "IdeUsinaOutorga", how='left').sort_values('IdeUsinaOutorga')

    list_casos = ['Caso_I','Caso_II_a','Caso_II_b','Caso_III']
    df_ug = pd.merge(df_usina[['IdeUsinaOutorga','dscjustificativaregranova','dsccriterionovo','DatMonitoramento','DscJustificativaPrevisao'] + list_casos],df_ug, on = "IdeUsinaOutorga",how='left').sort_values(list_id_ug)

    return (df_usina,df_ug)
