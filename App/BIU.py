import os, sys
from utils import *
from download_DB import download_db
import glob
import pandas as pd
import numpy as np
from Calcular_Previsao import calcular_previsao

from datetime import date,datetime,timedelta
hoje =  pd.to_datetime(date.today()) 

final_do_mes = pd.to_datetime(datetime(hoje.year,hoje.month+1,1) - timedelta(seconds=1))

biu_download_cols  = ['vmonitoramentoleilao', 'vmonitoramentoug', 'vmonitoramentousina', 'vrapeelacesso', 'vrapeelcontratorecurso', 'vrapeelcronograma', 'vrapeelempreendimento', 'vrapeellicenciamento', 'vrapeeloperacaoug']


def ultimo_rapeel(df):
    df_ultimo_envio = df.loc[df.groupby("IdeUsinaOutorga")['DthEnvio'].idxmax()][['IdeUsinaOutorga','DthEnvio']]
    return pd.merge(df_ultimo_envio,df,on=["IdeUsinaOutorga","DthEnvio"],how="left")


list_carac_usina = ['Usina_Sem_Rapeel','CondicaoAmbiental','Homologar_Marcos','Revisar_IO','Usina_Selecionada','Paralisada','Sem_Previsao','criterionovopmo','Prev_OC_passado','Em_teste','Sem_Monitoramento','Manual','UG_Sem_Rapeel']
list_carac_ug = ['FASE','flagOPTeste30dias','UG_Sem_Rapeel','criterionovopmo']
list_casos = ['caso_I','caso_II_a','caso_II_b','caso_III']
list_ambiental = ['DatValidadeLI','DatValidadeLP','DatValidadeLO','ValidadeAmbiental','CondicaoAmbiental']
list_contratos = ['NomSitContratoCCD', 'NomSitContratoCUST', 'NomSituacaoContratoCUSD', 'NomSitContratoCCT']
### start generate_BIU ###
def generate_BIU(biu_download_path, previsao_file, ):
    # Lista de colunas que serão usadas várias vezes para a realização de merges entre os dataframes
    list_id_data = ['DthEnvio','IdeUsinaOutorga']
    list_id_ug = ['IdeUsinaOutorga','NumOperacaoUg']
    # Informações de Usinas
    ## Carrega banco de dados
    cols = ["IdeUsinaOutorga",'NomUsina','CodCegFormatado','DatMonitoramento',"DatCanteiroObraRealizado","DatDesvioRioRealizado","DatEnchimentoRealizado","DatConclusaoSisTransRealizado","DatPrevisaoIniciobra","IdcObraParalisada","IdcUsinaMonitorada","DatConcretagemRealizado","DatConclusaoTorresRealizado","DatInicioObraOutorgado","DatSisTransmissaoRealizado","IdcSituacaoObra","IdcSemPrevisao","DatComissionamentoUGRealizado","SigTipoGeracao","DscComercializacaoEnergia","DatInicioObraRealizado","DatMontagemRealizado",'DscJustificativaPrevisao']
    # Carrega informações do monitoramento
    df_usina = pd.read_parquet(os.path.join(biu_download_path,"vmonitoramentousina.gzip"))[cols].rename(
     columns= {
     'DatPrevisaoIniciobra' : 'prev_IO_SFG'
     }
    )
    
    #Transforma coluna em booleano
    df_usina.IdcUsinaMonitorada = df_usina.IdcUsinaMonitorada.map({"Sim":"True","Não":False}).astype(bool)
    
    # Remove usinas que não são monitoradas
    df_usina = df_usina[df_usina.IdcUsinaMonitorada].reset_index(drop=True)
    # Excluir ? Não está sendo usado
    monitoramentoleilao = pd.read_parquet(os.path.join(biu_download_path,"vmonitoramentoleilao.gzip"))[['IdeUsinaOutorga','CodLeilao', 'DatInicioSuprimento']]
    monitoramentoleilao.dropna(subset='DatInicioSuprimento',inplace=True)
    # Mantém apenas primeira data de suprimento
    monitoramentoleilao = monitoramentoleilao.loc[monitoramentoleilao.groupby('IdeUsinaOutorga').DatInicioSuprimento.idxmin()]
    df_usina = pd.merge(df_usina,monitoramentoleilao,how="left",on="IdeUsinaOutorga")
    cols = ['IdeUsinaOutorga','CodCeg','DthEnvio','DatRealizacaoII','DatPrevistaAprovacaoIII','DatRealizacaoIII','DatRealizacaoIX','DatRealizacaoXI','DatRealizacaoXIII','DatRealizacaoXIV','DatRealizacaoXII','DatRealizacaoVII','DatRealizacaoVIII','DatRealizacaoVI','DatRealizacaoX','DatRealizacaoV','DatRealizacaoIV']
    rename_cols = {
     'DatRealizacaoII' : 'canteiroReal',
     'DatPrevistaAprovacaoIII' : 'prev_IO_rapeel',
     'DatRealizacaoIII' : 'IO_real',
     'DatRealizacaoIX' : 'DesvRio_real',
     'DatRealizacaoXI' : 'Ench_Real',
     'DatRealizacaoXIII' : 'IOTrans_Real',
     'DatRealizacaoXIV' : 'Conc_Trans_REAL',
     'DatRealizacaoXII' : 'Comiss_Real',
     'DatRealizacaoVII' : 'ME_Real_conc_eol',
     'DatRealizacaoVIII' : 'Conc_Combust_Real'}
    
    rapeel_cronograma = pd.read_parquet(os.path.join(biu_download_path,"vrapeelcronograma.gzip"))[cols].rename(columns=rename_cols)
    rapeel_cronograma = rapeel_cronograma.loc[rapeel_cronograma.groupby('IdeUsinaOutorga').DthEnvio.idxmax()]
    
    # Faz merge left entre as informações do monitoramento e do rapeel
    df_usina = pd.merge(df_usina,rapeel_cronograma,how="left",on="IdeUsinaOutorga")
    # Cria coluna para identificar usinas que não enviaram rapeel
    df_usina["Usina_Sem_Rapeel"] = True
    df_usina.loc[df_usina.IdeUsinaOutorga.isin(rapeel_cronograma.IdeUsinaOutorga),'Usina_Sem_Rapeel'] = False
    cols = ['DthEnvio','IdeUsinaOutorga','DatConclusaoIII',]
    rename_cols = {'DatConclusaoIII':'DatConclusaoACL'}
    mercado = pd.read_parquet(os.path.join(biu_download_path,"vrapeelcontratorecurso.gzip"))[cols].rename(columns=rename_cols)
    mercado = mercado.loc[mercado.groupby("IdeUsinaOutorga").DthEnvio.idxmax()]
    del mercado['DthEnvio']
    df_usina = pd.merge(df_usina,mercado,on="IdeUsinaOutorga",how="left")
    cols = ["DatValidadeIV","DatValidadeI","DthEnvio","IdeUsinaOutorga","DatValidadeV"]
    rename_cols = {'DatValidadeIV': 'DatValidadeLI','DatValidadeI': 'DatValidadeLP','DatValidadeV': 'DatValidadeLO'}
    ambiental_datas = pd.read_parquet(os.path.join(biu_download_path,"vrapeellicenciamento.gzip"))[cols].rename(columns=rename_cols)
    ambiental_datas = ambiental_datas.loc[ambiental_datas.groupby('IdeUsinaOutorga').DthEnvio.idxmax()]
    del ambiental_datas['DthEnvio']
    df_usina = pd.merge(df_usina,ambiental_datas,on="IdeUsinaOutorga",how="left")
    cols = ["DthEnvio","NomSitContratoI","NomSitContratoIV","NomSituacaoContratoIII","NomSitContratoII","IdeUsinaOutorga"]
    rename_cols = {'NomSitContratoI': 'NomSitContratoCCD','NomSitContratoIV': 'NomSitContratoCUST','NomSituacaoContratoIII': 'NomSituacaoContratoCUSD','NomSitContratoII': 'NomSitContratoCCT'}
    acesso_contratos = pd.read_parquet(os.path.join(biu_download_path,"vrapeelacesso.gzip"))[cols].rename(columns=rename_cols)
    acesso_contratos = acesso_contratos.loc[acesso_contratos.groupby('IdeUsinaOutorga').DthEnvio.idxmax()]
    del acesso_contratos['DthEnvio']
    df_usina = pd.merge(df_usina,acesso_contratos,on="IdeUsinaOutorga",how="left")
    ## Cria colunas com definições e regras
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
    cols = ['IdeUsinaOutorga'] + list(dict_validades.keys()) + ['ValidadeAmbiental']
    
    df_usina['CondicaoAmbiental'] = pd.NA
    
    for validade in reversed(dict_validades):
     df_usina.loc[df_usina[validade].notna(),'CondicaoAmbiental'] = dict_validades[validade]
    
    df_usina.loc[(df_usina.CondicaoAmbiental.isna()) & (df_usina.DthEnvio.notna()),'CondicaoAmbiental'] = 'Sem LP'
    df_usina.loc[(df_usina.CondicaoAmbiental.isna()),'CondicaoAmbiental'] = 'Não Informado'
    
    cols = ['IdeUsinaOutorga'] + list(dict_validades) + ['DthEnvio','ValidadeAmbiental','CondicaoAmbiental','NomUsina']
    
    
    
    df_usina['condicao_conexao'] = "Não informado"
    
    nsa = "Não se Aplica"
    nass = "Não Assinado"
    valido = "Válido"
    vencido = "Vencido"
    
    df_usina.loc[
     ((df_usina.NomSitContratoCCD == nsa)&
     (df_usina.NomSitContratoCCT == nsa) &
     df_usina.DthEnvio.notna()
     )
     ,'condicao_conexao'] = "Verificar"
    
    df_usina.loc[
    ( ((df_usina.NomSitContratoCCD == valido) |
     (df_usina.NomSitContratoCCT == valido)) &
     df_usina.DthEnvio.notna())
    
     ,'condicao_conexao'] = "OK"
    
    df_usina.loc[
     (((df_usina.NomSitContratoCCD == vencido) |
     (df_usina.NomSitContratoCCT == vencido) |
     (df_usina.NomSitContratoCCD == nass) |
     (df_usina.NomSitContratoCCT == nass )) &
     df_usina.DthEnvio.notna())
     ,'condicao_conexao'] = "Sem Conexão"
    
    cols = ['IdeUsinaOutorga'] + ['NomSitContratoCCD','NomSitContratoCCT','DthEnvio','condicao_conexao','NomUsina']
    
    
    
    
    
    df_usina['condicao_uso'] = "Não informado"
    
    df_usina.loc[
     ((df_usina.NomSituacaoContratoCUSD == nsa)&
     (df_usina.NomSitContratoCUST == nsa)
     )
     ,'condicao_uso'] = "Verificar"
    
    
    df_usina.loc[
     ((df_usina.NomSituacaoContratoCUSD == valido) |
     (df_usina.NomSitContratoCUST == valido)
     )
     ,'condicao_uso'] = "OK"
    
    df_usina.loc[
     (((df_usina.NomSituacaoContratoCUSD == vencido) |
     (df_usina.NomSitContratoCUST == vencido) |
     (df_usina.NomSituacaoContratoCUSD == nass) |
     (df_usina.NomSitContratoCUST == nass )))
     ,'condicao_uso'] = "Sem Uso"
    
    
    cols = ['IdeUsinaOutorga'] + ['NomSituacaoContratoCUSD','NomSitContratoCUST','condicao_uso','NomUsina']
    
    
    
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
    
    cols = ['IdeUsinaOutorga'] + ['DscComercializacaoEnergia','DatConclusaoACL','PPA','NomUsina']
    
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
     (( df_usina.IdcSituacaoObra == na) & (df_usina.PPA != nenhum) ),
     ((df_usina.IdcSituacaoObra == na) & (df_usina.PPA == nenhum) & (df_usina.CondicaoAmbiental.isin(["LI","LO"])) & (df_usina.condicao_uso == ok)),
     ((df_usina.IdcSituacaoObra == na) & (df_usina.PPA == nenhum) & (df_usina.CondicaoAmbiental == "LP") & (df_usina.condicao_uso == ok)),
     ((df_usina.IdcSituacaoObra == na) & (df_usina.PPA == nenhum) & (df_usina.CondicaoAmbiental.isin(["LI","LO"])) & (df_usina.condicao_uso != ok)),
     ((df_usina.IdcSituacaoObra == na) & (df_usina.PPA == nenhum) & (df_usina.CondicaoAmbiental == "LP") & (df_usina.condicao_uso != ok)),
     (df_usina.IdcSituacaoObra == paralisada),
     (df_usina.CondicaoAmbiental == "Sem LP"),
     ((df_usina.DthEnvio.isna()) & (df_usina.PPA == nenhum)),
    
     ],
     [9,0,1,2,4,3,5,8,6,7],
     default=-8
    )
    
    
    cols = ['IdeUsinaOutorga','IdcSituacaoObra','PPA','CondicaoAmbiental','condicao_uso','criterio_novo']
    
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
    
    df_usina['homologar_marcos'] = False
    df_usina['dsc_marcos_a_homologar'] = ''
    
    # Homologação de marcos
    
    for marco_monitoramento, marco_rapeel in dict_marcos_homologar.items():
     mask = (df_usina[marco_monitoramento].isna()) & (df_usina[marco_rapeel].notna())
     df_usina['homologar_marcos'] |= mask
     df_usina.loc[mask,'dsc_marcos_a_homologar'] += f'{marco_monitoramento}, '
    
    df_usina.dsc_marcos_a_homologar = df_usina.dsc_marcos_a_homologar.str.slice(0,-2)
    
    cols = ["IdeUsinaOutorga",'NomUsina'] + ['homologar_marcos','dsc_marcos_a_homologar']
    
    df_usina['revisar_IO'] = np.select(
     [(df_usina.prev_IO_SFG < final_do_mes) |
     (df_usina.prev_IO_rapeel > df_usina.prev_IO_SFG)],
     [True],
     default = False)
    cols = ['IdeUsinaOutorga', 'NomUsina','prev_IO_SFG','prev_IO_rapeel','revisar_IO']
    
    usinas_selecionadas = read_file("./usinas_selecionadas.txt").split(",")
    usinas_selecionadas = [int(num) for num in usinas_selecionadas]
    df_usina["Usina_Selecionada"] = False
    df_usina.loc[df_usina.IdeUsinaOutorga.isin(usinas_selecionadas),'Usina_Selecionada'] = True
    
    df_usina['Paralisada'] = False
    df_usina.loc[df_usina.IdcObraParalisada=="Sim",'Paralisada'] = True
    
    df_usina['Sem_Previsao'] = False
    df_usina.loc[df_usina.IdcSemPrevisao=="Sim",'Sem_Previsao'] = True
    
    cols = ['IdeUsinaOutorga', 'NomUsina','Usina_Selecionada','Paralisada','Sem_Previsao']
    
    # Informações de UGs
    ## Carrega banco de dados
    cols = ["IdeUsinaOutorga",'NumUgUsina',"IdcMonitorada",'DatLiberOpComerRealizado','DatPrevisaoSFGComercial']
    monitoramentoug = pd.read_parquet(os.path.join(biu_download_path,"vmonitoramentoug.gzip"))[cols].rename(columns={'NumUgUsina':'NumOperacaoUg'})
    # Não remover ugs não monitoradas nesse passo, pois serão usadas para encontrar ugs difentes entre
    # monitoramento e rapeel. São retiradas durante o merge
    monitoramentoug.IdcMonitorada = monitoramentoug.IdcMonitorada.map({"Sim":True,"Não":False}).astype(bool)
    
    
    cols = ["IdeUsinaOutorga",'NumOperacaoUg',"DthEnvio",'DatPrevistaComercial']
    ug_rapeel = pd.read_parquet(os.path.join(biu_download_path,"vrapeeloperacaoug.gzip"))[cols]
    ug_rapeel = ultimo_rapeel(ug_rapeel)
    
    cols_used_robot = ['DatInicioObraOutorgado','prev_IO_SFG','prev_IO_rapeel']
    list_cols_used = ['IdeUsinaOutorga','IdcObraParalisada','IdcSemPrevisao','criterio_novo','Usina_Sem_Rapeel',] + cols_used_robot
    # UGs não monitoradas são retiradas aqui
    df_ug = pd.merge(df_usina[list_cols_used],monitoramentoug[monitoramentoug.IdcMonitorada],on="IdeUsinaOutorga",how='left')
    df_ug.IdcMonitorada = df_ug.IdcMonitorada.astype(bool)
    df_ug = pd.merge(df_ug,ug_rapeel,on=list_id_ug,how='left') # poderia ser inner?
    # Remove usinas que já entraram em operação comercial ou usinas sem UGs monitoradas
    df_ug = df_ug.loc[df_ug.DatLiberOpComerRealizado.isna() & df_ug.NumOperacaoUg.notna()].reset_index(drop=True)
    # Carrega informações do previsor
    
    calculo_previsao = pd.read_parquet(previsao_file)
    calculo_previsao.FaseAtual =calculo_previsao.FaseAtual.str.slice(0,-3)
    calculo_previsao.Indicador =calculo_previsao.Indicador/100
    
    cols = ["Dat_OC_obrigacao","prev_IO_SFG","FASE","Ind_crono_norm","IdeUsinaOutorga","flagOPTeste30dias",'Previsao_OC']
    rename_cols = {'FaseAtual':'FASE','Indicador':'Ind_crono_norm','NumUgUsina':'NumOperacaoUg','Previsao_OC':'calculo_previsor_OC'}
    calculo_previsao = calculo_previsao.rename(columns=rename_cols)[['IdeUsinaOutorga','NumOperacaoUg','FASE','Ind_crono_norm','flagOPTeste30dias','calculo_previsor_OC','Dat_OC_obrigacao','MdaPotenciaUnitaria']]
    
    df_ug = pd.merge(df_ug,calculo_previsao,on=list_id_ug,how="left")
    
    monitoramentoug['existe_monitoramento'] = True # Adiciona colunas para checagem
    ug_rapeel['existe_rapeel'] = True # dos resultados
    
    # Faz merge tipo outer (todas as UGs) com os dados do
    # monitoramento e rapeel
    
    comp_ug = pd.merge(monitoramentoug[list_id_ug + ['existe_monitoramento',"IdcMonitorada"]],
     ug_rapeel[list_id_ug + ['existe_rapeel']], how="outer")
    
    
    # Inner join com usinas monitoradas
    comp_ug = pd.merge(df_usina[df_usina.IdcUsinaMonitorada][['IdeUsinaOutorga']],comp_ug,how="inner",on="IdeUsinaOutorga")
    
    
    
    # Inner join com usinas que já enviaram rapeel
    comp_ug = pd.merge(rapeel_cronograma[['IdeUsinaOutorga']],comp_ug,how="inner",on="IdeUsinaOutorga")
    
    
    
    # Das UGs que estão no monitoramento, remove-se UGs não monitoradas
    comp_ug = comp_ug[comp_ug.IdcMonitorada != False]
    
    
    
    # Preenche com o valor falso as usinas que não foram encontradas
    comp_ug[['existe_monitoramento','existe_rapeel']] = comp_ug[['existe_monitoramento','existe_rapeel']].fillna(False)
    
    
    
    # Coluna com diferenças
    comp_ug["Diff"] = ~(comp_ug.existe_monitoramento & comp_ug.existe_rapeel)
    
    
    df_UG_sem_rapeel = comp_ug[comp_ug.existe_monitoramento & comp_ug.Diff][['IdeUsinaOutorga']].drop_duplicates()
    df_UG_sem_rapeel['UG_sem_rapeel'] = True
    
    
    # Todas as UGs de uma usina que pelo menos uma UG não possua rapeel terão UG_sem_rapeel = True
    df_ug = pd.merge(df_ug,df_UG_sem_rapeel,on='IdeUsinaOutorga',how='left')
    df_ug.loc[df_ug.UG_sem_rapeel.isna(),'UG_sem_rapeel'] = False
    
    
    # Adiona a coluna PrevisaoOC_rapeel_max, que é a máxima previsão OC dentre todas as UGs enviadas no rapeel, independente se estão
    # no monitoramento ou não.
    df_ug = df_ug.merge(ug_rapeel.loc[ug_rapeel[ug_rapeel.DatPrevistaComercial.notna()].groupby("IdeUsinaOutorga").DatPrevistaComercial.idxmax()][['IdeUsinaOutorga','DatPrevistaComercial']].rename(columns={'DatPrevistaComercial':'PrevisaoOC_rapeel_max'}),
     how="left",on="IdeUsinaOutorga")
    
    
    # Para esses casos, em vez de DatPrevistaComercial, será usado PrevisaoOC_rapeel_max
    # Ver arquivo
    df_ug.loc[df_ug.UG_sem_rapeel | df_ug.DatPrevistaComercial.isna(),'DatPrevistaComercial'] = df_ug.PrevisaoOC_rapeel_max
    
    ## Cria colunas com definições e regras
    ################################ criterio_novo_pmo ################################
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
    
    df_ug['criterio_novo_pmo'] = np.select(list_condicoes,list_values)
    
    ################################ regranovapmo ################################
    df_ug.loc[(df_ug.criterio_novo_pmo == 0.1),'regranovapmo'] = df_ug[['DatPrevistaComercial','calculo_previsor_OC']].max(axis=1)
    
    df_ug.loc[(df_ug.criterio_novo_pmo == 0.2),'regranovapmo'] = ( hoje + pd.Timedelta(60,"D"))
    
    
    df_ug.loc[(df_ug.criterio_novo_pmo == 0.3) & (df_ug['calculo_previsor_OC'].isna()),'regranovapmo'] = df_ug['DatPrevistaComercial']
    
    
    df_ug.loc[(df_ug['criterio_novo'] == 0) & (df_ug['FASE'] == "OT") & (df_ug['calculo_previsor_OC'].notna()),'regranovapmo'] = df_ug['calculo_previsor_OC']
    
    
    df_ug.loc[(df_ug['criterio_novo']== 1) | (df_ug['criterio_novo']== 2) ,'regranovapmo'] = df_ug[['DatPrevistaComercial','calculo_previsor_OC','Dat_OC_obrigacao']].max(axis=1)
    
    df_ug["Handicap"] = hoje + pd.Timedelta(5*365,'D')
    df_ug.loc[((df_ug['criterio_novo'] == 3) | (df_ug['criterio_novo'] == 4) | (df_ug['criterio_novo'] == 5) ),'regranovapmo'] = df_ug[['DatPrevistaComercial','calculo_previsor_OC','Dat_OC_obrigacao','Handicap']].max(axis=1)
    
    
    df_ug.loc[df_ug['criterio_novo']== 6,'regranovapmo'] = pd.NA
    
    df_ug["Handicap"] = hoje + pd.Timedelta(6*365,'D')
    
    df_ug.loc[df_ug['criterio_novo']== 7,'regranovapmo'] = df_ug[['calculo_previsor_OC','Dat_OC_obrigacao','Handicap']].max(axis=1)
    
    
    df_ug.loc[df_ug['criterio_novo']== 8,'regranovapmo'] = df_ug[['DatPrevistaComercial','Dat_OC_obrigacao']].max(axis=1)
    
    df_ug.loc[(df_ug['criterio_novo'] == 9),'regranovapmo'] = pd.NA
    
    del df_ug['Handicap']
    serie_usinas_previsao_OC_passado = df_ug[df_ug.DatPrevisaoSFGComercial < final_do_mes].IdeUsinaOutorga.drop_duplicates()
    
    serie_usinas_previsao_em_teste = df_ug[df_ug.criterio_novo_pmo.isin([0.2,0.3])].IdeUsinaOutorga.drop_duplicates()
    
    serie_flagOPTeste30dias = df_ug[df_ug.flagOPTeste30dias == 1].IdeUsinaOutorga.drop_duplicates()
    df_usina["flagOPTeste30dias"] = False
    df_usina.loc[df_usina.IdeUsinaOutorga.isin(serie_flagOPTeste30dias),'flagOPTeste30dias'] = True
    
    serie_UG_sem_rapeel = df_ug[df_ug.UG_sem_rapeel].IdeUsinaOutorga.drop_duplicates()
    df_usina["UG_sem_rapeel"] = False
    df_usina.loc[df_usina.IdeUsinaOutorga.isin(serie_UG_sem_rapeel),'UG_sem_rapeel'] = True
    
    df_usina["flagOPTeste30dias"] = False
    df_usina.loc[df_usina.IdeUsinaOutorga.isin(serie_flagOPTeste30dias),'flagOPTeste30dias'] = True
    
    df_usina["prev_OC_SFG_passado"] = False
    df_usina.loc[df_usina.IdeUsinaOutorga.isin(serie_usinas_previsao_OC_passado),'prev_OC_SFG_passado'] = True
    
    df_usina["em_teste"] = False
    df_usina.loc[df_usina.IdeUsinaOutorga.isin(serie_usinas_previsao_em_teste),'em_teste'] = True
    
    df_usina["sem_monitoramento"] = False
    df_usina.loc[df_usina.DatMonitoramento < (hoje - pd.Timedelta(4*30, unit="D")),'sem_monitoramento'] = True
    df_usina["manual"] = False
    df_usina.loc[(df_usina.Paralisada | df_usina.Sem_Previsao |
     df_usina.Usina_Selecionada | df_usina.em_teste |
     (df_usina.DscJustificativaPrevisao == 'Situação das obras de conexão e linha de transmissão associada.')),
     'manual'] = True
    # Casos de seleção
    
    df_usina['caso_I'] = df_usina['caso_II_a'] = df_usina['caso_II_b'] = df_usina['caso_III'] = False
    list_casos = ['caso_I','caso_II_a','caso_II_b','caso_III']
    
    df_usina['caso_I'] = (~df_usina.manual) & (~df_usina.revisar_IO) & (~ df_usina.homologar_marcos)
    
    df_usina['caso_II_a'] = (~df_usina.manual) & (df_usina.revisar_IO) & (~ df_usina.homologar_marcos)
    
    df_usina['caso_II_b'] = (~df_usina.manual) & ((df_usina.homologar_marcos) | (df_usina.homologar_marcos & df_usina.revisar_IO))
    
    df_usina['caso_III'] = (df_usina.manual & (df_usina.sem_monitoramento | df_usina.homologar_marcos | df_usina.revisar_IO | df_usina.prev_OC_SFG_passado)) | df_usina.em_teste
    
    df_usina['selecionado_BIU'] = df_usina[list_casos].any(axis=1)
    list_criterios = ['criterio_novo',"criterio_novo_pmo"]
    
    df_usina_criterio = df_ug.loc[df_ug.groupby("IdeUsinaOutorga").criterio_novo_pmo.idxmax()][["IdeUsinaOutorga","FASE",'flagOPTeste30dias'] + list_criterios]
    
    ################################ dsc_justificativa_regra_nova ################################
    list_condicoes = [
     (df_usina_criterio['criterio_novo_pmo'] == 9),
     (df_usina_criterio['criterio_novo_pmo'] == 8),
     (df_usina_criterio['criterio_novo_pmo'] == 0.2),
     (df_usina_criterio['criterio_novo_pmo'] == 0.1) | (df_usina_criterio['criterio_novo_pmo'] == 0.3),
     (df_usina_criterio['criterio_novo_pmo'] == 1),
     df_usina_criterio['criterio_novo_pmo']==2,
     df_usina_criterio['criterio_novo_pmo']==3,
     (df_usina_criterio.criterio_novo_pmo== 4),
     (df_usina_criterio.criterio_novo_pmo== 5),
     (df_usina_criterio.criterio_novo_pmo== 6),
     (df_usina_criterio.criterio_novo_pmo== 7),
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
    
    df_usina_criterio['dsc_justificativa_regra_nova'] = np.select(list_condicoes,list_values)
    
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
     "Usina sem obras, sem cust, sem PPA e com LI = Previsão OC handicap de 5 anos",
     "Usina sem obras, com cust, sem PPA e sem LI = Previsão OC handicap de 5 anos",
     "Usina sem obras, sem cust, sem PPA e sem LI = Previsão OC handicap de 5 anos",
     "Usina sem obras, sem PPA e sem RAPEEL = Previsão OC handicap de 5 anos",
     "Usina sem LP = Previsão OC sem previsão","Usina obras paralisadas = Previsão OC maior entre data RAPEEL e data compromisso"
    ]
    df_usina_criterio['dsccriterionovo'] = np.select(list_condicoes,list_values)
    list_justificativas = ['dsccriterionovo','dsc_justificativa_regra_nova']
    df_usina = pd.merge(df_usina,
    df_usina_criterio[['IdeUsinaOutorga','criterio_novo_pmo'] + list_justificativas],
    on = "IdeUsinaOutorga", how='left')
    
    
    list_casos = ['caso_I','caso_II_a','caso_II_b','caso_III','selecionado_BIU']
    df_ug = pd.merge(df_usina[['IdeUsinaOutorga','dsc_justificativa_regra_nova','dsccriterionovo','DatMonitoramento','DscJustificativaPrevisao'] + list_casos],df_ug, on = "IdeUsinaOutorga",how='left')
    # Renomeia colunas para exportação
    dic = {
     'NumOperacaoUg':'NumUgUsina',
     'regranovapmo': 'PrevisaoOC_regra',
     'dsc_justificativa_regra_nova' : 'Justificativadaprevisao_new',
     'criterio_novo_pmo' : 'CriterioPrevisao',
     'dsccriterionovo':'DscCriterioPrevisao',
     'Previsao_OC':'CalculoPrevisorOC',
     'Dat_OC_obrigacao':'OC_Obrigacao',
     'DscJustificativaPrevisao':'DscJustificativaPrevisaoAtual',
     'calculo_previsor_OC':'CalculoPrevisorOC'
    }
    
    
    df_ug = df_ug.rename(columns=dic)
    df_usina = df_usina.rename(columns=dic)
    return (df_usina,df_ug)
    
### end generate_BIU ###





### start export_biu_files ###
def export_biu_files(biu_path, casos, df_usina, df_ug, ):
    cols_ug = ['IdeUsinaOutorga','NumUgUsina','MdaPotenciaUnitaria','PrevisaoOC_regra','Justificativadaprevisao_new','CriterioPrevisao','DscCriterioPrevisao','CalculoPrevisorOC','OC_Obrigacao','PrevisaoOC_rapeel_max','DscJustificativaPrevisaoAtual','DatMonitoramento','DthEnvio','FASE']
    
    cols_revisar_IO = ['IdeUsinaOutorga','DatInicioObraOutorgado','prev_IO_rapeel','prev_IO_SFG','DatMonitoramento','DthEnvio']
    
    if 'I' in casos:
     # Caso I
     file_name = os.path.join(biu_path,f"caso_I.xlsx")
     df_ug[df_ug.caso_I][cols_ug].to_excel(file_name,index=False)
    
    # Caso II_a
    if 'II_a' in casos:
     file_name = os.path.join(biu_path,f"caso_II_a.xlsx")
     df_usina[df_usina.caso_II_a][cols_revisar_IO].to_excel(file_name,index=False)
    
    if 'BIU' in casos:
     # Caso II-b e III
     file_name = os.path.join(biu_path,f"BIU.xlsx")
     df_usina[df_usina.caso_II_b | df_usina.caso_III].to_excel(file_name,index=False)
    
     file_name = os.path.join(biu_path,f"Usinas_reazer_robot.xlsx")
     df_usina[df_usina.caso_II_a | df_usina.caso_II_b][['IdeUsinaOutorga']].to_excel(file_name,index=False)
### end export_biu_files ###






def biu(biu_path,download_path):
    log_biu_file_name = os.path.join(biu_path,'biu_log.pickle')

    options = {
    0 : "Voltar",
    1 : "Gerar BIU (Estágio I)",
    2 : "Recalcular BIU (Estágio II)"
    }

    show_options(options)
    option = get_num(options)

    if option == 1:
        biu_download_path = os.path.join(biu_path,"Download")
        save_path = os.path.join(biu_path,"Estagio_I")
        if os.path.exists(log_biu_file_name):
            log = load_pickle(log_biu_file_name)
            if log['Terminado'] == False:
                print(f'Um BIU foi iniciado em {log["Inicio"].strftime("%d/%m/%Y")} e ainda não foi terminado.')
                print("Deseja terminá-lo? As informações serão apagadas.")
                options = {1 : "Sim", 2:"Não"}
                show_options(options)
                num = get_num(options)
                if num == 2: # Terminar biu
                    return False
                if num == 1:
                    verificacao = input("Digite Aneel e aperte enter para confirmar exclusão do BIU: ")
                    if verificacao.replace(" ",'').lower() != "aneel":
                        print("Verificação falhou")
                        return False
                    else:
                        print("Verificação concluída. Os arquivos serão sobrescritos.")
        log = {
            "Terminado":False,
            "Inicio": datetime.now(),
            "Termino": False
        }
        
        #download_db(biu_download_path,force_download=True)
        previsao_file = os.path.join(save_path,"Previsao_OC.gzip") #calcular_previsao(biu_download_path,save_path,perguntar=False)
        df_usina,df_ug = generate_BIU(biu_download_path,previsao_file)

        export_biu_files(save_path,['I','II_a','BIU'],df_usina,df_ug)
        save_pickle(log,log_biu_file_name)

    if option == 2:
        if os.path.exists(log_biu_file_name):
            log = load_pickle(log_biu_file_name)             
            save_path = os.path.join(biu_path,"Estagio_II")
            previus_path = os.path.join(biu_path,"Estagio_I")
            #download_db(download_path,force_download=True)
            previsao_file = os.path.join(save_path,"Previsao_OC.gzip") # calcular_previsao(download_path,save_path)
            df_usina,df_ug = generate_BIU(download_path,previsao_file)

            usinas_refazer_robot = pd.read_excel(os.path.join(previus_path,f"Usinas_reazer_robot.xlsx"))

            df_usina = pd.merge(usinas_refazer_robot,df_usina,how="left",on="IdeUsinaOutorga")
            df_ug = pd.merge(usinas_refazer_robot,df_ug,how="left",on="IdeUsinaOutorga")

            list_casos = ['caso_I','caso_II_a','caso_II_b','caso_III']

            df_ug[list_casos] = False
            df_usina[list_casos] = False
            df_ug.caso_I = True
            df_usina.caso_I = True
        
            export_biu_files(save_path,['I'],df_usina,df_ug)
            log["Termino"] = datetime.now()
            save_pickle(log,log_biu_file_name)
            print("Arquivo exportados...")
        else:
            print("Estagio I ainda não realizado.")



if __name__ == '__main__':
    biu_path = os.path.join(get_standard_folder_path("Documents"),"Previsor/BIU")
    biu_download_path = os.path.join(biu_path,"Download")
    previsao_file = os.path.join(biu_path,"Previsao_OC.gzip")
    download_path =  os.path.join(get_standard_folder_path("Documents"),"Previsor/Download")
    biu(biu_download_path,biu_path,download_path)

