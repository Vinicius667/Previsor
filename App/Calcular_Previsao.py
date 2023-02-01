import os,sys
from download_DB import download_db, atualizar_db
import pandas as pd
from scipy.stats import norm
from datetime import date
from utils import *
import psutil


def previsor(directory):
    hoje =  pd.to_datetime(date.today())
    hoje_str = hoje.strftime(r'%Y_%m_%d')

    lista_colunas_usinas = [
    'IdeUsinaOutorga',
    'SigTipoGeracao',
    'DscComercializacaoEnergia',
    'NomUsina',
    "DatPrevisaoIniciobra",
    "DatMonitoramento",
    'DatInicioObraOutorgado',
    'DatInicioObraRealizado',
    'DatConcretagemRealizado',
    'DatMontagemRealizado',
    'IdcUsinaMonitorada','IdcSemPrevisao']

    lista_colunas_ug = [
    'IdeUsinaOutorga','NumUgUsina','MdaPotenciaUnitaria',
    "DatLiberacaoSFGTeste","DatLiberOpComerRealizado","DatUGInicioOpComerOutorgado"]


    # Carrega dataframe com dados do skate
    vmonitoramentoug = pd.read_parquet(os.path.join(directory,"vmonitoramentoug.gzip"))[lista_colunas_ug]

    vmonitoramentousina = pd.read_parquet(os.path.join(directory,"vmonitoramentousina.gzip"))[lista_colunas_usinas]

    vmonitoramentoleilao = pd.read_parquet(os.path.join(directory,"vmonitoramentoleilao.gzip"))


    # Carrega dataframe com informações do previsor
    tabela_previsor = pd.read_parquet("./Tabelas_Previsor/tabela_previsor.gzip")
    del tabela_previsor["count"]

    # Cria dicionários com valores de media, desvio padrão e coeficientes
    dict_Media_Fase = tabela_previsor.groupby(["Tipo de geração","Fase","Comercialização"]).Media_Fase.apply(list)
    dict_Std_Fase = tabela_previsor.groupby(["Tipo de geração","Fase","Comercialização"])["Std_Fase"].apply(list)
    dict_a = tabela_previsor.groupby(["Tipo de geração","Fase","Comercialização"]).a.apply(list)
    dict_b = tabela_previsor.groupby(["Tipo de geração","Fase","Comercialização"]).b.apply(list)
    dict_Media_Atraso = tabela_previsor.groupby(["Tipo de geração","Fase","Comercialização"]).Media_Atraso.apply(list)
    dict_Std_Atraso = tabela_previsor.groupby(["Tipo de geração","Fase","Comercialização"]).Std_Atraso.apply(list)


    # Remove usinas que entraram em operação comercial.
    #print(" Quantidade de UGs ".center(60,"*") + "\n")
    #print(f"{vmonitoramentoug.shape[0]} UGs no SKATE.")
    vmonitoramentoug = vmonitoramentoug[vmonitoramentoug.DatLiberOpComerRealizado.isna()].copy() 
    #print(f"{vmonitoramentoug.shape[0]} UGs sem liberação para entrada em operação comercial.")
    #print("*".center(60,"*") + "\n")


    # Para usinas com mais de um data de início de suprimento, mantém apenas o primeiro. 
    vmonitoramentoleilao = vmonitoramentoleilao.dropna(subset="DatInicioSuprimento").reset_index(drop=True)
    vmonitoramentoleilao["DatInicioSuprimento"] = pd.to_datetime(vmonitoramentoleilao["DatInicioSuprimento"])
    vmonitoramentoleilao = vmonitoramentoleilao.loc[vmonitoramentoleilao.groupby('IdeUsinaOutorga').DatInicioSuprimento.idxmin()]


    # Junta informações de leilão com UGs
    vmonitoramentoug = vmonitoramentoug.merge(vmonitoramentoleilao[["IdeUsinaOutorga","DatInicioSuprimento"]],on="IdeUsinaOutorga",how="left")


    # Junta informações de referentes às usinas com informações referentes às UGs
    skate_merged = vmonitoramentoug.merge(vmonitoramentousina,on="IdeUsinaOutorga",how="left")

    df_acr_sem_leilao = skate_merged[(skate_merged.DscComercializacaoEnergia=="ACR") &  (skate_merged.DatInicioSuprimento.isna())]

    quant_acr_sem_leilao = df_acr_sem_leilao.shape[0]

    if quant_acr_sem_leilao > 0:
        return(("ErroLeilao",df_acr_sem_leilao))
        


    # Adiciona data de obrigação das usinas ao dataframe
    skate_merged['Dat_OC_obrigacao'] = skate_merged.DatInicioSuprimento
    skate_merged.loc[((skate_merged.DatInicioSuprimento.isna()) |
                      (skate_merged.DatInicioSuprimento < skate_merged.DatUGInicioOpComerOutorgado)),
    'Dat_OC_obrigacao'] = skate_merged.DatUGInicioOpComerOutorgado


    # Utilizar previsão de UTE para UTN
    skate_merged.loc[(skate_merged.SigTipoGeracao == "UTN"),"SigTipoGeracao"] = "UTE"

    # Casos sem previsão
    mask_remove =   (skate_merged.IdcUsinaMonitorada=="Não")  #| (skate_merged.IdcSemPrevisao == "Sim")
    skate_merged = skate_merged[~mask_remove].copy()


    # Dicionários com fase e marco que marca seu início
    dict_fase_marco = {
        "IO_OC" : "DatInicioObraRealizado",
        "CC_OC" : "DatConcretagemRealizado",
        "ME_OC" : "DatMontagemRealizado",
        "OT_OC" : "DatLiberacaoSFGTeste"
    }


    """
    # Escolhe coeficientes para o caso com menor erro

    rmse = pd.read_parquet("./Outputs/rmse_previsoes.gzip")
    rmse

    escolhas = tabela_previsor[tabela_previsor.Comercialização != "Ambos"].copy()
    escolhas.iloc[:,3:] = pd.NA
    escolhas["Escolha"] = pd.NA

    # Define quais coeficientes serão utilizados baseado no menor erro
    dict_coeffs = {}
    for fase,marco in reversed(dict_fase_marco.items()):
        dict_coeffs[fase] = {}
        for geracao in lista_geracao:
            dict_coeffs[fase][geracao] = {}
            mask = rmse.index.str.startswith(fase[:2])
            menor_erro = rmse.loc[mask,geracao].idxmin()
            for comer in lista_comer:
                mask = (escolhas["Tipo de geração"]==geracao) & (escolhas.Fase == fase) & (escolhas["Comercialização"] == comer)
                dict_coeffs[fase][geracao][comer] = {}
                try:
                    if menor_erro.endswith("(todos)"):
                        comer_escolhida = "Ambos"
                    else:
                        comer_escolhida = comer
                    escolhas.loc[mask,"Escolha"] = comer_escolhida
                    escolhas.loc[mask,"a"] = dict_coeffs[fase][geracao][comer]["a"] = dict_a[geracao][fase][comer_escolhida][0]
                    escolhas.loc[mask,"b"] = dict_coeffs[fase][geracao][comer]["b"] = dict_b[geracao][fase][comer_escolhida][0]
                    escolhas.loc[mask,"Media_Fase"] = dict_coeffs[fase][geracao][comer]["Media_Fase"] = dict_Media_Fase[geracao][fase][comer_escolhida][0]
                    escolhas.loc[mask,"Std_Fase"] = dict_coeffs[fase][geracao][comer]["Std_Fase"] = dict_Std_Fase[geracao][fase][comer_escolhida][0]

                    escolhas.loc[mask,"Media_Atraso"] = dict_coeffs[fase][geracao][comer]["Media_Atraso"] = dict_Media_Atraso[geracao][fase][comer_escolhida][0]
                    escolhas.loc[mask,"Std_Atraso"] = dict_coeffs[fase][geracao][comer]["Std_Atraso"] = dict_Std_Atraso[geracao][fase][comer_escolhida][0]
                except AttributeError:
                    pass
    escolhas = escolhas.astype("timedelta64[ns]",errors="ignore")
    """

    # Escolher sempre ambos
    escolhas = tabela_previsor[tabela_previsor.Comercialização=="Ambos"].copy()
    skate_merged.loc[:,"DscComercializacaoEnergia"] = "Ambos"


    # Listas com fases, marcos e geração
    lista_fases = list(dict_fase_marco.keys()) 
    lista_marcos = list(dict_fase_marco.values()) 
    lista_geracao = list(skate_merged.SigTipoGeracao.unique())
    lista_comer = list(skate_merged.DscComercializacaoEnergia.unique())


    skate_merged["FaseAtual"] = pd.NA
    skate_merged["DataMarcoAtual"] = pd.NA
    skate_merged["ProximaFase"] = pd.NA

    # Faz um loop com fases e marcos em ordem contrária de acontecimento, pois,
    # para descobrir a fase atual da usina, indentifica-se o último marco cumprido.

    for fase,marco in reversed(dict_fase_marco.items()):
        # Caso a coluna FaseAtual ainda não tenha sido preenchida e o marco dessa iteração
        # tenha sido cumprido, a FaseAtual é a fase dessa iteração.
        mask = skate_merged.FaseAtual.isna() & skate_merged[marco].notna()
        skate_merged.loc[mask,"FaseAtual"] = fase
        # Coloca na coluna DataMarcoAtual a data do marco
        skate_merged.loc[mask,"DataMarcoAtual"] = skate_merged[marco]


    for fase in lista_fases[0:-1]:
        # Adiciona coluna ProximaFase 
        mask = skate_merged.FaseAtual == fase
        proxima_fase = lista_fases[lista_fases.index(fase) + 1 ]
        skate_merged.loc[mask,"ProximaFase"] = proxima_fase
        if fase == "IO_OC":
            # Usinas UFV e UTE não possuem fase CC_OC
            skate_merged.loc[mask & ((skate_merged.SigTipoGeracao=="UFV") | (skate_merged.SigTipoGeracao=="UTE")),"ProximaFase"] = "ME_OC"

    # Adiciona colunas FaseAtual e ProximaFase para usinas que ainda não iniciaram obra
    skate_merged.loc[skate_merged.FaseAtual.isna(),"FaseAtual"] = "OUT_OC"
    skate_merged.loc[skate_merged.FaseAtual=="OUT_OC","ProximaFase"] = "IO_OC"


    
    '''    
    # Para usinas que ainda não iniciaram obra (em fase OUT_OC) assume-se maior data entre previsão SFG, previsão do agente e 4 meses a partir de hoje
    skate_merged.loc[skate_merged.FaseAtual == "OUT_OC","DatInicioObraRealizado"] = hoje + pd.Timedelta(4*30,unit="D")
    lista_colunas_previsao_IO = ["DatPrevisaoIniciobra","DatMonitoramento"]
    '''
    mask_out = skate_merged.FaseAtual == 'OUT_OC'
    skate_merged.loc[mask_out,"DatInicioObraRealizado"]  =    skate_merged.DatPrevisaoIniciobra           #skate_merged[lista_colunas_previsao_IO + ["DatInicioObraRealizado"]].max(axis=1)
    skate_merged.loc[mask_out & skate_merged.DatInicioObraRealizado.isna(),'DatInicioObraRealizado'] = hoje + pd.Timedelta(4*30,unit="D")
    
    
    
    skate_merged.loc[mask_out,"DataMarcoAtual"]  = skate_merged.DatInicioObraRealizado 

    # Temporariamente, para cálculo das previsões, assume-se que as usinas em fase OUT_OC estejam em fase IO_OC com a data de IO calculada acima
    mask_out = skate_merged.FaseAtual == 'OUT_OC'
    skate_merged.loc[mask_out,"FaseAtual"]  = "IO_OC"





    skate_merged["MarcoMedioAtual"] = pd.NA
    skate_merged["MarcoMedioProximo"] = pd.NA
    skate_merged["AtrasoMarcoAtual"] = pd.NA
    skate_merged["AtrasoMarcoProximo"] =  pd.NA


    for geracao in lista_geracao:
        for fase,marco in dict_fase_marco.items():
            for comer in lista_comer:
                mask_fase_atual = (skate_merged.SigTipoGeracao == geracao) & (skate_merged.FaseAtual == fase) & (skate_merged.DscComercializacaoEnergia == comer)
                skate_merged.loc[mask_fase_atual,"MarcoMedioAtual"] =  skate_merged.Dat_OC_obrigacao - dict_Media_Fase[geracao][fase][comer][0]
                if fase != "OT_OC":
                    proxima_fase = lista_fases[lista_fases.index(fase) + 1 ]
                    skate_merged.loc[mask_fase_atual,"MarcoMedioProximo"] =  skate_merged.Dat_OC_obrigacao - dict_Media_Fase[geracao][proxima_fase][comer][0]
                if (fase == "IO_OC") and ((geracao=="UFV") or (geracao=="UTE")):
                    # Usinas UFV e UTE não possuem fase CC_OC
                    skate_merged.loc[mask_fase_atual,"MarcoMedioProximo"] =  skate_merged.Dat_OC_obrigacao - dict_Media_Fase[geracao]["ME_OC"][comer][0]
                



    skate_merged.MarcoMedioAtual = pd.to_datetime(skate_merged.MarcoMedioAtual)
    skate_merged.MarcoMedioProximo = pd.to_datetime(skate_merged.MarcoMedioProximo)
    skate_merged.DataMarcoAtual = pd.to_datetime(skate_merged.DataMarcoAtual)



    skate_merged["AtrasoMarcoAtual"] =  skate_merged.DataMarcoAtual - skate_merged.MarcoMedioAtual
    skate_merged["AtrasoMarcoProximo"] =  hoje  + 40 * pd.to_timedelta(1, unit='D') - skate_merged.MarcoMedioProximo


    # Adiciona-se os coeficientes da reta ao dataframe
    # Muda-se nome das colunas para que se saiba se os coeficientes corresponda ao marco atual ou marco próximo

    dict_atual = {"a":"a_atual","b":"b_atual","Media_Fase":"Media_Fase_atual","Std_Fase":"Std_Fase_atual","Media_Atraso":"Media_Atraso_atual","Std_Atraso":"Std_Atraso_atual"}
    dict_proximo = {"a":"a_proximo","b":"b_proximo","Media_Fase":"Media_Fase_proximo","Std_Fase":"Std_Fase_proximo","Media_Atraso":"Media_Atraso_proximo","Std_Atraso":"Std_Atraso_proximo"}

    del escolhas["Comercialização"]

    skate_merged = pd.merge(skate_merged,
        escolhas.rename(columns=dict_atual),
        how="left",
        left_on=["SigTipoGeracao","FaseAtual"]   ,#"DscComercializacaoEnergia"],
        right_on = ["Tipo de geração","Fase"])    #,"Comercialização"])

    del skate_merged["Fase"]

    skate_merged = pd.merge(skate_merged,
        escolhas.rename(columns=dict_proximo),
        how="left",
        left_on=["SigTipoGeracao","ProximaFase"],#"DscComercializacaoEnergia"],
        right_on = ["Tipo de geração","Fase"])#"Comercialização"])

    del skate_merged["Fase"]



    # Calcula-se previsões OC baseadas no marco atual e próximo
    skate_merged["Previsao_OC_atual"] = skate_merged["Dat_OC_obrigacao"] + (skate_merged["AtrasoMarcoAtual"] * skate_merged["a_atual"]) + (skate_merged["b_atual"] * pd.to_timedelta(1, unit='D'))
    skate_merged["Previsao_OC_proximo"] = skate_merged["Dat_OC_obrigacao"] +( skate_merged["AtrasoMarcoProximo"] * skate_merged["a_proximo"]) + (skate_merged["b_proximo"] * pd.to_timedelta(1, unit='D'))


    # Define qual é a previsão OC definitiva
    # Por padrão é a previsão OC com base no marco atual
    skate_merged["Previsao_OC"] = skate_merged["Previsao_OC_atual"] 

    # Caso o atraso do próximo marco for maior que o atraso do marco atual, usa-se a previsão baseada no marco próximo
    mask_previsao_proximo = skate_merged.AtrasoMarcoProximo >=   skate_merged.AtrasoMarcoAtual
    skate_merged.loc[mask_previsao_proximo,"Previsao_OC"] = skate_merged["Previsao_OC_proximo"]


    indicador_atual = 100 * pd.Series(1 - norm.cdf((skate_merged.AtrasoMarcoAtual - skate_merged.Media_Atraso_atual)/skate_merged.Std_Atraso_atual))
    indicador_atual.index = skate_merged.index
    skate_merged["IndicadorAtual"] = indicador_atual

    indicador_proximo =  100 * pd.Series(1 - norm.cdf((skate_merged.AtrasoMarcoProximo - skate_merged.Media_Atraso_proximo)/skate_merged.Std_Atraso_proximo))
    indicador_proximo.index = skate_merged.index
    skate_merged["IndicadorProximo"] = indicador_proximo


    skate_merged["Indicador"] = skate_merged["IndicadorAtual"]
    skate_merged.loc[mask_previsao_proximo,"Indicador"] = skate_merged["IndicadorProximo"]


    # Após calculada a previsão, pode-se retornar a fase correta para aquelas usinas que estão em OUT_OC
    skate_merged.loc[skate_merged.FaseAtual==skate_merged.ProximaFase,"FaseAtual"] = "OUT_OC"
    skate_merged.loc[skate_merged.FaseAtual=="OUT_OC","Indicador"] = pd.NA


    skate_merged.loc[(skate_merged.FaseAtual=="OUT_OC"),'Indicador'] = skate_merged.IndicadorAtual
    skate_merged.loc[(skate_merged.FaseAtual=="OUT_OC"),'Previsao_OC'] = skate_merged.Previsao_OC_atual


    # Usinas que estão em teste e a previsão está no passado, muda previsão para os próximos 60 dias
    skate_merged.loc[(skate_merged.Previsao_OC < hoje) & (skate_merged.FaseAtual== "OT_OC"),"Previsao_OC"] = hoje + pd.Timedelta(60,"D")


    # Usinas cuja previsão OC esteja no passado e exista previsão OC para o próximo marco, usa-se a última
    mask_previsao_passado = (skate_merged.Previsao_OC < hoje) & (skate_merged.Previsao_OC_proximo.notna())
    skate_merged.loc[mask_previsao_passado,"Previsao_OC"] = skate_merged.Previsao_OC_proximo
    skate_merged.loc[mask_previsao_passado,"Indicador"] = skate_merged["IndicadorProximo"].astype(float)

    skate_merged["flagOPTeste30dias"] = 0
    skate_merged.loc[
                     skate_merged.DatLiberacaoSFGTeste.notna() &
                     ((hoje - skate_merged.DatLiberacaoSFGTeste) > pd.to_timedelta(30, unit='D'))
                        ,'flagOPTeste30dias'] = 1

    return("Ok",skate_merged)



def calcular_previsao(directory,previsoes_path,perguntar=False):
    cols_used = ['vmonitoramentoleilao', 'vmonitoramentoug', 'vmonitoramentousina']
    atualizar_db(directory,perguntar=perguntar)
    
    log = get_log_file(directory)
    file_name = 'Previsao_OC_' + get_standard_file_name(cols_used,log)
    file_name_path = os.path.join(previsoes_path,'Previsao_OC')
    file_name_excel = f'{file_name_path}.xlsx'
    file_name_parquet = f'{file_name_path}.gzip'
    necessario_calcular = True
    calculado_nessa_chamada = False

    #if((os.path.exists(file_name_parquet))):
    #   print("Previsão já calculada com os arquivos baixados anteriormente.")
    #    necessario_calcular = False

    if necessario_calcular:
        print("Calculando previsão...")
        result, skate_merged = previsor(directory)
        if result == "Ok":
            skate_merged.to_parquet(file_name_parquet, index=False,coerce_timestamps='ms',allow_truncated_timestamps= True)
            calculado_nessa_chamada = True
        else:
            pass
            # a implementar

    if perguntar:
        skate_export = skate_merged[['NomUsina', 'IdeUsinaOutorga', 'NumUgUsina', 'SigTipoGeracao',
        'Previsao_OC', 'Dat_OC_obrigacao', 'DatMonitoramento', 'FaseAtual', 'Indicador','flagOPTeste30dias','DatPrevisaoIniciobra','DatInicioObraOutorgado']]
        skate_export.to_excel(file_name_excel, index=False)
        
        print(f"Deseja exportar arquivo Excel detalhado?")

        options = {  0: "Não", 1: "Sim"}
        show_options(options)
        opcao_previsao = get_num(options)

        if opcao_previsao:
            print("Exportando arquivo...")
            if not calculado_nessa_chamada:
                skate_merged = pd.read_parquet(file_name_parquet)
            if opcao_previsao == 1:
                skate_merged.to_excel(file_name_excel, index=False)
            perguntar_abrir_pasta(previsoes_path)
    
    return file_name_parquet


        

        




if __name__ == "__main__":

    # Muda working directory
    change_2_script_dir()

    # Baixa informações do Skate. Caso já tenha sido baixado, informe a data em que foi baixado no formato yyyy_mm_dd. Por exemplo: 2022_08_17
    print(" Baixando arquivos ".center(60,"*") + "\n")

    hoje =  pd.to_datetime(date.today())
    hoje_str = hoje.strftime(r'%Y_%m_%d')

    directory = download_db(force_download=False, lista_download=["vmonitoramentoleilao","vmonitoramentoug" ,"vmonitoramentousina"])

    result,df = calcular_previsao(directory)
    if result == "Ok":
        print(df)
