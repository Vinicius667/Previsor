import os, sys
from utils import *
from download_DB import download_db
import glob
import pandas as pd
import numpy as np

from datetime import date,datetime,timedelta
hoje =  pd.to_datetime(date.today() + timedelta(days=25)) 
final_do_mes = pd.to_datetime(datetime(hoje.year,hoje.month+1,1) - timedelta(seconds=1))

# Lista de colunas que serão usadas várias vezes para a realização de merges entre os dataframes
list_id_data = ['DthEnvio','IdeUsinaOutorga']
list_id_ug = ['IdeUsinaOutorga','NumOperacaoUg']

def biu(directory):
    pass