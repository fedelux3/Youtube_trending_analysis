# Script per importare tutti i dati come dataframe una volta data una directory.

import pandas as pd 
import argparse
import numpy as np
import  os
from fix_datasets import delete
from fix_datasets import date_columns

#   Inizio con l'aprire tutti i dati presenti con nella directory data:
parser = argparse.ArgumentParser()
parser.add_argument('-d', '--data', type=str, required=False, help="Inserire la directory da cui prendere i dati", default="data")


args = parser.parse_args()
data = os.chdir(args.data)

# Mi occupo ora di importarli nella maniera corretta in pandas. Per prima cosa mette tutti i file in una lista e poi li importa
# come dataframe pandas. Questo funziona bene quando ho una directory sola.
'''
def conc_dataframe():
    files = os.listdir(data)

    # Uso la lista dfs per salvare i data frame pandas, una volta messi dentro posso concatenarli.
    dfs = []
    files_csv = [i for i in files if i.endswith('.csv')]


    for file in files_csv:
        dfs.append(pd.read_csv(file))  

    #   Adesso faccio la concatenazione dei dataframe presenti nella lista
    df = pd.concat(dfs)
'''

# Provo a fare la stessa cosa ma avendo file in più directory, non capisco perchè ma non trova i file quando devono essere concatenati
def conc_dataframe():
    files_csv = []
    dfl = [] #lista dei dataset da mergiare
    dyr = os.listdir(data)


    for directory in dyr:
        files = os.listdir(directory)
        for i in files:
            if i.endswith('.csv'):
                files_csv.append(i)
                dfs = pd.read_csv(directory + "\\" + i) #dataset che sto pulendo
                dfs_fixed = delete(dfs) #elimino le intestazioni sbagliate
                date_columns(dfs_fixed) #aggiungo le colonne delle date
                #se dobbiamo fare qualcosa prima del merge gigante
                #AGGIUNGI QUI :-) (btw occhio agli slash)
                dfl.append(dfs_fixed)
                df = pd.concat(dfl)  

    print(files_csv)
    print(df.tail())
