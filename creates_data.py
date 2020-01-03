# Script per importare tutti i dati come dataframe una volta data una directory.

import pandas as pd 
import argparse
import numpy as np
import  os


#   Inizio con l'aprire tutti i dati presenti con nella directory data:
parser = argparse.ArgumentParser()
parser.add_argument('-d', '--data', type=str, required=False, help="Inserire la directory da cui prendere i dati", default="data")


args = parser.parse_args()
data = os.chdir(args.data)

# Mi occupo ora di importarli nella maniera corretta in pandas. Per prima cosa mette tutti i file in una lista e poi li importa
# come dataframe pandas. Questo funziona bene quando ho una directory sola.
'''
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
files_csv = []
dfs = []
dyr = os.listdir(data)


for directory in dyr:
    files = os.listdir(directory)
    for i in files:
        if i.endswith('.csv'):
            files_csv.append(i)
            #dfs.append(pd.read_csv(i))
            #df = pd.concat(dfs)  

print(files_csv)
