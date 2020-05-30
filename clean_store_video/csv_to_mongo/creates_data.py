# Script per importare tutti i dati come dataframe una volta data una directory.
import pandas as pd 
import numpy as np
import os, json
from fix_datasets import delete
from fix_datasets import date_columns
from fix_datasets import add_country
from fix_datasets import fix_timestamp
from fix_datasets import add_country_name
from fix_datasets import add_category_name
# questa funzione pulisce i dati e restituisce il dataframe concatenato corretto (input la directory dei file)
def conc_dataframe(directory):
    files_csv = []
    dfl = [] #lista dei dataset da mergiare
    #carico il dizionario dei nomi dei paesi con i fusi orario
    with open("country_names.json", "r") as read_file:
        c_gmt = json.load(read_file)
    with open("category_id.json", "r") as read_file:
        categorys = json.load(read_file)['items']
    
    
    #data = os.chdir(directory)
    files = os.listdir(directory)

    for file in files:
        if file.endswith('.csv'):
            print(file)
            files_csv.append(file)
            dfs = pd.read_csv(directory + "\\" + file) #dataset che sto pulendo
            dfs_fixed = delete(dfs) #elimino le intestazioni sbagliate
            add_country(dfs_fixed,file) #aggiungo colonna del country_code
            fix_timestamp(dfs_fixed, c_gmt) #sistemo il timestamp in base al fuso orario
            add_country_name(dfs_fixed, c_gmt) #aggiungo i nomi dei paesi
            add_category_name(dfs_fixed, categorys) #aggiungo i nomi delle categorie
            ########################################################################
            #date_columns(dfs_fixed) #aggiungo le colonne delle date NON SERVONO
            ########################################################################
            #se dobbiamo fare qualcosa prima del merge gigante
            #AGGIUNGI QUI :-) (btw occhio agli slash)
            dfl.append(dfs_fixed)
            df = pd.concat(dfl)  
    
    #os.chdir("..")
    #print(df)
    return df
    #salvo il dataframe risultate nel database mongoDB
    # Salva il dataframe risultanete i un csv salvato in una cartella di output.
    #df.to_csv('merge.csv')
    