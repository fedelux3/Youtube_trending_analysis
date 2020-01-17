from datetime import datetime, timedelta
import pandas as pd 
import json
import os
#Script per eliminare le intestazioni ridondanti all'interno dei csv
#scrivere nella variabile in fondo al codice (variabile dir) il nome della directory
#il software genera una cartella fixed all'interno della stessa con i files sistemati

#elimina le righe di intestazione sbagliate
def delete(df): 
    df1 = df.loc[df['timestamp'] == 'timestamp']
    df2 = df.drop(df.index[df1.index])
    return df2
    #df2[df2['timestamp'] == 'timestamp']

#ricava il codice del country dal nome file
def add_country(df, nameFile):
    code = nameFile[11:13] #DA MIGLIORARE
    df['country_code'] = code

#sistema una riga del timestamp
def row_timestamp(row, c_gmt):
    fuso = c_gmt[str(row['country_code'])]['GMT']
    dt = datetime.strptime(row['timestamp'], "%d-%m-%Y %H:%M")
    dt_new = dt + timedelta(hours=fuso)
    row['timestamp'] = dt_new.strftime("%m-%d-%Y %H:%M") #riscrivo in modo americano per tableau
    
# corregge i timestamp in base ai fusi orario
# NB fare solo dopo aver aggiunto il country_code
def fix_timestamp(df, c_gmt):
    for index, row in df.iterrows():
        row_timestamp(row, c_gmt)
        
#aggiunge le colonne dei componenti della date
def date_columns(df):
    df['day'] = df['timestamp'].apply(lambda x: x[0:2])
    df['month'] = df['timestamp'].apply(lambda x: x[3:5])
    df['year'] = df['timestamp'].apply(lambda x: x[6:10])
    df['hour'] = df['timestamp'].apply(lambda x: x[11:13])
    df['min'] = df['timestamp'].apply(lambda x: x[14:16])
    df.drop('timestamp', axis = 1)

#prende tutti i file nella cartella e gli fa eseguire il delete
def delete_dir(dir):
    files = os.listdir('/' + dir)
    dir_output = dir + 'fixed\\'
    os.mkdir(dir_output)

    for file in files:
        df = pd.read_csv(dir + file)
        df_fixed = delete(df)
        df_fixed.to_csv(dir_output + file, index = False)
        print("funziona")

#prende tutti i file nella cartella e gli fa eseguire le funzioni per fixare
def fix_dir(dir):
    files = os.listdir('/' + dir)
    dir_output = dir + 'fixed\\'
    os.mkdir(dir_output)

    for file in files:
        df = pd.read_csv(dir + file)
        df_fixed = delete(df)
        date_columns(df_fixed)
        df_fixed.to_csv(dir_output + file, index = False)
        print("fatto")



