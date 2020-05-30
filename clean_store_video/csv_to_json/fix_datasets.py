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

# aggiunge colonna nome categoria
def add_category_name(df, categorys):
    df['category_name'] = df['categoryId'].apply(lambda  x : row_category(x,categorys))

# aggiunge riga nome categoria
def row_category(cat, categorys):
    for category in categorys:
        #print(category['id'])
        #print(cat)
        if category['id'] == str(cat):
            return category['snippet']['title']
            #break

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

# aggiunge colonna del nome country
def add_country_name(df, c_gmt):
    df['country_name'] = df['country_code'].apply(lambda x: c_gmt[str(x)]['name']) #aggiungo il nome del paese per esteso

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

