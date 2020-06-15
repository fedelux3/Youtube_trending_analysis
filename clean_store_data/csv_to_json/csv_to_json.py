from datetime import datetime, timedelta
import pandas as pd
import json
import os
import re

def tag_list(string):
    '''
    Converte la stringa dei tag (separati da "|") in una lista
    @params:
        string:     stringa dei tag
    @return:
        t_lista:    lista dei tag
    '''
    t_list = string.split("|")
    return t_list

def statistics_to_dict(row):
    '''
    Formatta le informazioni statistiche in un dizionario in output
    @params:
        row:    riga del dataset
    @return:
        stats:  dizionario in cui ho le informazioni statistiche dei video
    '''
    stats = {}
    stats["view_count"] = row["view_count"]
    stats["likes"] = row["likes"]
    stats["dislikes"] = row["dislikes"]
    stats["comment_count"] = row["comment_count"]
    return stats

def row_to_dict(row):
    '''
    Converte una riga del dataframe in un dizionario ben formattato
    @params:
        row:    riga del dataframe
    '''
    line = {}
    line['video_id'] = row['video_id'] 
    line['timestamp'] = row['timestamp']
    line['country_code'] = row['country_code'] 
    line['country_name'] = row['country_name'] 
    line['title'] = row['title']
    line['publishedAt'] = row['publishedAt']
    line['channelId'] = row['channelId']
    line['channelTitle'] = row['channelTitle']
    line['categoryId'] = row['categoryId']
    line['category_name'] = row['category_name']
    line['trending_date'] = row['trending_date']
    # Trasforma la stringa tag in una lista attraverso la funzione tag_list
    line['tags'] = tag_list(row['tags']) 
    # Struttura le informazioni statistiche in un dizionario nidificato attraverso la funzione statistic_to_dict
    line['statistics'] = statistics_to_dict(row)
    line['thumbnail_link'] = row['thumbnail_link']
    line['description'] = row['description']
    return line

def df_to_listdict(df):
    '''
    Converte il dataframe in input in una lista di dizionari formattati
    @params:
        df:     dataframe da convertire
    @return:
        out_dict:   lista di dizionari
    '''
    out_dict = [] 
    # Per ogni riga del dataframe applico la formattazione
    for index, row in df.iterrows():
        out_dict.append(row_to_dict(row))
    
    # Output una lista di dizionari
    return out_dict
    

def delete(df): 
    '''
    Eliminazione righe di intestazione errate
    @params:
        df: dataframe da correggere
    '''
    #Eliminazione delle righe con timestamp uguale alla stringa timestamp
    df1 = df.loc[df['timestamp'] == 'timestamp']
    df2 = df.drop(df.index[df1.index])
    return df2

def add_country(df, nameFile):
    '''
    Aggiunta colonna country_code ricavata dal nome del file
    @params:
        df:         dataframe di input
        nameFile:   nome del file csv
    '''
    r = re.search("_(\D{2})_", nameFile)
    code = r.group(1)
    df['country_code'] = code

def add_category_name(df, categories):
    '''
    Aggiunge colonna category_name con nome della categoria per esteso
    @params:
        df:     dataframe
        categories:  dizionario con id e nomi estesi delle categorie
    '''
    # Basandosi sul categoryId estrae il nome per esteso della categoria del video
    df['category_name'] = df['categoryId'].apply(lambda  x : row_category(x,categories))

def row_category(cat, categories):
    '''
    Ricerca la categoria in input
    @params:
        cat:    codice categoria da ricercare
        categories:  elenco id e nomi categorie
    '''
    for category in categories:
        if category['id'] == str(cat):
            return category['snippet']['title']

def row_timestamp(row, c_gmt):
    '''
    Correzione del timestamp di una riga
    @params:
        row:    riga da correggere (series)
        c_gmt:  dizionario con paesi e fusi orari
    '''
    # Estrazione del fuso orario del paese dalla row corrente
    fuso = c_gmt[str(row['country_code'])]['GMT']
    # Conversione del timestamp in datetime
    dt = datetime.strptime(row['timestamp'], "%d-%m-%Y %H:%M")
    # calcolo nuovo fuso orario tramite timedelta
    dt_new = dt + timedelta(hours=fuso)
    # Sostituisco con il nuovo timestamp in formato americano per facilitare il software di
    # visualizzazione Tableau
    row['timestamp'] = dt_new.strftime("%m-%d-%Y %H:%M") 

def fix_timestamp(df, c_gmt):
    '''
    Correzione timestamp in base al fuso orario
    @params:
        df:     dataframe
        c_gmt:  dizionario country e fusi orario
    '''
    for index, row in df.iterrows():
        row_timestamp(row, c_gmt)

def add_country_name(df, c_gmt):
    '''
    Aggiunge colonna country_name con nome del paese per esteso
    @params:
        df:     dataframe
        c_gmt:  dizionario con paesi e fusi orari
    '''
    # Basandosi sul country code estrae nome per esteso del paese
    df['country_name'] = df['country_code'].apply(lambda x: c_gmt[str(x)]['name']) #aggiungo il nome del paese per esteso

def conc_dataframe(directory):
    '''
    Pulizia dei dati presenti nei csv e concatenazione in un dataframe
    @params:
        directory:  path in cui prendere i file csv da concatenare
    @return:
        df:         dataframe concatenato e pulito
    '''
    #files_csv = [] # lista file csv
    dfl = [] # lista dei dataset per il merge

    # Carica il dizionario dei nomi dei paesi con i fusi orario
    with open("country_names.json", "r") as read_file:
        c_gmt = json.load(read_file)
    # Carica i nomi delle categorie di video associate ai codici
    with open("category_id.json", "r") as read_file:
        categories = json.load(read_file)['items']
    # Elenco dei file
    files = os.listdir(directory)

    # Per ogni file csv in elenco esegue le operazioni di pulizia
    for file in files:
        if file.endswith('.csv'):
            print(file)
            #files_csv.append(file)
            # Caricamento dataframe
            dfs = pd.read_csv(directory + "\\" + file) 
            # Eliminazione delle intestazioni sbagliate
            dfs_fixed = delete(dfs) 
            # Aggiunta colonna country code
            add_country(dfs_fixed,file)
            # Sistemazione dei timestamp in base al fuso orario
            fix_timestamp(dfs_fixed, c_gmt) 
            # Aggiunta colonna country name
            add_country_name(dfs_fixed, c_gmt)
            # Aggiunta nome categorie
            add_category_name(dfs_fixed, categories) 
            
            # Concatenazione
            dfl.append(dfs_fixed)
            df = pd.concat(dfl)  
    
    return df

