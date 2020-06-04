from datetime import datetime, timedelta
import pandas as pd
import json
import os

def tag_list(string):
    '''
    Converte la stringa dei tags (separati da "|") in una lista
    @params:
        string:     stringa dei tags
    @return:
        t_lista:    lista dei tags
    '''
    t_list = string.split("|")
    return t_list

def statistics_to_dict(row):
    '''
    Formatta le informazioni statistiche in un dizionario che ritorna
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
    # trasforma la stringa tag in una lista
    line['tags'] = tag_list(row['tags']) 
    # struttura le informazioni stastistiche in un dizionario innestato
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
    # per ogni riga del dataframe applico la formattazione
    for index, row in df.iterrows():
        out_dict.append(row_to_dict(row))
    
    return out_dict
    #return out_dict

def delete(df): 
    '''
    Eliminazione righe di intestazione sbagliate
    @params:
        df: dataframe da pulire
    '''
    df1 = df.loc[df['timestamp'] == 'timestamp']
    df2 = df.drop(df.index[df1.index])
    return df2

def add_country(df, nameFile):
    '''
    Aggiunta colonna country_code dal nome file
    @params:
        df:         dataframe di input
        nameFile:   nome del file csv
    '''
    code = nameFile[11:13] #DA MIGLIORARE
    df['country_code'] = code

def add_category_name(df, categorys):
    '''
    Aggiunge colonna category_name con nome categoria per esteso
    @params:
        df:     dataframe
        categorys:  dizionario con codici e nomi estesi di categorie
    '''
    # basandosi sul categoryId estraggo nome per esteso della categoria di video
    df['category_name'] = df['categoryId'].apply(lambda  x : row_category(x,categorys))

def row_category(cat, categorys):
    '''
    Ricerca la categoria in input
    @params:
        cat:    codice categoria da ricercare
        categorys:  elenco codice e nome categoria
    '''
    for category in categorys:
        if category['id'] == str(cat):
            return category['snippet']['title']

def row_timestamp(row, c_gmt):
    '''
    Correzione timestamp di una riga
    @params:
        row:    riga da correggere (series)
        c_gmt:  dizionario country e fusi orario
    '''
    # estraggo fuso orario del country della row corrente
    fuso = c_gmt[str(row['country_code'])]['GMT']
    # conversione del timestamp in datetime
    dt = datetime.strptime(row['timestamp'], "%d-%m-%Y %H:%M")
    # calcolo nuovo fuso orario tramite timedelta
    dt_new = dt + timedelta(hours=fuso)
    # sostituisco nuovo timestamp in formato americano per favorire software di
    # visualizzazione tableau
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
    Aggiunge colonna country_name con nome paese per esteso
    @params:
        df:     dataframe
        c_gmt:  dizionario con paesi e fusi orario
    '''
    # basandosi sul country code estraggo nome per esteso del paese
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
    dfl = [] # lista dei dataset da mergiare

    # carica il dizionario dei nomi dei paesi con i fusi orario
    with open("country_names.json", "r") as read_file:
        c_gmt = json.load(read_file)
    # carica i nomi delle categorie di video associate ai codici
    with open("category_id.json", "r") as read_file:
        categorys = json.load(read_file)['items']
    # elecon dei file
    files = os.listdir(directory)

    # per ogni file csv in elenco esegui le operazioni di pulizia
    for file in files:
        if file.endswith('.csv'):
            print(file)
            #files_csv.append(file)
            # caricamento dataframe
            dfs = pd.read_csv(directory + "\\" + file) 
            # eliminazione delle intestazioni sbagliate
            dfs_fixed = delete(dfs) 
            # aggiunta colonna country code
            add_country(dfs_fixed,file)
            # sistemazione dei timestamp in base al fuso orario
            fix_timestamp(dfs_fixed, c_gmt) 
            # aggiunta colonna country name
            add_country_name(dfs_fixed, c_gmt)
            # aggiunta nome categorie
            add_category_name(dfs_fixed, categorys) 
            
            # concatenazione
            dfl.append(dfs_fixed)
            df = pd.concat(dfl)  
    
    return df

