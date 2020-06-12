from pymongo import MongoClient
from datetime import datetime
import argparse
import os
import json
import pandas as pd
import time

def printProgressBar (iteration, total, prefix = '', suffix = '', decimals = 1, length = 100, fill = '█', printEnd = "\r"):
    """
    Chiamato nel ciclo per stampare barra di progressione
    @params:
        iteration   - Required  : iterazione corrente (Int)
        total       - Required  : iterazioini totali (Int)
        prefix      - Optional  : prefisso stringa (Str)
        suffix      - Optional  : suffisso string (Str)
        decimals    - Optional  : numero di cifre decimali nella percentuale (Int)
        length      - Optional  : lunghezza della barra (Int)
        fill        - Optional  : carattere riempitore della barra (Str)
        printEnd    - Optional  : carattere di chiusura (e.g. "\r", "\r\n") (Str)
    """
    percent = ("{0:." + str(decimals) + "f}").format(100 * (iteration / float(total)))
    filledLength = int(length * iteration // total)
    bar = fill * filledLength + '-' * (length - filledLength)
    print('\r%s |%s| %s%% %s' % (prefix, bar, percent, suffix), end = printEnd)
    # stampa una linea nuova al termine
    if iteration == total: 
        print()



def list_directory(data):
    '''
    Estrae lista di directory nelle quali ho i file json
    @params:
        data:   - Required   : cartella di sottocartelle di file json
    '''
    l = []
    print(data)
    for x in os.listdir(data + "\\"):
        l.append(x)
    return l

def merge_videos(videos, covid):
    '''
    Funzione di merge tra 
    @params:
        videos:     Youtube videos (lista dizionari)
        covid:      covid data (dataframe)

    '''
    # init barra di progressione
    tot = len(videos)
    printProgressBar(0, tot, prefix = 'Progress:', suffix = 'Complete', length = 50)

    for  i, video in enumerate(videos): #passa tutti i video
        date = video["trending_date"]
        c_name = video["country_name"]
        # cerca in covid date e c_name del video corrente
        row = covid.loc[(covid["date"] == date) & (covid["location"] == c_name)]
        # estrae le informazioni covid di interesse
        # le immagazzina in un dizionario
        d = {
            "cases_tot" : int(row["total_cases"].values[0]),
            "cases_new" : int(row["new_cases"].values[0]),
            "deaths_tot" : int(row["total_deaths"].values[0]),
            "deaths_new" : int(row["new_deaths"].values[0]),
            "country_population" : int(row["population"].values[0])
            }
        # aggiunge il dizionario al video
        video["covid"] = d
        
        # Update Progress Bar
        printProgressBar(i + 1, tot, prefix = 'Progress:', suffix = 'Complete', length = 50)

def convert_types(dict) :
    '''
    Converte i tipi di timestamp e dati integer
    @params:
        dict:   dizionario di un singolo video
    '''
    dict["timestamp"] = datetime.strptime(dict["timestamp"], "%m-%d-%Y %H:%M")
    dict["statistics"]["view_count"] = int(dict["statistics"]["view_count"]) 
    dict["statistics"]["likes"] = int(dict["statistics"]["likes"]) 
    dict["statistics"]["dislikes"] = int(dict["statistics"]["dislikes"]) 
    dict["statistics"]["comment_count"] = int(dict["statistics"]["comment_count"]) 


if __name__ == '__main__':
    '''
    @params:
        -d: directory di directories dei json file
        -dc: path file dati covid
        -u: mongoDB user
        -p: mongoDB password
        -port: mongoDB porta in cui comunica
        -db: nome database mongodb
        -c: nome collezione mongodb in cui immagazzinare i dati
    '''
    # start timer
    time_start = time.clock()
    parser = argparse.ArgumentParser()
    parser.add_argument('-d', '--data', type=str, required=False, help="Inserire la directory da cui ho directories json", default="data")
    parser.add_argument('-dc', '--data_covid', type=str, required=False, help="Inserire il path del .csv covid", default="covid_data.csv")
    parser.add_argument('-u', '--user', type=str, required=False, help="Inserire l'user mongodb", default="")
    parser.add_argument('-p', '--password', type=str, required=False, help="Inserire la password mongodb", default="")
    parser.add_argument('-port', '--port', type=str, required=False, help="Inserire la porta con cui dialogare con mongodb", default=27017)
    parser.add_argument('-db', '--database', type=str, required=False, help="Inserire il database mongo di output", default="YT_data")
    parser.add_argument('-c', '--collection', type=str, required=False, help="Inserire la collection in cui immagazzinare i dati", default="video")
    
    args = parser.parse_args()
    try:
        dir_main = args.data
        l_dir = list_directory(dir_main) # lista di directory da cui prendo i file mongo
        print(l_dir)
    except:
        print("error json directory path")
        exit()

    try:
        user = args.user
        password = args.password
        # definizione del client mongo da utilizzare
        client = MongoClient('localhost', args.port, username = user, password = password)
        db = client[args.database] # connessione al db
        col = db[args.collection] # connessione alla collection
    except:
        print("error mongo connection")
        exit()
    
    try:
        df_covid = pd.read_csv(args.data_covid) # lettura dati covid
    except:
        print("error covid data path")
        exit()

    list_videos = [] # lista in cui salvo i video da caricare su mongo

    for directory in l_dir: #per ogni cartella
        path = dir_main + "\\" + directory + "\\"
        files = os.listdir(path)
        
        for file in files: #per ogni file json nella cartella
            if file.endswith('.json'):
                # apre il file se .json
                with open(path + file, "r") as read_file:
                    j_file = json.load(read_file)
                # converte i tipi di dati utili su mongo
                for d in j_file:
                    convert_types(d)
                # appende alla lista i dizionari dei video del json
                list_videos.extend(j_file) # aggiunge gli elementi alla lista
        print("upload files")
        print(directory + " merging ...")
        # merging function
        merge_videos(list_videos, df_covid) 
        print("merge_fatto")
        # carico la lista di dizionari nella collezione del database mongo specificate
        col.insert_many(list_videos)
        list_videos = []
        print("directory " + directory + " correctly uploaded on mongoDB")

    print("files json correctly uploaded on mongoDB !!!")
    time_elapsed = (time.clock() - time_start)
    print("computation time: " + str(time_elapsed))