'''
File che carica i json in MongoDB
'''

from pymongo import MongoClient
import argparse
import os
import json

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

if __name__ == '__main__':
    '''
    @params:
        -d: directory di directories dei json file
        -u: mongoDB user
        -p: mongoDB password
        -port: mongoDB porta in cui comunica
        -db: nome database mongodb
        -c: nome collezione mongodb in cui immagazzinare i dati
    '''
    parser = argparse.ArgumentParser()
    parser.add_argument('-d', '--data', type=str, required=False, help="Inserire la directory da cui prendere i dati", default="data")
    parser.add_argument('-u', '--user', type=str, required=False, help="Inserire l'user mongodb", default="")
    parser.add_argument('-p', '--password', type=str, required=False, help="Inserire la password mongodb", default="")
    parser.add_argument('-port', '--port', type=str, required=False, help="Inserire la porta con cui dialogare con mongodb", default=27017)
    parser.add_argument('-db', '--database', type=str, required=False, help="Inserire il database mongo di output", default="YT_data")
    parser.add_argument('-c', '--collection', type=str, required=False, help="Inserire la mongodb di output", default="video")
    
    args = parser.parse_args()
    try:
        dir_main = args.data
        list_videos = [] # lista in cui salva i video da caricare su mongo
        l_dir = list_directory(dir_main) # lista di directory da cui prende i file mongo
        print(l_dir)
    except:
        print("error json directory path")
        exit()
    
    try:
        user = args.user
        password = args.password
        # definizione del client mongo da utilizzare
        client = MongoClient('localhost', args.port, username = args.user, password = args.password)
        db = client[args.database] # connessione al db
        col = db[args.collection] # connesione alla collection
    except:
        print("error mongo connection")
        exit()

    for directory in l_dir: #per ogni cartella nella lista
        path = dir_main + "\\" + directory + "\\"
        files = os.listdir(path)
        
        for file in files: #per ogni file json nella cartella
            # apre il file se .json
            if file.endswith('.json'):
                with open(path + file, "r") as read_file:
                    j_file = json.load(read_file)
                # appende alla lista i dizionari dei video del json
                list_videos.extend(j_file) 
        # carica la lista di dizionari nella collezione del database mongo specificate
        col.insert_many(list_videos)
        list_videos = []
        print("directory " + directory + " correctly uploaded on mongoDB")

    print("files json correctly uploaded on mongoDB !!!")
