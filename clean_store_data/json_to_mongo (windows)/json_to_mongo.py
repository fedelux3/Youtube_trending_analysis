'''
File che carica i json in MongoDB
@params:
    -d: directory di directories dei json file
    -u: mongoDB user
    -p: mongoDB password
    -port: mongoDB porta in cui comunica
    -db: nome database mongodb
    -c: nome collezione mongodb in cui immagazzinare i dati
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
    @return:
    	l: lista di directory
    '''
    l = []
    print(data)
    for x in os.listdir(data + "\\"):
        l.append(x)
        
    return l

def convert_types(dict) :
    '''
    Converte il formato dei timestamp e i dati in integer
    @params:
        dict:   dizionario di un singolo video
    '''
    dict["timestamp"] = datetime.strptime(dict["timestamp"], "%m-%d-%Y %H:%M")
    dict["statistics"]["view_count"] = int(dict["statistics"]["view_count"]) 
    dict["statistics"]["likes"] = int(dict["statistics"]["likes"]) 
    dict["statistics"]["dislikes"] = int(dict["statistics"]["dislikes"]) 
    dict["statistics"]["comment_count"] = int(dict["statistics"]["comment_count"]) 

if __name__ == '__main__':
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
        list_videos = [] # Lista in cui vengono salvati i video da caricare su mongo
        l_dir = list_directory(dir_main) # Lista di directory da cui vengono presi i file mongo
        print(l_dir)
    except:
        print("error json directory path")
        exit()
    
    try:
        user = args.user
        password = args.password
        # Definizione del client mongo da utilizzare
        client = MongoClient('localhost', int(args.port), username = args.user, password = args.password)
        db = client[args.database] # Connessione al db
        col = db[args.collection] # Connesione alla collection
    except:
        print("error mongo connection")
        exit()

    for directory in l_dir: # Per ogni cartella nella lista
        path = dir_main + "\\" + directory + "\\"
        files = os.listdir(path)
        
        for file in files: # Per ogni file json nella cartella
            # Apertura del file solo se in formato json
            if file.endswith('.json'):
                with open(path + file, "r") as read_file:
                    j_file = json.load(read_file)
                # Conversione dei formati dei dati attraverso la funzione convert_types
                for d in j_file:
                    convert_types(d)
                # Aggiunta alla lista dei dizionari con i video dentro al json
                list_videos.extend(j_file) 
        # Caricamento della lista di dizionari nella collezione del database mongo specificato
        col.insert_many(list_videos)
        list_videos = []
        print("directory " + directory + " correctly uploaded on mongoDB")

    print("files json correctly uploaded on mongoDB !!!")
