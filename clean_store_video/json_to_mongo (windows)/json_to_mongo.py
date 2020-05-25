from pymongo import MongoClient
import argparse
import os
import json

def list_directory(data):
    l = []
    print(data)
    for x in os.listdir(data + "\\"):
        l.append(x)
            #print(x)
    return l

if __name__ == '__main__':
    # ottengo la directory in cui applicare la ricerca
    parser = argparse.ArgumentParser()
    parser.add_argument('-d', '--data', type=str, required=False, help="Inserire la directory da cui prendere i dati", default="data")
    parser.add_argument('-u', '--user', type=str, required=False, help="Inserire la directory da cui prendere i dati", default="")
    parser.add_argument('-p', '--password', type=str, required=False, help="Inserire la directory da cui prendere i dati", default="")
    parser.add_argument('-port', '--port', type=str, required=False, help="Inserire la directory da cui prendere i dati", default=27017)
    parser.add_argument('-db', '--database', type=str, required=False, help="Inserire la directory da cui prendere i dati", default="YT_data")
    parser.add_argument('-c', '--collection', type=str, required=False, help="Inserire la directory da cui prendere i dati", default="videos_march")
    
    args = parser.parse_args()
    dir_main = args.data
    # definizione del client mongo da utilizzare
    client = MongoClient('localhost', args.port, username = args.user, password = args.password)
    db = client[args.database]
    col = db[args.collection]
    
    list_videos = [] # lista in cui salvo i video da caricare su mongo
    l_dir = list_directory(dir_main) # lista di directory da cui prendo i file mongo
    print(l_dir)

    for directory in l_dir: #per ogni cartella
        path = dir_main + "\\" + directory + "\\"
        files = os.listdir(path)
        
        for file in files: #per ogni file json nella cartella
            if file.endswith('.json'):

                with open(path + file, "r") as read_file:
                    j_file = json.load(read_file)
                
                list_videos.extend(j_file) # aggiunge gli elementi alla lista
        ##############################################
        #merge_videos(list_videos, covid)
        #############################################
        # carico la lista di dizionari nella collezione del database mongo specificate
        col.insert_many(list_videos)
        list_videos = []
        print("directory " + directory + " correctly uploaded on mongoDB")

    print("files json correctly uploaded on mongoDB !!!")
