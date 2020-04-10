from creates_data import conc_dataframe
from csv_to_mongo import df_to_mongo
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
    parser.add_argument('-c', '--collection', type=str, required=False, help="Inserire la directory da cui prendere i dati", default="videos")
    
    args = parser.parse_args()
    
    l_dir = list_directory(args.data) #lista di directory da inserire
    print(l_dir)

    for i in l_dir:
    # pulisco i dati e raggruppo in un unico dataframe
    ####################################################################
    # ATTENZIONE A NOME FILE IN INPUT ##################################
        df = conc_dataframe(args.data + '\\' + i)
        print("dataframe cleaned and merged")

        if df is None:
            continue
    # carico il dataframe nella collezione del database mongo specificate
    # definizione del client mongo da utilizzare
        client = MongoClient('localhost', args.port, username = args.user, password = args.password)
        db = client[args.database]
        col = db[args.collection]
        
        df_to_mongo(df, col)
        #salvataggio del file json NON FUNZIONA
        #n_json = i + '.json'
        #with open('json\\' + n_json, "w+") as file_json:
        #    json.dump(dic, file_json, skipkeys = True)
        #    print("json creato")

        print("dataframe correctly uploaded on mongoDB")
