from csv_to_json import df_to_listdict
#from fix_datasets import conc_dataframe
from csv_to_json import conc_dataframe
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
    Conversione dei file csv in input in file json immagazzinati nella cartella 
    "json" (la crea se non presente)
    @params:
        -d: directory di directorys dei file csv
    '''
    parser = argparse.ArgumentParser()
    parser.add_argument('-d', '--data', type=str, required=False, help="Inserire la directory da cui prendere i dati", default="data")
    
    args = parser.parse_args()
    
    try:
        l_dir = list_directory(args.data) #lista di directory da inserire
        print(l_dir)
    except:
        print("error import csv" + str(args.data))
        exit()

    # per ogni directory 
    for i in l_dir:
        print(i)
        # concatena i vari csv pulendoli
        df = conc_dataframe(args.data + '\\' + i)
        print("dataframe cleaned and merged")
        # nel caso in cui non venga generato alcun dataframe
        if df is None:
            print("df none")
            continue
        # trasforma il dataframe in lista di dizionari
        l_dict = df_to_listdict(df) 
        # salvataggio dei dizionari in un file json nella cartella json
        n_json = i + '.json'
        # se la cartella json non esiste la crea
        if not os.path.exists("json"):
            os.makedirs("json")
        with open('json\\' + n_json, "w") as file_json:
            json.dump(l_dict, file_json, skipkeys = True)

        print("dataframe correctly converted to json")
        
