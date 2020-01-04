import pandas as pd
import json
import os
import argparse

#inserire il path dove trovo il file csv da analizzare
parser = argparse.ArgumentParser()
parser.add_argument('-d', '--data', type=str, required=False, help="Inserire il path da cui prendere i dati", default="data")

args = parser.parse_args()

#funzione che dato un record restituisce i tag in una lista
def tag_list(row):
    t_list = str(row['tags']).split("|")
    return t_list

#ritorna il dizionario relativo ai dati temporali
def dict_date(row):
    return {"year": row["year"], "month": row["month"], "day": row["day"], "hour": row["hour"], "count": 1}

#inserisce i dati di un nuovo tag (valutato prima se è nuovo o meno)
def insert_new(row, tag, tags):
    date = dict_date(row)
    times = [date]
    category = [row["categoryId"]]
    count_tot = 1
    
    field = {
        "times" : times,
        "category" : category,
        "count_tot" : count_tot
    }
    tags[tag] = field

#inserisce la nuova data nel campo times oppure aggiorna il count
#sicuramente c'è un modo più elegante per farlo
def update_times(row, field_old):
    times = field_old["times"]
    year = row["year"]
    month = row["month"]
    day = row["day"]
    hour = row["hour"]
    for e in times:
        if e["year"] == year and e["month"] == month and e["day"] == day and e["hour"] == hour:
            e["count"] += 1
            return
    field_old["times"].append(dict_date(row)) #creo la nuova data nella lista

def update_category(row, field_old):
    category_old = field_old["category"]
    category_new = row["categoryId"]
    if category_new not in category_old:
        field_old["category"].append(category_new)
        field_old["category"].sort()

#inserisce i dati in un tag già esistente
def insert_old(row, tag, tags):
    field_old = tags.get(tag)
    update_times(row, field_old)
    update_category(row, field_old)
    field_old["count_tot"] += 1

#data una riga del dataframe riempie il dizionario dei tag
def insert_dict(row, tags):
    t_list = tag_list(row)
    for tag in t_list:
        if tag in tags.keys():
            insert_old(row, tag, tags)
            #print("c'è già")
        else:
            insert_new(row, tag, tags)
            #print(tag, tags[tag])
            #print("inserito")

#dato un dataset struttura nella variabile tags di output un dizionario per poter scrivere il json
def struct_tag_dict(df):
    tags = {} #dizionario in cui inserisco i tag
    for index, row in df.iterrows():
        insert_dict(row, tags)

    return tags 

def read_write_tags():
    #leggo il file di dati
    df = pd.read_csv(args.data)
    #genero il dizionario dei tags
    tags = struct_tag_dict(df)
    #output su file json
    with open("output_tags.json", "w") as outfile:
        json.dump(tags, outfile, indent=4)
'''
Esempio di funzionamento:

python create_tags.py -d "data\\01.01.20 csv files (fede) [full day]\\fixed\\2020.01.01_IT_videos.csv"

NB: DEVONO ESSERE FILE FIXED
'''
if __name__ == "__main__":
    read_write_tags()