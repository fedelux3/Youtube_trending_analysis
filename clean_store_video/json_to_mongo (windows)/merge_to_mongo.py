from pymongo import MongoClient
import argparse
import os
import json
import pandas as pd
import time

# Print iterations progress
def printProgressBar (iteration, total, prefix = '', suffix = '', decimals = 1, length = 100, fill = '█', printEnd = "\r"):
    """
    Call in a loop to create terminal progress bar
    @params:
        iteration   - Required  : current iteration (Int)
        total       - Required  : total iterations (Int)
        prefix      - Optional  : prefix string (Str)
        suffix      - Optional  : suffix string (Str)
        decimals    - Optional  : positive number of decimals in percent complete (Int)
        length      - Optional  : character length of bar (Int)
        fill        - Optional  : bar fill character (Str)
        printEnd    - Optional  : end character (e.g. "\r", "\r\n") (Str)
    """
    percent = ("{0:." + str(decimals) + "f}").format(100 * (iteration / float(total)))
    filledLength = int(length * iteration // total)
    bar = fill * filledLength + '-' * (length - filledLength)
    print('\r%s |%s| %s%% %s' % (prefix, bar, percent, suffix), end = printEnd)
    # Print New Line on Complete
    if iteration == total: 
        print()



def list_directory(data):
    l = []
    print(data)
    for x in os.listdir(data + "\\"):
        l.append(x)
            #print(x)
    return l

#funzione di merge
#input: -videos (list of dictionary) -covid (dataframe)
def merge_videos(videos, covid):
    video_covid = videos #output (list of dictionary)

    tot = len(videos)
    #perc_prev = 0
    #per barra progressione
    printProgressBar(0, tot, prefix = 'Progress:', suffix = 'Complete', length = 50)
    for  i, video in enumerate(video_covid): #passa tutti i video
        date = video["trending_date"]
        c_name = video["country_name"]
        #cercare in covid date e c_name
        row = covid.loc[(covid["date"] == date) & (covid["location"] == c_name)]
        #aggiungo i campi
        #print(video)
        video["covid_cases_tot"] = int(row["total_cases"].values[0])
        video["covid_cases_new"] = int(row["new_cases"].values[0])
        video["covid_deaths_tot"] = int(row["total_deaths"].values[0])
        video["covid_deaths_new"] = int(row["new_deaths"].values[0])
        video["country_population"] = int(row["population"].values[0])
        
        #perc = int(round((i/tot) *100))
        #if perc > perc_prev:
        #    print("merged: " + str(perc) + "%")
        #perc_prev = perc
        time.sleep(0.1)
        # Update Progress Bar
        printProgressBar(i + 1, tot, prefix = 'Progress:', suffix = 'Complete', length = 50)

if __name__ == '__main__':
    time_start = time.clock()
    # ottengo la directory in cui applicare la ricerca
    parser = argparse.ArgumentParser()
    parser.add_argument('-d', '--data', type=str, required=False, help="Inserire la directory da cui prendere i dati", default="data")
    parser.add_argument('-u', '--user', type=str, required=False, help="Inserire la directory da cui prendere i dati", default="")
    parser.add_argument('-p', '--password', type=str, required=False, help="Inserire la directory da cui prendere i dati", default="")
    parser.add_argument('-port', '--port', type=str, required=False, help="Inserire la directory da cui prendere i dati", default=27017)
    parser.add_argument('-db', '--database', type=str, required=False, help="Inserire la directory da cui prendere i dati", default="YT_data")
    parser.add_argument('-c', '--collection', type=str, required=False, help="Inserire la directory da cui prendere i dati", default="try_merge_2")
    
    args = parser.parse_args()
    dir_main = args.data
    # definizione del client mongo da utilizzare
    client = MongoClient('localhost', args.port, username = args.user, password = args.password)
    db = client[args.database]
    col = db[args.collection]
    df_covid = pd.read_csv("covid_data.csv")

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
        print("upload files")
        ##############################################
        print(directory + " merging ...")
        merge_videos(list_videos, df_covid)
        print("merge_fatto")
        #############################################
        # carico la lista di dizionari nella collezione del database mongo specificate
        col.insert_many(list_videos)
        list_videos = []
        print("directory " + directory + " correctly uploaded on mongoDB")

    print("files json correctly uploaded on mongoDB !!!")
    time_elapsed = (time.clock() - time_start)
    print("computaion time: " + str(time_elapsed))