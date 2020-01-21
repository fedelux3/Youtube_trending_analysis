import requests, sys, time, os, argparse
import json
from datetime import datetime
from pymongo import MongoClient
from ast import literal_eval

#set global variables
api_key = ""
format_date = "%d-%m-%Y %H:%M"
dt = datetime.now()

# Any characters to exclude, generally these are things that become problematic in CSV files
unsafe_characters = ['\n', '"']
format_date = "%d-%m-%Y %H:%M"

def setup(api_path):
    with open(api_path, 'r') as file:
        api_key = file.readline().strip()

    return api_key

#funzione che legge tutti i canali in tendenza e estrare gli id unici
def channel_list(col):
    d = col.distinct("channelId")
    print("channel id presi")
    return(d)
    
#legge i canali da file
def read_channels():
    #legge i canali da file
    with open("channels_id.txt", "r") as file:
        l_channels = literal_eval(file.read())
    return l_channels

#scrive i dati dei canali su file data la collezione
def write_channels(col_v):
    l_channels = channel_list(col_v) 

    #scrive i canali in un file
    with open("channels_try_id.txt", "w+") as file:
        file.write(str(l_channels))

#trasforma una lista in una stringa correta per le richieste API
def list_to_string(list):
    s = ""
    for ch in list:
        s = s + str(ch) + ","
    s = s[:len(s)-1]
    return s

def api_request(l_id):
    l_channel = []
    s_channel = list_to_string(l_id) #trasformo in string per poter fare la richiesta

    request_url = f"https://www.googleapis.com/youtube/v3/channels?part=id,contentDetails,contentOwnerDetails,snippet,statistics,topicDetails,status&id={s_channel}&maxResults=50&key={api_key}"
    print(request_url)
    request = requests.get(request_url)
    #print(request.json())
    #if request.json()['items'] is not None:
    l_channel = request.json()['items']
        
    '''
    alternativa vecchia
    for channel_id in l_id:
        request_url = f"https://www.googleapis.com/youtube/v3/channels?part=id,contentDetails,contentOwnerDetails,snippet,statistics,topicDetails,status&id={channel_id}&maxResults=50&key={api_key}"
        request = requests.get(request_url)
        #print(request.json())
        #if request.json()['items'] is not None:
        l_channel.append(request.json()['items'][0])
        #else:
         #   print("NON INSERITO\n" + str(request.json()))
    '''    
    return l_channel

def scrape(l_chan):
    global output_dir
    global api_key
    
    key_path = "api_key_chris.txt"
    output_dir = "output_timed\\"

    api_key = setup(key_path)
    
    l_json = api_request(l_chan)
    #print(l_json)
    return l_json

if __name__ == "__main__":
    
    parser = argparse.ArgumentParser()
    parser.add_argument('-u', '--user', type=str, required=False, help="Inserire utente di MongoDB", default="fedous")
    parser.add_argument('-p', '--password', type=str, required=False, help="Inserire la password MongoDB", default="DataMan2019")
    parser.add_argument('-port', '--port', type=str, required=False, help="Inserire la porta del server MongoDB", default=27017)
    parser.add_argument('-db', '--database', type=str, required=False, help="Inserire il database in cui inserire i dati", default="testDB")
    parser.add_argument('-c', '--collection', type=str, required=False, help="Inserire la collezione in cui salvare i dati", default="channels2")
    parser.add_argument('-cv', '--collection_video', type=str, required=False, help="Inserire la collezione in cui leggere i video", default="whytube")
    
    args = parser.parse_args()
    
    
    client = MongoClient('localhost', args.port, username = args.user, password = args.password)
    db = client[args.database]
    col = db[args.collection]
    col_v = db[args.collection_video]
    
    l_channels = read_channels()
    #write_channels(col_v) #legge i canali da mongo (ci mette tempo)
    
    range_ch = 50 #per fare le richeste a gruppi
    for i in range(0,len(l_channels), range_ch):

        if i+range_ch > len(l_channels): #se il range sfora i limiti della lista
            print(str(i) + " - " + str(len(l_channels)))
            l_ch = l_channels[i : len(l_channels)]
            #print(s[i:len(l_channels)])
        else :
            l_ch = l_channels[i:i+range_ch] #estraggo quei id canali scelti
        
        l_items = scrape(l_ch) #scrapo i canali
        print("canali scaricati")
        
        col.insert_many(l_items) #inserisco i canali in mongoDB
        print("caricati i dati")
        print(str(i) + " - " + str(i+range_ch))
        #print(l_channels[i:i+range_ch])
     

    