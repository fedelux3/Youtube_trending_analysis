'''
Scraper Kafka producer per scaricare i dati dei video in tendenza di youtube tramite
API keys. Li invia poi ad un canale Kafka.
@params:
    -o: output directory
    -kp:    path dove trovare il file della API key
    -co:    path dove trovare il file dei country codes
'''

import requests, sys, time, os, argparse
from kafka import KafkaProducer
import json
import sched
from datetime import datetime

def setup(api_path, code_path):
    '''
    Inizializzazione delle API e i codici dei paesi da cui voglio scaricare i dati.
    @params:
        api_path:   path in cui ho la chiave per le API di youtube
        code_path:  path in cui ho i codici dei paesi dello scraping
    @return:
        api_key:    chiave per API
        country_codes:  codici paesi scraping
    '''
    with open(api_path, 'r') as file:
        api_key = file.readline().strip()

    with open(code_path) as file:
        country_codes = [x.rstrip() for x in file]

    return api_key, country_codes

def api_request(page_token, country_code):
    '''
    Effettua la richesta API a Youtube
    @params:
        page_token: pagina token attuale con cui effettuo la richiesta
        country_code:   codice paese dei video
    @return:
        request.json:   json restituito da Youtube
    '''
    # Costruisce la richiesta URL con cui effettuo la richiesta 
    request_url = f"https://www.googleapis.com/youtube/v3/videos?part=id,statistics,snippet{page_token}chart=mostPopular&regionCode={country_code}&maxResults=50&key={api_key}"
    # esegue la richiesta
    request = requests.get(request_url)
    
    return request.json()

#ottiene le pagine di dati di un determinato paese
def get_pages(country_code, next_page_token="&"):
    '''
    Funzione per scaricare una pagina di dati (massimo 50 video) e successivamente
    li invia al canale kafka
    @params:
        country_code:   codice paese
        next_page_token:    carattere per richiedere la pagina dati successiva

    '''
    #itero sulle pagine restituite dallo scraper
    while next_page_token is not None:
        # una pagina è una lista di video (massimo 50)
        # mando la richiesta API
        video_data_page = api_request(next_page_token, country_code)
        #aggiungo il paese alle informazioni ricevute
        video_data_page['service'] = { "country" : country_code, "timestamp": dt.strftime(format_date)}
        
        #Mando i dati al topic kafka
        producer.send(topic="yt_video", value=video_data_page)
        #stampo per dare un feedback su console
        print("data page of " + str(country_code) + " sent")
        
        # ottengo il token per la prossima pagina di dati e costruisco la stringa per la prossima richiesta
        next_page_token = video_data_page.get("nextPageToken", None)
        next_page_token = f"&pageToken={next_page_token}&" if next_page_token is not None else next_page_token
        #finisce il ciclo termila la raccolta per quel paese
#end get_pages

def get_data():
    '''
    Esegue una sessione di scraping per ogni country code
    '''
    for country_code in country_codes:
        # scarica tutte le pagine di estrazioni disponibili per quel country
        get_pages(country_code)
    print("last sent: ", dt.strftime(format_date))

#funzione che inizializza lo scraping
def scrape(n_scheduler):
    '''
    Si occupa dello scraping e viene richiamata ogni volta che scatta lo scheduler
    @params:
        n_scheduler:    oggetto scheduler
    '''
    global output_dir
    global api_key, country_codes
    global dt
    
    # key_path = "api_key_fede.txt"
    # country_code_path = "country_codes.txt"
    # output_dir = "output_timed/"
    # api_key, country_codes = setup(key_path, country_code_path)
    dt = datetime.now()
    
    # lancio scraping
    get_data()
    #scheduler aspetta 6 ore
    scheduler.enter(21600, 1, scrape, (n_scheduler,))

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('-o', '--output', type=str, required=False, help="Inserire la directory di output", default="output/")
    parser.add_argument('-kp', '--key_path', type=str, required=False, help="Inserire il path del file dove c'è la API key", default="api_key_fede.txt")
    parser.add_argument('-co', '--country_path', type=str, required=False, help="Inserire il path del file dove ci sono i country code", default="country_codes.txt")
    args = parser.parse_args()

    # Setting variabili globali
    # scheduler inizializzazione
    scheduler = sched.scheduler(time.time, time.sleep)
    output_dir = args.output
    key_path = args.key_path
    country_code_path = args.country_path
    api_key = ""
    country_codes = ""
    format_date = "%d-%m-%Y %H:%M"

    #creazione oggetto producer per mandare i dati a kafka
    producer = KafkaProducer(
            bootstrap_servers=["kafka:9092"],
            value_serializer=lambda v: json.dumps(v).encode("utf-8"))

    # set-up variabili
    api_key, country_codes = setup(key_path, country_code_path)
    
    # scheduler start esegue funzione di scrape
    scheduler.enter(1, 1, scrape, (scheduler,))
    scheduler.run() 