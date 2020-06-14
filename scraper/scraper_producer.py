'''
Scraper Kafka producer per scaricare i dati dei video in tendenza di youtube tramite
API keys. Li invia poi ad un canale Kafka.
@params:
    -o: output directory
    -kp:    path dove trovare il file della API key
    -co:    path dove trovare il file dei country codes
    -ch:    kafka topic dove scrive i dati
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
    # Costruzione della richiesta URL con cui effettuare la richiesta 
    request_url = f"https://www.googleapis.com/youtube/v3/videos?part=id,statistics,snippet{page_token}chart=mostPopular&regionCode={country_code}&maxResults=50&key={api_key}"
    # Esecuzione della richiesta
    request = requests.get(request_url)
    
    return request.json()

def get_pages(country_code, next_page_token="&"):
    '''
    Funzione per scaricare una pagina di dati (massimo 50 video) e successivamente
    li invia al canale kafka
    @params:
        country_code:   codice paese
        next_page_token:    carattere per richiedere la pagina dati successiva

    '''
    #Iterazione sulle pagine trovate dallo scraper
    while next_page_token is not None:
        # Ogni pagina è una lista di video (massimo 50)
        # Invio della richiesta API
        video_data_page = api_request(next_page_token, country_code)
        # Aggiunta del paese alle informazioni ricevute
        video_data_page['service'] = { "country" : country_code, "timestamp": dt.strftime(format_date)}
        
        # Invio dei dati al topic kafka
        producer.send(topic=topic, value=video_data_page)
        # Print di un feedback su console
        print("data page of " + str(country_code) + " sent")
        
        # Ottenimento del token per la prossima pagina di dati e costruizione della stringa per la prossima richiesta
        next_page_token = video_data_page.get("nextPageToken", None)
        next_page_token = f"&pageToken={next_page_token}&" if next_page_token is not None else next_page_token
        # Termine del ciclo per la raccolta di quel paese

def get_data():
    '''
    Esegue una sessione di scraping per ogni country code
    '''
    for country_code in country_codes:
        # Download di tutte le pagine disponibili per quel paese
        get_pages(country_code)
    print("last sent: ", dt.strftime(format_date))

def scrape(n_scheduler):
    '''
    Si occupa dello scraping e viene richiamata ogni volta che scatta lo scheduler
    @params:
        n_scheduler:    oggetto scheduler
    '''
    global output_dir
    global api_key, country_codes
    global dt
    global topic
    
    # key_path = "api_key_fede.txt"
    # country_code_path = "country_codes.txt"
    # output_dir = "output_timed/"
    # api_key, country_codes = setup(key_path, country_code_path)
    dt = datetime.now()
    
    # Avvio scraping
    get_data()
    # Scheduler in attesa per 6 ore
    scheduler.enter(21600, 1, scrape, (n_scheduler,))

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('-o', '--output', type=str, required=False, help="Inserire la directory di output", default="output/")
    parser.add_argument('-kp', '--key_path', type=str, required=False, help="Inserire il path del file dove c'è la API key", default="api_key_fede.txt")
    parser.add_argument('-co', '--country_path', type=str, required=False, help="Inserire il path del file dove ci sono i country code", default="country_codes.txt")
    parser.add_argument('-ch', '--kafka_channel', type=str, required=False, help="Topic Kafka su cui scrivere i dati", default="yt_video")
    args = parser.parse_args()

    # Setting variabili globali
    # Inizializzazione scheduler
    scheduler = sched.scheduler(time.time, time.sleep)
    output_dir = args.output
    key_path = args.key_path
    country_code_path = args.country_path
    topic = args.kafka_channel
    api_key = ""
    country_codes = ""
    format_date = "%d-%m-%Y %H:%M"

    # Creazione oggetto producer per mandare i dati a kafka
    producer = KafkaProducer(
            bootstrap_servers=["kafka:9092"],
            value_serializer=lambda v: json.dumps(v).encode("utf-8"))

    # set-up delle variabili
    api_key, country_codes = setup(key_path, country_code_path)
    
    # scheduler start esegue la funzione di scrape
    scheduler.enter(1, 1, scrape, (scheduler,))
    scheduler.run() 
