import requests, sys, time, os, argparse
from kafka import KafkaProducer
import json
import sched
from datetime import datetime

# Set time scheduler
scheduler = sched.scheduler(time.time, time.sleep)
#set global variables
output_dir = "output/"
api_key = ""
country_codes = ""
format_date = "%d-%m-%Y %H:%M"
dt = datetime.now()
# List of simple to collect features
snippet_features = ["title",
                    "publishedAt",
                    "channelId",
                    "channelTitle",
                    "categoryId"]

#############################################################
#creazione oggetto producer per mandare i dati a kafka
producer = KafkaProducer(
        bootstrap_servers=["kafka:9092"],
        value_serializer=lambda v: json.dumps(v).encode("utf-8"))
#############################################################

def setup(api_path, code_path):
    with open(api_path, 'r') as file:
        api_key = file.readline().strip()

    with open(code_path) as file:
        country_codes = [x.rstrip() for x in file]

    return api_key, country_codes

#mando una richiesta API
def api_request(page_token, country_code):
    # Builds the URL and requests the JSON from it
    request_url = f"https://www.googleapis.com/youtube/v3/videos?part=id,statistics,snippet{page_token}chart=mostPopular&regionCode={country_code}&maxResults=50&key={api_key}"
    request = requests.get(request_url)
    
    return request.json()

#ottiene le pagine di dati di un determinato paese
def get_pages(country_code, next_page_token="&"):
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
    for country_code in country_codes:
        get_pages(country_code)
    print("last sent: ", dt.strftime(format_date))

#funzione che inizializza lo scraping
def scrape(n_scheduler):
    global output_dir
    global api_key, country_codes
    global dt
    
    key_path = "api_key_fede.txt"
    country_code_path = "country_codes.txt"
    output_dir = "output_timed/"
    api_key, country_codes = setup(key_path, country_code_path)
    dt = datetime.now()
    
    # lancio scraping
    get_data()
    #scheduler aspetta 6 ore
    scheduler.enter(21600, 1, scrape, (n_scheduler,))

#scheduler starts
scheduler.enter(1, 1, scrape, (scheduler,))
scheduler.run()