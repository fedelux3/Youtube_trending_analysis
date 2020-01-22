import requests, sys, time, os, argparse
from kafka import KafkaProducer
import json
import sched
from datetime import datetime


#########################
# Set time scheduler
scheduler = sched.scheduler(time.time, time.sleep)
# Inizializzazione delle variabili necessarie
output_dir = "output/"
api_key = ""
country_codes = ""
format_date = "%d-%m-%Y %H:%M"
dt = datetime.now()
#########################


# Any characters to exclude, generally these are things that become problematic in CSV files
unsafe_characters = ['\n', '"']
format_date = "%d-%m-%Y %H:%M"



#########################
# Funzione setup: Vengono passati i percorsi relativi ai txt contenenti api e i paesi richiesti e letti
def setup(api_path, code_path):
    with open(api_path, 'r') as file:
	# Lettura della api contenuta
        api_key = file.readline().strip()
    with open(code_path) as file:
	# Lettura dei paesi contenuti
        country_codes = [x.rstrip() for x in file]

    return api_key, country_codes
#########################


#########################
def prepare_feature(feature):
    # Removes any character from the unsafe characters list and surrounds the whole item in quotes
    for ch in unsafe_characters:
        feature = str(feature).replace(ch, "")
    return f'"{feature}"'
#########################


#########################
# La funzione api_request richiede il token e il paese richiesto, e restituisce un file json (o un dizionario?) con la richiesta effettuata
def api_request(page_token, country_code):
    # Builds the URL and requests the JSON from it
    request_url = f"https://www.googleapis.com/youtube/v3/videos?part=id,statistics,snippet{page_token}chart=mostPopular&regionCode={country_code}&maxResults=50&key={api_key}"
    request = requests.get(request_url)
    
    return request.json()
#########################



#########################
# La funzione utilizza un ciclo while per ottenere tutti i token fino a quando non si esauriscono. Durante il processo salva in formato json (o un dizionario?) i vari dati ottenuti e li invia a kafka. Il processo viene eseguito per il paese che viene passato alla funzione.
def get_pages(country_code, next_page_token="&"):
    country_data = []
    # Because the API uses page tokens (which are literally just the same function of numbers everywhere) it is much
    # more inconvenient to iterate over pages, but that is what is done here.
    while next_page_token is not None:
        # A page of data i.e. a list of videos and all needed data
		# Richiamata la funzione api_request che permette di usare la libreria request e ottenere i dati con le api
		# video_data_page è un file json
        video_data_page = api_request(next_page_token, country_code)

        # Add country and timestamp information 
		# Viene aggiunta al json la chiave service contenente i valori country e l'orario in cui è stato ricavato il dato
        video_data_page['service'] = { "country" : country_code, "timestamp": dt.strftime(format_date)}

        # video_data_page viene inviato al topic kafka: youtube_video
        producer.send(topic = "youtube_video", value = video_data_page)
        
        # Get the next page token and build a string which can be injected into the request with it, unless it's None,
        # then let the whole thing be None so that the loop ends after this cycle
        next_page_token = video_data_page.get("nextPageToken", None)
        next_page_token = f"&pageToken={next_page_token}&" if next_page_token is not None else next_page_token
#########################        



#########################
# La funzione get_data ottiene i dati per ogni paese richiesto
def get_data():
    for country_code in country_codes:
		# Viene richiamata la funzione get_pages a cui viene passato il paese richiesto
        get_pages(country_code)
        print("page sent")
    print("last write: ", dt.strftime(format_date))
#########################


#########################
# La funzione scrape avvia la presa dati ogni mezz'ora
def scrape(n_scheduler):
    global output_dir
    global api_key, country_codes
    global dt
    
    key_path = "api_key_fede.txt"
    country_code_path = "country_codes.txt"
    output_dir = "output_timed/"

	# Richiamata la funzione setup per leggere i file txt contenenti api e paesi
    api_key, country_codes = setup(key_path, country_code_path)

	# Richiama data ora del momento
    dt = datetime.now()

	# Richiamata la funzione get_data per ricavare i dati
    get_data()

    # scheduler aspetta mezz'ora prima di far ripartire il processo
    scheduler.enter(1800, 1, scrape, (n_scheduler,))
#########################



#########################
# Creazione dell'oggetto producer per mandare i dati a kafka
producer = KafkaProducer(
        bootstrap_servers = ["kafka:9092"],
        value_serializer = lambda v: json.dumps(v).encode("utf-8"))
########################



#########################
# Le scheduler parte dopo 5 secondi che è stato avviato il programma
scheduler.enter(5, 1, scrape, (scheduler,))
scheduler.run()
#########################






