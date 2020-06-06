'''
Scraper per scaricare i video in tendenza di youtube tramite API keys.
@params:
    -o: output directory
    -kp:    path dove trovare il file della API key
    -co:    path dove trovare il file dei country codes
'''

import requests, sys, time, os, argparse
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


def prepare_feature(feature):
    '''
    Rimuove qualsiasi carattere considerato non sicuro dalla lista definita inizialmente
    @params: 
        feature:    campo da sistemare
    @return:
        feature:    stringa della feature sistemata
    '''
    # Removes any character from the unsafe characters list and surrounds the whole item in quotes
    for ch in unsafe_characters:
        feature = str(feature).replace(ch, "")
    return f'"{feature}"'


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


def get_tags(tags_list):
    '''
    Da una lista di tag condensa in una stringa separando i singoli tago con un carattere '|'
    @params:
        tags_list:  lista dei tag
    @return:
        stringa dei tag
    '''
    return prepare_feature("|".join(tags_list))


def get_videos(items):
    '''
    Esegue la trasformazione della lista di dizionari sui video in input in lista di liste
    per il csv in output
    @params:
        items:  lista di dizionari
    @return:
        lines:  lista di liste
    '''
    lines = []
    for video in items:
        comments_disabled = False
        ratings_disabled = False

        # Se non ho le statistiche significa che non ho dati interessanti quindi 
        # salto al dizionario successivo
        if "statistics" not in video:
            continue

        # Snippet and statistics contengono le informazioni più interessanti
        snippet = video['snippet']
        statistics = video['statistics']
        # lista che contiene tutte le feature nello snippet tolti i caratteri pericolosi
        features = [prepare_feature(snippet.get(feature, "")) for feature in snippet_features]
        #  preprocessing di altre feature del video
        video_id = prepare_feature(video['id'])
        description = snippet.get("description", "")
        thumbnail_link = snippet.get("thumbnails", dict()).get("default", dict()).get("url", "")
        trending_date = time.strftime("%y.%d.%m")
        tags = get_tags(snippet.get("tags", ["[none]"]))
        view_count = statistics.get("viewCount", 0)

        # se i campi di like, dislike, comment count non sono presenti allora significa che sono stati disabilitati
        # quindi li setto a 0 per il seguente video
        if 'likeCount' in statistics and 'dislikeCount' in statistics:
            likes = statistics['likeCount']
            dislikes = statistics['dislikeCount']
        else:
            ratings_disabled = True
            likes = 0
            dislikes = 0
        if 'commentCount' in statistics:
            comment_count = statistics['commentCount']
        else:
            comments_disabled = True
            comment_count = 0

        # Aggrego tutte insieme le feature preprocessate aggiungendo in testa
        # il timestamp corrente
        line = [dt.strftime(format_date)] + [video_id] + features + [prepare_feature(x) for x in [trending_date, tags, view_count, likes, dislikes,
                                                                       comment_count, thumbnail_link, comments_disabled,
                                                                       ratings_disabled, description]]
        lines.append(",".join(line))
    return lines


def get_pages(country_code, next_page_token="&"):
    '''
    Funzione per scaricare una pagina di dati (massimo 50 video)
    @params:
        country_code:   codice paese
        next_page_token:    carattere per richiedere la pagina dati successiva
    @return:
        country_data:   dati del paese (lista) 
    '''
    country_data = []

    # Itero finchè non ho più pagine successive nello scraper
    while next_page_token is not None:
        # dowload di una pagina di dati, sostanzialmente i dati sui video 
        video_data_page = api_request(next_page_token, country_code)# Ottiene il token della pagina successiva altrimenti se non c'è restituisce None
        # così termina il loop di richieste
        next_page_token = video_data_page.get("nextPageToken", None)
        next_page_token = f"&pageToken={next_page_token}&" if next_page_token is not None else next_page_token

        # Ottiene tutte le informazioni in una lista dal campo items 
        items = video_data_page.get('items', [])
        # preprocessing sui dati prima di aggiungerli
        country_data += get_videos(items)

    return country_data


def write_to_file(country_code, country_data):
    '''
    Funzione che si occupa della scrittura su csv dei dati di un paese
    @params:
        country_code:   codice paese dei dati
        country_data:   dati del paese in questione
    '''
    print(f"Writing {country_code} data to file...")

    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    # formattazione del file output in base alla data e al paese
    # appende le informazioni, in questo modo non sovrascrive prese dati precedenti
    with open(f"{output_dir}/{time.strftime('%Y.%m.%d')}_{country_code}_videos.csv", "a+", encoding='utf-8') as file:
        for row in country_data:
            file.write(f"{row}\n")


def get_data():
    '''
    Esegue una sessione di scraping per ogni country code
    '''
    for country_code in country_codes:
        # eseguo get_pages per scaricare le pagine dati di un determinato
        # country_code
        country_data = [",".join(header)] + get_pages(country_code)
        # scrivo le tramite write_to_file
        write_to_file(country_code, country_data)
    print("last write: ", dt.strftime(format_date))

def scrape(n_scheduler):
    '''
    Si occupa dello scraping e viene richiamata ogni volta che scatta lo scheduler
    @params:
        n_scheduler:    oggetto scheduler
    '''
    global output_dir
    global api_key, country_codes
    global dt
    
    # ottiene la data e ora attuali
    dt = datetime.now()
    #avvia lo scraping
    get_data()

    #scheduler attende 6 ore
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
    # List of simple to collect features
    snippet_features = ["title", "publishedAt", "channelId",
                        "channelTitle", "categoryId"]
    # Usati per identificare le colonne di nostro interesse
    header = ["timestamp"] + ["video_id"] + snippet_features + ["trending_date", 
                    "tags", "view_count", "likes", "dislikes",
                    "comment_count", "thumbnail_link", "comments_disabled",
                    "ratings_disabled", "description"]
    # Elenco ci caratteri problematici da eliminare per i CSV
    unsafe_characters = ['\n', '"']

    # set-up variabili
    api_key, country_codes = setup(key_path, country_code_path)
    # scheduler start esegue funzione di scrape
    scheduler.enter(1, 1, scrape, (scheduler,))
    scheduler.run()    

