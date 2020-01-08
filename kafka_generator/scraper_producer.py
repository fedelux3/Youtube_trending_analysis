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

# Any characters to exclude, generally these are things that become problematic in CSV files
unsafe_characters = ['\n', '"']
format_date = "%d-%m-%Y %H:%M"
# Used to identify columns, currently hardcoded order
header = ["timestamp"] + ["video_id"] + snippet_features + ["trending_date", "tags", "view_count", "likes", "dislikes",
                                            "comment_count", "thumbnail_link", "comments_disabled",
                                            "ratings_disabled", "description"]
################################################3######
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


def prepare_feature(feature):
    # Removes any character from the unsafe characters list and surrounds the whole item in quotes
    for ch in unsafe_characters:
        feature = str(feature).replace(ch, "")
    return f'"{feature}"'


def api_request(page_token, country_code):
    # Builds the URL and requests the JSON from it
    request_url = f"https://www.googleapis.com/youtube/v3/videos?part=id,statistics,snippet{page_token}chart=mostPopular&regionCode={country_code}&maxResults=50&key={api_key}"
    request = requests.get(request_url)
    
    return request.json()


def get_pages(country_code, next_page_token="&"):
    country_data = []

    # Because the API uses page tokens (which are literally just the same function of numbers everywhere) it is much
    # more inconvenient to iterate over pages, but that is what is done here.
    while next_page_token is not None:
        # A page of data i.e. a list of videos and all needed data
        video_data_page = api_request(next_page_token, country_code)
        #aggiungo il paese dalle informazioni ricevute
        video_data_page['service'] = { "country" : country_code, "timestamp": dt.strftime(format_date)}
        #Mando i dati al consumer
        producer.send(topic="test_json", value=video_data_page)
        #print(video_data_page['service'])
        #sys.exit()
        # Get the next page token and build a string which can be injected into the request with it, unless it's None,
        # then let the whole thing be None so that the loop ends after this cycle
        next_page_token = video_data_page.get("nextPageToken", None)
        next_page_token = f"&pageToken={next_page_token}&" if next_page_token is not None else next_page_token

        # Get all of the items as a list and let get_videos return the needed features
        #items = video_data_page.get('items', [])
        #country_data += get_videos(items)
    #print(country_data)
    #sys.exit()
    return country_data

def get_data():
    for country_code in country_codes:
        country_data = [",".join(header)] + get_pages(country_code)
        #write_to_file(country_code, country_data)
    print("last write: ", dt.strftime(format_date))

def scrape(n_scheduler):
    global output_dir
    global api_key, country_codes
    global dt
    
    key_path = "api_key_fede.txt"
    country_code_path = "country_codes.txt"
    output_dir = "output_timed/"

    api_key, country_codes = setup(key_path, country_code_path)
    dt = datetime.now()
   
    get_data()
    #scheduler waits
    scheduler.enter(1800, 1, scrape, (n_scheduler,))

#scheduler starts
scheduler.enter(1, 1, scrape, (scheduler,))
scheduler.run()