import requests, sys, time, os, argparse
from kafka import KafkaConsumer
import sched
from datetime import datetime
import json

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

# Used to identify columns, currently hardcoded order
header = ["timestamp"] + ["video_id"] + snippet_features + ["trending_date", "tags", "view_count", "likes", "dislikes",
                                            "comment_count", "thumbnail_link", "comments_disabled",
                                            "ratings_disabled", "description"]
################################################################################
#Definisco il consumer
consumer = KafkaConsumer(
  bootstrap_servers=["kafka:9092"],
  auto_offset_reset="latest",
  value_deserializer=lambda v: json.loads(v.decode("utf-8")))
consumer.subscribe(["test_json"])
################################################################################

def setup(code_path):
    with open(code_path) as file:
        country_codes = [x.rstrip() for x in file]

    return country_codes


def prepare_feature(feature):
    # Removes any character from the unsafe characters list and surrounds the whole item in quotes
    for ch in unsafe_characters:
        feature = str(feature).replace(ch, "")
    return f'"{feature}"'

def get_tags(tags_list):
    # Takes a list of tags, prepares each tag and joins them into a string by the pipe character
    return prepare_feature("|".join(tags_list))


def get_videos(items):
    lines = []
    
    for video in items:
        comments_disabled = False
        ratings_disabled = False

        # We can assume something is wrong with the video if it has no statistics, often this means it has been deleted
        # so we can just skip it
        if "statistics" not in video:
            continue

        # A full explanation of all of these features can be found on the GitHub page for this project
        video_id = video['id']

        # Snippet and statistics are sub-dicts of video, containing the most useful info
        snippet = video['snippet']
        statistics = video['statistics']

        # This list contains all of the features in snippet that are 1 deep and require no special processing
        features = [snippet.get(feature, "") for feature in snippet_features]

        # The following are special case features which require unique processing, or are not within the snippet dict
        description = snippet.get("description", "")
        thumbnail_link = snippet.get("thumbnails", dict()).get("default", dict()).get("url", "")
        trending_date = time.strftime("%y.%d.%m")
        tags = snippet.get("tags", ["[none]"])
        view_count = statistics.get("viewCount", 0)

        # This may be unclear, essentially the way the API works is that if a video has comments or ratings disabled
        # then it has no feature for it, thus if they don't exist in the statistics dict we know they are disabled
        #if 'likeCount' in statistics and 'dislikeCount' in statistics:
         #   likes = statistics['likeCount']
          #  dislikes = statistics['dislikeCount']
        #else:
         #   ratings_disabled = True
          #  likes = 0
           # dislikes = 0

        #if 'commentCount' in statistics:
         #   comment_count = statistics['commentCount']
        #else:
         #   comments_disabled = True
          #  comment_count = 0

        # Compiles all of the various bits of info into one consistently formatted line
        line = {}
        line['video_id'] = video_id 
        line['timestamp'] = dt.strftime(format_date)
        #line['features'] = features 
        line['title'] = features[0]
        line['publishedAt'] = features[1]
        line['channelId'] = features[2]
        line['channelTitle'] = features[3]
        line['categoryId'] = features[4]
        line['trending_date'] = trending_date
        line['tags'] = tags 
        line['statistics'] = statistics
        line['thumbnail_link'] = thumbnail_link
        line['description'] = description
        #line['view_count'] = prepare_feature(view_count)
        #line['likes'] = prepare_feature(likes)
        #line['dislikes'] = prepare_feature(dislikes)
        #line['comment_count'] = prepare_feature(comment_count)
        #line['comment_count'] = prepare_feature(thumbnail_link)
        
        #[prepare_feature(x) for x in [view_count, likes, dislikes, comment_count, thumbnail_link, comments_disabled, ratings_disabled, description]]
        lines.append(line)
        
    return lines


def write_to_file(output_data):

    print(f"Writing data to file...")

    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    with open(f"{output_dir}/{time.strftime('%Y.%m.%d')}_videos.json", "a+", encoding='utf-8') as file:
        for row in output_data:
            print(str(row) + "\n")
            json.dump(row, file, indent=3)
            file.write("\n")


def get_data():
    #video è un json fatto con i suoi campi
    for video in consumer:
        #qua va fatto il parser del consumer
        l_video = get_videos(video.value['items'])
        write_to_file(l_video)
        #print(l_video)
        #print(video.value['items'][0]['snippet']) 
        #write_to_file(country_code, country_data)

def consume():
    global output_dir
    global country_codes
    global dt
    
    country_code_path = "country_codes.txt"
    output_dir = "output_consumed/"

    country_codes = setup(country_code_path)
   
    get_data()
    
consume()