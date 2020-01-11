import pandas as pd
from pymongo import MongoClient
import json
from mongoFunctions import insert_video_mongo

#funzione che dato una stringa di tag restituisce i tag in una lista
def tag_list(string):
    t_list = string.split("|")
    return t_list

#restituisce in un dict le statistics del video
def statistics_to_dict(row):
    stats = {}
    stats["view_count"] = row["view_count"]
    stats["likes"] = row["likes"]
    stats["dislikes"] = row["dislikes"]
    stats["comment_count"] = row["comment_count"]
    return stats

#restituisce il dizionario di una riga del df
def row_to_dict(row):
    line = {}
    line['video_id'] = row['video_id'] 
    line['timestamp'] = row['timestamp']
    #line['country_code'] = rowcountry CAPIRE COME GESTIRE IL COUNTRY_CODE
    line['title'] = row['title']
    line['publishedAt'] = row['publishedAt']
    line['channelId'] = row['channelId']
    line['channelTitle'] = row['channelTitle']
    line['categoryId'] = row['categoryId']
    line['trending_date'] = row['trending_date']
    line['tags'] = tag_list(row['tags']) 
    line['statistics'] = statistics_to_dict(row)
    line['thumbnail_link'] = row['thumbnail_link']
    line['description'] = row['description']
    return line

def df_to_mongo(df):
    out_dict = [] #lista da restituire
    #line_dict = {} #dizionario di una riga
    for index, row in df.iterrows():
        out_dict.append(row_to_dict(row))
    insert_video_mongo(out_dict)
    print("inserimento completato")
    
if __name__ == "__main__": 
    dirt = "data/"
    df = pd.read_csv(dirt + "2020.01.01_IT_videos.csv")
    df_to_mongo(df)