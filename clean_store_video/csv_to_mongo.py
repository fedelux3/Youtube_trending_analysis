import pandas as pd
from pymongo import MongoClient
import json

#funzione che dato una stringa di tag restituisce i tag in una lista
def tag_list(string):
    t_list = string.split("|")
    return t_list

#restituisce la struttura legata alla data e ora
def time_struct(row):
    out = {}
    out['year'] = row['year']
    out['month'] = row['month']
    out['day'] = row['day']
    out['hour'] = row['hour']
    return out

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
    line['time'] = time_struct(row)
    line['country_code'] = row['country_code'] #CAPIRE COME GESTIRE IL COUNTRY_CODE
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

def df_to_mongo(df, col):
    out_dict = [] #lista da inserire
    for index, row in df.iterrows():
        out_dict.append(row_to_dict(row))
    
    col.insert_many(out_dict)
    #insert_video_mongo(out_dict)
    print("inserimento completato")
    
if __name__ == "__main__": 
    dirt = "data/"
    df = pd.read_csv(dirt + "2020.01.01_IT_videos.csv")
    df_to_mongo(df)