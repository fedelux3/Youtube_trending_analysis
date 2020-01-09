from pymongo import MongoClient
import json

# definizione del client mongo da utilizzare
client = MongoClient('mongo', 27017, username = 'admin', password = 'DataMan2019!')

def insert_video_mongo(l_video):
    # Ottiene il database "test" (lo crea se non esiste)
    db = client.testDB
    # Ottiene la collezione "video" (la crea se non esiste)
    col_video = db.videos
    # Inserisce nuovi documenti formattati in un oggetto iterabile (es. lista)    
    col_video.insert_many(l_video)
    print("inserted_mongo")