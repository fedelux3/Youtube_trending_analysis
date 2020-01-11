from creates_data import conc_dataframe
from csv_to_mongo import df_to_mongo
from pymongo import MongoClient
import argparse
import  os

if __name__ == '__main__':
    # ottengo la directory in cui applicare la ricerca
    parser = argparse.ArgumentParser()
    parser.add_argument('-d', '--data', type=str, required=False, help="Inserire la directory da cui prendere i dati", default="data")
    parser.add_argument('-u', '--user', type=str, required=False, help="Inserire la directory da cui prendere i dati", default="admin")
    parser.add_argument('-p', '--password', type=str, required=False, help="Inserire la directory da cui prendere i dati", default="DataMan2019!")
    parser.add_argument('-port', '--port', type=str, required=False, help="Inserire la directory da cui prendere i dati", default=27017)
    parser.add_argument('-db', '--database', type=str, required=False, help="Inserire la directory da cui prendere i dati", default="testDB")
    parser.add_argument('-c', '--collection', type=str, required=False, help="Inserire la directory da cui prendere i dati", default="csv_videos_cleaned")
    
    args = parser.parse_args()
    # pulisco i dati e raggruppo in un unico dataframe
    df = conc_dataframe(args.data)
    print("dataframe cleaned and merged")
    # carico il dataframe nella collezione del database mongo specificate
    # definizione del client mongo da utilizzare
    client = MongoClient('mongo', args.port, username = args.user, password = args.password)
    db = client[args.database]
    col = db[args.collection]
    
    df_to_mongo(df, col)
    print("dataframe correctly uploaded on mongoDB")
