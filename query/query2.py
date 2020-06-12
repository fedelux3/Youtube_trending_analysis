'''
Query mongo che risponde alla domanda: 
Elenco video covid il giorno del minimo aumento giornaliero di casi in Italia
@params:
    -u: mongoDB user
    -p: mongoDB password
    -port: mongoDB porta in cui comunica
    -db: nome database mongodb
    -c: nome collezione mongodb in cui immagazzinare i dati
'''

from pymongo import MongoClient
import argparse
from datetime import datetime


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-u', '--user', type=str, required=False, help="Inserire l'user mongodb", default="")
    parser.add_argument('-p', '--password', type=str, required=False, help="Inserire la password mongodb", default="")
    parser.add_argument('-port', '--port', type=str, required=False, help="Inserire la porta con cui dialogare con mongodb", default=27017)
    parser.add_argument('-db', '--database', type=str, required=False, help="Inserire il database mongo di output", default="YT_data")
    parser.add_argument('-c', '--collection', type=str, required=False, help="Inserire la mongodb di output", default="merge_c")
    
    args = parser.parse_args()

    try:
        user = args.user
        password = args.password
        # definizione del client mongo da utilizzare
        client = MongoClient('localhost', args.port, username = args.user, password = args.password)
        db = client[args.database] # connessione al db
        col = db[args.collection] # connesione alla collection
    except:
        print("error mongo connection")
        exit()

    result = col.aggregate([
    {
        '$project': {
            'country_name': '$country_name', 
            'title': '$title', 
            'covid_tags': '$covid_tags', 
            'covid_title': '$covid_title', 
            'casi_day': '$covid.cases_new'
        }
    }, {
        '$match': {
            'country_name': 'Italia', 
            '$or': [
                {
                    'covid_tags': {
                        '$eq': True
                    }
                }, {
                    'covid_title': {
                        '$eq': True
                    }
                }
            ]
        }
    }, {
        '$group': {
            '_id': {
                'casi': '$casi_day'
            }, 
            'video': {
                '$push': '$title'
            }
        }
    }, {
        '$sort': {
            '_id.casi': 1
        }
    }, {
        '$limit': 1
    }
    ])

    for e in result:
        video = e["video"]
        i = 1
        with open("resultQuery2.txt", "w", encoding="utf8") as file:
            for v in video:
                file.write(str(i) + ". " + v)
                file.write("\n")
                i += 1
    
    print("file generato")