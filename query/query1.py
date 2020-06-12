'''
Query mongo che risponde alla domanda: 
In che giorno è stato rilevato il video con maggior numero di visualizzazioni in Francia?
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
        client = MongoClient('localhost', int(args.port), username = args.user, password = args.password)
        db = client[args.database] # connessione al db
        col = db[args.collection] # connesione alla collection
    except:
        print("error mongo connection")
        exit()

    result = col.aggregate([
    {
        '$project': {
            'country_name': '$country_name', 
            'timestamp': '$timestamp', 
            'date': {
                '$dateToParts': {
                    'date': '$timestamp'
                }
            }, 
            'title': '$title', 
            'giorno': '$date.hour', 
            'visualizzazioni': '$statistics.view_count'
        }
    }, {
        '$match': {
            'country_name': 'Francia'
        }
    }, {
        '$group': {
            '_id': {
                'timestamp': '$timestamp', 
                'title': '$title'
            }, 
            'somma': {
                '$sum': '$visualizzazioni'
            }
        }
    }, {
        '$group': {
            '_id': '$_id', 
            'massViz': {
                '$max': '$somma'
            }
        }
    }, {
        '$sort': {
            'massViz': -1
        }
    }, {
        '$limit': 1
        }
    ])

    for e in result:
        title = e["_id"]["title"]
        data = e["_id"]["timestamp"]
        string = "Il video che ha ricevuto più visualizzazioni in Francia è: " + str(title) + "ed è stato rilevato il "  + str(data.day) + " " + str(data.month) + " " + str(data.year)
        print(string)
        with open("resultQuery1.txt", "w") as file:
            file.write(string)