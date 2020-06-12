'''
Query mongo che risponde alla domanda: 
Contare numero di video covid per paese
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

    pipeline = [
    {
        '$project': {
            'country_name': '$country_name', 
            'title': '$title', 
            'covid_tags': '$covid_tags', 
            'covid_title': '$covid_title'
        }
    }, {
        '$match': {
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
                'country': '$country_name', 
                'title': '$title'
            }
        }
    }, {
        '$group': {
            '_id': '$_id.country', 
            'count': {
                '$sum': 1
            }
        }
    }
    ]
    result = col.aggregate(pipeline)

    explain = db.command('aggregate', args.collection, pipeline = pipeline, explain = True)

    with open("resultQuery3.txt", "w") as file:
        for e in result:
            c = e["_id"]
            num_v = e["count"]
            print(("Il numero di video a tema covid del paese " + str(c) + " è: " + str(num_v)))
            file.write("Il numero di video a tema covid del paese " + str(c) + " è: " + str(num_v))
            file.write("\n")
        file.write("Explain computation: \n")
        file.write(str(explain))