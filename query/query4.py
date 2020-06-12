'''
Query mongo che risponde alla domanda: 
Elenco video a tema covid della categoria Intrattenimento d'Italia
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

    filter={
    'country_name': 'Italia', 
    '$or': [
        {
            'covid_title': True
        }, {
            'covid_tags': True
        }
    ], 
        'category_name': 'Entertainment'
    }

    result = col.find(
        filter=filter).explain()["executionStats"]
    print(result)