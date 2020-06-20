'''
Query mongo che risponde alla domanda: 
Elenco video a tema covid della categoria News & Politics in Italia e Germania

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
        client = MongoClient('localhost', int(args.port), username = args.user, password = args.password)
        db = client[args.database] # connessione al db
        col = db[args.collection] # connesione alla collection
    except:
        print("error mongo connection")
        exit()

    filter={
    '$or' :[{'country_name': 'Germania'},
            {'country_name': 'Italia'}],
    '$or': [
        {
            'covid_title': True
        }, {
            'covid_tags': True
        }
    ], 
    'category_name': 'News & Politics'
    }

    result = col.find(
        filter=filter)
    explain = col.find(filter=filter).explain()["executionStats"]
    with open("resultQuery6.txt", "w", encoding = "utf8") as file:
        for e in explain:
            file.write(str(e) + ": " + str(explain[e]) + "\n")
    
    for e in explain:
        print(e, explain[e])