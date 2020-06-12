'''
Update di tutti i documenti per applicare i flag covid
@params:
    -e: path file ER
    -u: mongoDB user
    -p: mongoDB password
    -port: mongoDB porta in cui comunica
    -db: nome database mongodb
    -c: nome collezione mongodb in cui immagazzinare i dati
'''

from pymongo import MongoClient
import argparse
from datetime import datetime
import re

def query1(coll):
    query = {}
    newvalues = {"$set" : {"covid_tags" : False, "covid_title" : False}}
    result = coll.update_many(query, newvalues)
    return result

def query2(coll, ER):
    regex = re.compile(ER, re.IGNORECASE)
    query = {"tags" : {"$in" : [regex]}}
    newvalues = {"$set" : {"covid_tags" : True}}
    result = coll.update_many(query, newvalues)
    return result

def query3(coll, ER):
    regex = re.compile(ER, re.IGNORECASE)
    query = {"title" : {"$in" : [regex]}}
    newvalues = {"$set" : {"covid_title" : True}}
    result = coll.update_many(query, newvalues)
    return result


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-e', '--er', type=str, required=False, help="Inserire path file ER", default="ER.txt")
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

    with open(args.er, "r", encoding='utf8') as file:
        ER = file.readline()
    
    result = query1(col)
    print("Query1", result.modified_count, "documenti modificati")
    result = query2(col, ER)
    print("Query2 (covid_tags)", result.modified_count, "documenti modificati")
    result = query3(col, ER)
    print("Query3 (covid_title)", result.modified_count, "documenti modificati") 