from pymongo import MongoClient
import pandas as pd
import json

df = pd.read_csv("covid_data.csv")

#j = df.to_json(orient = "table")

#with open("prova.json", "r") as file:
 #   j = json.load(file)
#print(j["data"])

l_covid = df.to_dict('records')
#elimino "Unnamed"

for x in l_covid:
    try:
        x.pop("Unnamed: 0")    
    except KeyError:
        print("Key not found")

#set up mongo
client = MongoClient("localhost", 27017)
database = "YT_data"
collection = "covid"

#upload
db = client[database]
col = db[collection]
col.insert_many(l_covid)
print("inserted in mongo")

