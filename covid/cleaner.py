import pandas as pd


dati = pd.read_csv("covid.csv", sep = ",")
print(dati.columns)

dati = dati[['iso_code', 'location', 'date', 'total_cases','new_cases',
       'total_deaths', 'new_deaths','population']]

dati = dati.to_csv("dati_corretti.csv")