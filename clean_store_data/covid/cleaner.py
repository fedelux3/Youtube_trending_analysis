'''
Riceve i dati sul covid e li pulisce.
@params:
    -d: file csv in cui sono salvati i dati dei covid.
'''
import pandas as pd
import re
import datetime
import argparse

def cleaner(data):
    '''
    Riceve i dati del covid e li pulisce
    @params:
        data:   Dati sul covid in formato csv
    '''
    # Import the data
    dati = pd.read_csv(data, sep = ",")

    # Take only the useful columns
    dati = dati[['iso_code', 'location', 'date', 'total_cases','new_cases',
        'total_deaths', 'new_deaths','population']]

    # Take only the useful countries:
    dati = dati.loc[(dati['location'] == 'Italy') | (dati['location'] == 'United States') | (dati['location'] == 'Brazil') | (dati['location'] == 'Canada') | (dati['location'] == 'France')
                    | (dati['location'] == 'Japan') | (dati['location'] == 'Mexico') | (dati['location'] == 'Germany') | (dati['location'] == 'India') | (dati['location'] == 'Russia')
                    | (dati['location'] == 'South Korea') | (dati['location'] == 'United Kingdom')]

    # Take only useful days:
    dati = dati.loc[(dati['date'] <= '2020-05-06') & (dati['date'] >= '2020-03-18')]

    # Rename the location in Italian:
    dati = dati.replace(to_replace = 'Italy', value = 'Italia')
    dati = dati.replace(to_replace = 'United States', value = 'USA')
    dati = dati.replace(to_replace = 'Brazil', value = 'Brasile')
    dati = dati.replace(to_replace = 'Japan', value = 'Giappone')
    dati = dati.replace(to_replace = 'Mexico', value = 'Messico')
    dati = dati.replace(to_replace = 'United Kingdom', value = 'Regno Unito')
    dati = dati.replace(to_replace = 'South Korea', value = 'Corea del sud')
    dati = dati.replace(to_replace = 'France', value = 'Francia')
    dati = dati.replace(to_replace = 'Germany', value = 'Germania')

    # Change the format
    dati['date'] = pd.to_datetime(dati['date'], format = "%Y-%m-%d", dayfirst= False, yearfirst= True)
    dati['date'] = dati['date'].apply(lambda x: datetime.datetime.strftime(x, "%y.%d.%m"))

    # Save the file
    dati = dati.to_csv("covid_data.csv")

if __name__ == '__main__':
    '''
    Esegue la funzione cleaner
    '''
    parser = argparse.ArgumentParser()
    parser.add_argument('-d', '--data', type=str, required=False, default="covid.csv", help="Inserire i dati")
    args = parser.parse_args()
    cleaner(args.data)
