import pandas as pd

# Import the data
dati = pd.read_csv("covid.csv", sep = ",")

# Take only the useful columns
dati = dati[['iso_code', 'location', 'date', 'total_cases','new_cases',
       'total_deaths', 'new_deaths','population']]

# Take only the useful countries:

dati = dati.loc[(dati['location'] == 'Italy') | (dati['location'] == 'United States') | (dati['location'] == 'Brazil') | (dati['location'] == 'Canada') | (dati['location'] == 'France')
                | (dati['location'] == 'Japan') | (dati['location'] == 'Mexico') | (dati['location'] == 'Germany') | (dati['location'] == 'India') | (dati['location'] == 'Russia')
                | (dati['location'] == 'South Korea') | (dati['location'] == 'Great Britain')]


# Save the file
dati = dati.to_csv("dati_corretti.csv")