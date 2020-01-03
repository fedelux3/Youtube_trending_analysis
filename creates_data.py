# Script per importare tutti i dati come dataframe una volta data una directory.

import pandas as pd 
import argparse
import numpy as np
import glob, os


#   Inizio con l'aprire tutti i dati presenti con nella directory data:
parser = argparse.ArgumentParser()
parser.add_argument('-d', '--data', type=str, required=False, help="Inserire la directory da cui prendere i dati", default="data")


args = parser.parse_args()
data = os.chdir(args.data)

# Mi occupo ora di importarli nella maniera corretta in pandas.

for file in glob.glob("*.csv"):
    file =  pd.read_csv(file)

print(file.head())