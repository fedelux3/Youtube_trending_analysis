# Script per importare tutti i dati come dataframe una volta data una directory.

import pandas as pd 
import argparse
import numpy as np


parser = argparse.ArgumentParser()
parser.add_argument('-d', '--data', type=str, required=False, help="Inserire la directory da cui prendere i dati", default="data")


args = parser.parse_args()

data = np.load(args.data)