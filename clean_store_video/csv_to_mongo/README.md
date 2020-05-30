## PULIZIA csv e UPLOAD su mongo

Per il funzionamento lanciare **main.py** che ha i seguenti parametri:
- -d directory in cui ho i dati
- -u utente del database mongo
- -p passoword del database mongo
- -port porta del database mongo
- -db nome database mongo
- -c collezione del database mongo

ha già dei valori di default in ogni caso

#### tempi di computazione 
per circa 100 MB di dati: 18 sec la pulizia dei dataframe e 36 secondi la formattazione e caricamento su mongo: **TOT** di 54 secondi