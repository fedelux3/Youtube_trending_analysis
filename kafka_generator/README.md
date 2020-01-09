# Funzionamento Kafka - Mongo Load Data
Vi sono due processi:
- Producer: fa le richieste API a youtube per quanto riguarda i video in tendenza e li manda così come gli arrivano al topic kafka "youtube_video"
- Consumer: legge i dati arrivati sul topic "youtube_video" li riformatta e li carica in una collezione di un dabase MongoDB da me definita (hardcoddata nel file mongoFunctions)

Per far si che tutto funzioni bisogna in sequenza:
1. Accendere zookeeper e kafka (vedi slide di Tundo 4a esercitazione)
2. Accendere un gestiore di database MongoDB (io ho utilizzato lo stesso di Tundo - proprio copiato in tutto però non per forza si deve usare quello)
3. Scrivere nel codice del file _mongoFunctions.py_ alla riga 5 i dati del proprio client mongo e alla riga 9 e 11 rispettivamente il database e la collezione di destinazione
4. Aprire un terminale e accendere il **consumer** (scraper_consumer.py) e se tutto funziona correttamente rimane in attesa
5. Aprire un altro terminale e accendere il **producer** (scraper_producer.py) e se tutto funziona correttamente inizierà a fare richieste API dopo 5 secondi dall'avvio

A questo punto nel terminale del producer dovrebbe stampare ogni tot "page sent" e in quello del consumer "inserted_mongo". Se tutto ciò succede allora si stanno inserendo i dati all'interno del database direttamente su mongo. Da lì poi si possono interrogare, modificare, scaricare e visualizzare.

versioni software:
- zookeeper v3.4.10 - Port 2181
- kafka v2.3.0 - Port 9092
- mongoDB v4.2 - Port 27017 (credenziali admin/DataMan2019!)