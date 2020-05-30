# Youtube_trending_analysis
In questa repo ci sono i file e i dati relativi alla mia analisi dei trend di youtube.
File:
- Il file scraping.py contiene il codice per lo scraping, l'ho preso da questa repo: https://github.com/mitchelljy/Trending-YouTube-Scraper leggermente modificato perchè non funzionava
- Il dataset di riferimento è: https://www.kaggle.com/datasnaek/youtube-new
- Le categorie sono state estratte in formato Json 
- Il notebook study_variables.ipynb è una mia piccolissima analisi preliminare sui dati

### Le 3 V
- __Velocity__ i dati sono presi giornalmente quindi si possono considerare real-time
- __Variety__ i dati salvati sono in csv e le categorie in Json, di per se c'è già variety anche se è un po' debole, si potrebbe pensare di salvare i json in un documentale per rendere la cosa interessante o boh vediamo
- __Volume__ il tizio su kaggle ha raccolto circa 500 MB in 6 mesi di rilevazioni, secondo me non ci arriveremo a 2Gb però se integriamo con altro magari sì

Lo scraping avviene tramite API di youtube: https://developers.google.com/youtube/registering_an_application (la mia chiave è salvata nel file api_text.txt - per ora se volete potete usare la mia)
### Possibili analisi
Come vi ho detto nell'audio vi sono possibili utilizzi del dataset:

__Idea 1__: Studiare le tendenze del periodo natalizio, se comincio ora a farlo tutti i giorni fino a 6 gennaio abbiamo buoni dati. Si possono fare belle visualizzazioni per categoria di video, proviamo a correlare le visualizzazioni con i likes o cose del genere.  
__Idea 2__: Fare un'analisi descrittiva in modo da estrarre il modello di video che, per ogni paese, avrebbe più successo. Attraverso semplici strumenti statistici come media e varianza, non credo che il prof voglia molto di più.  
__Idea 3__: correlarlo con qualcos'altro che secondo me l'unico difetto di questa analisi è che usamo "solo" i dati di youtube

Ovviamente si può pensare di utilizzare altri attributi, esplorate il dataset per vedere se vi viene qualche idea.

### Appunti chiamata 23-12-19
__Chistian__: capire se posso fare scraping dei canali, numero visualizzazioni, iscritti ... <br>
__Marco__: capire se nei paesi dove sono concentrati il maggior numero di iscritti ci sia qualcosa di interessante ... (normalizzare sulla popolazione) <br>
__Fede__: sono riuscito a scrapare i canali Youtube tramite API ufficiali, ho il numero di iscritti e le visualizzazioni totali

### Appunti telefonata 30.12.19
Potremmo calcolare:
- indice di partecipazione (chi fa più like/visual) se ci indica che il video è in tendenza
- analisi dei tag, salvarli in un dataset mongo e vedere se possono essere studiati
- trovare video-modello che va più in tendenza nel periodo natalizio (tag + usati e titoli)
- se replichiamo i dati (nella raccolta) fa niente, poi eliminiamo i doppioni

#### SCADENZE:
- 10.01.20: presentazione a Cabitza
- 16.01.20: scritto

#### PROCEDURA per fare il "mergione":
run del file fuori dalla directory data con cartelle dei dati
1. create_data (mergione) su tutti
2. delete_dir (elimina le intestazioni)
3. add date columns 
4. dataFrame gigante
