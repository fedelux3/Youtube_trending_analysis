'''
Si occupa di effettuare un'analisi dei tempi di esecuzione dei task sulle infografiche in modo
da capire se sono di immediata lettura.
'''

import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns

def box_plot(data, out_dir):
    '''
    Disegna il box plot dei dati
    @params:
        data:   Dati di cui disegnare i violin plot.
    '''
    data = data.melt(var_name='Tasks', value_name='Times')
    fig = plt.subplots(figsize=(15, 8))
    sns.boxplot(x="Tasks", y="Times",data = data, palette="Set3")
    plt.grid()
    plt.show()
    plt.savefig(out_dir + '/tempi_box_plot_seaborn.png', bbox_inches='tight', dpi = 600)

def times(out_dir):
    '''
    Legge i tempi di esecuzione che sono stati registrati e disegna i violin plot.
    '''
    tempi = pd.read_csv("tempi.csv")

    # Creates the new summarize column
    
    print('-----------------------------------------------------')
    a = pd.DataFrame(round(tempi.describe(),2))
    print(a)

    # Calcoliamo ora com'è la correlazione tra la valutazione totale di un utente e la percezione teorica. Mostriamo anche su uno scatter plot la distribuzione.
    print('-----------------------------------------------------')

    # Disegno tutti i grafici.
    box_plot(tempi,out_dir)
