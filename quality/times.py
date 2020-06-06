'''
Si occupa di effettuare un'analisi dei tempi di esecuzione dei task sulle infografiche in modo
da capire se sono di immediata lettura.
'''

import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
import seaborn as sns

def violin_plot(data, out_dir):
    '''
    Disegna il violin plot dei dati
    @params:
        data:   Dati di cui disegnare i violin plot.
    '''
    fig, ax= plt.subplots(figsize=(15, 8))

    bp = ax.violinplot(data, showmeans=True, showmedians=True,
        showextrema=True)
    ax.yaxis.grid(True, linestyle='-', which='major', color='lightgrey',
               alpha=0.5)

    pos   = [1, 2, 3]
    label = ['task_1', 'task_2', 'task_3']
    ax.set_axisbelow(True)
    ax.set_title('Esecuzione dei task')
    ax.set_xlabel('Task')
    ax.set_ylabel('Valori')
    ax.set_ylim(0,6.5)
    ax.set_xticks(pos)
    ax.set_xticklabels(label)


    colors = [(0.2,0.1,0.3), (0.9,0.8,0.4), (0.2,0.3,0.7)]

    for pc, color in zip(bp['bodies'],colors):
        pc.set_facecolor(color)
        pc.set_edgecolor(color)
        pc.set_alpha(1)

    plt.show()
    fig.savefig(out_dir + '/tempi_violin_plot.png', bbox_inches='tight', dpi = 600)


def times(out_dir):
    '''
    Legge i tempi di esecuzione che sono stati registrati e disegna i violin plot.
    '''
    tempi = pd.read_csv("tempi.csv")

    # Rename the columns.

    tempi.columns = ['id_persona', 'task_1', 'task_2', 'task_3']

    print(tempi)

    # Creates the new summarize column
    
    print('-----------------------------------------------------')
    a = pd.DataFrame(round(tempi.describe(),2))
    print(a)
    dati = [tempi['task_1'], tempi['task_2'], tempi['task_3']]

    # Calcoliamo ora com'è la correlazione tra la valutazione totale di un utente e la percezione teorica. Mostriamo anche su uno scatter plot la distribuzione.
    print('-----------------------------------------------------')

    # Disegno tutti i grafici.
    violin_plot(dati, out_dir)
