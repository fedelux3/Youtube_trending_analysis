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
    fig = plt.figure(figsize=(15, 8)) 
    sns.boxplot(x="Tasks", y="Times",data = data, palette="Set3", notch = True)
    plt.grid()
    plt.title("Box plot dei tempi di esecuzione dei task", size = 30, pad = 20)
    plt.show()
    fig.savefig(out_dir + '/tempi_box_plot_seaborn.png', bbox_inches='tight', dpi = 600)

def times(out_dir):
    '''
    Legge i tempi di esecuzione che sono stati registrati e disegna i violin plot.
    '''
    tempi = pd.read_csv("tempi.csv")
    tempi = tempi.rename(columns = {"task_1":"Task 1", "task_2":"Task 2", "task_3":"Task 3", "task_4":"Task 4", "task_5":"Task 5", "task_6":"Task 6"})

    # Disegno tutti i grafici.
    box_plot(tempi,out_dir)
