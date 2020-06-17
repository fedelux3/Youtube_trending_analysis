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
    svm = sns.boxplot(x="Tasks", y="Times",data = data, palette="Set3", notch = True)
    plt.grid()
    plt.title("Box plot tempi esecuzione task", size = 25, pad = 20)
    plt.show()
    fig = svm.get_figure()
    fig.savefig(out_dir + '/tempi_box_plot_seaborn.png', bbox_inches='tight', dpi = 300)

def violin_plot(data, out_dir):
    '''
    Disegna il box plot dei dati
    @params:
        data:   Dati di cui disegnare i violin plot.
    '''
    data = data.melt(var_name='Tasks', value_name='Times')
    fig = plt.figure(figsize=(15, 8)) 
    svm = sns.violinplot(x="Tasks", y="Times",data = data, palette="Set3")
    plt.grid()
    plt.title("Violin plot tempi esecuzione task", size = 25, pad = 20)
    plt.show()
    fig = svm.get_figure()
    fig.savefig(out_dir + '/tempi_violin_plot_seaborn.png', bbox_inches='tight', dpi = 300)

def times(out_dir):
    '''
    Legge i tempi di esecuzione che sono stati registrati e disegna i violin plot.
    '''
    tempi = pd.read_csv("tempi.csv")
    tempi = tempi.rename(columns = {"task_1":"Task 1 prima", "task_2":"Task 2 prima", "task_3":"Task 3 prima", 
                                    "task_4":"Task 1 seconda", "task_5":"Task 2 seconda", "task_6":"Task 3 seconda"})

    # Disegno tutti i grafici.
    box_plot(tempi,out_dir)
    violin_plot(tempi,out_dir)
