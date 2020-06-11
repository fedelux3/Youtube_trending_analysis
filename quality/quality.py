# -*- coding: iso-8859-15 -*-

'''
Breve file che mostra i grafici più significativi delle risposte date durante l'analisi di qualita
In fase di importazione:
'''

import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
from scipy.stats import linregress
import seaborn as sns
from matplotlib.collections import EllipseCollection

def correlation_plot(data, out_dir):
    '''
    Disegna un grafico di correlazione:
    @params:
        data:   dati di cui diesgnare le correlazioni.
    '''
    corr = data.corr()
    mask = np.zeros_like(corr, dtype=np.bool)
    mask[np.triu_indices_from(mask)] = True

    # Set up the matplotlib figure
    fig, ax = plt.subplots(figsize=(11, 9))

    # Generate a custom diverging colormap
    cmap = sns.diverging_palette(220, 20, sep=20, as_cmap=True)

    # Draw the heatmap with the mask and correct aspect ratio
    
    sns.heatmap(corr, cmap=cmap,  annot=True, 
            linewidths=1.3, linecolor='black', cbar=True, ax=ax)
    plt.yticks(rotation = 0)
    plt.title("Correlazioni tra le variabili", size = 30, pad = 20)
    plt.show()
    fig.savefig(out_dir + '/risposte_correlation_plot.png', bbox_inches='tight', dpi = 600)

def box_plot_1(data, out_dir):
    '''
    Disegna il box plot della prima infografica
    @params:
        data:   Dati di cui disegnare i violin plot.
    '''
    data = data.melt(var_name='Categories', value_name='Vote')
    sns.boxplot(x="Categories", y="Vote",data = data, palette="Set3", notch = True)
    plt.grid()
    plt.title("Risposte della prima infografica", pad = 20)
    plt.show()
    plt.savefig(out_dir + '/risposte_box_plot_first.png', bbox_inches='tight', dpi = 600)

def box_plot_2(data, out_dir):
    '''
    Disegna il box plot della prima infografica
    @params:
        data:   Dati di cui disegnare i violin plot.
    '''
    data = data.melt(var_name='Categories', value_name='Vote')
    sns.boxplot(x="Categories", y="Vote",data = data, palette="Set3", notch = True)
    plt.grid()
    plt.title("Risposte della seconda infografica", pad = 20)
    plt.show()  
    plt.savefig(out_dir + '/risposte_box_plot_second.png', bbox_inches='tight', dpi = 600)

def scatter_plot_1(x, y, out_dir):
    '''
    Disegna uno scatter plot per il percepito della seconda infografica:
    @params:
        x:  Prima grandezza da confrontare 
        y:  Seconda grandezza da confrontare
    '''
    
    plt.plot(x,y, '+', color = (0.2,0.1,0.3))

    
    #Plot of the bisector, the line in which the poinst must be in the neighborhood
    plt.plot([0,6],[0,6], "r--", color = "deepskyblue", label = "Perfect Correlation")
    plt.xticks( fontsize = 20)
    plt.yticks( fontsize = 20)
    plt.xlim(0,6)
    plt.ylim(0,6)
    plt.xlabel('Dichiarato', size = 35) 
    plt.ylabel('Calcolato', size = 35) 
    plt.legend(loc="best", prop={'size': 15})
    plt.title("Scatter plot seconda infografica", size = 35)
    plt.show()
    plt.ioff()
    plt.savefig(out_dir + '/risposte_scatter_plot_first.png', bbox_inches='tight', dpi = 600)


def scatter_plot_2(x, y, out_dir):
    '''
    Disegna uno scatter plot per il percepito della seconda infografica:
    @params:
        x:  Prima grandezza da confrontare 
        y:  Seconda grandezza da confrontare
    '''
        
    plt.plot(x,y, '+', color = (0.2,0.1,0.3))


    #Plot of the bisector, the line in which the poinst must be in the neighborhood
    plt.plot([0,6],[0,6], "r--", color = "deepskyblue", label = "Perfect Correlation")
    plt.xticks( fontsize = 20)
    plt.yticks( fontsize = 20)
    plt.xlim(0,6)
    plt.ylim(0,6)
    plt.xlabel('Dichiarato', size = 35) 
    plt.ylabel('Calcolato', size = 35) 
    plt.legend(loc="best", prop={'size': 15})
    plt.title("Scatter plot seconda infografica", size = 35)
    plt.show()
    plt.ioff()
    plt.savefig(out_dir + '/risposte_scatter_plot_second.png', bbox_inches='tight', dpi = 600)

def quality(out_dir):
    '''
    Esegue l'analisi di qualità dei dati. Si occupa di effettuare la regressione lineare delle grandezze
    'chiarezza', 'utilità', 'bellezza', 'intuitività', 'informatività' per valutare la bontà totale di un'infografica.
    '''
    primo_coefficiente = 0.213
    secondo_coefficiente = 0.199
    terzo_coefficiente = 0.190
    quarto_coefficiente = 0.174
    quinto_coefficiente = 0.151
    risposte_prima_info = pd.read_csv("risposte_prima_info.csv")
    risposte_seconda_info = pd.read_csv("risposte_seconda_info.csv")

    # Rename the columns.

    risposte_prima_info.columns = ['timestamp', 'chiarezza', 'utilita', 'bellezza', 'intuitivita', 'informativita','totale']
    risposte_seconda_info.columns = ['timestamp', 'chiarezza', 'utilita', 'bellezza', 'intuitivita', 'informativita','totale']

    # Drop the column time_stemp

    risposte_prima_info = risposte_prima_info.drop('timestamp', axis = 1)
    risposte_seconda_info = risposte_seconda_info.drop('timestamp', axis = 1)

    
    risposte_prima_info['percepito'] = primo_coefficiente*risposte_prima_info['chiarezza'] + secondo_coefficiente*risposte_prima_info['utilita'] + terzo_coefficiente*risposte_prima_info['bellezza'] 
    + quarto_coefficiente*risposte_prima_info['intuitivita'] + quinto_coefficiente*risposte_prima_info['informativita']

    risposte_seconda_info['percepito'] = primo_coefficiente*risposte_seconda_info['chiarezza'] + secondo_coefficiente*risposte_seconda_info['utilita'] + terzo_coefficiente*risposte_seconda_info['bellezza'] 
    + quarto_coefficiente*risposte_seconda_info['intuitivita'] + quinto_coefficiente*risposte_seconda_info['informativita']
    
    # Creates the new summarize column
 
    
    dati_prima_info = risposte_prima_info[['chiarezza','utilita','bellezza','intuitivita','informativita','totale']]
    dati_seconda_info = risposte_seconda_info[['chiarezza','utilita','bellezza','intuitivita','informativita','totale']]
    # Calcoliamo ora com'è la correlazione tra la valutazione totale di un utente e la percezione teorica. Mostriamo anche su uno scatter plot la distribuzione.

    '''
    totale = np.array(risposte_seconda_info['totale'])
    percepito = np.array(risposte_seconda_info['percepito'])
    r = round((np.corrcoef(totale, percepito)[0, 1])**2, 3)

    
    print("L'indice r^2 vale", r)
    '''
    # Disegno tutti i grafici.
    box_plot_1(dati_prima_info, out_dir)
    box_plot_2(dati_seconda_info, out_dir)

    scatter_plot_1(risposte_prima_info['totale'], risposte_prima_info['percepito'], out_dir)
    scatter_plot_2(risposte_seconda_info['totale'], risposte_seconda_info['percepito'], out_dir)

    correlation_plot(risposte_prima_info, out_dir)

    

    
