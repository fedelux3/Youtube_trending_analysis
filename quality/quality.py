import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
from scipy.stats import linregress
import seaborn as sns
import argparse
from matplotlib.collections import EllipseCollection

parser = argparse.ArgumentParser()
parser.add_argument('-d', '--output', type=str, required=True, help="Inserire la directory corrente")
args = parser.parse_args()


def correlation_plot(data):
    corr = data.corr()
    mask = np.zeros_like(corr, dtype=np.bool)
    mask[np.triu_indices_from(mask)] = True

    # Set up the matplotlib figure
    fig, ax = plt.subplots(figsize=(11, 9))

    # Generate a custom diverging colormap
    cmap = sns.diverging_palette(h_neg=130, h_pos=10, s=99, l=55, sep=3, as_cmap=True)

    # Draw the heatmap with the mask and correct aspect ratio
    
    sns.heatmap(corr, cmap=cmap,  annot=True, 
            linewidths=1.3, linecolor='black', cbar=True, ax=ax)
    plt.yticks(rotation = 0)
    plt.show()
    fig.savefig(args.output + '/correlation_plot.png', bbox_inches='tight')

def box_plot(data):
    # Se si vuole visualizzare in stile seaborn (a me non piace)
    #plt.style.use('seaborn')
    fig, ax= plt.subplots(figsize=(15, 8))

    bp = ax.boxplot(data, sym='k+', 
                notch=True, bootstrap=5000, patch_artist=True)
    ax.yaxis.grid(True, linestyle='-', which='major', color='lightgrey',
               alpha=0.5)

    ax.set_axisbelow(True)
    ax.set_title('Visualizzazione della questionario di qualità')
    ax.set_xlabel('Distribuzione')
    ax.set_ylabel('Valori')
    ax.set_ylim(0,6.5)
    ax.set_xticklabels(['chiarezza', 'utilità', 'bellezza', 'intuitività', 'informatività','totale'],
                    rotation=45, fontsize=8)
    

    colors = [(0.2,0.1,0.3), (0.9,0.8,0.4), (0.2,0.3,0.7), (0.7,0.2,0.4), (0.5,0.3,0.2), (0.3,0.2,0.5)]

    for box, color in zip(bp['boxes'], colors):
        box.set(color= color, linewidth=2)
        box.set(facecolor = color )
        box.set(hatch = '/')

    plt.show()
    fig.savefig(args.output + '/box_plot.png', bbox_inches='tight')
def violin_plot(data):
    fig, ax= plt.subplots(figsize=(15, 8))

    bp = ax.violinplot(data, showmeans=True, showmedians=True,
        showextrema=True)
    ax.yaxis.grid(True, linestyle='-', which='major', color='lightgrey',
               alpha=0.5)

    ax.set_axisbelow(True)
    ax.set_title('Visualizzazione della questionario di qualità')
    ax.set_xlabel('Distribuzione')
    ax.set_ylabel('Valori')
    ax.set_ylim(0,6.5)
    # Non so perché ma scala le etichette dei valori a destra di 1, quindi inserisco un valore prima in modo da averli tutti
    ax.set_xticklabels(['0', 'chiarezza', 'utilità', 'bellezza', 'intuitività', 'informatività','totale'],
                    rotation=45, fontsize=8)

    colors = [(0.2,0.1,0.3), (0.9,0.8,0.4), (0.2,0.3,0.7), (0.7,0.2,0.4), (0.5,0.3,0.2), (0.3,0.2,0.5)]

    for pc, color in zip(bp['bodies'],colors):
        pc.set_facecolor(color)
        pc.set_edgecolor(color)
        pc.set_alpha(1)

    plt.show()
    fig.savefig(args.output + '/violin_plot.png', bbox_inches='tight')

def scatter_plot(x, y):
        
    fig = plt.figure(figsize=(15,8))
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
    plt.show()
    plt.ioff()
    fig.savefig(args.output + '/scatter_plot.png', bbox_inches='tight')

def quality():
    primo_coefficiente = 0.213
    secondo_coefficiente = 0.199
    terzo_coefficiente = 0.190
    quarto_coefficiente = 0.174
    quinto_coefficiente = 0.151
    risposte = pd.read_csv("risposte.csv")

    # Rename the columns.

    risposte.columns = ['time_stemp', 'chiarezza', 'utilità', 'bellezza', 'intuitività', 'informatività','totale']

    # Drop the column time_stemp

    risposte = risposte.drop('time_stemp', axis = 1)
    print(risposte)
    risposte['percepito'] = primo_coefficiente*risposte['chiarezza'] + secondo_coefficiente*risposte['utilità'] + terzo_coefficiente*risposte['bellezza'] 
    + quarto_coefficiente*risposte['intuitività'] + quinto_coefficiente*risposte['informatività']

    # Creates the new summarize column
    
    print('-----------------------------------------------------')
    a = pd.DataFrame(round(risposte.describe(),2))
    print(a)
    dati = [risposte['chiarezza'], risposte['utilità'], risposte['bellezza'], risposte['intuitività'], risposte['informatività'], risposte['totale']]

    # Implemento anche l'indice R^2 per vedere come si comporta.
    
    
    # Calcoliamo ora com'è la correlazione tra la valutazione totale di un utente e la percezione teorica. Mostriamo anche su uno scatter plot la distribuzione.
    print('-----------------------------------------------------')

    totale = np.array(risposte['totale'])
    percepito = np.array(risposte['percepito'])
    r = round((np.corrcoef(totale, percepito)[0, 1])**2, 3)

    
    print("L'indice r^2 vale", r)

    # Disegno tutti i grafici.

    #box_plot(dati)
    violin_plot(dati)
    #scatter_plot(risposte['totale'], risposte['percepito'])
    #plot_corr(risposte)
    correlation_plot(risposte)
    

    
