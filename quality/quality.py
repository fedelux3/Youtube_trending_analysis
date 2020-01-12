import matplotlib.pyplot as plt
import pandas as pd


def box_plot(data):
    fig, ax= plt.subplots(figsize=(9, 4))

    bp = ax.boxplot(data, sym='k+', 
                notch=True, bootstrap=5000, patch_artist=True)
    ax.yaxis.grid(True, linestyle='-', which='major', color='lightgrey',
               alpha=0.5)

    ax.set_axisbelow(True)
    ax.set_title('Visualizzazione della questionario di qualità')
    ax.set_xlabel('Distribuzione')
    ax.set_ylabel('Valori')
    ax.set_ylim(0,6.5)
    ax.set_xticklabels(['chiarezza', 'utilità', 'bellezza', 'intuitività', 'informatività','percepito'],
                    rotation=45, fontsize=8)

    colors = ['pink', 'lightblue', 'lightgreen', 'pink', 'lightblue', 'lightgreen']

    for box, color in zip(bp['boxes'], colors):
        box.set(color= color, linewidth=2)
        box.set(facecolor = color )
        box.set(hatch = '/')
    
    #ax.legend([bp["boxes"], loc = 'upper right')

    plt.show()

def violin_plot(data):
    fig, ax= plt.subplots(figsize=(9, 4))

    bp = ax.violinplot(data, showmeans=True, showmedians=True,
        showextrema=True)
    ax.yaxis.grid(True, linestyle='-', which='major', color='lightgrey',
               alpha=0.5)

    ax.set_axisbelow(True)
    ax.set_title('Visualizzazione della questionario di qualità')
    ax.set_xlabel('Distribuzione')
    ax.set_ylabel('Valori')
    ax.set_ylim(0,6.5)
    ax.set_xticklabels(['chiarezza', 'utilità', 'bellezza', 'intuitività', 'informatività','percepito'],
                    rotation=45, fontsize=8)

    colors = ['pink', 'lightblue', 'lightgreen', 'pink', 'lightblue', 'lightgreen']

    for pc, color in zip(bp['bodies'],colors):
        pc.set_facecolor(color)
        pc.set_edgecolor(color)
        pc.set_alpha(1)

    plt.show()



def pulizia():
    primo_coefficiente = 0.213
    secondo_coefficiente = 0.199
    terzo_coefficiente = 0.190
    quarto_coefficiente = 0.174
    quinto_coefficiente = 0.151
    risposte = pd.read_csv("risposte.csv")

    # Rename the columns.

    risposte.columns = ['time_stemp', 'chiarezza', 'utilità', 'bellezza', 'intuitività', 'informatività']

    # Drop the column time_stemp

    risposte = risposte.drop('time_stemp', axis = 1)

    risposte['percepito'] = primo_coefficiente*risposte['chiarezza'] + secondo_coefficiente*risposte['utilità'] + terzo_coefficiente*risposte['bellezza'] 
    + quarto_coefficiente*risposte['intuitività'] + quinto_coefficiente*risposte['informatività']

    # Creates the new summarize column
    print(risposte)
    dati = [risposte['chiarezza'], risposte['utilità'], risposte['bellezza'], risposte['intuitività'], risposte['informatività'], risposte['percepito']]
    box_plot(dati)
    violin_plot(dati)


pulizia()
