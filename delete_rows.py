import pandas as pd 
import os
#Script per eliminare le intestazioni ridondanti all'interno dei csv
#scrivere nella variabile in fondo al codice (variabile dir) il nome della directory
#il software genera una cartella fixed all'interno della stessa con i files sistemati

#elimina le righe
def delete(df): 
    df1 = df.loc[df['timestamp'] == 'timestamp']
    df2 = df.drop(df.index[df1.index])
    return df2
    #df2[df2['timestamp'] == 'timestamp']
    
#prende tutti i file nella cartella e gli fa eseguire il delete
def delete_dir(dir):
    files = os.listdir('.\\' + dir)
    dir_output = dir + 'fixed\\'
    os.mkdir(dir_output)

    for file in files:
        df = pd.read_csv(dir + file)
        df_fixed = delete(df)
        df_fixed.to_csv(dir_output + file)
        print("funziona")
    
if __name__ == '__main__':
    #!!!
    #dir è il nome della cartella in cui devo fixare i dati
    #importante che:
    # - non ci siano altri dati se non i csv
    # - mettere i due \\ in fondo alla stringa
    #se no non funziona
    dir = '26.12.19 csv files (fede) [full day]\\'
    delete_dir(dir)
    print('cancellato')