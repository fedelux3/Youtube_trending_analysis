'''
Esegue i plot sulla qualità
@params:
    -o: Directory in cui vengono salvati i grafici (default '.')

'''
import quality
import times
import argparse

if __name__ == '__main__':
    global out

    parser = argparse.ArgumentParser()
    parser.add_argument('-o', '--output', type=str, required=False, help="Inserire la directory corrente", default=".")
    args = parser.parse_args()
    out = args.output
    quality.quality(out)
    #times.times(out)